import { createHash } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";
import { mkdirSync, readFileSync, readdirSync, renameSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const RECENT = "aemet-recent-climate-30.json";
const MONTHLY = /^aemet-monthly-([A-Za-z0-9_-]{1,16})\.json$/;
export const MAX_BUNDLE_BYTES = 10 * 1024 * 1024;
export const sha256 = (buffer) => createHash("sha256").update(buffer).digest("hex");

export function readWeatherFiles(cacheDir) {
  return Object.fromEntries(readdirSync(cacheDir).filter((name) => name === RECENT || MONTHLY.test(name))
    .sort().map((name) => [name, JSON.parse(readFileSync(join(cacheDir, name), "utf8"))]));
}

function validateDailyPoints(station, points, now) {
  if (!/^[A-Za-z0-9_-]{1,16}$/.test(station) || !Array.isArray(points) || points.length > 30) throw new Error("Invalid station history.");
  let previous = "";
  for (const point of points) {
    const date = point?.fecha;
    if (typeof date !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(Date.parse(date)) ||
        new Date(date).toISOString().slice(0, 10) !== date || date > now.toISOString().slice(0, 10) || date <= previous ||
        !Number.isFinite(point.precipitacion) || point.precipitacion < 0 ||
        !(point.temperaturaMedia === null || Number.isFinite(point.temperaturaMedia)) ||
        !(point.humedadMedia === null || (Number.isFinite(point.humedadMedia) && point.humedadMedia >= 0 && point.humedadMedia <= 100))) {
      throw new Error(`Invalid daily weather values: ${station}`);
    }
    previous = date;
  }
  return previous;
}

export function validateWeatherFiles(files, { now = new Date(), minStations = 300 } = {}) {
  if (!files || typeof files !== "object" || Array.isArray(files) || Object.keys(files).length > 2000) {
    throw new Error("Invalid weather file set.");
  }
  let freshStations = 0;
  let monthlyStations = 0;
  let latestDate = "";
  const fresh = (date) => {
    const age = (now - new Date(`${date}T00:00:00Z`)) / 86_400_000;
    return Number.isFinite(age) && age >= 0 && age <= 14;
  };
  for (const [name, payload] of Object.entries(files)) {
    if (name !== RECENT && !MONTHLY.test(name)) throw new Error(`Unexpected weather file: ${name}`);
    if (!payload || !Number.isFinite(payload.timestamp) || payload.timestamp > now.getTime() + 60_000 ||
        now.getTime() - payload.timestamp > 14 * 86_400_000) throw new Error(`Stale/invalid weather timestamp: ${name}`);
    if (name === RECENT) {
      if (payload.rangeDays !== 30 || !payload.data || Array.isArray(payload.data) || typeof payload.data !== "object") {
        throw new Error("Invalid recent weather payload.");
      }
      for (const [station, points] of Object.entries(payload.data)) {
        const previous = validateDailyPoints(station, points, now);
        if (previous > latestDate) latestDate = previous;
        if (points.length >= 7 && fresh(previous)) freshStations++;
        if (!Object.hasOwn(files, `aemet-monthly-${station}.json`)) throw new Error(`Missing monthly weather: ${station}`);
      }
    } else {
      if (!Array.isArray(payload.data) || payload.data.length > 12) throw new Error(`Invalid monthly weather: ${name}`);
      let previous = "";
      for (const point of payload.data) {
        if (!point || !/^\d{4}-(0[1-9]|1[0-2])$/.test(point.mes) || point.mes <= previous ||
            point.mes > now.toISOString().slice(0, 7) || typeof point.etiqueta !== "string" || point.etiqueta.length > 32 ||
            !Number.isFinite(point.precipitacion) || point.precipitacion < 0) throw new Error(`Invalid monthly values: ${name}`);
        previous = point.mes;
      }
      const oldestCurrentMonth = new Date(now);
      oldestCurrentMonth.setUTCDate(1);
      oldestCurrentMonth.setUTCMonth(oldestCurrentMonth.getUTCMonth() - 1);
      if (payload.data.length >= 10 && previous >= oldestCurrentMonth.toISOString().slice(0, 7)) monthlyStations++;
    }
  }
  if (!Object.hasOwn(files, RECENT) || freshStations < minStations || monthlyStations < minStations) {
    throw new Error(`Incomplete/stale weather: daily=${freshStations}, monthly=${monthlyStations}.`);
  }
  return { latestDate, stations: Object.keys(files[RECENT].data).length, timestamp: files[RECENT].timestamp };
}

export function createWeatherBundle(cacheDir, options) {
  const files = readWeatherFiles(cacheDir);
  validateWeatherFiles(files, options);
  return gzipSync(JSON.stringify(files));
}

export function decodeWeatherBundle(buffer, hash, options) {
  if (buffer.length > MAX_BUNDLE_BYTES || sha256(buffer) !== hash) throw new Error("Weather bundle size or SHA-256 mismatch.");
  const files = JSON.parse(gunzipSync(buffer, { maxOutputLength: 32 * 1024 * 1024 }).toString("utf8"));
  validateWeatherFiles(files, options);
  return files;
}

export function installWeatherFiles(files, cacheDir, options = {}) {
  const summary = validateWeatherFiles(files, options);
  files = { ...files, [RECENT]: { ...files[RECENT], data: { ...files[RECENT].data } } };
  // Reject regressions before touching any cache. Dormant stations may have no history.
  for (const [name, payload] of Object.entries(files)) {
    let current;
    try { current = JSON.parse(readFileSync(join(cacheDir, name), "utf8")); }
    catch (error) { if (error.code !== "ENOENT" && !(error instanceof SyntaxError)) throw error; }
    if (current?.timestamp > payload.timestamp) throw new Error(`Weather timestamp would regress: ${name}`);
    if (name === RECENT && current?.data) {
      for (const [station, points] of Object.entries(current.data)) {
        if (!Object.hasOwn(payload.data, station)) {
          // An inventory change must not erase a station's last-known history.
          try {
            const date = validateDailyPoints(station, points, options.now ?? new Date());
            payload.data[station] = points;
            if (date > summary.latestDate) summary.latestDate = date;
          } catch { /* Do not propagate corrupt local history. */ }
          continue;
        }
        const incoming = payload.data[station];
        if (Array.isArray(points) && points.length && (!incoming?.length || points.at(-1).fecha > incoming.at(-1).fecha)) {
          throw new Error(`Weather history would regress: ${station}`);
        }
      }
    } else if (current?.data?.length && (!payload.data.length || current.data.at(-1).mes > payload.data.at(-1).mes)) {
      throw new Error(`Monthly weather would regress: ${name}`);
    }
  }
  summary.stations = Object.keys(files[RECENT].data).length;
  mkdirSync(cacheDir, { recursive: true });
  let changes = 0;
  for (const [name, payload] of Object.entries(files)) {
    const path = join(cacheDir, name);
    const contents = JSON.stringify(payload);
    let existing;
    try { existing = readFileSync(path, "utf8"); } catch (error) { if (error.code !== "ENOENT") throw error; }
    if (existing === contents) continue;
    writeFileSync(`${path}.tmp`, contents);
    renameSync(`${path}.tmp`, path);
    changes++;
  }
  return { ...summary, changes };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const output = process.argv[2];
  if (!output) throw new Error("Usage: node scripts/weather-publication.mjs <output.gz>");
  const buffer = createWeatherBundle(resolve("data/cache"));
  writeFileSync(output, buffer);
  console.log(sha256(buffer));
}
