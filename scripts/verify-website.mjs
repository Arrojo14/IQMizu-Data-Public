import Database from "better-sqlite3";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { getSnapshot, validateSnapshot } from "./database-snapshot.mjs";
import { readWeatherFiles, validateWeatherFiles } from "./weather-publication.mjs";

async function verifyResponse(path, matches, { fetchImpl = fetch, attempts = 24, delayMs = 5000, baseUrl = "https://iqmizu.com" } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const response = await fetchImpl(`${baseUrl}${path}?publication=${Date.now()}`, {
        signal: AbortSignal.timeout(10_000), cache: "no-store",
      });
      if (!response.ok) throw new Error(`Website HTTP ${response.status}`);
      const value = await response.json();
      if (!matches(value)) throw new Error(`Website does not match published data: ${path}`);
      console.log(`[website] Public API verified: ${path}`);
      return value;
    } catch (error) {
      lastError = error;
      if (attempt < attempts) await sleep(delayMs);
    }
  }
  throw lastError;
}

export async function verifyWebsite(expected, options) {
  const latest = (rows) => Array.isArray(rows) ? rows.reduce((a, b) => !a || b.fecha > a.fecha ? b : a, null) : null;
  const rows = await verifyResponse("/api/nacional/historico", (rows) => {
    const value = latest(rows);
    return value?.fecha === expected.fecha && Number.isFinite(value?.agua_actual_hm3) && Number.isFinite(value?.agua_total_hm3) &&
      Math.abs(value.agua_actual_hm3 - expected.aguaActualHm3) <= 0.01 && Math.abs(value.agua_total_hm3 - expected.aguaTotalHm3) <= 0.01;
  }, options);
  return latest(rows);
}

export async function verifyWebsiteWeather(files, options) {
  const expected = validateWeatherFiles(files, options);
  await verifyResponse("/api/data-status", ({ weather }) => weather?.timestamp === expected.timestamp &&
    weather.latestDate >= expected.latestDate && weather.stations >= expected.stations, options);
  const samples = Object.entries(files).filter(([name, payload]) => name.startsWith("aemet-monthly-") && payload.data.length).slice(0, 3);
  for (const [name, payload] of samples) {
    const station = name.slice("aemet-monthly-".length, -".json".length);
    await verifyResponse(`/api/estacion/${station}/mensual`, (value) => JSON.stringify(value) === JSON.stringify(payload.data), options);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv.includes("--weather")) {
    await verifyWebsiteWeather(readWeatherFiles(resolve("data/cache")));
  } else {
    const db = new Database(resolve("data/embalses.db"), { readonly: true });
    let expected;
    try { expected = validateSnapshot(getSnapshot(db)); } finally { db.close(); }
    await verifyWebsite(expected);
  }
}
