import Database from "better-sqlite3";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { getSnapshot, validateSnapshot } from "./database-snapshot.mjs";
import { readWeatherFiles, validateWeatherFiles } from "./weather-publication.mjs";

const USER_AGENT = "IQMizu-Data-Public publication check (+https://github.com/Arrojo14/IQMizu-Data-Public)";

// The public check runs through Hostinger's CDN, which can answer with short
// 429/5xx bursts. Retry for about 8 minutes with backoff and log every failed
// attempt so a failing run says why.
async function verifyResponse(path, matches, { fetchImpl = fetch, attempts = 24, delayMs = 5000, maxDelayMs = 30_000,
  baseUrl = "https://iqmizu.com", log = console.log } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    let retryAfterMs = 0;
    try {
      const response = await fetchImpl(`${baseUrl}${path}?publication=${Date.now()}`, {
        signal: AbortSignal.timeout(15_000), cache: "no-store", headers: { "user-agent": USER_AGENT },
      });
      if (!response.ok) {
        retryAfterMs = Math.min(Number(response.headers.get("retry-after")) * 1000 || 0, 120_000);
        const body = (await response.text().catch(() => "")).replace(/\s+/g, " ").slice(0, 160);
        throw new Error(`Website HTTP ${response.status}${body ? `: ${body}` : ""}`);
      }
      const value = await response.json();
      if (!matches(value)) throw new Error(`Website does not match published data: ${path} ${JSON.stringify(value).slice(0, 200)}`);
      log(`[website] Public API verified: ${path}`);
      return value;
    } catch (error) {
      lastError = error;
      log(`[website] ${path} attempt ${attempt}/${attempts} failed: ${error.message}`);
      if (attempt < attempts) await sleep(Math.max(retryAfterMs, Math.min(delayMs * 2 ** Math.floor((attempt - 1) / 4), maxDelayMs)));
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
