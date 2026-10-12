import Database from "better-sqlite3";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { getSnapshot, validateSnapshot } from "./database-snapshot.mjs";
import { readWeatherFiles, validateWeatherFiles } from "./weather-publication.mjs";

const USER_AGENT = "IQMizu-Data-Public publication check (+https://github.com/Arrojo14/IQMizu-Data-Public)";

// The public check runs through Hostinger's CDN, which can answer with short
// 429/5xx bursts. Retry for about 8 minutes with backoff and log every failed
// attempt so a failing run says why. A page check (`page: true`) reads the HTML,
// because the API can be current while pages still come from an older render; it
// returns null at once for a page that does not exist.
async function verifyResponse(path, matches, { fetchImpl = fetch, attempts = 24, delayMs = 5000, maxDelayMs = 30_000,
  baseUrl = "https://iqmizu.com", log = console.log, page = false } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    let retryAfterMs = 0;
    try {
      const response = await fetchImpl(`${baseUrl}${path}?publication=${Date.now()}`, {
        signal: AbortSignal.timeout(15_000), cache: "no-store", headers: { "user-agent": USER_AGENT },
      });
      if (page && response.status === 404) {
        log(`[website] ${path} does not exist`);
        return null;
      }
      if (!response.ok) {
        retryAfterMs = Math.min(Number(response.headers.get("retry-after")) * 1000 || 0, 120_000);
        const body = (await response.text().catch(() => "")).replace(/\s+/g, " ").slice(0, 160);
        throw new Error(`Website HTTP ${response.status}${body ? `: ${body}` : ""}`);
      }
      const value = page ? await response.text() : await response.json();
      if (!matches(value)) {
        throw new Error(`Website does not match published data: ${path}${page ? " (the page shows older data)" : ` ${JSON.stringify(value).slice(0, 200)}`}`);
      }
      log(`[website] Public ${page ? "page" : "API"} verified: ${path}`);
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
  // Inicio prints the bulletin date ("Actualizado 6 de octubre de 2026").
  const date = new Intl.DateTimeFormat("es-ES", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" })
    .format(new Date(`${expected.fecha}T00:00:00Z`));
  if (await verifyResponse("/", (html) => html.includes(date), { ...options, page: true }) === null) {
    throw new Error("Website home page not found");
  }
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
  // One station page whose 30 days reach the latest date, a different one each
  // delivery, so the check also covers pages nobody has opened for a while. Stations
  // missing from AEMET's live inventory have no page; try the next one.
  const recent = files["aemet-recent-climate-30.json"].data;
  const current = Object.keys(recent).filter((station) => recent[station].at(-1)?.fecha === expected.latestDate).sort();
  const day = new RegExp(`fecha\\\\?":\\\\?"${expected.latestDate}`);
  for (let i = 0; i < Math.min(current.length, 5); i++) {
    const station = current[(expected.timestamp + i) % current.length];
    if (await verifyResponse(`/estacion/${station}`, (html) => day.test(html), { ...options, page: true }) !== null) return;
  }
  throw new Error("No station page found to verify");
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
