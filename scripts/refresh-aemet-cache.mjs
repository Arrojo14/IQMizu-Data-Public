import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const AEMET_BASE = "https://opendata.aemet.es/opendata";
const CACHE_DIR = resolve("data", "cache");
const MONTHLY_PREFIX = "aemet-monthly-";
const AEMET_WEB_BASE = "https://www.aemet.es";
const RECENT_RANGE_DAYS = Number(process.env.AEMET_RECENT_RANGE_DAYS || 30);
const LOOKBACK_DAYS = Number(process.env.AEMET_HISTORY_RANGE_DAYS || 364);
const REQUEST_TIMEOUT_MS = Number(process.env.AEMET_REQUEST_TIMEOUT_MS || 12_000);
const MAX_ALL_STATIONS_DAILY_RANGE_DAYS = 15;
const STATION_CACHE_LIMIT = Number(process.env.STATION_CACHE_LIMIT || 0);
const MAX_RETRIES = Number(process.env.STATION_CACHE_MAX_RETRIES || 6);
const CHUNK_DELAY_MS = Number(process.env.STATION_CACHE_CHUNK_DELAY_MS || 1500);
const RECENT_OBS_OVERLAY_ENABLED = process.env.AEMET_RECENT_OBS_OVERLAY !== "0";
const RECENT_OBS_CONCURRENCY = Number(process.env.AEMET_RECENT_OBS_CONCURRENCY || 4);
const RECENT_OBS_DELAY_MS = Number(process.env.AEMET_RECENT_OBS_DELAY_MS || 100);
const RECENT_OBS_MAX_RETRIES = Number(process.env.AEMET_RECENT_OBS_MAX_RETRIES || 3);
const RECENT_OBS_TIMEOUT_MS = Number(process.env.AEMET_RECENT_OBS_TIMEOUT_MS || REQUEST_TIMEOUT_MS);
const AEMET_REQUIRED = process.env.AEMET_REQUIRED !== "0";

loadEnvFile(resolve(".env.local"));
loadEnvFile(resolve(".env"));

const AEMET_API_KEY = process.env.AEMET_API_KEY?.trim();

function loadEnvFile(filePath) {
  try {
    if (!existsSync(filePath)) return;
    const contents = readFileSync(filePath, "utf8");

    for (const line of contents.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;

      const separator = trimmed.indexOf("=");
      if (separator === -1) continue;

      const key = trimmed.slice(0, separator).trim();
      let value = trimmed.slice(separator + 1).trim();
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }

      if (!(key in process.env)) {
        process.env[key] = value;
      }
    }
  } catch {
    // Ignore local env loading errors.
  }
}

function validatePositiveInteger(name, value, minimum = 1) {
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`${name} no es valido.`);
  }
}

function validateNonNegativeInteger(name, value) {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} no es valido.`);
  }
}

function decodeJsonBuffer(buffer) {
  const text = new TextDecoder("iso-8859-15").decode(buffer);
  return JSON.parse(text);
}

function formatAemetDate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function safeIndicativo(indicativo) {
  return indicativo.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function sleep(ms) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

function isInsideSpainBounds(lat, lon) {
  return lat >= 27 && lat <= 44 && lon >= -19 && lon <= 5;
}

function parseNullableNumber(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value !== "string") return null;
  const normalized = value.trim();
  if (!normalized || normalized === "Ip") return 0;

  const parsed = Number.parseFloat(normalized.replace(/\./g, "").replace(/,/g, "."));
  return Number.isFinite(parsed) ? parsed : null;
}

function parseCsvNumber(value) {
  if (typeof value !== "string") return null;

  const normalized = value.trim();
  if (!normalized || normalized === "Ip") return 0;

  const parsed = Number.parseFloat(normalized.replace(",", "."));
  return Number.isFinite(parsed) ? parsed : null;
}

function isRetryableAemetError(error) {
  return (
    error instanceof Error &&
    (error.message.includes("AEMET API error: 429") ||
      error.message.includes("AEMET API error: 500") ||
      error.message.includes("AEMET API timeout") ||
      error.message.includes("AEMET data timeout") ||
      error.message.includes("AEMET data fetch error: 500"))
  );
}

async function fetchAemetDataOnce(endpoint) {
  if (!AEMET_API_KEY) {
    throw new Error("AEMET_API_KEY not configured");
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  let res;
  try {
    res = await fetch(`${AEMET_BASE}${endpoint}`, {
      headers: { api_key: AEMET_API_KEY },
      cache: "no-store",
      signal: controller.signal,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error("AEMET API timeout");
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }

  if (!res.ok) {
    throw new Error(`AEMET API error: ${res.status}`);
  }

  const meta = await res.json();
  if (meta?.estado !== 200 || !meta?.datos) {
    throw new Error(`AEMET API returned: ${meta?.descripcion ?? "unknown error"}`);
  }

  const dataController = new AbortController();
  const dataTimeoutId = setTimeout(() => dataController.abort(), REQUEST_TIMEOUT_MS);
  let dataRes;
  try {
    dataRes = await fetch(meta.datos, {
      cache: "no-store",
      signal: dataController.signal,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error("AEMET data timeout");
    }
    throw error;
  } finally {
    clearTimeout(dataTimeoutId);
  }

  if (!dataRes.ok) {
    throw new Error(`AEMET data fetch error: ${dataRes.status}`);
  }

  return decodeJsonBuffer(await dataRes.arrayBuffer());
}

async function fetchAemetData(endpoint) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      return await fetchAemetDataOnce(endpoint);
    } catch (error) {
      if (!isRetryableAemetError(error) || attempt === MAX_RETRIES) {
        throw error;
      }

      const backoffMs = Math.min(30_000, 3000 * attempt);
      console.warn(
        `[aemet-cache] Reintento ${attempt}/${MAX_RETRIES} tras error temporal: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      await sleep(backoffMs);
    }
  }

  throw new Error("Unexpected retry flow");
}

function decodeCsvBuffer(buffer) {
  return new TextDecoder("iso-8859-15").decode(buffer);
}

function parseCsvLine(line) {
  const fields = [];
  let current = "";
  let quoted = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === '"') {
      if (quoted && next === '"') {
        current += '"';
        index += 1;
      } else {
        quoted = !quoted;
      }
      continue;
    }

    if (char === "," && !quoted) {
      fields.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  fields.push(current);
  return fields.map((field) => field.trim());
}

function parseSpanishSummaryDate(value) {
  const match = value
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\./g, "")
    .match(/^(\d{1,2})\s+([a-z]+)\s+(\d{4})$/);

  if (!match) return null;

  const [, dayRaw, monthRaw, yearRaw] = match;
  const months = new Map([
    ["ene", "01"],
    ["enero", "01"],
    ["feb", "02"],
    ["febrero", "02"],
    ["mar", "03"],
    ["marzo", "03"],
    ["abr", "04"],
    ["abril", "04"],
    ["may", "05"],
    ["mayo", "05"],
    ["jun", "06"],
    ["junio", "06"],
    ["jul", "07"],
    ["julio", "07"],
    ["ago", "08"],
    ["agosto", "08"],
    ["sep", "09"],
    ["sept", "09"],
    ["septiembre", "09"],
    ["oct", "10"],
    ["octubre", "10"],
    ["nov", "11"],
    ["noviembre", "11"],
    ["dic", "12"],
    ["diciembre", "12"],
  ]);
  const month = months.get(monthRaw);
  const day = dayRaw.padStart(2, "0");

  return month ? `${yearRaw}-${month}-${day}` : null;
}

function parseRecentObservationCsv(csvText) {
  const lines = csvText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const headerIndex = lines.findIndex((line) => line.includes("Precipitaci"));
  if (headerIndex === -1) return [];

  const points = [];
  for (const line of lines.slice(headerIndex + 1)) {
    const fields = parseCsvLine(line);
    if (fields.length < 7) continue;

    const fecha = parseSpanishSummaryDate(fields[0]);
    if (!fecha) continue;

    points.push({
      fecha,
      precipitacion: parseCsvNumber(fields[6]) ?? 0,
      temperaturaMedia: parseCsvNumber(fields[3]),
      humedadMedia: null,
    });
  }

  return sortDailyPoints(points);
}

function isRetryableRecentObservationError(error) {
  return (
    error instanceof Error &&
    (error.message.includes("AEMET recent observations error: 429") ||
      error.message.includes("AEMET recent observations error: 500") ||
      error.message.includes("AEMET recent observations timeout"))
  );
}

async function fetchRecentObservationCsvOnce(indicativo) {
  const encodedIndicativo = encodeURIComponent(indicativo);
  const url = `${AEMET_WEB_BASE}/es/eltiempo/observacion/ultimosdatos_${encodedIndicativo}_resumenes-diarios-anteriores.csv?l=${encodedIndicativo}&datos=det&w=2&f=tmax&x=`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), RECENT_OBS_TIMEOUT_MS);

  let res;
  try {
    res = await fetch(url, {
      headers: {
        accept: "text/csv,text/plain,*/*",
        "user-agent": "IQMizu data updater (https://iqmizu.com)",
      },
      cache: "no-store",
      signal: controller.signal,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error("AEMET recent observations timeout");
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }

  if (!res.ok) {
    throw new Error(`AEMET recent observations error: ${res.status}`);
  }

  return decodeCsvBuffer(await res.arrayBuffer());
}

async function fetchRecentObservationCsv(indicativo) {
  for (let attempt = 1; attempt <= RECENT_OBS_MAX_RETRIES; attempt += 1) {
    try {
      return await fetchRecentObservationCsvOnce(indicativo);
    } catch (error) {
      if (!isRetryableRecentObservationError(error) || attempt === RECENT_OBS_MAX_RETRIES) {
        throw error;
      }

      await sleep(Math.min(15_000, 2000 * attempt));
    }
  }

  throw new Error("Unexpected recent observations retry flow");
}

function getLatestPointDate(points) {
  return points.length > 0 ? points[points.length - 1].fecha : null;
}

async function runWithConcurrency(items, concurrency, worker) {
  let index = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (index < items.length) {
      const item = items[index];
      index += 1;
      await worker(item);
    }
  });

  await Promise.all(workers);
}

async function overlayRecentObservationSummaries(pointsByStation, indicativos, latestClosedDate) {
  const candidates = indicativos.filter((indicativo) => {
    const latest = getLatestPointDate(pointsByStation.get(indicativo) ?? []);
    return latest === null || latest < latestClosedDate;
  });

  let fetched = 0;
  let skippedUpToDate = indicativos.length - candidates.length;
  let updatedStations = 0;
  let addedRows = 0;
  let failed = 0;
  const failures = [];

  console.log(
    `[aemet-cache] Overlay provisional AEMET web: ${candidates.length} estaciones candidatas (${skippedUpToDate} al dia).`
  );

  await runWithConcurrency(candidates, RECENT_OBS_CONCURRENCY, async (indicativo) => {
    try {
      if (RECENT_OBS_DELAY_MS > 0) {
        await sleep(RECENT_OBS_DELAY_MS);
      }

      const existingPoints = pointsByStation.get(indicativo) ?? [];
      const existingDates = new Set(existingPoints.map((point) => point.fecha));
      const csvText = await fetchRecentObservationCsv(indicativo);
      fetched += 1;

      const overlayPoints = parseRecentObservationCsv(csvText).filter(
        (point) => point.fecha <= latestClosedDate && !existingDates.has(point.fecha)
      );

      if (overlayPoints.length === 0) return;

      pointsByStation.set(indicativo, sortDailyPoints([...existingPoints, ...overlayPoints]));
      updatedStations += 1;
      addedRows += overlayPoints.length;
    } catch (error) {
      failed += 1;
      if (failures.length < 10) {
        failures.push({
          indicativo,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  });

  return {
    enabled: true,
    latestClosedDate,
    candidates: candidates.length,
    skippedUpToDate,
    fetched,
    updatedStations,
    addedRows,
    failed,
    failures,
  };
}

async function fetchObservations() {
  return fetchAemetData("/api/observacion/convencional/todas");
}

async function fetchAllStationsDailyClimateChunk(startDate, endDate) {
  const start = `${formatAemetDate(startDate)}T00:00:00UTC`;
  const end = `${formatAemetDate(endDate)}T23:59:59UTC`;

  return fetchAemetData(
    `/api/valores/climatologicos/diarios/datos/fechaini/${start}/fechafin/${end}/todasestaciones`
  );
}

function getActiveStationIndicativos(observations) {
  const active = new Set();
  for (const item of observations) {
    if (
      typeof item?.idema === "string" &&
      typeof item?.lat === "number" &&
      typeof item?.lon === "number" &&
      isInsideSpainBounds(item.lat, item.lon)
    ) {
      active.add(item.idema);
    }
  }
  return [...active].sort((a, b) => a.localeCompare(b, "es"));
}

function normalizeDailyPoint(item) {
  return {
    fecha: typeof item?.fecha === "string" ? item.fecha : "",
    precipitacion: parseNullableNumber(item?.prec) ?? 0,
    temperaturaMedia: parseNullableNumber(item?.tmed),
    humedadMedia: parseNullableNumber(item?.hrMedia),
  };
}

function sortDailyPoints(points) {
  return [...points].sort((a, b) => a.fecha.localeCompare(b.fecha));
}

function buildMonthlyPrecipitation(points) {
  const monthly = new Map();

  for (const point of points) {
    const monthKey = point.fecha.slice(0, 7);
    monthly.set(monthKey, (monthly.get(monthKey) ?? 0) + point.precipitacion);
  }

  return [...monthly.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .slice(-12)
    .map(([mes, precipitacion]) => {
      const date = new Date(`${mes}-01T00:00:00`);
      return {
        mes,
        etiqueta: date.toLocaleDateString("es-ES", {
          month: "short",
          year: "2-digit",
        }),
        precipitacion: Math.round(precipitacion * 10) / 10,
      };
    });
}

function writeJsonAtomic(filePath, value) {
  mkdirSync(dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.tmp`;
  writeFileSync(tempPath, JSON.stringify(value));
  if (existsSync(filePath)) {
    unlinkSync(filePath);
  }
  renameSync(tempPath, filePath);
}

function finalizeDailyPoints(pointsByStation) {
  const finalized = new Map();

  for (const [indicativo, points] of pointsByStation) {
    const deduped = new Map();
    for (const point of points) {
      if (!point.fecha) continue;
      deduped.set(point.fecha, point);
    }
    finalized.set(indicativo, sortDailyPoints(deduped.values()));
  }

  return finalized;
}

function buildRecentClimatePayload(pointsByStation, recentRangeDays, timestamp) {
  const entries = [...pointsByStation.entries()]
    .sort(([a], [b]) => a.localeCompare(b, "es"))
    .map(([indicativo, points]) => [indicativo, points.slice(-recentRangeDays)]);

  return {
    timestamp,
    rangeDays: recentRangeDays,
    data: Object.fromEntries(entries),
  };
}

function syncMonthlyFiles(pointsByStation, timestamp) {
  mkdirSync(CACHE_DIR, { recursive: true });

  const expectedFiles = new Set();
  for (const [indicativo, points] of [...pointsByStation.entries()].sort(([a], [b]) =>
    a.localeCompare(b, "es")
  )) {
    const fileName = `${MONTHLY_PREFIX}${safeIndicativo(indicativo)}.json`;
    expectedFiles.add(fileName);
    writeJsonAtomic(join(CACHE_DIR, fileName), {
      timestamp,
      data: buildMonthlyPrecipitation(points),
    });
  }

  for (const entry of readdirSync(CACHE_DIR)) {
    if (!entry.startsWith(MONTHLY_PREFIX) || !entry.endsWith(".json")) continue;
    if (expectedFiles.has(entry)) continue;
    unlinkSync(join(CACHE_DIR, entry));
  }

  return expectedFiles.size;
}

export async function refreshAemetCaches() {
  validatePositiveInteger("AEMET_RECENT_RANGE_DAYS", RECENT_RANGE_DAYS);
  validatePositiveInteger("AEMET_HISTORY_RANGE_DAYS", LOOKBACK_DAYS);
  validatePositiveInteger("AEMET_REQUEST_TIMEOUT_MS", REQUEST_TIMEOUT_MS);
  validatePositiveInteger("STATION_CACHE_MAX_RETRIES", MAX_RETRIES);
  validatePositiveInteger("AEMET_RECENT_OBS_CONCURRENCY", RECENT_OBS_CONCURRENCY);
  validatePositiveInteger("AEMET_RECENT_OBS_MAX_RETRIES", RECENT_OBS_MAX_RETRIES);
  validatePositiveInteger("AEMET_RECENT_OBS_TIMEOUT_MS", RECENT_OBS_TIMEOUT_MS);
  validateNonNegativeInteger("AEMET_RECENT_OBS_DELAY_MS", RECENT_OBS_DELAY_MS);

  if (RECENT_RANGE_DAYS > LOOKBACK_DAYS + 1) {
    throw new Error("AEMET_RECENT_RANGE_DAYS no puede ser mayor que el historico descargado.");
  }

  if (!AEMET_API_KEY) {
    if (AEMET_REQUIRED) {
      throw new Error("AEMET_API_KEY no configurada y AEMET_REQUIRED no es 0.");
    }
    console.warn("[aemet-cache] AEMET_API_KEY no configurada. Se omite la actualizacion.");
    return null;
  }

  console.log("\n[aemet-cache] Cargando estaciones activas...");
  const observations = await fetchObservations();
  let indicativos = getActiveStationIndicativos(Array.isArray(observations) ? observations : []);

  if (Number.isFinite(STATION_CACHE_LIMIT) && STATION_CACHE_LIMIT > 0) {
    indicativos = indicativos.slice(0, STATION_CACHE_LIMIT);
    console.log(`[aemet-cache] Modo limitado: ${indicativos.length} estaciones.`);
  } else {
    console.log(`[aemet-cache] Estaciones activas detectadas: ${indicativos.length}`);
  }

  if (indicativos.length === 0) {
    throw new Error("AEMET no devolvio estaciones activas para generar historico.");
  }

  const indicativoSet = new Set(indicativos);
  const endDate = new Date();
  endDate.setDate(endDate.getDate() - 1);
  const startDate = addDays(endDate, -LOOKBACK_DAYS);
  const merged = new Map(indicativos.map((indicativo) => [indicativo, []]));

  let chunkStart = new Date(startDate);
  let chunkCount = 0;
  let totalRawRows = 0;

  while (chunkStart <= endDate) {
    const chunkEnd = addDays(chunkStart, MAX_ALL_STATIONS_DAILY_RANGE_DAYS - 1);
    const effectiveEnd = chunkEnd < endDate ? chunkEnd : endDate;
    chunkCount += 1;

    console.log(
      `[aemet-cache] Descargando tramo ${chunkCount}: ${formatAemetDate(chunkStart)} -> ${formatAemetDate(effectiveEnd)}`
    );

    const rawChunk = await fetchAllStationsDailyClimateChunk(chunkStart, effectiveEnd);
    const chunk = Array.isArray(rawChunk) ? rawChunk : [];
    totalRawRows += chunk.length;

    for (const item of chunk) {
      if (typeof item?.indicativo !== "string" || !indicativoSet.has(item.indicativo)) {
        continue;
      }

      const point = normalizeDailyPoint(item);
      if (!point.fecha) continue;

      const bucket = merged.get(item.indicativo);
      if (bucket) {
        bucket.push(point);
      }
    }

    chunkStart = addDays(effectiveEnd, 1);
    if (chunkStart <= endDate && CHUNK_DELAY_MS > 0) {
      await sleep(CHUNK_DELAY_MS);
    }
  }

  const finalized = finalizeDailyPoints(merged);
  const latestClosedDate = formatAemetDate(endDate);
  const recentObservationOverlay = RECENT_OBS_OVERLAY_ENABLED
    ? await overlayRecentObservationSummaries(finalized, indicativos, latestClosedDate)
    : { enabled: false };
  const timestamp = Date.now();
  const recentPayload = buildRecentClimatePayload(finalized, RECENT_RANGE_DAYS, timestamp);
  const recentFilePath = join(CACHE_DIR, `aemet-recent-climate-${RECENT_RANGE_DAYS}.json`);

  writeJsonAtomic(recentFilePath, recentPayload);
  const monthlyFileCount = syncMonthlyFiles(finalized, timestamp);
  const populatedStationCount = [...finalized.values()].filter((points) => points.length > 0).length;

  console.log(`[aemet-cache] Cache reciente actualizada: ${recentFilePath}`);
  console.log(`[aemet-cache] Estaciones con historico: ${populatedStationCount}/${indicativos.length}`);
  console.log(`[aemet-cache] Ficheros mensuales regenerados: ${monthlyFileCount}`);
  if (recentObservationOverlay.enabled) {
    console.log(
      `[aemet-cache] Overlay provisional: ${recentObservationOverlay.addedRows} filas nuevas en ${recentObservationOverlay.updatedStations} estaciones; fallos ${recentObservationOverlay.failed}.`
    );
  } else {
    console.log("[aemet-cache] Overlay provisional desactivado.");
  }

  return {
    cacheDir: CACHE_DIR,
    recentFilePath,
    recentRangeDays: RECENT_RANGE_DAYS,
    activeStations: indicativos.length,
    populatedStations: populatedStationCount,
    monthlyFileCount,
    chunkCount,
    totalRawRows,
    recentObservationOverlay,
  };
}

async function main() {
  const summary = await refreshAemetCaches();
  if (summary) {
    console.log(`[aemet-cache] Actualizacion completada: ${JSON.stringify(summary)}`);
  }
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : null;
const currentPath = fileURLToPath(import.meta.url);

if (invokedPath === currentPath) {
  main().catch((error) => {
    console.error("[aemet-cache] Error:", error);
    process.exit(1);
  });
}
