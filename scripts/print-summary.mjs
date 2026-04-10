import Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

const DB_PATH = resolve("data/embalses.db");
const CACHE_DIR = resolve("data/cache");
const RECENT_RANGE_DAYS = Number(process.env.AEMET_RECENT_RANGE_DAYS || 30);
const MONTHLY_PREFIX = "aemet-monthly-";

function sha256File(filePath) {
  return createHash("sha256").update(readFileSync(filePath)).digest("hex");
}

const summary = {};

if (!existsSync(DB_PATH)) {
  console.error(`No existe ${DB_PATH}. Ejecuta primero: npm run data:update`);
  process.exit(1);
}

const db = new Database(DB_PATH, { readonly: true });

const latest = db.prepare("SELECT MAX(fecha) AS fecha FROM datos_semanales").get();
const earliest = db.prepare("SELECT MIN(fecha) AS fecha FROM datos_semanales").get();
const totals = db.prepare("SELECT COUNT(*) AS c FROM datos_semanales").get();
const embalses = db.prepare("SELECT COUNT(*) AS c FROM embalses").get();
const cuencas = db.prepare("SELECT COUNT(*) AS c FROM cuencas").get();

summary.db = {
  dbPath: DB_PATH,
  earliestDate: earliest.fecha,
  latestDate: latest.fecha,
  cuencas: cuencas.c,
  embalses: embalses.c,
  weeklyRows: totals.c,
  fileSizeBytes: statSync(DB_PATH).size,
  sha256: sha256File(DB_PATH),
};

db.close();

const recentPath = join(CACHE_DIR, `aemet-recent-climate-${RECENT_RANGE_DAYS}.json`);
if (existsSync(recentPath)) {
  const parsed = JSON.parse(readFileSync(recentPath, "utf8"));
  summary.aemetRecent = {
    filePath: recentPath,
    rangeDays: parsed?.rangeDays ?? null,
    timestamp:
      typeof parsed?.timestamp === "number"
        ? new Date(parsed.timestamp).toISOString()
        : null,
    stations:
      parsed?.data && typeof parsed.data === "object" ? Object.keys(parsed.data).length : 0,
    fileSizeBytes: statSync(recentPath).size,
    sha256: sha256File(recentPath),
  };
}

if (existsSync(CACHE_DIR)) {
  const monthlyFiles = readdirSync(CACHE_DIR)
    .filter((entry) => entry.startsWith(MONTHLY_PREFIX) && entry.endsWith(".json"))
    .sort((a, b) => a.localeCompare(b, "es"));

  summary.aemetMonthly = {
    files: monthlyFiles.length,
    totalSizeBytes: monthlyFiles.reduce(
      (sum, entry) => sum + statSync(join(CACHE_DIR, entry)).size,
      0
    ),
    firstFile: monthlyFiles[0] ?? null,
    lastFile: monthlyFiles.at(-1) ?? null,
  };
}

console.log(JSON.stringify(summary, null, 2));
