import MDBReader from "mdb-reader";
import Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ZIP_URL = "https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico/Historico-de-embalses/BD-Embalses.zip";
const COLUMNS = ["FECHA", "AMBITO_NOMBRE", "EMBALSE_NOMBRE", "AGUA_ACTUAL", "AGUA_TOTAL"];

export function createSchema(db) {
  db.pragma("foreign_keys = ON");
  db.exec(`
    CREATE TABLE IF NOT EXISTS cuencas (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS embalses (
      id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL, cuenca_id INTEGER NOT NULL,
      capacidad_hm3 REAL NOT NULL DEFAULT 0, electrico INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (cuenca_id) REFERENCES cuencas(id)
    );
    CREATE TABLE IF NOT EXISTS datos_semanales (
      id INTEGER PRIMARY KEY AUTOINCREMENT, embalse_id INTEGER NOT NULL, fecha TEXT NOT NULL,
      agua_actual_hm3 REAL, agua_total_hm3 REAL,
      FOREIGN KEY (embalse_id) REFERENCES embalses(id)
    );
    CREATE INDEX IF NOT EXISTS idx_datos_fecha ON datos_semanales(fecha);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_datos_embalse_fecha_unique ON datos_semanales(embalse_id, fecha);
    CREATE TABLE IF NOT EXISTS update_state (clave TEXT PRIMARY KEY, valor TEXT NOT NULL);
  `);
  if (!db.prepare("PRAGMA table_info(datos_semanales)").all().some((column) => column.name === "fuente")) {
    db.exec("ALTER TABLE datos_semanales ADD COLUMN fuente TEXT NOT NULL DEFAULT 'legacy'");
  }
  // The unique (reservoir, date) index also serves both old lookup indexes.
  // Compact only during this migration, not on every scheduled run.
  const oldIndexes = db.prepare("PRAGMA index_list(datos_semanales)").all()
    .some((index) => ["idx_datos_embalse", "idx_datos_embalse_fecha"].includes(index.name));
  if (oldIndexes) {
    db.exec("DROP INDEX IF EXISTS idx_datos_embalse; DROP INDEX IF EXISTS idx_datos_embalse_fecha;");
    db.exec("VACUUM");
  }
}

export function getState(db, key) {
  return db.prepare("SELECT valor FROM update_state WHERE clave = ?").get(key)?.valor ?? null;
}

function setState(db, key, value) {
  db.prepare(`INSERT INTO update_state VALUES (?, ?)
    ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor WHERE valor IS NOT excluded.valor`).run(key, value);
}

export function getSnapshot(db) {
  return db.prepare(`SELECT MAX(fecha) AS fecha, COUNT(*) AS filas,
    COUNT(DISTINCT embalse_id) AS embalses,
    SUM(agua_actual_hm3) AS aguaActualHm3, SUM(agua_total_hm3) AS aguaTotalHm3,
    SUM(CASE WHEN agua_actual_hm3 IS NULL OR agua_actual_hm3 < 0 OR agua_total_hm3 <= 0
      OR agua_total_hm3 IS NULL OR agua_actual_hm3 > agua_total_hm3 * 1.02 THEN 1 ELSE 0 END) AS invalidas
    FROM datos_semanales WHERE fecha = (SELECT MAX(fecha) FROM datos_semanales)`).get();
}

export function validateSnapshot(snapshot, { now = new Date(), minRows = 300, maxAgeDays = 14 } = {}) {
  const ageDays = (now - new Date(`${snapshot.fecha}T00:00:00Z`)) / 86_400_000;
  if (!Number.isFinite(ageDays) || ageDays < -1 || ageDays > maxAgeDays ||
      snapshot.embalses < minRows || snapshot.filas !== snapshot.embalses || snapshot.invalidas > 0 ||
      !(snapshot.aguaTotalHm3 > 0) || !(snapshot.aguaActualHm3 >= 0) ||
      snapshot.aguaActualHm3 > snapshot.aguaTotalHm3 * 1.02) {
    throw new Error(`DB incompleta, no plausible o desactualizada: ${JSON.stringify(snapshot)}`);
  }
  return snapshot;
}

function parseNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const parsed = Number(raw.includes(",") ? raw.replace(/\./g, "").replace(",", ".") : raw);
  return Number.isFinite(parsed) ? parsed : null;
}

export function readOfficialRows(reader) {
  const tables = reader.getTableNames().map((name) => reader.getTable(name))
    .filter((table) => COLUMNS.every((column) => table.getColumnNames().includes(column)));
  if (tables.length !== 1) throw new Error(`Se esperaba una tabla de embalses; encontradas: ${tables.length}.`);
  return tables[0].getData();
}

export function importOfficialRows(db, rawRows, hash, { minRows = 300 } = {}) {
  const rows = rawRows.map((row) => {
    const date = new Date(row.FECHA);
    return {
      fecha: row.FECHA && Number.isFinite(date.getTime()) ? date.toISOString().slice(0, 10) : null,
      cuenca: String(row.AMBITO_NOMBRE ?? "").trim(), nombre: String(row.EMBALSE_NOMBRE ?? "").trim(),
      actual: parseNumber(row.AGUA_ACTUAL), total: parseNumber(row.AGUA_TOTAL),
      electrico: Number(row.ELECTRICO_FLAG) === 1 ? 1 : 0,
    };
  }).filter((row) => row.fecha && row.cuenca && row.nombre);
  const latest = rows.reduce((date, row) => row.fecha > date ? row.fecha : date, "");
  const latestRows = rows.filter((row) => row.fecha === latest);
  const keys = new Set(latestRows.map((row) => `${row.cuenca}\0${row.nombre}`));
  if (keys.size < minRows || keys.size !== latestRows.length || latestRows.some((row) =>
    row.actual === null || row.actual < 0 || !(row.total > 0) || row.actual > row.total * 1.02)) {
    throw new Error(`Ultima semana MITECO incompleta o no plausible: ${latest}, ${keys.size} embalses.`);
  }

  // Reconcile official corrections even when provisional data is newer.
  // Long outages import every missing week; bootstrap imports the full history.
  const previous = getSnapshot(db).fecha;
  const recentStart = new Date(`${latest}T00:00:00Z`);
  recentStart.setUTCDate(recentStart.getUTCDate() - 180);
  const cutoff = previous ? [previous, recentStart.toISOString().slice(0, 10)].sort()[0] : "";
  const cuencas = new Map(db.prepare("SELECT id, nombre FROM cuencas").all().map((row) => [row.nombre, row.id]));
  const embalses = new Map(db.prepare(`SELECT e.id, e.nombre, c.nombre AS cuenca FROM embalses e
    JOIN cuencas c ON c.id = e.cuenca_id`).all().map((row) => [`${row.cuenca}\0${row.nombre}`, row.id]));
  const addCuenca = db.prepare("INSERT INTO cuencas(nombre) VALUES (?)");
  const addEmbalse = db.prepare("INSERT INTO embalses(nombre, cuenca_id, capacidad_hm3, electrico) VALUES (?, ?, ?, ?)");
  const updateEmbalse = db.prepare(`UPDATE embalses SET capacidad_hm3 = ?, electrico = ? WHERE id = ?
    AND (capacidad_hm3 IS NOT ? OR electrico IS NOT ?)`);
  const existing = db.prepare("SELECT id, agua_actual_hm3 AS actual, agua_total_hm3 AS total, fuente FROM datos_semanales WHERE embalse_id = ? AND fecha = ?");
  const insert = db.prepare("INSERT INTO datos_semanales(embalse_id, fecha, agua_actual_hm3, agua_total_hm3, fuente) VALUES (?, ?, ?, ?, 'oficial')");
  const update = db.prepare("UPDATE datos_semanales SET agua_actual_hm3 = ?, agua_total_hm3 = ?, fuente = 'oficial' WHERE id = ?");
  let changed = 0;
  db.transaction(() => {
    for (const row of rows) {
      if (row.fecha < cutoff) continue;
      let cuenca = cuencas.get(row.cuenca);
      if (!cuenca) {
        cuenca = Number(addCuenca.run(row.cuenca).lastInsertRowid);
        cuencas.set(row.cuenca, cuenca);
      }
      const key = `${row.cuenca}\0${row.nombre}`;
      let id = embalses.get(key);
      if (!id) {
        id = Number(addEmbalse.run(row.nombre, cuenca, row.total ?? 0, row.electrico).lastInsertRowid);
        embalses.set(key, id);
      }
      if (row.fecha === latest) updateEmbalse.run(row.total, row.electrico, id, row.total, row.electrico);
      const current = existing.get(id, row.fecha);
      if (!current) {
        insert.run(id, row.fecha, row.actual, row.total);
        changed++;
      } else if (current.actual !== row.actual || current.total !== row.total || current.fuente !== "oficial") {
        update.run(row.actual, row.total, current.id);
        changed++;
      }
    }
    setState(db, "miteco_official_latest_date", latest);
    setState(db, "miteco_archive_sha256", hash);
  })();
  console.log(`[miteco] Fecha oficial=${latest}; filas modificadas=${changed}.`);
  return changed;
}

export async function downloadArchive(url, { fetchImpl = fetch, timeoutMs = 90_000, attempts = 3, retryDelayMs = 3000 } = {}) {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const response = await fetchImpl(url, { signal: AbortSignal.timeout(timeoutMs) });
      if (!response.ok) throw new Error(`MITECO HTTP ${response.status}`);
      const buffer = Buffer.from(await response.arrayBuffer());
      if (buffer.length < 100_000 || buffer.readUInt32LE(0) !== 0x04034b50) throw new Error("Respuesta MITECO no es un ZIP valido.");
      return buffer;
    } catch (error) {
      if (attempt === attempts) throw error;
      console.warn(`[miteco] Reintento ${attempt}/${attempts}: ${error.message}`);
      await sleep(retryDelayMs * attempt);
    }
  }
}

function findMdb(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      const found = findMdb(path);
      if (found) return found;
    } else if (entry.name.toLowerCase().endsWith(".mdb")) return path;
  }
  return null;
}

export async function syncData({ dbPath = resolve(ROOT, "data/embalses.db"), download = downloadArchive,
  readRows, provisional = true, now = new Date() } = {}) {
  const dataDir = dirname(dbPath);
  mkdirSync(dataDir, { recursive: true });
  const workDir = mkdtempSync(join(dataDir, ".miteco-"));
  const stagedPath = join(workDir, "embalses.db");
  let db;
  try {
    // This repository stores a closed Git artifact, not the web application's live DB.
    // A byte copy also avoids SQLite backup changing headers on otherwise unchanged runs.
    if (existsSync(dbPath)) copyFileSync(dbPath, stagedPath);
    db = new Database(stagedPath);
    db.pragma("journal_mode = DELETE");
    createSchema(db);
    const before = getSnapshot(db);
    const zip = await download(ZIP_URL);
    const hash = createHash("sha256").update(zip).digest("hex");
    if (hash === getState(db, "miteco_archive_sha256")) {
      console.log("[miteco] ZIP sin cambios; se omite extraccion, lectura MDB e importacion.");
    } else {
      let rows;
      if (readRows) rows = readRows(zip);
      else {
        const zipPath = join(workDir, "source.zip");
        const extracted = join(workDir, "source");
        writeFileSync(zipPath, zip);
        if (process.platform === "win32") {
          execFileSync("powershell", ["-NoProfile", "-Command",
            `Expand-Archive -LiteralPath '${zipPath.replace(/'/g, "''")}' -DestinationPath '${extracted.replace(/'/g, "''")}'`],
          { stdio: "inherit", timeout: 60_000 });
        } else {
          execFileSync("unzip", ["-q", zipPath, "-d", extracted], { stdio: "inherit", timeout: 60_000 });
        }
        const mdb = findMdb(extracted);
        if (!mdb) throw new Error("No se encontro MDB en el ZIP.");
        console.log("[miteco] Leyendo nuevo archivo MDB...");
        rows = readOfficialRows(new MDBReader(readFileSync(mdb)));
      }
      importOfficialRows(db, rows, hash);
    }
    db.close();
    db = null;
    if (provisional) {
      try {
        execFileSync(process.execPath, [resolve(ROOT, "scripts/update-provisional.mjs")], {
          stdio: "inherit", timeout: 180_000,
          env: { ...process.env, SQLITE_DB_PATH: stagedPath },
        });
      } catch (error) {
        console.warn(`::warning::BoleHWeb no disponible; se valida la DB oficial. ${error.message}`);
      }
    }
    db = new Database(stagedPath);
    const after = validateSnapshot(getSnapshot(db), { now });
    if (before.fecha && (after.fecha < before.fecha || after.embalses < before.embalses * 0.9)) {
      throw new Error(`La publicacion pierde fecha o cobertura: ${JSON.stringify({ before, after })}`);
    }
    if (db.pragma("quick_check", { simple: true }) !== "ok") throw new Error("SQLite quick_check fallo.");
    const checkpoint = db.pragma("wal_checkpoint(TRUNCATE)");
    if (checkpoint.some((row) => row.busy)) throw new Error("SQLite checkpoint ocupado.");
    db.close();
    db = null;
    // Git publishes a single, closed SQLite file. Never manually delete live WAL files.
    renameSync(stagedPath, dbPath);
    console.log(`[reservoirs] DB publicada: ${JSON.stringify(after)}`);
    return after;
  } finally {
    if (db) db.close();
    if (!resolve(workDir).startsWith(`${resolve(dataDir)}${sep}.miteco-`)) throw new Error("Directorio temporal fuera de data.");
    rmSync(workDir, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  syncData().catch((error) => { console.error(error); process.exitCode = 1; });
}
