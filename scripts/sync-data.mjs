import MDBReader from "mdb-reader";
import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { execFileSync, spawnSync } from "node:child_process";
import { join, resolve } from "node:path";

const DB_PATH = resolve("data/embalses.db");
const DATA_DIR = resolve("data");
const ZIP_URL =
  "https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico/Historico-de-embalses/BD-Embalses.zip";
const ZIP_PATH = join(DATA_DIR, "BD-Embalses-update.zip");
const EXTRACT_DIR = join(DATA_DIR, "BD-Embalses-update");
const TABLE_NAME = "T_Datos Embalses 1988-2026";

async function downloadFile(url, dest) {
  console.log(`Descargando ${url}...`);
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`);
  }

  const buffer = Buffer.from(await res.arrayBuffer());
  writeFileSync(dest, buffer);
  console.log(`  Guardado: ${dest} (${(buffer.length / 1024 / 1024).toFixed(1)} MB)`);
}

function extractZip(zipPath, destDir) {
  console.log("Extrayendo ZIP...");
  mkdirSync(destDir, { recursive: true });

  if (process.platform === "win32") {
    execFileSync(
      "powershell",
      [
        "-NoProfile",
        "-Command",
        `Expand-Archive -Path '${zipPath.replace(/'/g, "''")}' -DestinationPath '${destDir.replace(/'/g, "''")}' -Force`,
      ],
      { stdio: "inherit" }
    );
    return;
  }

  const hasCommand = (command, args = ["--version"]) => {
    const result = spawnSync(command, args, { stdio: "ignore" });
    return result.status === 0;
  };

  if (hasCommand("unzip", ["-v"])) {
    execFileSync("unzip", ["-o", zipPath, "-d", destDir], { stdio: "inherit" });
    return;
  }

  if (hasCommand("python3")) {
    execFileSync(
      "python3",
      [
        "-c",
        "import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])",
        zipPath,
        destDir,
      ],
      { stdio: "inherit" }
    );
    return;
  }

  throw new Error("No se encontro una herramienta para extraer ZIP. Instala 'unzip' o 'python3'.");
}

function findFirstMdb(dir) {
  const entries = readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      const nested = findFirstMdb(fullPath);
      if (nested) return nested;
    } else if (entry.name.toLowerCase().endsWith(".mdb")) {
      return fullPath;
    }
  }
  return null;
}

function parseNumber(value) {
  const parsed = Number.parseFloat(String(value).replace(",", "."));
  return Number.isFinite(parsed) ? parsed : null;
}

function toIsoDate(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().split("T")[0];
}

function createSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS cuencas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nombre TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS embalses (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nombre TEXT NOT NULL,
      cuenca_id INTEGER NOT NULL,
      capacidad_hm3 REAL NOT NULL DEFAULT 0,
      electrico INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (cuenca_id) REFERENCES cuencas(id)
    );

    CREATE TABLE IF NOT EXISTS datos_semanales (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      embalse_id INTEGER NOT NULL,
      fecha TEXT NOT NULL,
      agua_actual_hm3 REAL,
      agua_total_hm3 REAL,
      FOREIGN KEY (embalse_id) REFERENCES embalses(id)
    );

    CREATE INDEX IF NOT EXISTS idx_datos_embalse ON datos_semanales(embalse_id);
    CREATE INDEX IF NOT EXISTS idx_datos_fecha ON datos_semanales(fecha);
    CREATE INDEX IF NOT EXISTS idx_datos_embalse_fecha ON datos_semanales(embalse_id, fecha);
    CREATE INDEX IF NOT EXISTS idx_embalses_nombre ON embalses(nombre);
    CREATE INDEX IF NOT EXISTS idx_embalses_cuenca ON embalses(cuenca_id);
  `);
}

function buildFreshDatabase(db, rows) {
  console.log("Creando DB desde cero...");
  createSchema(db);

  const cuencaSet = new Set();
  const embalseMap = new Map();

  for (const row of rows) {
    cuencaSet.add(row.AMBITO_NOMBRE);
    const nombre = row.EMBALSE_NOMBRE;
    const capacidad = parseNumber(row.AGUA_TOTAL) ?? 0;
    const electrico = row.ELECTRICO_FLAG === "1" || row.ELECTRICO_FLAG === 1;

    if (!embalseMap.has(nombre)) {
      embalseMap.set(nombre, {
        cuenca: row.AMBITO_NOMBRE,
        capacidad,
        electrico,
      });
    } else if (capacidad > 0) {
      embalseMap.get(nombre).capacidad = capacidad;
    }
  }

  const insertCuenca = db.prepare("INSERT INTO cuencas (nombre) VALUES (?)");
  const cuencaIdMap = new Map();
  for (const nombre of cuencaSet) {
    const result = insertCuenca.run(nombre);
    cuencaIdMap.set(nombre, result.lastInsertRowid);
  }

  const insertEmbalse = db.prepare(
    "INSERT INTO embalses (nombre, cuenca_id, capacidad_hm3, electrico) VALUES (?, ?, ?, ?)"
  );
  const embalseIdMap = new Map();
  for (const [nombre, info] of embalseMap) {
    const result = insertEmbalse.run(
      nombre,
      cuencaIdMap.get(info.cuenca),
      info.capacidad,
      info.electrico ? 1 : 0
    );
    embalseIdMap.set(nombre, result.lastInsertRowid);
  }

  const insertDato = db.prepare(
    "INSERT INTO datos_semanales (embalse_id, fecha, agua_actual_hm3, agua_total_hm3) VALUES (?, ?, ?, ?)"
  );

  const BATCH_SIZE = 10000;
  const insertBatch = db.transaction((batch) => {
    for (const row of batch) {
      const embalseId = embalseIdMap.get(row.EMBALSE_NOMBRE);
      const fecha = toIsoDate(row.FECHA);
      if (!embalseId || !fecha) continue;

      insertDato.run(
        embalseId,
        fecha,
        parseNumber(row.AGUA_ACTUAL),
        parseNumber(row.AGUA_TOTAL)
      );
    }
  });

  let batch = [];
  let processed = 0;
  for (const row of rows) {
    batch.push(row);
    if (batch.length >= BATCH_SIZE) {
      insertBatch(batch);
      processed += batch.length;
      process.stdout.write(`\r  Procesadas ${processed}/${rows.length} filas`);
      batch = [];
    }
  }
  if (batch.length > 0) {
    insertBatch(batch);
    processed += batch.length;
  }

  console.log(`\nDB inicial creada con ${processed} filas semanales.`);
}

function updateExistingDatabase(db, rows) {
  console.log("Actualizando DB existente...");
  createSchema(db);

  const latestRow = db.prepare("SELECT MAX(fecha) AS f FROM datos_semanales").get();
  const latestDate = latestRow?.f || "1900-01-01";
  console.log(`  Ultima fecha en DB: ${latestDate}`);

  const cuencaIds = new Map(
    db.prepare("SELECT id, nombre FROM cuencas").all().map((row) => [row.nombre, row.id])
  );
  const embalseIds = new Map(
    db.prepare("SELECT id, nombre FROM embalses").all().map((row) => [row.nombre, row.id])
  );

  const newRows = [];
  const newCuencas = new Set();
  const newEmbalses = new Set();

  for (const row of rows) {
    const fecha = toIsoDate(row.FECHA);
    if (!fecha || fecha <= latestDate) continue;

    if (!cuencaIds.has(row.AMBITO_NOMBRE)) newCuencas.add(row.AMBITO_NOMBRE);
    if (!embalseIds.has(row.EMBALSE_NOMBRE)) newEmbalses.add(row.EMBALSE_NOMBRE);
    newRows.push(row);
  }

  console.log(`  Filas nuevas: ${newRows.length}`);

  if (newRows.length === 0) {
    console.log("La DB ya estaba actualizada.");
    return;
  }

  if (newCuencas.size > 0) {
    const insertCuenca = db.prepare("INSERT INTO cuencas (nombre) VALUES (?)");
    for (const nombre of newCuencas) {
      const result = insertCuenca.run(nombre);
      cuencaIds.set(nombre, result.lastInsertRowid);
    }
  }

  if (newEmbalses.size > 0) {
    const insertEmbalse = db.prepare(
      "INSERT INTO embalses (nombre, cuenca_id, capacidad_hm3, electrico) VALUES (?, ?, ?, ?)"
    );
    const embalseInfo = new Map();

    for (const row of rows) {
      if (!newEmbalses.has(row.EMBALSE_NOMBRE)) continue;
      const capacidad = parseNumber(row.AGUA_TOTAL) ?? 0;
      if (!embalseInfo.has(row.EMBALSE_NOMBRE) || capacidad > 0) {
        embalseInfo.set(row.EMBALSE_NOMBRE, {
          cuenca: row.AMBITO_NOMBRE,
          capacidad,
          electrico: row.ELECTRICO_FLAG === "1" || row.ELECTRICO_FLAG === 1,
        });
      }
    }

    for (const [nombre, info] of embalseInfo) {
      const result = insertEmbalse.run(
        nombre,
        cuencaIds.get(info.cuenca),
        info.capacidad,
        info.electrico ? 1 : 0
      );
      embalseIds.set(nombre, result.lastInsertRowid);
    }
  }

  const insertDato = db.prepare(
    "INSERT INTO datos_semanales (embalse_id, fecha, agua_actual_hm3, agua_total_hm3) VALUES (?, ?, ?, ?)"
  );
  const insertBatch = db.transaction((batch) => {
    for (const row of batch) {
      const embalseId = embalseIds.get(row.EMBALSE_NOMBRE);
      const fecha = toIsoDate(row.FECHA);
      if (!embalseId || !fecha) continue;

      insertDato.run(
        embalseId,
        fecha,
        parseNumber(row.AGUA_ACTUAL),
        parseNumber(row.AGUA_TOTAL)
      );
    }
  });

  const BATCH_SIZE = 5000;
  let batch = [];
  let processed = 0;
  for (const row of newRows) {
    batch.push(row);
    if (batch.length >= BATCH_SIZE) {
      insertBatch(batch);
      processed += batch.length;
      process.stdout.write(`\r  Procesadas ${processed}/${newRows.length} filas nuevas`);
      batch = [];
    }
  }
  if (batch.length > 0) {
    insertBatch(batch);
    processed += batch.length;
  }

  console.log(`\nActualizacion completada con ${processed} filas nuevas.`);
}

function printSummary(db) {
  const latest = db.prepare("SELECT MAX(fecha) AS f FROM datos_semanales").get();
  const earliest = db.prepare("SELECT MIN(fecha) AS f FROM datos_semanales").get();
  const totals = db.prepare("SELECT COUNT(*) AS c FROM datos_semanales").get();
  const embalses = db.prepare("SELECT COUNT(*) AS c FROM embalses").get();
  const cuencas = db.prepare("SELECT COUNT(*) AS c FROM cuencas").get();

  console.log("\n=== RESUMEN DB ===");
  console.log(`Cuencas: ${cuencas.c}`);
  console.log(`Embalses: ${embalses.c}`);
  console.log(`Datos semanales: ${totals.c}`);
  console.log(`Rango fechas: ${earliest.f} -> ${latest.f}`);
  console.log(`DB: ${DB_PATH}`);
}

function cleanup() {
  try {
    if (existsSync(ZIP_PATH)) unlinkSync(ZIP_PATH);
    if (existsSync(EXTRACT_DIR)) {
      rmSync(EXTRACT_DIR, { recursive: true, force: true });
    }
  } catch {
    // ignore cleanup errors
  }
}

async function main() {
  mkdirSync(DATA_DIR, { recursive: true });

  let db;
  try {
    await downloadFile(ZIP_URL, ZIP_PATH);
    extractZip(ZIP_PATH, EXTRACT_DIR);

    const mdbPath = findFirstMdb(EXTRACT_DIR);
    if (!mdbPath) {
      throw new Error("No se encontro archivo .mdb en el ZIP descargado.");
    }

    console.log(`MDB encontrado: ${mdbPath}`);
    console.log(`Leyendo tabla ${TABLE_NAME}...`);

    const reader = new MDBReader(readFileSync(mdbPath));
    const table = reader.getTable(TABLE_NAME);
    const rows = table.getData();
    console.log(`  Filas leidas: ${rows.length}`);

    db = new Database(DB_PATH);
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = OFF");

    if (!existsSync(DB_PATH) || !db.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'datos_semanales'"
    ).get()) {
      buildFreshDatabase(db, rows);
    } else {
      updateExistingDatabase(db, rows);
    }

    printSummary(db);
  } finally {
    if (db) db.close();
    cleanup();
  }
}

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});
