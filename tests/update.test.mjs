import test from "node:test";
import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { createSchema, getSnapshot, getState, importOfficialRows, readOfficialRows, syncData, validateSnapshot, downloadArchive } from "../scripts/sync-data.mjs";
import { fetchAemetDataOnce, isRetryableAemetError } from "../scripts/refresh-aemet-cache.mjs";

const now = new Date("2026-09-06T12:00:00Z");
function week(fecha = "2026-09-01", count = 374, water = 50) {
  return Array.from({ length: count }, (_, index) => ({
    FECHA: fecha, AMBITO_NOMBRE: "Cuenca", EMBALSE_NOMBRE: `Embalse ${index}`,
    AGUA_ACTUAL: water, AGUA_TOTAL: 100, ELECTRICO_FLAG: 0,
  }));
}
function temporary(t) {
  const directory = mkdtempSync(join(tmpdir(), "iqmizu-test-"));
  t.after(() => {
    assert.ok(resolve(directory).startsWith(`${resolve(tmpdir())}${sep}iqmizu-test-`));
    rmSync(directory, { recursive: true, force: true });
  });
  return directory;
}
function memory(t) {
  const db = new Database(":memory:");
  t.after(() => db.close());
  createSchema(db);
  return db;
}

test("official corrections replace provisional values, preserve IDs and later weeks", (t) => {
  const db = memory(t);
  importOfficialRows(db, [...week("1987-10-06"), ...week()], "first");
  const id = db.prepare("SELECT id FROM embalses WHERE nombre = 'Embalse 0'").get().id;
  db.exec("UPDATE datos_semanales SET agua_actual_hm3 = 40, fuente = 'provisional' WHERE fecha = '2026-09-01'");
  db.prepare(`INSERT INTO datos_semanales(embalse_id, fecha, agua_actual_hm3, agua_total_hm3, fuente)
    SELECT id, '2026-09-08', 45, 100, 'provisional' FROM embalses`).run();
  importOfficialRows(db, week(), "corrected");
  assert.equal(db.prepare("SELECT id FROM embalses WHERE nombre = 'Embalse 0'").get().id, id);
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM datos_semanales WHERE fecha = '2026-09-01' AND fuente = 'oficial' AND agua_actual_hm3 = 50").get().n, 374);
  assert.equal(getSnapshot(db).fecha, "2026-09-08");
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM datos_semanales WHERE fecha = '1987-10-06'").get().n, 374);
  assert.equal(importOfficialRows(db, week(), "corrected"), 0);
});

test("same reservoir name in different basins remains distinct", (t) => {
  const db = memory(t);
  importOfficialRows(db, [week()[0], { ...week()[0], AMBITO_NOMBRE: "Otra cuenca" }], "hash", { minRows: 2 });
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM embalses").get().n, 2);
});

test("migration removes redundant indexes and remains idempotent", (t) => {
  const db = memory(t);
  importOfficialRows(db, week(), "hash");
  db.exec(`CREATE INDEX idx_datos_embalse ON datos_semanales(embalse_id);
    CREATE INDEX idx_datos_embalse_fecha ON datos_semanales(embalse_id, fecha);`);
  const before = getSnapshot(db);
  createSchema(db);
  const indexes = db.prepare("PRAGMA index_list(datos_semanales)").all().map((index) => index.name).sort();
  assert.deepEqual(indexes, ["idx_datos_embalse_fecha_unique", "idx_datos_fecha"]);
  assert.deepEqual(getSnapshot(db), before);
  const migrated = db.serialize();
  createSchema(db);
  assert.deepEqual(db.serialize(), migrated);
});

test("incomplete or implausible official weeks do not change rows or successful hash", (t) => {
  const db = memory(t);
  importOfficialRows(db, week(), "good");
  const before = db.serialize();
  for (const rows of [week("2026-09-08", 299), week("2026-09-08", 374, 200), [...week(), week()[0]]]) {
    assert.throws(() => importOfficialRows(db, rows, "bad"), /incompleta/);
    assert.deepEqual(db.serialize(), before);
  }
  assert.equal(getState(db, "miteco_archive_sha256"), "good");
});

test("MDB discovery survives a year change and rejects ambiguous tables", () => {
  const columns = Object.keys(week()[0]);
  const table = { getColumnNames: () => columns, getData: () => week() };
  assert.equal(readOfficialRows({ getTableNames: () => ["1988-2027"], getTable: () => table }).length, 374);
  assert.throws(() => readOfficialRows({ getTableNames: () => ["one", "two"], getTable: () => table }), /encontradas: 2/);
});

test("stale, future, incomplete and duplicate snapshots fail validation", (t) => {
  const db = memory(t);
  importOfficialRows(db, week(), "hash");
  const snapshot = getSnapshot(db);
  assert.equal(validateSnapshot(snapshot, { now }).fecha, "2026-09-01");
  for (const change of [{ fecha: "2026-08-01" }, { fecha: "2026-10-01" }, { embalses: 299 }, { filas: 375 }, { invalidas: 1 }]) {
    assert.throws(() => validateSnapshot({ ...snapshot, ...change }, { now }), /DB incompleta/);
  }
});

test("unchanged archive skips MDB parsing; failed publication preserves original file", async (t) => {
  const directory = temporary(t);
  const dbPath = join(directory, "embalses.db");
  let parses = 0;
  const options = { dbPath, now, provisional: false, download: async () => Buffer.from("fixture"),
    readRows: () => { parses++; return week(); } };
  await syncData(options);
  const published = readFileSync(dbPath);
  await syncData(options);
  assert.equal(parses, 1);
  assert.deepEqual(readFileSync(dbPath), published, "unchanged data must not create another binary Git commit");
  const before = readFileSync(dbPath);
  await assert.rejects(syncData({ ...options, now: new Date("2026-10-01") }), /desactualizada/);
  assert.deepEqual(readFileSync(dbPath), before);
  await assert.rejects(syncData({ ...options, download: async () => { throw new Error("network failed"); } }), /network failed/);
  assert.deepEqual(readFileSync(dbPath), before);
  await assert.rejects(syncData({ ...options, download: async () => Buffer.from("new"), readRows: () => week("2026-10-01") }), /desactualizada/);
  assert.deepEqual(readFileSync(dbPath), before);
  assert.deepEqual(readdirSync(directory), ["embalses.db"]);
});

test("download retries transient failures and rejects an HTML response", async () => {
  const zip = Buffer.alloc(100_000);
  zip.writeUInt32LE(0x04034b50);
  let calls = 0;
  assert.deepEqual(await downloadArchive("https://example.test", { retryDelayMs: 1, fetchImpl: async () => {
    calls++;
    return calls === 1 ? new Response("unavailable", { status: 503 }) : new Response(zip);
  } }), zip);
  assert.equal(calls, 2);
  await assert.rejects(downloadArchive("https://example.test", { attempts: 1, fetchImpl: async () => new Response("<html>error</html>") }), /ZIP valido/);
});

test("AEMET HTTP-200 error envelopes retain retryable status and reject credentials errors", async () => {
  for (const status of [429, 500, 503, 401]) {
    await assert.rejects(fetchAemetDataOnce("/test", { apiKey: "fixture", fetchImpl: async () =>
      Response.json({ estado: status, descripcion: "upstream failure" }) }), (error) => {
      assert.equal(isRetryableAemetError(error), status !== 401);
      return true;
    });
  }
  assert.ok(isRetryableAemetError(new Error("AEMET API error: 400: La fecha final no puede ser mayor que la fecha inicial")));
});

test("AEMET timeout remains active while reading the response body", async () => {
  await assert.rejects(fetchAemetDataOnce("/test", { apiKey: "fixture", timeoutMs: 10,
    fetchImpl: async (_url, { signal }) => ({ ok: true, json: async () => {
      await sleep(30);
      signal.throwIfAborted();
    } }),
  }), (error) => isRetryableAemetError(error));
});

test("live MITECO archive updates a copy and skips parsing on repeat", { skip: process.env.MITECO_LIVE_TEST !== "1", timeout: 600_000 }, async (t) => {
  const directory = temporary(t);
  const dbPath = join(directory, "embalses.db");
  const source = new Database(resolve("data/embalses.db"), { readonly: true });
  try { await source.backup(dbPath); } finally { source.close(); }
  const firstStart = performance.now();
  const first = await syncData({ dbPath });
  const firstMs = performance.now() - firstStart;
  const published = readFileSync(dbPath);
  const repeatStart = performance.now();
  const repeat = await syncData({ dbPath, readRows: () => { throw new Error("Unchanged archive was parsed again"); } });
  assert.deepEqual(repeat, first);
  assert.deepEqual(readFileSync(dbPath), published);
  console.log(JSON.stringify({ firstMs, repeatMs: performance.now() - repeatStart, bytes: published.length, snapshot: repeat }));
});
