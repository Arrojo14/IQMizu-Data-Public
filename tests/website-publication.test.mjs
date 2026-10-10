import test from "node:test";
import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { createSchema, importOfficialRows } from "../scripts/sync-data.mjs";
import { correctRegions, describeDatabase, importWebsiteDatabase, parsePublishCommand, publishWebsiteData, REGIONAL_CORRECTIONS } from "../scripts/publish-website-data.mjs";
import { verifyWebsite } from "../scripts/verify-website.mjs";

const now = new Date("2026-09-06T12:00:00Z");
function week(fecha, water = 50) {
  return Array.from({ length: 374 }, (_, index) => ({
    FECHA: fecha, AMBITO_NOMBRE: "Cuenca", EMBALSE_NOMBRE: `Embalse ${index}`,
    AGUA_ACTUAL: water, AGUA_TOTAL: 100, ELECTRICO_FLAG: 0,
  }));
}
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "iqmizu-publication-test-"));
  t.after(() => {
    assert.ok(resolve(root).startsWith(`${resolve(tmpdir())}${sep}iqmizu-publication-test-`));
    rmSync(root, { recursive: true, force: true });
  });
  mkdirSync(join(root, "data"));
  const source = join(root, "incoming.db");
  const target = join(root, "data", "embalses.db");
  for (const path of [source, target]) {
    const db = new Database(path);
    createSchema(db);
    importOfficialRows(db, week("2026-08-11"), "old");
    if (path === source) importOfficialRows(db, [...week("2026-08-11", 51), ...week("2026-09-01", 45)], "new");
    else {
      db.exec("ALTER TABLE embalses ADD COLUMN latitud REAL; UPDATE embalses SET latitud = 41;");
      db.pragma("journal_mode = WAL");
    }
    db.close();
  }
  return { root, source, target };
}
function describe(path) {
  const db = new Database(path, { readonly: true });
  try { return describeDatabase(db); } finally { db.close(); }
}

test("publication keeps live readers, IDs and metadata while applying missing weeks and corrections", async (t) => {
  const { source, target, root } = fixture(t);
  const reader = new Database(target, { readonly: true });
  try {
    const inode = statSync(target).ino;
    reader.exec("BEGIN");
    assert.equal(describeDatabase(reader).fecha, "2026-08-11");
    const backupPath = join(root, "backups", "previous.db");
    const result = await importWebsiteDatabase(source, target, { backupPath, now });
    assert.equal(result.after.fecha, "2026-09-01");
    assert.equal(describeDatabase(reader).fecha, "2026-08-11", "active readers keep a consistent snapshot");
    reader.exec("COMMIT");
    assert.equal(describeDatabase(reader).fecha, "2026-09-01");
    assert.equal(statSync(target).ino, inode, "never replace the live database inode");
    assert.equal(reader.prepare("SELECT COUNT(*) AS n FROM embalses WHERE latitud = 41").get().n, 374);
    assert.equal(reader.prepare("SELECT COUNT(*) AS n FROM datos_semanales WHERE fecha = '2026-08-11' AND agua_actual_hm3 = 51").get().n, 374);
    assert.equal(describe(backupPath).fecha, "2026-08-11");
    assert.equal((await importWebsiteDatabase(source, target, { now })).changes, 0);
  } finally { reader.close(); }
});

test("publication refuses mismatched reservoir IDs before changing the live database", async (t) => {
  const { source, target } = fixture(t);
  const db = new Database(source);
  db.exec("UPDATE embalses SET nombre = 'Wrong reservoir' WHERE id = 1");
  db.close();
  const before = describe(target);
  await assert.rejects(importWebsiteDatabase(source, target, { now }), /IDs differ/);
  assert.deepEqual(describe(target), before);
});

test("an import failure rolls back corrections as well as newly inserted weeks", async (t) => {
  const { source, target } = fixture(t);
  const db = new Database(target);
  db.exec(`CREATE TRIGGER reject_new_week BEFORE INSERT ON datos_semanales
    WHEN NEW.fecha = '2026-09-01' BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END;`);
  db.close();
  const before = describe(target);
  await assert.rejects(importWebsiteDatabase(source, target, { now }), /simulated write failure/);
  assert.deepEqual(describe(target), before);
  const check = new Database(target, { readonly: true });
  assert.equal(check.prepare("SELECT COUNT(*) AS n FROM datos_semanales WHERE agua_actual_hm3 = 50").get().n, 374);
  check.close();
});

test("the restricted SSH receiver rejects shell commands and accepts only exact hashes", () => {
  const command = `publish ${"a".repeat(40)} ${"b".repeat(64)}`;
  assert.deepEqual(parsePublishCommand(command), { kind: "publish", commit: "a".repeat(40), hash: "b".repeat(64) });
  for (const invalid of ["ls", `${command}; pwd`, `${command}\n`, `publish main ${"b".repeat(64)}`, ""]) {
    assert.throws(() => parsePublishCommand(invalid), /Only/);
  }
});

test("a successful receipt skips future downloads and restarts but still rejects stale data", async (t) => {
  const { source, root, target } = fixture(t);
  const buffer = readFileSync(source);
  const hash = createHash("sha256").update(buffer).digest("hex");
  const command = `publish ${"a".repeat(40)} ${hash}`;
  let downloads = 0;
  const fetchImpl = async () => { downloads++; return new Response(buffer); };
  await publishWebsiteData(command, { root, fetchImpl, now });
  const restartTime = statSync(join(root, "tmp", "restart.txt")).mtimeMs;
  assert.equal((await publishWebsiteData(command, { root, fetchImpl, now })).changes, 0);
  assert.equal(downloads, 1);
  assert.equal(statSync(join(root, "tmp", "restart.txt")).mtimeMs, restartTime);
  assert.equal(describe(target).fecha, "2026-09-01");
  await assert.rejects(publishWebsiteData(command, { root, fetchImpl, now: new Date("2026-10-01") }), /desactualizada/);
});

test("delivery retries the restart after a committed import even when the database is unchanged", async (t) => {
  const { source, root, target } = fixture(t);
  const buffer = readFileSync(source);
  const hash = createHash("sha256").update(buffer).digest("hex");
  const command = `publish ${"a".repeat(40)} ${hash}`;
  const options = { root, now, fetchImpl: async () => new Response(buffer) };
  // Simulate a filesystem failure after SQLite commits but before restart/receipt.
  writeFileSync(join(root, "tmp"), "restart directory unavailable");
  await assert.rejects(publishWebsiteData(command, options), /EEXIST|ENOTDIR/);
  assert.equal(describe(target).fecha, "2026-09-01");
  assert.equal(existsSync(join(root, "data", "last-github-publication.json")), false);
  unlinkSync(join(root, "tmp"));
  assert.equal((await publishWebsiteData(command, options)).changes, 0);
  assert.ok(existsSync(join(root, "tmp", "restart.txt")));
  assert.ok(existsSync(join(root, "data", "last-github-publication.json")));
});

test("publication keeps database, backup and receipt outside the replaceable app directory", async (t) => {
  const { source, root, target } = fixture(t);
  const appRoot = join(root, "release");
  mkdirSync(appRoot);
  const buffer = readFileSync(source);
  const hash = createHash("sha256").update(buffer).digest("hex");
  const options = { root: appRoot, dbPath: target, now, fetchImpl: async () => new Response(buffer) };
  const command = `publish ${"a".repeat(40)} ${hash}`;
  await publishWebsiteData(command, options);
  assert.equal(describe(target).fecha, "2026-09-01");
  assert.equal(describe(join(root, "data", "backups", "before-github-publication.db")).fecha, "2026-08-11");
  assert.ok(existsSync(join(root, "data", "last-github-publication.json")));
  assert.ok(existsSync(join(appRoot, "tmp", "restart.txt")));
  assert.equal(existsSync(join(appRoot, "data")), false);
  assert.equal((await publishWebsiteData(command, options)).changes, 0);
});

test("invalid or stale incoming data preserves the website and its metadata", async (t) => {
  const { source, target } = fixture(t);
  const before = describe(target);
  await assert.rejects(importWebsiteDatabase(source, target, { now: new Date("2026-10-01") }), /desactualizada/);
  const incoming = new Database(source);
  incoming.exec("UPDATE datos_semanales SET agua_actual_hm3 = -1 WHERE fecha = '2026-09-01'");
  incoming.close();
  await assert.rejects(importWebsiteDatabase(source, target, { now }), /no plausible/);
  assert.deepEqual(describe(target), before);
  const live = new Database(target, { readonly: true });
  try { assert.equal(live.prepare("SELECT COUNT(*) AS n FROM embalses WHERE latitud = 41").get().n, 374); }
  finally { live.close(); }
});

test("public verification waits for cache refresh and fails when the site stays stale", async () => {
  const expected = { fecha: "2026-09-01", aguaActualHm3: 35875, aguaTotalHm3: 56043 };
  let attempts = 0;
  const fetchImpl = async () => {
    attempts++;
    return Response.json([{ fecha: attempts === 1 ? "2026-08-11" : "2026-09-01", agua_actual_hm3: 35875, agua_total_hm3: 56043 }]);
  };
  assert.equal((await verifyWebsite(expected, { fetchImpl, attempts: 2, delayMs: 1 })).fecha, expected.fecha);
  await assert.rejects(verifyWebsite(expected, { fetchImpl: async () => Response.json([]), attempts: 1 }), /does not match/);
});

test("public verification retries CDN errors, honours retry-after and logs each failure", async () => {
  const expected = { fecha: "2026-09-01", aguaActualHm3: 35875, aguaTotalHm3: 56043 };
  const responses = [
    new Response("Too many requests", { status: 429, headers: { "retry-after": "0" } }),
    new Response("<html>Service Unavailable</html>", { status: 503 }),
    Response.json([{ fecha: "2026-09-01", agua_actual_hm3: 35875, agua_total_hm3: 56043 }]),
  ];
  const lines = [];
  const latest = await verifyWebsite(expected, { fetchImpl: async () => responses.shift(), delayMs: 1, maxDelayMs: 1, log: (line) => lines.push(line) });
  assert.equal(latest.fecha, expected.fecha);
  assert.match(lines[0], /attempt 1\/24 failed: Website HTTP 429: Too many requests/);
  assert.match(lines[1], /HTTP 503: <html>Service Unavailable/);
  assert.match(lines[2], /verified/);
});

test("regional corrections are applied once, only to rows whose name still matches", () => {
  const db = new Database(":memory:");
  try {
    db.exec("CREATE TABLE embalses (id INTEGER PRIMARY KEY, nombre TEXT, provincia TEXT, comunidad TEXT)");
    const insert = db.prepare("INSERT INTO embalses VALUES (?, ?, 'Wrong', 'Wrong')");
    for (const { id, nombre } of REGIONAL_CORRECTIONS) insert.run(id, id === 324 ? "Renamed" : nombre);
    assert.equal(correctRegions(db), REGIONAL_CORRECTIONS.length - 1);
    assert.equal(correctRegions(db), 0, "idempotent");
    assert.deepEqual(db.prepare("SELECT provincia, comunidad FROM embalses WHERE id = 6").get(), { provincia: "Ourense", comunidad: "Galicia" });
    assert.equal(db.prepare("SELECT provincia FROM embalses WHERE id = 324").get().provincia, "Wrong");
  } finally { db.close(); }
  const bare = new Database(":memory:");
  try {
    bare.exec("CREATE TABLE embalses (id INTEGER PRIMARY KEY, nombre TEXT)");
    assert.equal(correctRegions(bare), 0, "databases without regional columns are left alone");
  } finally { bare.close(); }
});

test("an unchanged publication still applies new regional corrections and requests a restart", async (t) => {
  const { source, root, target } = fixture(t);
  const buffer = readFileSync(source);
  const hash = createHash("sha256").update(buffer).digest("hex");
  const command = `publish ${"b".repeat(40)} ${hash}`;
  await publishWebsiteData(command, { root, fetchImpl: async () => new Response(buffer), now });
  const db = new Database(target);
  db.exec("ALTER TABLE embalses ADD COLUMN provincia TEXT; ALTER TABLE embalses ADD COLUMN comunidad TEXT;");
  db.prepare("UPDATE embalses SET nombre = 'Castrelo', provincia = 'A Coruña', comunidad = 'Galicia' WHERE id = 6").run();
  db.close();
  const fetchImpl = async () => { throw new Error("must not download"); };
  assert.equal((await publishWebsiteData(command, { root, fetchImpl, now })).changes, 1);
  const check = new Database(target, { readonly: true });
  try { assert.equal(check.prepare("SELECT provincia FROM embalses WHERE id = 6").get().provincia, "Ourense"); } finally { check.close(); }
  const receipt = JSON.parse(readFileSync(join(root, "data", "last-github-publication.json"), "utf8"));
  assert.equal(receipt.changes, 1);
  assert.equal((await publishWebsiteData(command, { root, fetchImpl, now })).changes, 0);
});
