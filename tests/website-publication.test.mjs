import test from "node:test";
import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { createSchema, importOfficialRows } from "../scripts/sync-data.mjs";
import { describeDatabase, importWebsiteDatabase, parsePublishCommand, publishWebsiteData } from "../scripts/publish-website-data.mjs";
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
  assert.deepEqual(parsePublishCommand(command), { commit: "a".repeat(40), hash: "b".repeat(64) });
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
