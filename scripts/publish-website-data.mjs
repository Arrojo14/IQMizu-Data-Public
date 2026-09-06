// Installed on Hostinger. The dedicated SSH key can invoke only this receiver.
import Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { getSnapshot, validateSnapshot } from "./database-snapshot.mjs";
import { decodeWeatherBundle, installWeatherFiles, MAX_BUNDLE_BYTES } from "./weather-publication.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function parsePublishCommand(command) {
  const match = /^(publish|publish-weather) ([a-f0-9]{40}) ([a-f0-9]{64})$/.exec(command || "");
  if (!match) throw new Error("Only publish/publish-weather with exact commit and artifact hashes are allowed.");
  return { kind: match[1], commit: match[2], hash: match[3] };
}

export function describeDatabase(db) {
  return { ...getSnapshot(db), ...db.prepare("SELECT COUNT(*) AS historyRows, MIN(fecha) AS earliest FROM datos_semanales").get() };
}

export function snapshotsMatch(a, b) {
  return a?.fecha === b?.fecha && a?.earliest === b?.earliest &&
    ["filas", "embalses", "historyRows", "aguaActualHm3", "aguaTotalHm3", "invalidas"].every((key) =>
      Number.isFinite(a?.[key]) && Number.isFinite(b?.[key]) && Math.abs(a[key] - b[key]) < 0.01);
}

export async function importWebsiteDatabase(sourcePath, dbPath, { backupPath, now = new Date() } = {}) {
  const incoming = new Database(sourcePath, { readonly: true });
  let live;
  try {
    if (incoming.pragma("quick_check", { simple: true }) !== "ok") throw new Error("Incoming SQLite integrity failure.");
    const expected = describeDatabase(incoming);
    validateSnapshot(expected, { now });
    live = new Database(dbPath, { fileMustExist: true });
    live.pragma("busy_timeout = 30000");
    live.pragma("foreign_keys = ON");
    const before = describeDatabase(live);
    if (expected.fecha < before.fecha || expected.historyRows < before.historyRows || expected.earliest > before.earliest) {
      throw new Error("Incoming database would lose dates or history.");
    }
    live.prepare("ATTACH DATABASE ? AS incoming").run(sourcePath);
    const mismatchedBasin = live.prepare(`SELECT c.id FROM cuencas c JOIN incoming.cuencas s ON c.id = s.id
      WHERE c.nombre IS NOT s.nombre LIMIT 1`).get();
    const mismatchedReservoir = live.prepare(`SELECT e.id FROM embalses e JOIN incoming.embalses s ON e.id = s.id
      WHERE e.nombre IS NOT s.nombre OR e.cuenca_id IS NOT s.cuenca_id LIMIT 1`).get();
    if (mismatchedBasin || mismatchedReservoir) throw new Error("Reservoir/basin IDs differ; refusing to break website metadata.");
    if (backupPath) {
      mkdirSync(dirname(backupPath), { recursive: true });
      await live.backup(`${backupPath}.tmp`);
      renameSync(`${backupPath}.tmp`, backupPath);
    }
    // Keep the live inode and WAL: existing website readers stay valid throughout the transaction.
    live.pragma("journal_mode = WAL");
    const result = live.transaction(() => {
      const changesBefore = live.prepare("SELECT total_changes() AS n").get().n;
      live.exec(`
        INSERT INTO cuencas(id, nombre) SELECT id, nombre FROM incoming.cuencas WHERE true
          ON CONFLICT(id) DO NOTHING;
        INSERT INTO embalses(id, nombre, cuenca_id, capacidad_hm3, electrico)
          SELECT id, nombre, cuenca_id, capacidad_hm3, electrico FROM incoming.embalses WHERE true
          ON CONFLICT(id) DO UPDATE SET capacidad_hm3 = excluded.capacidad_hm3, electrico = excluded.electrico
          WHERE embalses.capacidad_hm3 IS NOT excluded.capacidad_hm3 OR embalses.electrico IS NOT excluded.electrico;
        UPDATE datos_semanales AS target
          SET agua_actual_hm3 = source.agua_actual_hm3, agua_total_hm3 = source.agua_total_hm3
          FROM incoming.datos_semanales AS source
          WHERE target.embalse_id = source.embalse_id AND target.fecha = source.fecha
            AND (target.agua_actual_hm3 IS NOT source.agua_actual_hm3 OR target.agua_total_hm3 IS NOT source.agua_total_hm3);
        INSERT INTO datos_semanales(embalse_id, fecha, agua_actual_hm3, agua_total_hm3)
          SELECT source.embalse_id, source.fecha, source.agua_actual_hm3, source.agua_total_hm3
          FROM incoming.datos_semanales AS source
          WHERE NOT EXISTS (SELECT 1 FROM datos_semanales target
            WHERE target.embalse_id = source.embalse_id AND target.fecha = source.fecha);
      `);
      const after = describeDatabase(live);
      if (!snapshotsMatch(after, expected)) throw new Error("Imported database differs from the validated source; transaction rolled back.");
      return { before, after, changes: live.prepare("SELECT total_changes() AS n").get().n - changesBefore };
    }).immediate();
    live.pragma("wal_checkpoint(PASSIVE)");
    return result;
  } finally {
    live?.close();
    incoming.close();
  }
}

export async function publishWebsiteData(command, { root = ROOT, dbPath = resolve(root, process.env.SQLITE_DB_PATH?.trim() || "data/embalses.db"),
  fetchImpl = fetch, now = new Date(), weatherInput = process.stdin } = {}) {
  const { kind, commit, hash } = parsePublishCommand(command);
  const dataDir = dirname(dbPath);
  if (kind === "publish-weather") {
    const chunks = [];
    let bytes = 0;
    const timeout = setTimeout(() => weatherInput.destroy(new Error("Weather input timeout.")), 90_000);
    try {
      for await (const chunk of weatherInput) {
        bytes += chunk.length;
        if (bytes > MAX_BUNDLE_BYTES) throw new Error("Weather input too large.");
        chunks.push(chunk);
      }
    } finally { clearTimeout(timeout); }
    const files = decodeWeatherBundle(Buffer.concat(chunks), hash, { now });
    const statusPath = join(dataDir, "last-weather-publication.json");
    let last;
    try { last = JSON.parse(readFileSync(statusPath, "utf8")); } catch { /* First publication. */ }
    const result = installWeatherFiles(files, join(dataDir, "cache"), { now });
    if (last?.hash === hash && result.changes === 0) return result;
    completePublication(root, statusPath, { commit, hash, ...result });
    console.log(`[weather] PUBLISHED ${JSON.stringify(result)}`);
    return result;
  }
  const statusPath = join(dataDir, "last-github-publication.json");
  let last;
  try { last = JSON.parse(readFileSync(statusPath, "utf8")); } catch { /* First publication. */ }
  const live = new Database(dbPath, { readonly: true, fileMustExist: true });
  let current;
  try { current = describeDatabase(live); } finally { live.close(); }
  if (last?.hash === hash && snapshotsMatch(last.after, current)) {
    validateSnapshot(current, { now });
    console.log(`[website] UNCHANGED ${JSON.stringify(current)}`);
    return { after: current, changes: 0 };
  }

  const work = mkdtempSync(join(dataDir, ".github-publication-"));
  try {
    const url = `https://raw.githubusercontent.com/Arrojo14/IQMizu-Data-Public/${commit}/data/embalses.db`;
    let buffer;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await fetchImpl(url, { signal: AbortSignal.timeout(90_000) });
        if (!response.ok) throw new Error(`GitHub database HTTP ${response.status}`);
        buffer = Buffer.from(await response.arrayBuffer());
        if (createHash("sha256").update(buffer).digest("hex") !== hash) throw new Error("Database SHA-256 mismatch.");
        break;
      } catch (error) {
        if (attempt === 3) throw error;
        await sleep(attempt * 2000);
      }
    }
    const sourcePath = join(work, "incoming.db");
    writeFileSync(sourcePath, buffer);
    const result = await importWebsiteDatabase(sourcePath, dbPath, {
      backupPath: join(dataDir, "backups", "before-github-publication.db"), now,
    });
    // A previous attempt may have committed but failed before requesting restart.
    // Only a completed receipt (the fast path above) can safely skip this step.
    completePublication(root, statusPath, { commit, hash, ...result });
    console.log(`[website] PUBLISHED ${JSON.stringify(result)}`);
    return result;
  } finally {
    if (!resolve(work).startsWith(`${resolve(dataDir)}${sep}.github-publication-`)) throw new Error("Invalid temporary directory.");
    rmSync(work, { recursive: true, force: true });
  }
}

function completePublication(root, statusPath, result) {
  mkdirSync(join(root, "tmp"), { recursive: true });
  writeFileSync(join(root, "tmp", "restart.txt"), `${new Date().toISOString()}\n`);
  writeFileSync(`${statusPath}.tmp`, JSON.stringify({ ...result, completedAt: new Date().toISOString() }, null, 2));
  renameSync(`${statusPath}.tmp`, statusPath);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  publishWebsiteData(process.env.SSH_ORIGINAL_COMMAND || process.argv.slice(2).join(" "))
    .catch((error) => { console.error(`[website] ERROR: ${error.message}`); process.exitCode = 1; });
}
