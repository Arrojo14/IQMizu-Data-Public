import Database from "better-sqlite3";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { getSnapshot, validateSnapshot } from "./database-snapshot.mjs";

export async function verifyWebsite(expected, { fetchImpl = fetch, attempts = 24, delayMs = 5000 } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const response = await fetchImpl(`https://iqmizu.com/api/nacional/historico?publication=${Date.now()}`, {
        signal: AbortSignal.timeout(10_000), cache: "no-store",
      });
      if (!response.ok) throw new Error(`Website HTTP ${response.status}`);
      const rows = await response.json();
      const latest = Array.isArray(rows) ? rows.reduce((a, b) => !a || b.fecha > a.fecha ? b : a, null) : null;
      if (latest?.fecha !== expected.fecha ||
          !Number.isFinite(latest?.agua_actual_hm3) || !Number.isFinite(latest?.agua_total_hm3) ||
          Math.abs(latest.agua_actual_hm3 - expected.aguaActualHm3) > 0.01 ||
          Math.abs(latest.agua_total_hm3 - expected.aguaTotalHm3) > 0.01) {
        throw new Error(`Website does not match published database: ${JSON.stringify(latest)}`);
      }
      console.log(`[website] Public API verified: ${JSON.stringify(latest)}`);
      return latest;
    } catch (error) {
      lastError = error;
      if (attempt < attempts) await sleep(delayMs);
    }
  }
  throw lastError;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const db = new Database(resolve("data/embalses.db"), { readonly: true });
  let expected;
  try { expected = validateSnapshot(getSnapshot(db)); } finally { db.close(); }
  verifyWebsite(expected).catch((error) => { console.error(error.message); process.exitCode = 1; });
}
