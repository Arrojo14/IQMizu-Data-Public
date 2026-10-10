import Database from "better-sqlite3";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { getSnapshot, validateSnapshot } from "./database-snapshot.mjs";

const USER_AGENT = "IQMizu-Data-Public publication check (+https://github.com/Arrojo14/IQMizu-Data-Public)";

// The public check runs through Hostinger's CDN, which can answer with short
// 429/5xx bursts. Retry for about 8 minutes with backoff and log every failed
// attempt so a failing run says why.
export async function verifyWebsite(expected, { fetchImpl = fetch, attempts = 24, delayMs = 5000, maxDelayMs = 30_000, log = console.log } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    let retryAfterMs = 0;
    try {
      const response = await fetchImpl(`https://iqmizu.com/api/nacional/historico?publication=${Date.now()}`, {
        signal: AbortSignal.timeout(15_000), cache: "no-store", headers: { "user-agent": USER_AGENT },
      });
      if (!response.ok) {
        retryAfterMs = Math.min(Number(response.headers.get("retry-after")) * 1000 || 0, 120_000);
        const body = (await response.text().catch(() => "")).replace(/\s+/g, " ").slice(0, 160);
        throw new Error(`Website HTTP ${response.status}${body ? `: ${body}` : ""}`);
      }
      const rows = await response.json();
      const latest = Array.isArray(rows) ? rows.reduce((a, b) => !a || b.fecha > a.fecha ? b : a, null) : null;
      if (latest?.fecha !== expected.fecha ||
          !Number.isFinite(latest?.agua_actual_hm3) || !Number.isFinite(latest?.agua_total_hm3) ||
          Math.abs(latest.agua_actual_hm3 - expected.aguaActualHm3) > 0.01 ||
          Math.abs(latest.agua_total_hm3 - expected.aguaTotalHm3) > 0.01) {
        throw new Error(`Website does not match published database: ${JSON.stringify(latest)} (expected ${JSON.stringify(expected)})`);
      }
      log(`[website] Public API verified: ${JSON.stringify(latest)}`);
      return latest;
    } catch (error) {
      lastError = error;
      log(`[website] Attempt ${attempt}/${attempts} failed: ${error.message}`);
      if (attempt < attempts) await sleep(Math.max(retryAfterMs, Math.min(delayMs * 2 ** Math.floor((attempt - 1) / 4), maxDelayMs)));
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
