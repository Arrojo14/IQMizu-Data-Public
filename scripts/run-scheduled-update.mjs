import { closeSync, existsSync, mkdirSync, openSync, statSync, unlinkSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { execFileSync } from "node:child_process";

const LOCK_PATH = resolve(".update-data.lock");
const STALE_LOCK_MS = Number(process.env.UPDATE_LOCK_STALE_MS || 6 * 60 * 60 * 1000);

function acquireLock() {
  mkdirSync(dirname(LOCK_PATH), { recursive: true });

  if (existsSync(LOCK_PATH)) {
    const ageMs = Date.now() - statSync(LOCK_PATH).mtimeMs;
    if (ageMs > STALE_LOCK_MS) {
      console.warn(
        `Aviso: lock antiguo detectado (${Math.round(ageMs / 60000)} min). Eliminando ${LOCK_PATH}.`
      );
      unlinkSync(LOCK_PATH);
    }
  }

  try {
    const fd = openSync(LOCK_PATH, "wx");
    closeSync(fd);
    return true;
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "EEXIST") {
      console.log(`Ya hay una actualizacion en curso. Se omite esta ejecucion: ${LOCK_PATH}`);
      return false;
    }
    throw error;
  }
}

function releaseLock() {
  if (existsSync(LOCK_PATH)) {
    unlinkSync(LOCK_PATH);
  }
}

function main() {
  if (!Number.isFinite(STALE_LOCK_MS) || STALE_LOCK_MS <= 0) {
    throw new Error("UPDATE_LOCK_STALE_MS no es valido.");
  }

  if (!acquireLock()) {
    return;
  }

  console.log(`[scheduled-update] Inicio: ${new Date().toISOString()}`);

  try {
    execFileSync(process.execPath, [resolve("scripts/sync-data.mjs")], {
      stdio: "inherit",
      env: process.env,
    });
    console.log(`[scheduled-update] Fin OK: ${new Date().toISOString()}`);
  } finally {
    releaseLock();
  }
}

main();
