import Database from "better-sqlite3";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const DB_PATH = resolve("data/embalses.db");

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

console.log(JSON.stringify({
  dbPath: DB_PATH,
  earliestDate: earliest.fecha,
  latestDate: latest.fecha,
  cuencas: cuencas.c,
  embalses: embalses.c,
  weeklyRows: totals.c,
}, null, 2));

db.close();
