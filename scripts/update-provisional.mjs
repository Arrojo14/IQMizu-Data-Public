import Database from "better-sqlite3";
import https from "node:https";
import { existsSync, unlinkSync } from "node:fs";
import { resolve } from "node:path";

const DB_PATH = resolve("data/embalses.db");
const BOLEH_BASE_URL = "https://sede.miteco.gob.es/BoleHWeb";
const REQUEST_TIMEOUT_MS = Number(process.env.BOLEH_TIMEOUT_MS || 30_000);
const REQUESTED_DATE = process.env.BOLEH_DATE || formatDateForBoleh(new Date());
const ALLOW_INSECURE_TLS_FALLBACK = process.env.BOLEH_ALLOW_INSECURE_TLS !== "0";
const ALLOW_DATE_FALLBACK = process.env.BOLEH_ALLOW_DATE_FALLBACK === "1";

const HTML_ENTITY_MAP = new Map(
  Object.entries({
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    apos: "'",
    nbsp: " ",
    aacute: "\u00e1",
    eacute: "\u00e9",
    iacute: "\u00ed",
    oacute: "\u00f3",
    uacute: "\u00fa",
    Aacute: "\u00c1",
    Eacute: "\u00c9",
    Iacute: "\u00cd",
    Oacute: "\u00d3",
    Uacute: "\u00da",
    ntilde: "\u00f1",
    Ntilde: "\u00d1",
    ccedil: "\u00e7",
    Ccedil: "\u00c7",
    deg: "\u00b0",
    ordm: "\u00ba",
    ordf: "\u00aa",
  })
);

const MOJIBAKE_REPLACEMENTS = [
  ["\u00c3\u00a1", "\u00e1"],
  ["\u00c3\u00a9", "\u00e9"],
  ["\u00c3\u00ad", "\u00ed"],
  ["\u00c3\u00b3", "\u00f3"],
  ["\u00c3\u00ba", "\u00fa"],
  ["\u00c3\u00b1", "\u00f1"],
  ["\u00c3\u0081", "\u00c1"],
  ["\u00c3\u0089", "\u00c9"],
  ["\u00c3\u008d", "\u00cd"],
  ["\u00c3\u0093", "\u00d3"],
  ["\u00c3\u009a", "\u00da"],
  ["\u00c3\u0091", "\u00d1"],
  ["\u00c2\u00ba", "\u00ba"],
  ["\u00c2\u00aa", "\u00aa"],
  ["\u00c2", ""],
];

const MONTHS_ES = new Map([
  ["enero", 1],
  ["febrero", 2],
  ["marzo", 3],
  ["abril", 4],
  ["mayo", 5],
  ["junio", 6],
  ["julio", 7],
  ["agosto", 8],
  ["septiembre", 9],
  ["setiembre", 9],
  ["octubre", 10],
  ["noviembre", 11],
  ["diciembre", 12],
]);

let tlsFallbackWasUsed = false;

function formatDateForBoleh(date) {
  return `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`;
}

function fixMojibake(text) {
  let out = text;
  for (const [bad, good] of MOJIBAKE_REPLACEMENTS) {
    out = out.split(bad).join(good);
  }
  return out;
}

function decodeHtmlEntities(input) {
  return input.replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z]+);/g, (full, code) => {
    if (code.startsWith("#")) {
      const isHex = code[1]?.toLowerCase() === "x";
      const raw = isHex ? code.slice(2) : code.slice(1);
      const value = Number.parseInt(raw, isHex ? 16 : 10);
      return Number.isFinite(value) ? String.fromCodePoint(value) : full;
    }
    return HTML_ENTITY_MAP.get(code) ?? full;
  });
}

function stripHtml(html) {
  return decodeHtmlEntities(fixMojibake(html.replace(/<[^>]*>/g, " ")))
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeForMatch(value) {
  return decodeHtmlEntities(fixMojibake(value))
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[.,;:()'"`\u00b4]/g, " ")
    .replace(/-/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseDdMmYyyyToIso(value) {
  const match = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) return null;
  const day = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const year = Number.parseInt(match[3], 10);
  if (day < 1 || day > 31 || month < 1 || month > 12) return null;
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function parseSpanishLongDateToIso(value) {
  const normalized = normalizeForMatch(value);
  const match = normalized.match(/^(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})$/);
  if (!match) return null;
  const day = Number.parseInt(match[1], 10);
  const month = MONTHS_ES.get(match[2]);
  const year = Number.parseInt(match[3], 10);
  if (!month) return null;
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function parseSpanishNumber(value) {
  const cleaned = String(value ?? "")
    .replace(/\s+/g, "")
    .replace(/\./g, "")
    .replace(/,/g, ".")
    .replace(/[^0-9.-]/g, "");
  if (!cleaned || cleaned === "-" || cleaned === "." || cleaned === "-.") return null;
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function isTlsCertificateError(error) {
  return (
    error &&
    typeof error === "object" &&
    "code" in error &&
    [
      "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
      "SELF_SIGNED_CERT_IN_CHAIN",
      "DEPTH_ZERO_SELF_SIGNED_CERT",
      "CERT_HAS_EXPIRED",
      "ERR_TLS_CERT_ALTNAME_INVALID",
    ].includes(error.code)
  );
}

function fetchBolehRaw(path, { method = "GET", formData = null, rejectUnauthorized = true } = {}) {
  return new Promise((resolve, reject) => {
    const body = formData ? new URLSearchParams(formData).toString() : null;
    const headers = {
      "User-Agent": "Mozilla/5.0 (compatible; embalses-data-public-updater/1.0)",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      Connection: "keep-alive",
      ...(body
        ? {
            "Content-Type": "application/x-www-form-urlencoded",
            "Content-Length": Buffer.byteLength(body),
          }
        : {}),
    };

    const request = https.request(
      `${BOLEH_BASE_URL}${path}`,
      { method, headers, timeout: REQUEST_TIMEOUT_MS, rejectUnauthorized },
      (response) => {
        const chunks = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => {
          const status = response.statusCode ?? 0;
          if (status < 200 || status >= 300) {
            reject(new Error(`HTTP ${status} en ${path}`));
            return;
          }

          const contentType = Array.isArray(response.headers["content-type"])
            ? response.headers["content-type"][0]
            : response.headers["content-type"] || "";
          const latin1 = /charset\s*=\s*iso-8859-1/i.test(contentType);
          resolve(Buffer.concat(chunks).toString(latin1 ? "latin1" : "utf8"));
        });
      }
    );

    request.on("timeout", () => request.destroy(new Error(`Timeout al consultar ${path}`)));
    request.on("error", reject);
    if (body) request.write(body);
    request.end();
  });
}

async function fetchBoleh(path, options = {}) {
  try {
    return await fetchBolehRaw(path, { ...options, rejectUnauthorized: true });
  } catch (error) {
    if (!isTlsCertificateError(error) || !ALLOW_INSECURE_TLS_FALLBACK) {
      throw error;
    }

    if (!tlsFallbackWasUsed) {
      console.warn("Aviso: usando fallback TLS inseguro solo para sede.miteco.gob.es.");
      tlsFallbackWasUsed = true;
    }

    return fetchBolehRaw(path, { ...options, rejectUnauthorized: false });
  }
}

function extractSelectedCalendarDate(html) {
  const match = html.match(/name=['"]fechaCalendario['"][^>]*value=([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i);
  return match?.[1] ?? null;
}

function extractBulletinEndDateIso(html) {
  const text = stripHtml(html);
  const match = text.match(/hasta el\s+(\d{1,2}\s+de\s+[a-zA-Z\u00c0-\u017f]+\s+de\s+\d{4})/i);
  return match ? parseSpanishLongDateToIso(match[1]) : null;
}

function parseReserveMenu(html) {
  const xValMatch = html.match(/name=['"]xVal['"][^>]*value=([0-9]+)/i);
  if (!xValMatch) {
    throw new Error("No se pudo extraer xVal del menu de reserva.");
  }

  const buttons = [];
  const regex =
    /<button[^>]*id="(btnMod_Reserva_Hidraulica_Datos_[0-9]+)"[^>]*name="([^"]+)"[^>]*>([\s\S]*?)<\/button>/gi;
  for (const match of html.matchAll(regex)) {
    buttons.push({
      id: match[1],
      postName: match[2],
      cuencaLabel: stripHtml(match[3]),
    });
  }

  if (buttons.length === 0) {
    throw new Error("No se encontraron botones de cuencas en el menu de reserva.");
  }

  return { xVal: xValMatch[1], buttons };
}

function parseEmbalseRowsFromDetail(html) {
  const rows = [];

  for (const trMatch of html.matchAll(/<TR>([\s\S]*?)<\/TR>/gi)) {
    const tds = [...trMatch[1].matchAll(/<TD\b[^>]*>([\s\S]*?)<\/TD>/gi)].map((match) =>
      stripHtml(match[1])
    );

    if (tds.length !== 7) continue;

    const embalseNombre = tds[0].replace(/^\*\s*/, "").trim();
    if (!embalseNombre || /^embalses$/i.test(embalseNombre) || /^total$/i.test(embalseNombre)) {
      continue;
    }

    const aguaTotalHm3 = parseSpanishNumber(tds[2]);
    const aguaActualHm3 = parseSpanishNumber(tds[3]);
    if (aguaActualHm3 === null && aguaTotalHm3 === null) continue;

    rows.push({ embalseNombre, aguaActualHm3, aguaTotalHm3 });
  }

  return rows;
}

function buildNameVariants(name) {
  const clean = name.replace(/^\*\s*/, "").trim();
  const variants = new Set([clean]);

  const trailingArticle = clean.match(/^(.+),\s*(El|La|Los|Las)$/i);
  if (trailingArticle) variants.add(`${trailingArticle[2]} ${trailingArticle[1]}`.trim());

  const leadingArticle = clean.match(/^(El|La|Los|Las)\s+(.+)$/i);
  if (leadingArticle) variants.add(`${leadingArticle[2]}, ${leadingArticle[1]}`.trim());

  return [...variants];
}

function buildDbLookups(db) {
  const cuencas = db.prepare("SELECT id, nombre FROM cuencas").all();
  const embalses = db.prepare("SELECT id, nombre, cuenca_id FROM embalses").all();

  const cuencasByNormalized = new Map();
  for (const cuenca of cuencas) {
    const key = normalizeForMatch(cuenca.nombre);
    if (!cuencasByNormalized.has(key)) cuencasByNormalized.set(key, []);
    cuencasByNormalized.get(key).push(cuenca);
  }

  const embalsesByCuenca = new Map();
  for (const embalse of embalses) {
    const bucket = embalsesByCuenca.get(embalse.cuenca_id) ?? {
      exact: new Map(),
      normalized: new Map(),
    };

    bucket.exact.set(embalse.nombre.toLowerCase(), embalse.id);
    const normalized = normalizeForMatch(embalse.nombre);
    if (!bucket.normalized.has(normalized)) bucket.normalized.set(normalized, []);
    bucket.normalized.get(normalized).push(embalse.id);
    embalsesByCuenca.set(embalse.cuenca_id, bucket);
  }

  return { cuencasByNormalized, embalsesByCuenca };
}

function resolveCuencaId(label, cuencasByNormalized) {
  const matches = cuencasByNormalized.get(normalizeForMatch(label));
  return matches?.length === 1 ? matches[0].id : null;
}

function resolveEmbalseId(name, bucket) {
  for (const variant of buildNameVariants(name)) {
    const exact = bucket.exact.get(variant.toLowerCase());
    if (exact) return exact;
  }

  for (const variant of buildNameVariants(name)) {
    const matches = bucket.normalized.get(normalizeForMatch(variant));
    if (matches?.length === 1) return matches[0];
  }

  return null;
}

function sameNullableNumber(a, b) {
  if (a === null && b === null) return true;
  if (a === null || b === null) return false;
  return Math.abs(a - b) < 1e-9;
}

function cleanupDbSidecars() {
  for (const filePath of [`${DB_PATH}-wal`, `${DB_PATH}-shm`]) {
    try {
      if (existsSync(filePath)) unlinkSync(filePath);
    } catch {
      // Ignore sidecar cleanup errors.
    }
  }
}

async function main() {
  if (!Number.isFinite(REQUEST_TIMEOUT_MS) || REQUEST_TIMEOUT_MS <= 0) {
    throw new Error("BOLEH_TIMEOUT_MS no es valido.");
  }

  const db = new Database(DB_PATH);
  let shouldClose = true;

  try {
    console.log(`Consulta BoleHWeb para fecha solicitada: ${REQUESTED_DATE}`);

    const initialHtml = await fetchBoleh("/bolehSRV", {
      method: "POST",
      formData: { screen_language: "", date: REQUESTED_DATE },
    });

    const selectedCalendarDate = extractSelectedCalendarDate(initialHtml);
    if (!selectedCalendarDate) {
      throw new Error("No se pudo extraer fechaCalendario de la respuesta inicial.");
    }

    let bulletinDateIso = extractBulletinEndDateIso(initialHtml);
    if (!bulletinDateIso && ALLOW_DATE_FALLBACK) {
      bulletinDateIso = parseDdMmYyyyToIso(selectedCalendarDate);
      console.warn("Aviso: usando fechaCalendario por BOLEH_ALLOW_DATE_FALLBACK=1.");
    }
    if (!bulletinDateIso) {
      throw new Error(
        "No se pudo determinar la fecha efectiva del boletin. Para forzar fallback usa BOLEH_ALLOW_DATE_FALLBACK=1."
      );
    }

    const latestDbDate =
      db.prepare("SELECT MAX(fecha) AS fecha FROM datos_semanales").get()?.fecha ?? "1900-01-01";

    console.log(`Fecha seleccionada en formulario: ${selectedCalendarDate}`);
    console.log(`Fecha efectiva del boletin: ${bulletinDateIso}`);
    console.log(`Ultima fecha actual en DB: ${latestDbDate}`);

    if (bulletinDateIso < latestDbDate) {
      console.log("La fecha provisional es anterior a la DB. No se aplica actualizacion.");
      db.close();
      shouldClose = false;
      cleanupDbSidecars();
      return;
    }

    const reserveMenuHtml = await fetchBoleh("/bolehSRV", {
      method: "POST",
      formData: { fechaCalendario: selectedCalendarDate, btnMnuReserva: "btnMnuReserva" },
    });
    const { xVal, buttons } = parseReserveMenu(reserveMenuHtml);
    const { cuencasByNormalized, embalsesByCuenca } = buildDbLookups(db);

    const dataByEmbalseId = new Map();
    const unknownCuencas = new Set();
    const unknownEmbalses = [];
    let parsedRows = 0;

    for (const button of buttons) {
      const cuencaId = resolveCuencaId(button.cuencaLabel, cuencasByNormalized);
      if (!cuencaId) {
        unknownCuencas.add(button.cuencaLabel);
        continue;
      }

      const detailHtml = await fetchBoleh("/bolehSRV", {
        method: "POST",
        formData: { fechaCalendario: selectedCalendarDate, xVal, [button.postName]: button.id },
      });

      const rows = parseEmbalseRowsFromDetail(detailHtml);
      parsedRows += rows.length;
      const bucket = embalsesByCuenca.get(cuencaId);
      if (!bucket) {
        unknownCuencas.add(button.cuencaLabel);
        continue;
      }

      for (const row of rows) {
        const embalseId = resolveEmbalseId(row.embalseNombre, bucket);
        if (!embalseId) {
          unknownEmbalses.push({ cuenca: button.cuencaLabel, embalse: row.embalseNombre });
          continue;
        }

        dataByEmbalseId.set(embalseId, {
          embalseId,
          fecha: bulletinDateIso,
          aguaActualHm3: row.aguaActualHm3,
          aguaTotalHm3: row.aguaTotalHm3,
        });
      }
    }

    const rowsToUpsert = [...dataByEmbalseId.values()];
    if (rowsToUpsert.length === 0) {
      throw new Error("No se pudieron mapear datos provisionales a embalses locales.");
    }

    const selectExisting = db.prepare(
      `SELECT agua_actual_hm3 AS aguaActualHm3, agua_total_hm3 AS aguaTotalHm3
       FROM datos_semanales
       WHERE embalse_id = ? AND fecha = ?`
    );
    const insertDato = db.prepare(
      `INSERT INTO datos_semanales (embalse_id, fecha, agua_actual_hm3, agua_total_hm3)
       VALUES (?, ?, ?, ?)`
    );
    const updateDato = db.prepare(
      `UPDATE datos_semanales
       SET agua_actual_hm3 = ?, agua_total_hm3 = ?
       WHERE embalse_id = ? AND fecha = ?`
    );

    let inserted = 0;
    let updated = 0;
    let unchanged = 0;

    db.transaction((rows) => {
      for (const row of rows) {
        const current = selectExisting.get(row.embalseId, row.fecha);
        if (!current) {
          insertDato.run(row.embalseId, row.fecha, row.aguaActualHm3, row.aguaTotalHm3);
          inserted += 1;
          continue;
        }

        if (
          sameNullableNumber(current.aguaActualHm3, row.aguaActualHm3) &&
          sameNullableNumber(current.aguaTotalHm3, row.aguaTotalHm3)
        ) {
          unchanged += 1;
          continue;
        }

        updateDato.run(row.aguaActualHm3, row.aguaTotalHm3, row.embalseId, row.fecha);
        updated += 1;
      }
    })(rowsToUpsert);

    const latestAfter = db.prepare("SELECT MAX(fecha) AS fecha FROM datos_semanales").get()?.fecha;
    const totalsAfter = db
      .prepare(
        `SELECT COUNT(*) AS filas,
                COUNT(DISTINCT embalse_id) AS embalses,
                SUM(agua_actual_hm3) AS aguaActualHm3,
                SUM(agua_total_hm3) AS aguaTotalHm3
         FROM datos_semanales
         WHERE fecha = ?`
      )
      .get(bulletinDateIso);

    db.pragma("wal_checkpoint(TRUNCATE)");

    console.log("\n=== ACTUALIZACION PROVISIONAL COMPLETADA ===");
    console.log(`Filas parseadas en BoleH: ${parsedRows}`);
    console.log(`Filas mapeadas a DB: ${rowsToUpsert.length}`);
    console.log(`Insertadas: ${inserted}`);
    console.log(`Actualizadas: ${updated}`);
    console.log(`Sin cambios: ${unchanged}`);
    console.log(
      `Totales ${bulletinDateIso}: embalses=${totalsAfter.embalses}, agua_actual=${totalsAfter.aguaActualHm3}, capacidad=${totalsAfter.aguaTotalHm3}`
    );
    console.log(`Nueva fecha maxima en DB: ${latestAfter}`);

    if (unknownCuencas.size > 0) {
      console.warn(`Cuencas no reconocidas (${unknownCuencas.size}):`);
      for (const name of unknownCuencas) console.warn(`  - ${name}`);
    }

    if (unknownEmbalses.length > 0) {
      console.warn(`Embalses no reconocidos (${unknownEmbalses.length}):`);
      for (const item of unknownEmbalses.slice(0, 20)) {
        console.warn(`  - [${item.cuenca}] ${item.embalse}`);
      }
      if (unknownEmbalses.length > 20) console.warn(`  ... y ${unknownEmbalses.length - 20} mas`);
    }
  } finally {
    if (shouldClose) db.close();
    cleanupDbSidecars();
  }
}

main().catch((error) => {
  console.error("Error en actualizacion provisional:", error);
  process.exit(1);
});
