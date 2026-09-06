export function getSnapshot(db) {
  return db.prepare(`SELECT MAX(fecha) AS fecha, COUNT(*) AS filas,
    COUNT(DISTINCT embalse_id) AS embalses,
    SUM(agua_actual_hm3) AS aguaActualHm3, SUM(agua_total_hm3) AS aguaTotalHm3,
    SUM(CASE WHEN agua_actual_hm3 IS NULL OR agua_actual_hm3 < 0 OR agua_total_hm3 <= 0
      OR agua_total_hm3 IS NULL OR agua_actual_hm3 > agua_total_hm3 * 1.02 THEN 1 ELSE 0 END) AS invalidas
    FROM datos_semanales WHERE fecha = (SELECT MAX(fecha) FROM datos_semanales)`).get();
}

export function validateSnapshot(snapshot, { now = new Date(), minRows = 300, maxAgeDays = 14 } = {}) {
  const ageDays = (now - new Date(`${snapshot.fecha}T00:00:00Z`)) / 86_400_000;
  if (!Number.isFinite(ageDays) || ageDays < -1 || ageDays > maxAgeDays ||
      snapshot.embalses < minRows || snapshot.filas !== snapshot.embalses || snapshot.invalidas > 0 ||
      !(snapshot.aguaTotalHm3 > 0) || !(snapshot.aguaActualHm3 >= 0) ||
      snapshot.aguaActualHm3 > snapshot.aguaTotalHm3 * 1.02) {
    throw new Error(`DB incompleta, no plausible o desactualizada: ${JSON.stringify(snapshot)}`);
  }
  return snapshot;
}
