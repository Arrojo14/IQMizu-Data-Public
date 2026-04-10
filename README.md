# Embalses Data Public

Repo minimo para mantener publicados los datos persistidos que usa IQMizu para embalses y lluvia historica.

Incluye solo:

- `cuencas`
- `embalses`
- `datos_semanales`
- `data/cache/aemet-recent-climate-30.json`
- `data/cache/aemet-monthly-*.json`
- un cron de GitHub Actions listo para ejecutarse cada dia

No incluye:

- el frontend de la web
- secretos
- tablas auxiliares no esenciales

## Que hace

El flujo descarga el ZIP oficial de MITECO, localiza el `.mdb`, y:

- crea `data/embalses.db` desde cero si no existe
- o la actualiza incrementalmente si ya existe
- refresca el historico diario reciente de lluvia de AEMET para las estaciones activas
- recalcula el acumulado mensual de AEMET a partir del historico diario

La base generada contiene solo estas tablas:

- `cuencas(id, nombre)`
- `embalses(id, nombre, cuenca_id, capacidad_hm3, electrico)`
- `datos_semanales(id, embalse_id, fecha, agua_actual_hm3, agua_total_hm3)`

Los artefactos de AEMET se publican como JSON para que la app pueda servir:

- grafica diaria: `data/cache/aemet-recent-climate-30.json`
- grafica mensual: `data/cache/aemet-monthly-*.json`
## Notas

- La primera ejecucion puede tardar bastante mas que las siguientes porque regenera la DB y la cache historica de AEMET.
- GitHub puede avisar si `data/embalses.db` supera `50 MB`.
- El origen de datos es MITECO y puede cambiar nombre/ruta del archivo con el tiempo.
- El workflow hace commit tanto de `data/embalses.db` como de `data/cache/` cuando detecta cambios reales.
