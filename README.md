# Embalses Data Public

Repo minimo para mantener publicados los datos persistidos que usa IQMizu para embalses y lluvia historica.

Incluye solo:

- `cuencas`
- `embalses`
- `datos_semanales`
- `update_state` (huella del archivo oficial y ultima fecha oficial aplicada)
- `data/cache/aemet-recent-climate-30.json`
- `data/cache/aemet-monthly-*.json`
- actualizaciones de GitHub Actions a las 11:23 y 15:23 UTC

No incluye:

- el frontend de la web
- secretos

## Que hace

El job `reservoirs` descarga el ZIP oficial de MITECO y compara su SHA-256 con el ultimo archivo importado. Si no cambia, omite la extraccion y lectura del MDB (mas de 700.000 filas). Cuando cambia, descubre la tabla por sus columnas, sin depender del ano de su nombre, y:

- crea `data/embalses.db` desde cero si no existe
- o reconcilia los ultimos 180 dias y todas las semanas pendientes si ya existe, conservando los identificadores
- sustituye los valores provisionales por oficiales, incluso si ya existe esa fecha
- aplica la ultima fecha provisional disponible en BoleHWeb cuando el ZIP oficial va con retraso
- valida integridad, cobertura y antiguedad antes de reemplazar y publicar la DB

La actualizacion se prepara en una copia temporal. Un error de descarga, importacion o validacion conserva el archivo publicado. Una caida de BoleHWeb permite publicar los datos oficiales solo si la DB final sigue siendo valida y tiene como maximo 14 dias de antiguedad. Los datos incompletos o antiguos hacen fallar el job; no se confunde una ejecucion con datos frescos.

El job independiente `weather` refresca AEMET una vez al dia, en la primera ventana, y tambien en las ejecuciones manuales. Se ejecuta despues del job de embalses y publica su propio commit. Un secreto AEMET ausente, un timeout o un error de su API no bloquea la publicacion de embalses. El historico anual de lluvia se mantiene completo; no se reconstruye otra vez en la ventana de recuperacion de embalses.

La base generada contiene estas tablas:

- `cuencas(id, nombre)`
- `embalses(id, nombre, cuenca_id, capacidad_hm3, electrico)`
- `datos_semanales(id, embalse_id, fecha, agua_actual_hm3, agua_total_hm3, fuente)`
- `update_state(clave, valor)`

Los artefactos de AEMET se publican como JSON para que la app pueda servir:

- grafica diaria: `data/cache/aemet-recent-climate-30.json`
- grafica mensual: `data/cache/aemet-monthly-*.json`
## Notas

- La primera ejecucion tras esta migracion necesita leer el MDB. Las siguientes solo vuelven a leerlo cuando cambia el ZIP.
- GitHub puede avisar si `data/embalses.db` supera `50 MB`.
- El origen de datos es MITECO y puede cambiar nombre/ruta del archivo con el tiempo.
- El workflow hace commits independientes de `data/embalses.db` y `data/cache/` cuando detecta cambios reales. Un fallo de commit o push se muestra como error.
- GitHub serializa las ejecuciones con `concurrency`; se ha eliminado el wrapper con lock local. Estos scripts trabajan sobre el artefacto de este repositorio: no ejecutarlos simultaneamente ni sobre la SQLite abierta de la web.
- GitHub puede retrasar u omitir eventos programados. Las dos ventanas evitan el inicio de la hora y permiten recuperacion automatica, pero no garantizan una hora exacta. Ver [documentacion de schedule](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).
- El cron solo queda activo cuando el workflow esta en la rama predeterminada. Ver los jobs `reservoirs` y `weather` por separado en Actions. Las notificaciones de fallos dependen de la configuracion de notificaciones de GitHub de cada usuario.
- Publicar este repositorio no despliega la web de Hostinger: su DB se gestiona por separado.

## Comandos y comprobaciones

Requiere Node.js 22 o superior; el runner usa Node.js 22 y `npm ci`.

- `npm run data:update`: actualizar y validar embalses, sin depender de AEMET.
- `npm run data:update:scheduled`: alias compatible del mismo comando.
- `npm run data:update:aemet`: refrescar lluvia; requiere `AEMET_API_KEY`.
- `npm run data:summary`: consultar fechas, cobertura y huellas de los artefactos.
- `npm test`: pruebas aisladas sin red, incluidos errores de AEMET, correcciones oficiales y conservacion de la DB ante fallos.
- `MITECO_LIVE_TEST=1 node --test --test-name-pattern='live MITECO' tests/update.test.mjs`: prueba real con una copia temporal de la DB; no modifica el artefacto publicado. En PowerShell, establecer antes `$env:MITECO_LIVE_TEST='1'`.

Incidente de referencia: el [run del 2 de septiembre de 2026](https://github.com/Arrojo14/IQMizu-Data-Public/actions/runs/33665711547) completo MITECO y BoleHWeb, pero AEMET rechazo un rango historico valido y el workflow anterior omitio todo el commit. El fallo de AEMET y el aviso de runtime Node 20 de las acciones eran problemas distintos.
