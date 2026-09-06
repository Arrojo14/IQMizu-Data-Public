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
- En `main`, el job de embalses tambien publica y verifica la DB de Hostinger. Las ramas de prueba solo actualizan el repositorio.

## Publicacion en Hostinger

La proxima version de la app incluye el receptor de embalses y lluvia. Instalar primero esa version y configurar el directorio persistente antes de activar este workflow en `main`. El procedimiento de migracion esta en `docs/DEPLOY_HARDENING.md` de IQMizu; no se ha ejecutado como parte del refactor.

El receptor acepta solo dos comandos SSH exactos:

- `publish <commit SHA> <database SHA-256>` descarga la DB de ese commit, comprueba huella, integridad, antiguedad e identificadores e importa correcciones y semanas pendientes en una transaccion. Conserva IDs, metadatos locales, el archivo SQLite y sus lectores WAL. Un fallo revierte la transaccion.
- `publish-weather <commit SHA> <bundle SHA-256>` lee un JSON comprimido por stdin (maximo 10 MB comprimido / 32 MB expandido), verifica su huella y permite solo el cache reciente y los mensuales. Valida fechas, cobertura y valores antes de sustituir cada JSON atomicamente. Un rechazo conserva los archivos anteriores; un fallo de escritura parcial se completa al reintentar. Nunca modifica SQLite.

La DB, `cache/`, `backups/` y ambos recibos viven junto al `SQLITE_DB_PATH` absoluto, fuera del directorio reemplazable de la app. El backup SQLite previo a publicacion es `backups/before-github-publication.db`. Tras aplicar datos, el receptor solicita el reinicio de Passenger en `tmp/restart.txt` de la app y escribe `last-github-publication.json` o `last-weather-publication.json` en el directorio persistente.

La entrega no depende de crear un nuevo commit: ambos jobs reintentan artefactos sin cambios. Solo un recibo completado y datos coincidentes permiten omitir el reinicio. Si la transaccion termino pero fallo el reinicio, el siguiente intento vuelve a solicitarlo. El workflow verifica `/api/nacional/historico`, `/api/data-status` y tres respuestas mensuales de estaciones; una entrega o verificacion fallida hace fallar el job.

Configuracion para la proxima version:

1. Instalar juntos `scripts/publish-website-data.mjs`, `scripts/database-snapshot.mjs` y `scripts/weather-publication.mjs` en la app con Node.js 22 y su dependencia `better-sqlite3`. `npm run deploy:package` en IQMizu copia estos modulos canonicos; no existe otra implementacion del receptor.
2. Con las entregas pausadas y tras migrar los datos, configurar la entrada de la clave exclusiva de Actions en `authorized_keys` con `restrict` y este comando forzado (verificar primero las rutas reales):

   ```text
   restrict,command="/usr/bin/env SQLITE_DB_PATH=/home/u773681749/domains/iqmizu.com/iqmizu-data/embalses.db /usr/bin/flock -n -E 75 /home/u773681749/domains/iqmizu.com/iqmizu-data/.github-publication.lock /opt/alt/alt-nodejs22/root/usr/bin/node /home/u773681749/domains/iqmizu.com/nodejs/scripts/publish-website-data.mjs" <clave publica existente>
   ```

3. Conservar el secreto `HOSTINGER_PUBLISH_KEY` y `deploy/hostinger_known_hosts`; verificar cualquier cambio de clave del servidor antes de sustituirla. La clave de Actions no permite shell arbitraria, subida de codigo ni destinos elegidos por el cliente.
4. Configurar el mismo `SQLITE_DB_PATH` para Passenger, activar el workflow y comprobar ambas entregas. No instalar otro cron de datos en Hostinger.

AEMET sigue siendo independiente: una caida de su API no bloquea embalses. Su generador combina las respuestas con el ultimo historico valido y valida el conjunto antes de escribir; no borra estaciones ausentes ni meses anteriores por una respuesta parcial. Los JSON publicados conservan sus rutas y formatos.

## Comandos y comprobaciones

Requiere Node.js 22 o superior; el runner usa Node.js 22 y `npm ci`.

- `npm run data:update`: actualizar y validar embalses, sin depender de AEMET.
- `npm run data:update:scheduled`: alias compatible del mismo comando.
- `npm run data:update:aemet`: refrescar lluvia; requiere `AEMET_API_KEY`.
- `npm run data:summary`: consultar fechas, cobertura y huellas de los artefactos.
- `npm test`: pruebas aisladas sin red, incluidos errores de AEMET, correcciones oficiales y conservacion de la DB ante fallos.
- `MITECO_LIVE_TEST=1 node --test --test-name-pattern='live MITECO' tests/update.test.mjs`: prueba real con una copia temporal de la DB; no modifica el artefacto publicado. En PowerShell, establecer antes `$env:MITECO_LIVE_TEST='1'`.

Incidente de referencia: el [run del 2 de septiembre de 2026](https://github.com/Arrojo14/IQMizu-Data-Public/actions/runs/33665711547) completo MITECO y BoleHWeb, pero AEMET rechazo un rango historico valido y el workflow anterior omitio todo el commit. El fallo de AEMET y el aviso de runtime Node 20 de las acciones eran problemas distintos.
