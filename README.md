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

## Uso local

```bash
npm install
npm run data:update
```

Para refrescar solo la cache historica de AEMET:

```bash
npm run data:update:aemet
```

Ver resumen:

```bash
npm run data:summary
```

Variables esperadas:

```env
AEMET_API_KEY=tu_api_key_real
```

## Cron en GitHub Actions

El workflow esta en:

- `.github/workflows/update-db.yml`

Se ejecuta todos los dias a las `15:00 UTC`, que equivale a `16:00 UTC+1`.

Si quieres `16:00 hora de Madrid` todo el ano, tendras que ajustar manualmente el cron cuando cambie el horario de verano, porque GitHub Actions usa UTC.

## Publicar en GitHub

1. Crea un repo publico vacio en GitHub.
2. Desde esta carpeta:

```bash
git add .
git commit -m "Initial public data updater"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/TU_REPO.git
git push -u origin main
```

3. En GitHub, ve a `Settings -> Actions -> General` y asegurate de permitir:
   `Read and write permissions` para que el workflow pueda hacer commit del DB actualizado.

4. En `Settings -> Secrets and variables -> Actions`, crea el secreto:

- `AEMET_API_KEY`

## Notas

- La primera ejecucion puede tardar bastante mas que las siguientes porque regenera la DB y la cache historica de AEMET.
- GitHub puede avisar si `data/embalses.db` supera `50 MB`.
- El origen de datos es MITECO y puede cambiar nombre/ruta del archivo con el tiempo.
- El workflow hace commit tanto de `data/embalses.db` como de `data/cache/` cuando detecta cambios reales.
