# Embalses Data Public

Repo minimo para mantener una base SQLite publica con la informacion esencial de embalses de Espana.

Incluye solo:

- `cuencas`
- `embalses`
- `datos_semanales`
- un cron de GitHub Actions listo para ejecutarse cada dia

No incluye:

- el frontend de la web
- secretos
- tablas auxiliares no esenciales

## Que hace

El flujo descarga el ZIP oficial de MITECO, localiza el `.mdb`, y:

- crea `data/embalses.db` desde cero si no existe
- o la actualiza incrementalmente si ya existe

La base generada contiene solo estas tablas:

- `cuencas(id, nombre)`
- `embalses(id, nombre, cuenca_id, capacidad_hm3, electrico)`
- `datos_semanales(id, embalse_id, fecha, agua_actual_hm3, agua_total_hm3)`

## Uso local

```bash
npm install
npm run data:update
```

Ver resumen:

```bash
npm run data:summary
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

## Notas

- La primera ejecucion puede tardar bastante mas que las siguientes.
- GitHub puede avisar si `data/embalses.db` supera `50 MB`.
- El origen de datos es MITECO y puede cambiar nombre/ruta del archivo con el tiempo.
