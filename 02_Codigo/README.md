# Código fuente

Todo el código del proyecto, una subcarpeta por capa. Los comandos se ejecutan desde la **raíz del proyecto**.

| Carpeta | Capa | Contenido |
|---|---|---|
| [`ingesta/`](ingesta/README.md) | 1 · Ingesta | Scripts que construyen los dos pipelines de Apache NiFi por su API REST, esquemas Avro, especificaciones Jolt, definiciones exportadas de los flujos y el script de capturas |
| [`procesamiento/`](procesamiento/README.md) | 2 · Procesamiento | Job de PySpark (limpieza, enriquecimiento, KMeans, carga) y su entorno Docker |
| [`almacenamiento/`](almacenamiento/README.md) | 3 · Almacenamiento | Esquema de PostgreSQL con particionamiento, migraciones, respaldo y [diccionario de datos](almacenamiento/DICCIONARIO_DE_DATOS.md) |
| [`analisis/`](analisis/RENDIMIENTO.md) | 4 · Análisis | Cinco consultas SQL avanzadas, benchmark automatizado e informe de rendimiento |
| `visualizacion/` | 5 · Visualización | Generador de dashboards y aprovisionamiento de Grafana (datasource y dashboards como código) |

## Servicios (`docker-compose.yml`)

| Servicio | Para qué | Acceso |
|---|---|---|
| `postgres` | Base de datos `sismos` | `localhost:5433` |
| `grafana` | Dashboards (aprovisionados al arrancar) | <http://localhost:3000> |
| `nifi` | Ingesta (solo si se regenera desde las fuentes) | <https://localhost:8443/nifi> |
| `spark` | Procesamiento, bajo demanda (`docker compose run --rm spark`) | — |

Variables de entorno en `.env` (se crea con `cp .env.example .env`): `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`, `NIFI_USER`, `NIFI_PASSWORD` (mínimo 12 caracteres), `NIFI_PORT`, `GRAFANA_USER`, `GRAFANA_PASSWORD`, `GRAFANA_PORT`.

## Ejecución rápida (con el respaldo de la base)

```bash
cp .env.example .env
docker compose up -d postgres grafana

mkdir -p data/backups
curl -L -o data/backups/sismos_20261017_092202.dump \
  https://github.com/ezejivilla-dotcom/proyecto-final-monitor-sismico/releases/download/datos-v2/sismos_20261017_092202.dump
docker exec -i sismos-postgres pg_restore -U sismos -d sismos --data-only --no-owner < data/backups/sismos_20261017_092202.dump
```

Con esto la base queda con los 81 044 sismos y los dos dashboards de Grafana funcionan en <http://localhost:3000>.

## Regenerar todo desde las fuentes del USGS (~10 minutos, requiere internet)

Requiere Python 3 con `requests` (`pip install requests`).

```bash
docker compose up -d                                   # incluye NiFi; tarda 1-2 min en estar listo
cd 02_Codigo/ingesta/scripts
python3 construir_pipeline.py                          # crea el pipeline en vivo
python3 construir_historico.py                         # crea el pipeline histórico (61 ventanas)
python3 iniciar_flujos.py                              # arranca ambos
# esperar ~6 min a que baje el histórico: ls ../../../data/raw/historico | wc -l  -> 61
cd ../../..
docker compose run --rm spark                          # procesa data/raw y carga PostgreSQL (~40 s)
python3 02_Codigo/ingesta/scripts/iniciar_flujos.py --detener    # opcional: detener NiFi al terminar
```

## Por capa

- **Ingesta:** `python3 02_Codigo/ingesta/scripts/exportar_metricas_nifi.py [--loop]` guarda las métricas de NiFi en la base (alimentan el dashboard operacional).
- **Procesamiento:** `docker compose run --rm spark`. Es idempotente: repetirlo no duplica datos.
- **Almacenamiento:** `./02_Codigo/almacenamiento/scripts/backup.sh` genera un respaldo comprimido en `data/backups/`.
- **Análisis:**
  ```bash
  for f in 02_Codigo/analisis/0*.sql; do docker exec -i sismos-postgres psql -U sismos -d sismos < "$f"; done
  pip install psycopg2-binary
  python 02_Codigo/analisis/medir_rendimiento.py       # lee el .env y regenera analisis/RENDIMIENTO.md
  ```
- **Visualización:** los dashboards se aprovisionan solos. Para modificarlos se edita `02_Codigo/visualizacion/generar_dashboards.py` y se ejecuta; Grafana recarga los archivos en unos 10 segundos.

## Carpeta `data/`

No se versiona. `data/raw/` es la zona intermedia entre NiFi y Spark (`salida/consolidado/`, `historico/`, `historico_impacto/`, `alertas/`, `errores/`) y `data/backups/` guarda los respaldos de la base.
