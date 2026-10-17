# Evidencias visuales

## Dashboards de Grafana (capa 5)

| Archivo | Qué muestra |
|---|---|
| `01_dashboard_sismos_kmeans_negocio.png` | Dashboard de negocio: KPI, mapa de epicentros (Geomap), dispersión magnitud-profundidad, serie temporal con doble eje, comparativa y auditoría de los 12 clústeres KMeans |
| `02_dashboard_salud_pipeline_monitoreo.png` | Dashboard operacional: eventos en la base, latencia de NiFi, duración de Spark, rechazos, throughput, tasas de éxito/duplicados/errores y auditoría de descartes |

## Ingesta con Apache NiFi (capa 1) — `ingesta/`

Capturas de la interfaz real de NiFi con los dos flujos en ejecución. Se regeneran con `02_Codigo/ingesta/scripts/capturar_evidencias.mjs` (ver `02_Codigo/ingesta/README.md`).

| # | Archivo | Qué evidencia |
|---|---|---|
| 01 | `01_nifi_funcional.png` | NiFi operativo con los dos pipelines (en vivo e histórico) corriendo |
| 02 | `02_feed_vista_general.png` | Pipeline de sismos en vivo: los 5 grupos, el flujo entre ellos y el grupo de errores |
| 03 | `03_feed_pg2_validacion.png` | Grupo 2: extracción de campos, 6 reglas de calidad, deduplicación y rechazos |
| 04 | `04_feed_config_reglas.png` | Configuración de las 6 reglas de calidad (`RouteOnAttribute`) |
| 05 | `05_feed_backpressure.png` | Backpressure de la conexión entre grupos: 10 000 objetos / 1 GB |
| 06 | `06_feed_pg4_salida.png` | Grupo 4: consolidación en CSV por categoría y generación de alertas |
| 07 | `07_feed_pg5_errores.png` | Grupo 5: registro y archivo de rechazos por etapa y motivo |
| 08 | `08_historico_vista_general.png` | Pipeline histórico: calendario de ventanas, rate limiting y las dos ramas (CSV y GeoJSON). Alta resolución (3600×2000) |
| 09 | `09_historico_rate_limiting.png` | `ControlRate`: 1 ventana cada 5 s (parámetro `hist_ritmo`) |
| 10 | `10_historico_validaterecord.png` | `ValidateRecord` contra el esquema Avro, con tipado estricto |
| 11 | `11_controller_services.png` | Servicios de controlador: caché de deduplicación, lector JSON con esquema Avro y escritor CSV |
| 12 | `12_estadisticas_summary.png` | Resumen de NiFi: registros procesados por cada procesador en la ventana de 5 min |
| 13 | `13_provenance_eventos.png` | Eventos de *provenance* de un FlowFile: de la separación a `PutFile` |
| 14 | `14_provenance_linaje.png` | Grafo de linaje del mismo FlowFile (trazabilidad completa) |
| 15 | `15_archivos_salida.png` | Zona raw resultante en `data/raw/`: alertas, errores, histórico, impacto y consolidados |
