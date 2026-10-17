# Diccionario de datos

Descripción de las tablas de la base `sismos` (PostgreSQL 16). La definición ejecutable está en [`init/01_esquema.sql`](init/01_esquema.sql). La tabla principal, `sismos.eventos`, es el resultado de unir y depurar las dos fuentes del USGS con el procesamiento de Spark.

## Garantías de integridad de `sismos.eventos`

- **Un registro por `evento_id`** (verificado: 0 repetidos). Si el USGS corrige la fecha del evento, la carga elimina la versión anterior.
- **Sincronizado con el catálogo:** eventos revisados por debajo de M 4.0 o retirados por el USGS se podan dentro de los meses que el histórico cubre en cada corrida.
- **Idempotente:** re-ejecutar Spark no cambia el resultado ni genera tuplas muertas.

## Fuentes

| Fuente | Tipo | Origen | Zona raw |
|---|---|---|---|
| `usgs_feed` | API REST, casi tiempo real | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson` (cada 5 min) | `data/raw/salida/consolidado/*.csv` |
| `usgs_historico` | Batch (ventanas mensuales, **2 consultas por ventana**) | API FDSN `.../fdsnws/event/1/query`: `format=csv` (calidad de la medición) + `format=geojson` (impacto humano). Últimos 5 años hasta hoy, M ≥ 4.0 | `data/raw/historico/*.csv` y `data/raw/historico_impacto/*.csv` |

Las dos se unifican y deduplican en Spark. Si un evento está en ambas, gana el revisado (`reviewed`) y luego el histórico.
**Alcance:** solo se cargan eventos con `magnitud >= 4.0` (el feed trae magnitudes menores y se descartan). Ver las reglas de uso al final de este documento.

## Conexión

| | Desde el Mac (host) | Desde otro contenedor |
|---|---|---|
| Host | `localhost` | `postgres` |
| Puerto | `5433` (valor de `POSTGRES_PORT`; el 5432 suele estar ocupado) | `5432` |
| BD / usuario / clave | `.env` (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`) | igual |

Esquema: `sismos`. Zona horaria de almacenamiento: UTC (`timestamptz`).

## Tablas

### `sismos.eventos` — hechos (46 columnas; particionada por rango mensual sobre `fecha_evento`, 2021-01 … 2027-12 + `default`)

Una fila por sismo. PK `(evento_id, fecha_evento)`. Índices: `(region)`, `(magnitud)`, `(cluster_id)`.
La columna **Completitud** es el % de filas con valor (medido sobre 81 044 filas, 2026-10-17).

**Descripción del evento**

| Columna | Tipo | Compl. | Notas |
|---|---|---:|---|
| `evento_id` | text | 100 % | ID USGS |
| `fecha_evento` | timestamptz | 100 % | UTC. **Clave de partición** |
| `magnitud` | numeric(4,2) | 100 % | Siempre ≥ 4.0 (alcance) |
| `tipo_magnitud` | text | 100 % | `mb` (88 %), `mww`, `mwr`, `mw`… |
| `categoria_magnitud` | text | 100 % | `moderado` (4–<5.5) / `fuerte` (5.5–<7) / `mayor` (≥7). No existe `menor` |
| `profundidad_km` | numeric(8,3) | 100 % | Puede ser negativa (sobre el nivel del mar) |
| `profundidad_fija` | boolean | 100 % | `true` = exactamente 10 km: **valor por defecto del USGS, no medido** (41.3 %) |
| `categoria_profundidad` | text | 100 % | `superficial` (<70) / `intermedia` (70–300) / `profunda` (>300) |
| `latitud`, `longitud` | double precision | 100 % | |
| `lugar` | text | 100 % | Texto original del USGS (`"185 km SW of Nikolski, Alaska"`) |
| `region` | text | 100 % | País/estado/zona normalizada (376 valores). Ej. `Indonesia`, `California`, `Fiji`. Los sismos con nombre propio (p. ej. *Kahramanmaras Earthquake Sequence*) se asignan al país (`Turkey`) |
| `tipo_evento` | text | 100 % | `earthquake` (99.98 %; 18 eventos no son terremotos: erupciones, deslizamientos, explosiones) |
| `estado_revision` | text | 100 % | `reviewed` (casi todos) / `automatic` (preliminar, 1 evento) |

**Calidad de la medición** (fuente: CSV del catálogo; nulos en eventos que solo vienen del feed)

| Columna | Tipo | Compl. | Notas |
|---|---|---:|---|
| `nst` | integer | 88.4 % | Estaciones que lo detectaron |
| `gap` | double | 99.6 % | Brecha azimutal (°). Menor = mejor localización |
| `dmin` | double | 99.6 % | Distancia a la estación más cercana (°) |
| `rms` | double | 100 % | Error residual del modelo (s) |
| `error_horizontal_km` | double | 99.6 % | Incertidumbre de la posición |
| `error_profundidad_km` | double | 100 % | Incertidumbre de la profundidad (ver regla 4 de uso) |
| `error_magnitud` | double | 99.1 % | Incertidumbre de la magnitud |
| `n_estaciones_magnitud` | integer | 99.3 % | Estaciones usadas para la magnitud |
| `red_sismica` | text | 100 % | Red/agencia (`us`, `ci`, `nc`… 15 valores) |
| `fuente_ubicacion`, `fuente_magnitud` | text | 100 % | Agencia que calculó cada cosa |
| `actualizado_en` | timestamptz | 100 % | Última revisión en el USGS |

**Impacto humano** (fuente: GeoJSON del catálogo; **disperso por naturaleza**: solo hay dato cuando aplica)

| Columna | Tipo | Compl. | Notas |
|---|---|---:|---|
| `reportes_felt` | integer | 12.6 % | Personas que reportaron haberlo sentido (máx. 184 674) |
| `intensidad_cdi` | double | 12.6 % | Intensidad percibida (Did You Feel It), 1–10 |
| `intensidad_mmi` | double | 6.8 % | Intensidad instrumental (Mercalli modificada) |
| `alerta_pager` | text | 4.9 % | `green` (3 788) / `yellow` (125) / `orange` (25) / `red` (23). Solo eventos con impacto estimado |
| `tsunami` | boolean | 100 % | `true` en 557 eventos (bandera de tsunami del USGS) |
| `significancia` | integer | 100 % | Índice `sig` del USGS (combina magnitud, impacto y reportes) |

**Derivadas** (calculadas en Spark)

| Columna | Tipo | Notas |
|---|---|---|
| `anio`, `mes`, `hora_utc` | smallint | Del `fecha_evento` (UTC) |
| `dia_semana` | smallint | 1 = lunes … 7 = domingo |
| `energia_joules` | double | `10^(1.5·M + 4.8)`. Un M8.8 ≈ 1 000 PJ. Para sumar energía por región |
| `distancia_ref_km`, `direccion_ref`, `localidad_ref` | double / text / text | Parseo de `lugar`: `185 / SW / Nikolski`. **Nulo en 26.5 %** (zonas oceánicas sin "N km DIR of") |
| `hemisferio_ns`, `hemisferio_ew` | text | `N`/`S`, `E`/`W` |
| `replica_probable` | boolean | **Heurística**: hubo un evento principal M ≥ 5.5 (y ≥ +0.5 M) en la misma celda de 3°×3° en las 72 h previas. 14.5 % de filas |

**Linaje / modelo**

| Columna | Tipo | Notas |
|---|---|---|
| `fuente` | text | `usgs_feed` \| `usgs_historico` (gana el histórico si está en ambas; tras refrescar el histórico hasta hoy, todos los eventos vigentes son `usgs_historico`) |
| `cluster_id` | integer | Zona sísmica, KMeans k=12 (ver `sismos.clusters`) |
| `ingestado_en` | timestamptz | Auditoría |

> Al consultar: filtrar por `fecha_evento` activa el *partition pruning*; sin filtro se leen ~85 particiones.
> Las reglas para usar estas columnas sin sesgo están al final de este documento.

### Agregados (los recalcula Spark en cada corrida: `TRUNCATE` + carga)

- `sismos.agg_region_mes` — `region`, `mes` (date), `n_eventos`, `magnitud_prom`, `magnitud_max`, `profundidad_prom`, `n_fuertes` (magnitud ≥ 5.5). PK `(region, mes)`.
- `sismos.agg_ventana_horaria` — `ventana_inicio`, `region`, `n_eventos` (≥ 2), `magnitud_max`. Sirve para detectar enjambres/réplicas. PK `(ventana_inicio, region)`.
- `sismos.clusters` — `cluster_id`, `centro_lat`, `centro_lon`, `n_eventos`, `magnitud_prom`.

### Operacional

- `sismos.metricas_pipeline` — `ts`, `componente` (`nifi_feed` / `nifi_historico` / `spark`), `corrida_id`, `registros_in`, `registros_ok`, `registros_duplicados`, `registros_error`, `latencia_ms_prom`, `duracion_s`.
  - Spark: una fila por corrida (`duracion_s` informada). Cada corrida relee toda la zona raw, por lo que `registros_in` es el total leído, **no** lo nuevo: no se deben sumar corridas.
  - NiFi: una fila por lectura de `exportar_metricas_nifi.py`; los contadores son de la **ventana móvil de 5 min** de NiFi, no acumulados. Tampoco se suman: para estado actual usar la última lectura por `componente`.
- `sismos.rechazados` — `ts`, `fuente`, `motivo`, `detalle`, **`cantidad`** (eventos que representa la fila). Muestra (máx. 2000) de registros descartados por calidad en Spark más una fila-resumen `fuera_de_alcance_magnitud` (eventos M < 4.0 del feed, que no son errores). Se recarga en cada corrida de Spark.
- `sismos.eventos_staging` — **interna** (carga de Spark antes del MERGE). No consultar.


## Volumen actual (referencia)

81 044 eventos (2021-10-01 → 2026-10-17), 376 regiones, 12 clusters, 92 tablas en el esquema (85 particiones de `eventos`), backup de ~6 MB.
Agregados: `agg_region_mes` 6 863 filas, `agg_ventana_horaria` 6 434 filas.

## Reglas de uso de los datos

Para que el análisis no saque conclusiones falsas:

1. **Solo terremotos naturales:** filtrar `tipo_evento = 'earthquake'` (hay unos 18 eventos que son erupciones, deslizamientos o explosiones).
2. **Profundidad medida:** para cualquier promedio, correlación o gráfico de profundidad, usar `profundidad_fija = false`. El 41.3 % de las profundidades son exactamente 10 km, valor por defecto del USGS y no una medición.
3. **Años completos:** 2021 solo tiene octubre a diciembre y 2026 llega hasta la fecha. Las comparaciones anuales deben usar 2022-2025.
4. **Errores de profundidad:** `error_profundidad_km` no es comparable entre eventos de profundidad fija y medida.
5. **Campos de impacto humano dispersos:** `reportes_felt`, `intensidad_cdi`, `intensidad_mmi` y `alerta_pager` son nulos en la mayoría de eventos por naturaleza del dato; analizarlos solo sobre las filas con valor.
6. **`replica_probable` es una heurística**, no un modelo sismológico: un evento principal de magnitud 5.5 o más en la misma celda de 3° × 3° durante las 72 horas previas.
7. **Métricas del pipeline:** no sumar filas de `sismos.metricas_pipeline` entre corridas (Spark relee todo; NiFi reporta ventanas móviles de 5 minutos). Para el estado actual, usar la última lectura por componente.
8. **Consultar por `fecha_evento`:** filtrar por `anio`/`mes` no activa la poda de particiones; hay que filtrar por rango de `fecha_evento`.
