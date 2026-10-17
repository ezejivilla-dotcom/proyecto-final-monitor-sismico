# Capa 2 — Procesamiento (PySpark 3.5, batch)

`procesar_sismos.py` lee la zona raw que deja NiFi y carga PostgreSQL. Se ejecuta con:

```bash
docker compose run --rm spark          # ~40 s para ~81 mil registros
```

## Qué hace (en orden)

1. **Lectura y normalización** de 3 entradas: feed en vivo, histórico CSV e histórico GeoJSON-impacto (unidos por `evento_id`).
2. **Limpieza**: trim/lower, validación de id, fecha, magnitud (−2…10), latitud, longitud. Rechazos con motivo → `sismos.rechazados`.
3. **Alcance**: solo `magnitud >= MAG_MIN` (4.0). Lo descartado se cuenta como "fuera de alcance", no como error.
4. **Deduplicación** por `evento_id`: gana `reviewed`, luego el histórico, luego el más reciente.
5. **Enriquecimiento**: `region` normalizada, categorías de magnitud y profundidad, `profundidad_fija`, calendario,
   `energia_joules`, referencia (`distancia_ref_km`, `direccion_ref`, `localidad_ref`), hemisferios, `replica_probable`.
6. **KMeans** (k=12, coordenadas esféricas) → `cluster_id` y `sismos.clusters`.
7. **Agregaciones**: `agg_region_mes`, `agg_ventana_horaria` (ventana de 1 h, ≥ 2 eventos).
8. **Carga**: staging → `MERGE` idempotente a `sismos.eventos` (generado desde `COLS_EVENTOS`), agregados con `TRUNCATE`+carga,
   métricas de la corrida en `sismos.metricas_pipeline`.

## Parámetros (variables de entorno)

| Variable | Defecto | Efecto |
|---|---|---|
| `MAG_MIN` | `4.0` | Magnitud mínima que entra a la base |
| `DATA_RAW` | `/data/raw` | Carpeta raw (montada desde `./data/raw`) |
| `POSTGRES_*` | las del `.env` | Conexión |

Constantes en el script: `K_CLUSTERS=12`, `UMBRAL_PRINCIPAL=5.5` (réplicas).

## Si cambias columnas

Edita `COLS_EVENTOS` en el script, `01_esquema.sql` y agrega una `migracion_00X` en `02_Codigo/almacenamiento/scripts/`.
Luego actualiza `02_Codigo/almacenamiento/DICCIONARIO_DE_DATOS.md`.
