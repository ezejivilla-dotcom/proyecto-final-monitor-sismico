# Capa 3 — Almacenamiento (PostgreSQL 16)

| Archivo | Para qué |
|---|---|
| `init/01_esquema.sql` | Crea el esquema `sismos` (se ejecuta solo la primera vez que arranca el contenedor): `eventos` particionada por mes (2021-01…2027-12 + `default`), staging, agregados, métricas y rechazados |
| `scripts/migracion_001_profundidad_fija.sql` | Para bases anteriores: columna `profundidad_fija` |
| `scripts/migracion_002_columnas_ampliadas.sql` | Para bases anteriores: 25 columnas (calidad, impacto, derivadas) |
| `scripts/migracion_003_rechazados_cantidad.sql` | Para bases anteriores: columna `rechazados.cantidad` |
| `scripts/backup.sh` | `pg_dump -Fc` a `data/backups/` con rotación (conserva los 7 últimos) |

## Diseño

- **Partición por rango mensual** sobre `fecha_evento`: las consultas con filtro de fechas solo leen las particiones necesarias.
- **PK** `(evento_id, fecha_evento)`: la clave de partición debe estar en la PK.
- **Staging + carga transaccional** (generada por Spark): borra versiones obsoletas del mismo `evento_id`, hace upsert solo de lo que cambió y poda eventos que el catálogo ya no devuelve. Es idempotente y no genera tuplas muertas.
- **Índices**: `region`, `magnitud`, `cluster_id` (se propagan a cada partición).
- **Backup**: lógico, comprimido, ~6 MB; publicado en la release `datos-v2` del repo (81 044 eventos). Restauración con `pg_restore --data-only` sobre el esquema creado por `init/` (comando en la cabecera de `backup.sh` y en la guía del compañero). Verificado: 0 errores, 80 665 filas, 85 particiones.

## Nota sobre fechas anteriores a 2021

Las filas anteriores a 2021-01 caerían en `eventos_default` y perderían el partition pruning.
Si se amplía el histórico hacia atrás, extender el bucle `WHILE` de `01_esquema.sql` (fecha inicial) y recrear la base.

Diccionario de datos completo y reglas de uso: [`DICCIONARIO_DE_DATOS.md`](DICCIONARIO_DE_DATOS.md).
