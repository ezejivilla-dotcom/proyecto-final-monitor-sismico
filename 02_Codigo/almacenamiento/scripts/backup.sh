#!/usr/bin/env bash
# Backup logico de la base (formato custom, comprimido) y rotacion simple (conserva los ultimos 7).
# Uso:      ./02_Codigo/almacenamiento/scripts/backup.sh
# Restaurar (sobre una base recien creada por el compose, con el esquema ya aplicado por init/01_esquema.sql):
#   docker exec -i sismos-postgres pg_restore -U sismos -d sismos --data-only --no-owner < data/backups/<archivo>.dump
# Si la base ya tenia datos: TRUNCATE sismos.eventos, sismos.agg_region_mes, sismos.agg_ventana_horaria, sismos.clusters, sismos.metricas_pipeline, sismos.rechazados; antes.
set -euo pipefail
RAIZ="$(cd "$(dirname "$0")/../../.." && pwd)"
source "$RAIZ/.env"
DEST="$RAIZ/data/backups"; mkdir -p "$DEST"
ARCH="$DEST/sismos_$(date +%Y%m%d_%H%M%S).dump"
docker exec sismos-postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc --schema=sismos --exclude-table-data=sismos.eventos_staging > "$ARCH"
echo "Backup: $ARCH ($(du -h "$ARCH" | cut -f1))"
ls -1t "$DEST"/sismos_*.dump 2>/dev/null | tail -n +8 | xargs -I{} rm -f "{}"
