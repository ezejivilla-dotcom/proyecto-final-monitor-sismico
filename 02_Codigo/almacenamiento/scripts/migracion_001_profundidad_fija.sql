-- Migracion para bases creadas ANTES de agregar profundidad_fija (los clones nuevos ya la traen en 01_esquema.sql).
-- Uso: docker exec -i sismos-postgres psql -U sismos -d sismos < 02_Codigo/almacenamiento/scripts/migracion_001_profundidad_fija.sql
ALTER TABLE sismos.eventos         ADD COLUMN IF NOT EXISTS profundidad_fija boolean;
ALTER TABLE sismos.eventos_staging ADD COLUMN IF NOT EXISTS profundidad_fija boolean;
