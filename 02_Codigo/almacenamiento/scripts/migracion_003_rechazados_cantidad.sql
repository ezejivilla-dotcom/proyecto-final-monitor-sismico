-- Migracion para bases creadas ANTES de agregar rechazados.cantidad (los clones nuevos ya la traen en 01_esquema.sql).
-- Uso: docker exec -i sismos-postgres psql -U sismos -d sismos < 02_Codigo/almacenamiento/scripts/migracion_003_rechazados_cantidad.sql
ALTER TABLE sismos.rechazados ADD COLUMN IF NOT EXISTS cantidad integer NOT NULL DEFAULT 1;
