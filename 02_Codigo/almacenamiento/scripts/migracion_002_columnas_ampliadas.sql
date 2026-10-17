-- Migracion para bases creadas ANTES de ampliar columnas (los clones nuevos ya las traen en 01_esquema.sql).
-- Uso: docker exec -i sismos-postgres psql -U sismos -d sismos < 02_Codigo/almacenamiento/scripts/migracion_002_columnas_ampliadas.sql
DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['sismos.eventos', 'sismos.eventos_staging'] LOOP
        EXECUTE format($f$ALTER TABLE %s
            ADD COLUMN IF NOT EXISTS nst integer,
            ADD COLUMN IF NOT EXISTS gap double precision,
            ADD COLUMN IF NOT EXISTS dmin double precision,
            ADD COLUMN IF NOT EXISTS rms double precision,
            ADD COLUMN IF NOT EXISTS error_horizontal_km double precision,
            ADD COLUMN IF NOT EXISTS error_profundidad_km double precision,
            ADD COLUMN IF NOT EXISTS error_magnitud double precision,
            ADD COLUMN IF NOT EXISTS n_estaciones_magnitud integer,
            ADD COLUMN IF NOT EXISTS red_sismica text,
            ADD COLUMN IF NOT EXISTS fuente_ubicacion text,
            ADD COLUMN IF NOT EXISTS fuente_magnitud text,
            ADD COLUMN IF NOT EXISTS actualizado_en timestamptz,
            ADD COLUMN IF NOT EXISTS intensidad_cdi double precision,
            ADD COLUMN IF NOT EXISTS intensidad_mmi double precision,
            ADD COLUMN IF NOT EXISTS alerta_pager text,
            ADD COLUMN IF NOT EXISTS anio smallint,
            ADD COLUMN IF NOT EXISTS mes smallint,
            ADD COLUMN IF NOT EXISTS dia_semana smallint,
            ADD COLUMN IF NOT EXISTS hora_utc smallint,
            ADD COLUMN IF NOT EXISTS energia_joules double precision,
            ADD COLUMN IF NOT EXISTS distancia_ref_km double precision,
            ADD COLUMN IF NOT EXISTS direccion_ref text,
            ADD COLUMN IF NOT EXISTS localidad_ref text,
            ADD COLUMN IF NOT EXISTS hemisferio_ns text,
            ADD COLUMN IF NOT EXISTS hemisferio_ew text,
            ADD COLUMN IF NOT EXISTS replica_probable boolean$f$, t);
    END LOOP;
END $$;
