-- Capa 3: esquema del data warehouse de sismos (PostgreSQL 16).
-- Se ejecuta automáticamente la primera vez que arranca el contenedor (docker-entrypoint-initdb.d).
CREATE SCHEMA IF NOT EXISTS sismos;

-- ---------------------------------------------------------------------------
-- Hechos: un registro por sismo. Particionada por RANGO MENSUAL sobre fecha_evento:
-- las consultas por rango de fechas solo tocan las particiones necesarias (partition pruning).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sismos.eventos (
    evento_id             text             NOT NULL,
    fecha_evento          timestamptz      NOT NULL,
    -- Descripción del evento
    magnitud              numeric(4,2)     NOT NULL,
    tipo_magnitud         text,            -- mb, mww, ml...
    categoria_magnitud    text,            -- moderado (4-<5.5) | fuerte (5.5-<7) | mayor (>=7)
    profundidad_km        numeric(8,3),
    profundidad_fija      boolean,         -- true = 10 km exactos (valor por defecto del USGS, no medido)
    categoria_profundidad text,            -- superficial (<70) | intermedia (70-300) | profunda (>300)
    latitud               double precision NOT NULL,
    longitud              double precision NOT NULL,
    lugar                 text,            -- texto original del USGS ("185 km SW of Nikolski, Alaska")
    region                text,            -- pais/estado/zona normalizada
    tipo_evento           text,            -- earthquake, quarry blast, ...
    estado_revision       text,            -- reviewed | automatic
    -- Calidad de la medición (CSV del catálogo)
    nst                   integer,         -- estaciones que lo detectaron
    gap                   double precision,-- brecha azimutal (grados)
    dmin                  double precision,-- distancia a la estacion mas cercana (grados)
    rms                   double precision,-- error residual del modelo (s)
    error_horizontal_km   double precision,
    error_profundidad_km  double precision,
    error_magnitud        double precision,
    n_estaciones_magnitud integer,
    red_sismica           text,            -- red/agencia (us, ci, nc...)
    fuente_ubicacion      text,
    fuente_magnitud       text,
    actualizado_en        timestamptz,     -- ultima revision en el USGS
    -- Impacto humano (GeoJSON del catálogo; disperso: solo hay dato cuando aplica)
    reportes_felt         integer,         -- personas que lo reportaron como sentido
    intensidad_cdi        double precision,-- intensidad percibida (Did You Feel It)
    intensidad_mmi        double precision,-- intensidad instrumental (Mercalli modificada)
    alerta_pager          text,            -- green | yellow | orange | red
    tsunami               boolean,
    significancia         integer,
    -- Derivadas (Spark)
    anio                  smallint,
    mes                   smallint,
    dia_semana            smallint,        -- 1 = lunes ... 7 = domingo
    hora_utc              smallint,
    energia_joules        double precision,-- 10^(1.5*M + 4.8)
    distancia_ref_km      double precision,-- distancia a la localidad de referencia
    direccion_ref         text,            -- N, NNE, SW...
    localidad_ref         text,            -- "Nikolski"
    hemisferio_ns         text,            -- N | S
    hemisferio_ew         text,            -- E | W
    replica_probable      boolean,         -- hay un principal M>=5.5 (y >= +0.5 M) en la misma celda de 3 grados en las 72 h previas
    -- Linaje / modelo
    fuente                text,            -- usgs_feed | usgs_historico
    cluster_id            integer,         -- zona sismica (KMeans k=12)
    ingestado_en          timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (evento_id, fecha_evento)
) PARTITION BY RANGE (fecha_evento);

-- Particiones mensuales 2021-01 .. 2027-12 + partición por defecto (nada se pierde)
DO $$
DECLARE
    d date := date '2021-01-01';
BEGIN
    WHILE d < date '2028-01-01' LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS sismos.eventos_%s PARTITION OF sismos.eventos FOR VALUES FROM (%L) TO (%L)',
            to_char(d, 'YYYY_MM'), d, (d + interval '1 month')::date);
        d := (d + interval '1 month')::date;
    END LOOP;
END $$;
CREATE TABLE IF NOT EXISTS sismos.eventos_default PARTITION OF sismos.eventos DEFAULT;

-- Índices (se propagan a cada partición)
CREATE INDEX IF NOT EXISTS ix_eventos_region    ON sismos.eventos (region);
CREATE INDEX IF NOT EXISTS ix_eventos_magnitud  ON sismos.eventos (magnitud);
CREATE INDEX IF NOT EXISTS ix_eventos_cluster   ON sismos.eventos (cluster_id);

-- Staging: Spark carga aquí (TRUNCATE + append) y luego se hace MERGE idempotente a eventos.
CREATE TABLE IF NOT EXISTS sismos.eventos_staging (LIKE sismos.eventos INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- Agregados precalculados (los llena Spark)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sismos.agg_region_mes (
    region            text    NOT NULL,
    mes               date    NOT NULL,
    n_eventos         integer NOT NULL,
    magnitud_prom     numeric(5,3),
    magnitud_max      numeric(4,2),
    profundidad_prom  numeric(8,3),
    n_fuertes         integer,
    PRIMARY KEY (region, mes)
);

CREATE TABLE IF NOT EXISTS sismos.agg_ventana_horaria (
    ventana_inicio  timestamptz NOT NULL,
    region          text        NOT NULL,
    n_eventos       integer     NOT NULL,
    magnitud_max    numeric(4,2),
    PRIMARY KEY (ventana_inicio, region)
);

CREATE TABLE IF NOT EXISTS sismos.clusters (
    cluster_id     integer PRIMARY KEY,
    centro_lat     double precision,
    centro_lon     double precision,
    n_eventos      integer,
    magnitud_prom  numeric(5,3)
);

-- ---------------------------------------------------------------------------
-- Operacional (dashboard operacional de la capa 5)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sismos.metricas_pipeline (
    ts                   timestamptz NOT NULL DEFAULT now(),
    componente           text        NOT NULL,   -- nifi_feed | nifi_historico | spark
    corrida_id           text,
    registros_in         bigint,
    registros_ok         bigint,
    registros_duplicados bigint,
    registros_error      bigint,
    latencia_ms_prom     double precision,
    duracion_s           double precision
);
CREATE INDEX IF NOT EXISTS ix_metricas_ts ON sismos.metricas_pipeline (ts);

CREATE TABLE IF NOT EXISTS sismos.rechazados (
    ts       timestamptz NOT NULL DEFAULT now(),
    fuente   text,
    motivo   text,
    detalle  text,
    cantidad integer     NOT NULL DEFAULT 1   -- eventos que representa la fila (filas-resumen agrupan varios)
);
