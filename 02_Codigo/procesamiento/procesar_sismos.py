"""
Capa 2 - Procesamiento (PySpark, batch).

Lee la zona raw que deja NiFi, unifica las fuentes, limpia, enriquece, agrega,
agrupa epicentros con KMeans y carga en PostgreSQL.

    data/raw/salida/consolidado/*.csv     fuente 1: API USGS en vivo (ya clasificada por NiFi)
    data/raw/historico/*.csv              fuente 2: catalogo historico FDSN (calidad de la medicion)
    data/raw/historico_impacto/*.csv      fuente 2b: GeoJSON del catalogo (felt, cdi, mmi, alert, tsunami, sig)
        -> union / limpieza -> alcance (M >= MAG_MIN) -> deduplicacion -> enriquecimiento
        -> columnas derivadas (calendario, energia, referencia, replica probable)
        -> agregaciones (mes, ventana horaria) -> KMeans
        -> PostgreSQL (staging + MERGE idempotente)

Ejecucion:  docker compose run --rm spark
"""
import math
import os
import time
from datetime import datetime, timezone

import psycopg2
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = os.environ.get("DATA_RAW", "/data/raw")
PG = dict(host=os.environ.get("POSTGRES_HOST", "postgres"), port=os.environ.get("POSTGRES_PORT", "5432"),
          dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
          password=os.environ["POSTGRES_PASSWORD"])
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
JDBC_PROPS = {"user": PG["user"], "password": PG["password"], "driver": "org.postgresql.Driver"}
K_CLUSTERS = 12
UMBRAL_PRINCIPAL = 5.5     # magnitud minima del evento principal para marcar replicas
# El catalogo historico es completo desde M4.0; el feed trae todas las magnitudes (hasta M<0). Para que las dos
# fuentes sean comparables solo se conservan eventos con magnitud >= MAG_MIN.
MAG_MIN = float(os.environ.get("MAG_MIN", "4.0"))
CORRIDA = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
T0 = time.time()

# El feed del USGS abrevia estados de EE.UU. con 2 letras ("..., CA"): se expande para tener regiones legibles.
ESTADOS_US = {"CA": "California", "NV": "Nevada", "HI": "Hawaii", "AK": "Alaska", "OK": "Oklahoma", "TX": "Texas",
              "WA": "Washington", "OR": "Oregon", "UT": "Utah", "ID": "Idaho", "MT": "Montana", "WY": "Wyoming",
              "NM": "New Mexico", "AZ": "Arizona", "KS": "Kansas", "CO": "Colorado", "MO": "Missouri",
              "TN": "Tennessee", "AR": "Arkansas", "PR": "Puerto Rico"}
# Nombres de zona (Flinn-Engdahl) que el USGS usa fuera de tierra firme: "South of the Fiji Islands", "Japan region"...
PREFIJO_DIRECCION = (r"(?i)^(north|south|east|west|northeast|northwest|southeast|southwest|northern|southern|"
                     r"eastern|western|central)\s+of\s+(the\s+)?")
PREFIJO_COSTA = r"(?i)^(off|near)\s+the\s+((north|south|east|west)\w*\s+)?coast\s+of\s+(the\s+)?"
ALIAS_REGION = {"Fiji Islands": "Fiji", "Mariana Islands": "Northern Mariana Islands", "Tonga Islands": "Tonga",
                "Kahramanmaras": "Turkey"}

# Columnas finales de sismos.eventos. El MERGE se genera a partir de esta lista (una sola fuente de verdad).
COLS_EVENTOS = [
    "evento_id", "fecha_evento", "magnitud", "tipo_magnitud", "categoria_magnitud", "profundidad_km",
    "profundidad_fija", "categoria_profundidad", "latitud", "longitud", "lugar", "region", "tipo_evento",
    "estado_revision", "nst", "gap", "dmin", "rms", "error_horizontal_km", "error_profundidad_km", "error_magnitud",
    "n_estaciones_magnitud", "red_sismica", "fuente_ubicacion", "fuente_magnitud", "actualizado_en",
    "reportes_felt", "intensidad_cdi", "intensidad_mmi", "alerta_pager", "tsunami", "significancia",
    "anio", "mes", "dia_semana", "hora_utc", "energia_joules", "distancia_ref_km", "direccion_ref", "localidad_ref",
    "hemisferio_ns", "hemisferio_ew", "replica_probable", "fuente", "cluster_id", "ingestado_en"]
CLAVE = ("evento_id", "fecha_evento")

spark = (SparkSession.builder.appName("sismos-procesamiento").master("local[*]")
         .config("spark.jars", "/opt/postgresql.jar")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def log(msg):
    print(f"[{time.time() - T0:6.1f}s] {msg}", flush=True)


def nulo(tipo):
    return F.lit(None).cast(tipo)


# ------------------------------------------------------------------ 1. LECTURA Y NORMALIZACION
def leer_feed():
    df = spark.read.option("header", True).csv(f"{RAW}/salida/consolidado/*.csv")
    return df.select(
        F.col("evento_id"),
        F.to_timestamp("fecha_evento_utc").alias("fecha_evento"),
        F.col("magnitud").cast("double").alias("magnitud"),
        F.col("tipo_magnitud"),
        F.col("profundidad_km").cast("double").alias("profundidad_km"),
        F.col("latitud").cast("double").alias("latitud"),
        F.col("longitud").cast("double").alias("longitud"),
        F.col("lugar"), F.col("tipo_evento"), F.col("estado_revision"),
        nulo("int").alias("nst"), nulo("double").alias("gap"), nulo("double").alias("dmin"),
        nulo("double").alias("rms"), nulo("double").alias("error_horizontal_km"),
        nulo("double").alias("error_profundidad_km"), nulo("double").alias("error_magnitud"),
        nulo("int").alias("n_estaciones_magnitud"), F.col("red_sismica"),
        nulo("string").alias("fuente_ubicacion"), nulo("string").alias("fuente_magnitud"),
        nulo("timestamp").alias("actualizado_en"),
        F.col("reportes_sentido").cast("int").alias("reportes_felt"),
        nulo("double").alias("intensidad_cdi"), nulo("double").alias("intensidad_mmi"),
        nulo("string").alias("alerta_pager"),
        (F.col("alerta_tsunami").cast("int") == 1).alias("tsunami"),
        F.col("significancia").cast("int").alias("significancia"),
        F.lit("usgs_feed").alias("fuente"),
        F.to_timestamp("timestamp_procesamiento").alias("version_ts"),
    )


def leer_historico():
    h = spark.read.option("header", True).csv(f"{RAW}/historico/*.csv")
    # campos de impacto: vienen del GeoJSON (otro archivo por ventana); se unen por id
    i = (spark.read.option("header", True).csv(f"{RAW}/historico_impacto/*.csv")
         .select("evento_id", F.col("felt").cast("int").alias("reportes_felt"),
                 F.col("cdi").cast("double").alias("intensidad_cdi"),
                 F.col("mmi").cast("double").alias("intensidad_mmi"), F.col("alerta_pager"),
                 (F.col("tsunami").cast("int") == 1).alias("tsunami"),
                 F.col("significancia").cast("int").alias("significancia"))
         .dropDuplicates(["evento_id"]))
    h = h.withColumnRenamed("id", "evento_id").join(i, "evento_id", "left")
    return h.select(
        F.col("evento_id"),
        F.to_timestamp("time").alias("fecha_evento"),
        F.col("mag").cast("double").alias("magnitud"),
        F.col("magType").alias("tipo_magnitud"),
        F.col("depth").cast("double").alias("profundidad_km"),
        F.col("latitude").cast("double").alias("latitud"),
        F.col("longitude").cast("double").alias("longitud"),
        F.col("place").alias("lugar"), F.col("type").alias("tipo_evento"), F.col("status").alias("estado_revision"),
        F.col("nst").cast("int").alias("nst"), F.col("gap").cast("double").alias("gap"),
        F.col("dmin").cast("double").alias("dmin"), F.col("rms").cast("double").alias("rms"),
        F.col("horizontalError").cast("double").alias("error_horizontal_km"),
        F.col("depthError").cast("double").alias("error_profundidad_km"),
        F.col("magError").cast("double").alias("error_magnitud"),
        F.col("magNst").cast("int").alias("n_estaciones_magnitud"),
        F.col("net").alias("red_sismica"), F.col("locationSource").alias("fuente_ubicacion"),
        F.col("magSource").alias("fuente_magnitud"), F.to_timestamp("updated").alias("actualizado_en"),
        "reportes_felt", "intensidad_cdi", "intensidad_mmi", "alerta_pager", "tsunami", "significancia",
        F.lit("usgs_historico").alias("fuente"),
        F.to_timestamp("updated").alias("version_ts"),
    )


# ------------------------------------------------------------------ 2. LIMPIEZA
def limpiar(df):
    """Devuelve (validos, rechazados) con el motivo de rechazo en cada fila rechazada."""
    df = (df.withColumn("evento_id", F.trim("evento_id"))
            .withColumn("lugar", F.trim("lugar"))
            .withColumn("tipo_evento", F.lower(F.trim("tipo_evento")))
            .withColumn("tipo_magnitud", F.lower(F.trim("tipo_magnitud")))
            .withColumn("estado_revision", F.lower(F.trim("estado_revision")))
            .withColumn("alerta_pager", F.lower(F.trim("alerta_pager")))
            .withColumn("red_sismica", F.lower(F.trim("red_sismica"))))
    motivo = (F.when(F.col("evento_id").isNull() | (F.col("evento_id") == ""), "id_faltante")
               .when(F.col("fecha_evento").isNull(), "fecha_invalida")
               .when(F.col("magnitud").isNull() | ~F.col("magnitud").between(-2, 10), "magnitud_invalida")
               .when(F.col("latitud").isNull() | ~F.col("latitud").between(-90, 90), "latitud_invalida")
               .when(F.col("longitud").isNull() | ~F.col("longitud").between(-180, 180), "longitud_invalida"))
    df = df.withColumn("motivo_rechazo", motivo)
    return df.filter("motivo_rechazo IS NULL").drop("motivo_rechazo"), df.filter("motivo_rechazo IS NOT NULL")


def deduplicar(df):
    """Un solo registro por evento: gana el revisado, luego el catalogo historico, luego el mas reciente."""
    w = Window.partitionBy("evento_id").orderBy(
        (F.col("estado_revision") == "reviewed").desc(),
        (F.col("fuente") == "usgs_historico").desc(),
        F.col("version_ts").desc_nulls_last())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "version_ts")


# ------------------------------------------------------------------ 3. ENRIQUECIMIENTO
def enriquecer(df):
    mapa_us = F.create_map(*[x for kv in ESTADOS_US.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    mapa_alias = F.create_map(*[x for kv in ALIAS_REGION.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    cola = F.regexp_extract("lugar", r",\s*([^,]+)$", 1)
    region = F.when(cola == "", F.col("lugar")).otherwise(cola)
    region = F.coalesce(mapa_us[region], region)
    region = F.regexp_replace(region, PREFIJO_DIRECCION, "")           # "South of the Fiji Islands" -> "Fiji Islands"
    region = F.regexp_replace(region, PREFIJO_COSTA, "")               # "Off the coast of Oregon" -> "Oregon"
    region = F.regexp_replace(region, r"(?i)\s+region$", "")           # "Fiji region" -> "Fiji"
    region = F.regexp_replace(region, r"(?i)\s+earthquake(\s+sequence)?$", "")   # sismos con nombre propio
    region = F.regexp_replace(region, r"^\d{4}\s+", "")                    # "2025 Drake Passage" -> "Drake Passage"
    region = F.initcap(F.trim(region))
    region = F.coalesce(mapa_alias[region], region)

    # "185 km SW of Nikolski, Alaska" -> 185 / SW / Nikolski
    patron = r"^(\d+(?:\.\d+)?)\s*km\s+([NSEW]{1,3})\s+of\s+(.+)$"
    resto = F.regexp_extract("lugar", patron, 3)
    ref_loc = F.regexp_extract(resto, r"^(.*?),\s*[^,]+$", 1)
    ref_loc = F.when(resto == "", None).when(ref_loc == "", resto).otherwise(ref_loc)

    return (df
            .withColumn("region", F.coalesce(region, F.lit("Desconocida")))
            .withColumn("categoria_magnitud",
                        F.when(F.col("magnitud") < 4, "menor").when(F.col("magnitud") < 5.5, "moderado")
                         .when(F.col("magnitud") < 7, "fuerte").otherwise("mayor"))
            .withColumn("categoria_profundidad",
                        F.when(F.col("profundidad_km").isNull(), None)
                         .when(F.col("profundidad_km") < 70, "superficial")
                         .when(F.col("profundidad_km") <= 300, "intermedia").otherwise("profunda"))
            # 10 km exactos = profundidad por defecto del USGS cuando no se pudo calcular (no es una medicion)
            .withColumn("profundidad_fija", F.col("profundidad_km") == 10.0)
            # --- derivadas de calendario
            .withColumn("anio", F.year("fecha_evento").cast("short"))
            .withColumn("mes", F.month("fecha_evento").cast("short"))
            .withColumn("dia_semana", (((F.dayofweek("fecha_evento") + 5) % 7) + 1).cast("short"))   # 1 = lunes
            .withColumn("hora_utc", F.hour("fecha_evento").cast("short"))
            # --- energia sismica liberada (J), relacion de Gutenberg-Richter: log10 E = 1.5 M + 4.8
            .withColumn("energia_joules", F.pow(F.lit(10.0), F.col("magnitud") * 1.5 + 4.8))
            # --- referencia textual del USGS
            .withColumn("distancia_ref_km",
                        F.when(F.col("lugar").rlike(patron), F.regexp_extract("lugar", patron, 1).cast("double")))
            .withColumn("direccion_ref",
                        F.when(F.col("lugar").rlike(patron), F.regexp_extract("lugar", patron, 2)))
            .withColumn("localidad_ref", F.trim(ref_loc))
            .withColumn("hemisferio_ns", F.when(F.col("latitud") >= 0, "N").otherwise("S"))
            .withColumn("hemisferio_ew", F.when(F.col("longitud") >= 0, "E").otherwise("W")))


def marcar_replicas(df):
    """Replica probable: en la misma celda de 3x3 grados hubo, en las 72 h previas, un evento principal
    (M >= UMBRAL_PRINCIPAL y al menos 0.5 mayor). Es una heuristica (no un modelo de secuencias sismicas)."""
    celda = F.concat_ws(":", F.floor(F.col("latitud") / 3).cast("int"), F.floor(F.col("longitud") / 3).cast("int"))
    w = (Window.partitionBy(celda).orderBy(F.col("fecha_evento").cast("long"))
         .rangeBetween(-72 * 3600, -1))
    previo = F.max("magnitud").over(w)
    minimo = F.greatest(F.lit(UMBRAL_PRINCIPAL), F.col("magnitud") + 0.5)
    return df.withColumn("replica_probable", F.coalesce(previo >= minimo, F.lit(False)))


# ------------------------------------------------------------------ 4. KMEANS (epicentros -> zonas)
def agrupar_zonas(df):
    """KMeans sobre coordenadas esfericas (x,y,z) para no romper en el antimeridiano (+-180)."""
    rad = F.radians
    base = (df.withColumn("x", F.cos(rad("latitud")) * F.cos(rad("longitud")))
              .withColumn("y", F.cos(rad("latitud")) * F.sin(rad("longitud")))
              .withColumn("z", F.sin(rad("latitud"))))
    vec = VectorAssembler(inputCols=["x", "y", "z"], outputCol="features").transform(base)
    modelo = KMeans(k=K_CLUSTERS, seed=42, maxIter=40, featuresCol="features", predictionCol="cluster_id").fit(vec)
    pred = modelo.transform(vec).drop("x", "y", "z", "features")
    return pred, modelo


# ------------------------------------------------------------------ 5. CARGA A POSTGRES
def sql(consulta, params=None):
    with psycopg2.connect(**PG) as con, con.cursor() as cur:
        cur.execute(consulta, params)


def escribir(df, tabla, truncar=True):
    (df.write.mode("overwrite" if truncar else "append").option("truncate", "true")
       .jdbc(JDBC_URL, tabla, properties=JDBC_PROPS))


def sql_merge():
    """Carga idempotente staging -> eventos (una transaccion, SQL generado desde COLS_EVENTOS).

    1. DELETE de versiones obsoletas: el USGS a veces corrige `fecha_evento` por milisegundos (o cambia de mes).
       Como la PK es (evento_id, fecha_evento), sin este paso la version corregida se INSERTARIA como un
       segundo evento. Se borra toda fila con el mismo evento_id y distinta fecha.
    2. INSERT ... ON CONFLICT DO UPDATE, que solo reescribe una fila si cambio algo distinto de `ingestado_en`
       (conserva la fecha de la primera carga; evita tuplas muertas en cada corrida).
    3. Poda de eventos que el catalogo ya no devuelve (p. ej. revisados por debajo de M4.0 o eliminados). Solo se
       poda dentro de los MESES que el historico cubre en esta corrida: una zona raw parcial nunca borra el resto.
    """
    cols = ", ".join(COLS_EVENTOS)
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLS_EVENTOS if c not in CLAVE + ("ingestado_en",))
    borrar = ("DELETE FROM sismos.eventos e USING sismos.eventos_staging s "
              "WHERE e.evento_id = s.evento_id AND e.fecha_evento <> s.fecha_evento")
    upsert = (f"INSERT INTO sismos.eventos AS t ({cols}) SELECT {cols} FROM sismos.eventos_staging "
              f"ON CONFLICT ({', '.join(CLAVE)}) DO UPDATE SET {sets} "
              f"WHERE (to_jsonb(t) - 'ingestado_en') IS DISTINCT FROM (to_jsonb(EXCLUDED) - 'ingestado_en')")
    podar = ("DELETE FROM sismos.eventos e WHERE e.fuente = 'usgs_historico' "
             "AND date_trunc('month', e.fecha_evento) IN "
             "(SELECT DISTINCT date_trunc('month', fecha_evento) FROM sismos.eventos_staging "
             "WHERE fuente = 'usgs_historico') "
             "AND NOT EXISTS (SELECT 1 FROM sismos.eventos_staging s WHERE s.evento_id = e.evento_id)")
    return f"{borrar}; {upsert}; {podar}"


def analizar_tablas():
    """VACUUM ANALYZE tras la carga: recupera tuplas muertas y refresca estadisticas del planificador."""
    con = psycopg2.connect(**PG)
    con.autocommit = True                       # VACUUM no puede correr dentro de una transaccion
    with con.cursor() as cur:
        for tabla in ("sismos.eventos", "sismos.agg_region_mes", "sismos.agg_ventana_horaria", "sismos.clusters"):
            cur.execute(f"VACUUM (ANALYZE) {tabla}")
    con.close()


def main():
    # ---- lectura
    feed = leer_feed()
    hist = leer_historico()
    n_feed, n_hist = feed.count(), hist.count()
    log(f"Leidos: feed={n_feed}  historico={n_hist}")
    crudo = feed.unionByName(hist)
    n_in = n_feed + n_hist

    # ---- limpieza, alcance, dedup
    validos, rechazados = limpiar(crudo)
    n_rech = rechazados.count()
    en_alcance = validos.filter(F.col("magnitud") >= MAG_MIN)
    n_fuera = validos.count() - en_alcance.count()
    unicos = deduplicar(en_alcance).cache()
    n_unicos = unicos.count()
    n_dup = n_in - n_rech - n_fuera - n_unicos
    log(f"Rechazados={n_rech}  fuera de alcance (M<{MAG_MIN})={n_fuera}  duplicados={n_dup}  unicos={n_unicos}")

    # ---- enriquecimiento, derivadas, KMeans
    eventos = marcar_replicas(enriquecer(unicos))
    eventos, modelo = agrupar_zonas(eventos)
    eventos = eventos.cache()
    log(f"Enriquecido; KMeans k={K_CLUSTERS} entrenado")

    # ---- agregaciones
    agg_mes = (eventos.groupBy("region", F.date_trunc("month", "fecha_evento").cast("date").alias("mes"))
               .agg(F.count("*").cast("int").alias("n_eventos"),
                    F.round(F.avg("magnitud"), 3).alias("magnitud_prom"),
                    F.max("magnitud").alias("magnitud_max"),
                    F.round(F.avg("profundidad_km"), 3).alias("profundidad_prom"),
                    F.sum(F.when(F.col("magnitud") >= 5.5, 1).otherwise(0)).cast("int").alias("n_fuertes")))

    agg_hora = (eventos.groupBy(F.window("fecha_evento", "1 hour").alias("w"), "region")
                .agg(F.count("*").cast("int").alias("n_eventos"), F.max("magnitud").alias("magnitud_max"))
                .filter("n_eventos >= 2")                       # solo ventanas con actividad agrupada (posibles enjambres)
                .select(F.col("w.start").alias("ventana_inicio"), "region", "n_eventos", "magnitud_max"))

    # centros reales de cada zona (x,y,z -> lat/lon): promediar longitudes fallaria en el antimeridiano
    centros = []
    for i, c in enumerate(modelo.clusterCenters()):
        x, y, z = float(c[0]), float(c[1]), float(c[2])
        centros.append((i, math.degrees(math.asin(z / math.sqrt(x * x + y * y + z * z))),
                        math.degrees(math.atan2(y, x))))
    df_centros = spark.createDataFrame(centros, "cluster_id int, centro_lat double, centro_lon double")
    agg_cluster = (eventos.groupBy("cluster_id")
                   .agg(F.count("*").cast("int").alias("n_eventos"),
                        F.round(F.avg("magnitud"), 3).alias("magnitud_prom"))
                   .join(df_centros, "cluster_id")
                   .select("cluster_id", "centro_lat", "centro_lon", "n_eventos", "magnitud_prom"))

    # ---- carga
    staging = eventos.withColumn("ingestado_en", F.current_timestamp()).select(*COLS_EVENTOS)
    escribir(staging, "sismos.eventos_staging")
    sql(sql_merge())
    log("eventos cargados (MERGE idempotente)")

    escribir(agg_mes, "sismos.agg_region_mes")
    escribir(agg_hora, "sismos.agg_ventana_horaria")
    escribir(agg_cluster, "sismos.clusters")
    analizar_tablas()
    log("agregados cargados; VACUUM ANALYZE ejecutado")

    # ---- calidad: muestra de rechazados + metricas de la corrida
    sql("TRUNCATE sismos.rechazados")
    muestra = (rechazados.select(F.col("fuente"), F.col("motivo_rechazo").alias("motivo"),
                                 F.concat_ws(" | ", "evento_id", "lugar").alias("detalle"))
               .limit(2000).withColumn("ts", F.current_timestamp()))
    escribir(muestra.select("ts", "fuente", "motivo", "detalle"), "sismos.rechazados", truncar=False)
    sql("INSERT INTO sismos.rechazados (fuente, motivo, detalle, cantidad) "
        "VALUES ('todas', 'fuera_de_alcance_magnitud', %s, %s)",
        (f"{n_fuera} eventos con magnitud < {MAG_MIN} descartados por criterio de alcance (no son errores)", n_fuera))

    dur = time.time() - T0
    sql("""INSERT INTO sismos.metricas_pipeline
           (componente, corrida_id, registros_in, registros_ok, registros_duplicados, registros_error, duracion_s)
           VALUES ('spark', %s, %s, %s, %s, %s, %s)""", (CORRIDA, n_in, n_unicos, n_dup, n_rech, dur))
    log(f"FIN  in={n_in} ok={n_unicos} duplicados={n_dup} rechazados={n_rech} fuera_alcance={n_fuera} "
        f"duracion={dur:.1f}s")


if __name__ == "__main__":
    main()
