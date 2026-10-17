-- ============================================================================
-- Capa 4: Consultas Analíticas Avanzadas - Monitor Sísmico Global
-- Archivo: 05_auditoria_clusters_kmeans_riesgo.sql
--
-- Pregunta de Negocio / Científica:
-- ¿Cuál es el grado de compacidad y dispersión espacial de los clústeres sismotectónicos
-- generados por el modelo KMeans (k=12), calculando la distancia ortodrómica de Haversine
-- desde cada epicentro individual hacia el centroide de su clúster, y cómo se jerarquizan
-- dichas zonas mediante un Índice Compuesto de Amenaza Sísmica normalizado con CUME_DIST() OVER?
--
-- Fundamento Geodésico y Metodológico:
-- 1. Distancia Ortodrómica de Haversine (Esfera Terrestre R = 6371.0 km):
--    - a = sin²(Delta_lat / 2) + cos(lat1) * cos(lat2) * sin²(Delta_lon / 2)
--    - d = 2 * R * asin(sqrt(a))
--    - Mide la distancia geodésica real en superficie sobre el elipsoide/esfera sin distorsiones
--      de proyecciones cartográficas planas.
-- 2. Métricas de Compacidad Espacial:
--    - Radio Medio: Grado de concentración de los eventos respecto a su centroide.
--    - Radio P95 (Percentil 95): Radio que contiene el 95% de la sismicidad del clúster (área de confinamiento sismogénico).
--    - Desviación Estándar: Variabilidad morfológica de la zona sismogénica.
-- 3. Índice Compuesto de Amenaza Sísmica (Composite Seismic Hazard Index):
--    - Integra multivariadamente: energía acumulada (PJ), conteo de sismos fuertes (M >= 5.5),
--      sismos mayores destructivos (M >= 7.0), eventos tsunamigénicos e intensidad máxima registrada.
-- 4. Normalización No Paramétrica por CUME_DIST():
--    - CUME_DIST() OVER (ORDER BY score_amenaza ASC) asigna el rango percentil acumulado [0.0, 1.0]
--      permitiendo clasificar objetivamente los clústeres en cuartiles y deciles de criticidad.
--
-- Invariantes Cumplidas:
-- - tipo_evento = 'earthquake'
-- - profundidad_fija = false para el cálculo de profundidad promedio medida
-- - Intervalo temporal acotado a 4 años completos: [2022-01-01 00:00:00+00, 2026-01-01 00:00:00+00)
-- - Sin consultas a eventos_staging.
-- ============================================================================

WITH geodistancia_epicentral AS (
    -- Paso 1: Cálculo de la distancia ortodrómica de Haversine entre cada sismo y el centroide del clúster
    SELECT
        e.evento_id,
        e.cluster_id,
        c.centro_lat,
        c.centro_lon,
        e.magnitud,
        e.profundidad_km,
        e.profundidad_fija,
        e.tsunami,
        e.energia_joules,
        -- Fórmula de Haversine en kilómetros con defensas numéricas ante desbordamientos de precisión
        (2.0 * 6371.0 * asin(
            sqrt(
                least(1.0, greatest(0.0,
                    sin(radians(e.latitud - c.centro_lat) / 2.0)^2 +
                    cos(radians(c.centro_lat)) * cos(radians(e.latitud)) *
                    sin(radians(e.longitud - c.centro_lon) / 2.0)^2
                ))
            )
        )) AS distancia_centroide_km
    FROM sismos.eventos e
    INNER JOIN sismos.clusters c ON e.cluster_id = c.cluster_id
    WHERE e.tipo_evento = 'earthquake'
      AND e.fecha_evento >= '2022-01-01 00:00:00+00'
      AND e.fecha_evento < '2026-01-01 00:00:00+00'
      AND e.cluster_id IS NOT NULL
),
auditoria_compacidad_cluster AS (
    -- Paso 2: Agregación de métricas de compacidad espacial e inventario de severidad sísmica
    SELECT
        g.cluster_id,
        count(*) AS total_sismos,
        round(avg(g.centro_lat)::numeric, 2) AS centroide_lat,
        round(avg(g.centro_lon)::numeric, 2) AS centroide_lon,
        -- Estadísticos de dispersión y compacidad geométrica
        round(avg(g.distancia_centroide_km)::numeric, 1) AS radio_medio_km,
        round(stddev_samp(g.distancia_centroide_km)::numeric, 1) AS stddev_radio_km,
        round(percentile_cont(0.50) WITHIN GROUP (ORDER BY g.distancia_centroide_km)::numeric, 1) AS radio_mediana_km,
        round(percentile_cont(0.95) WITHIN GROUP (ORDER BY g.distancia_centroide_km)::numeric, 1) AS radio_p95_km,
        round(max(g.distancia_centroide_km)::numeric, 1) AS radio_max_km,
        -- Variables determinantes de la amenaza geofísica
        round((sum(g.energia_joules) / 1e15)::numeric, 2) AS energia_acumulada_pj,
        count(*) FILTER (WHERE g.magnitud >= 5.5) AS n_sismos_fuertes_m55,
        count(*) FILTER (WHERE g.magnitud >= 7.0) AS n_sismos_mayores_m7,
        count(*) FILTER (WHERE g.tsunami) AS n_sismos_tsunami,
        max(g.magnitud) AS magnitud_maxima,
        -- Profundidad promedio evaluada estrictamente sobre eventos con hipocentro medido
        round(avg(g.profundidad_km) FILTER (WHERE NOT g.profundidad_fija)::numeric, 1) AS prof_media_medida_km
    FROM geodistancia_epicentral g
    GROUP BY g.cluster_id
),
evaluacion_amenaza AS (
    -- Paso 3: Construcción de la función de scoring compuesto de amenaza sísmica
    SELECT
        acc.cluster_id,
        acc.centroide_lat,
        acc.centroide_lon,
        acc.total_sismos,
        acc.radio_medio_km,
        acc.stddev_radio_km,
        acc.radio_mediana_km,
        acc.radio_p95_km,
        acc.radio_max_km,
        acc.energia_acumulada_pj,
        acc.n_sismos_fuertes_m55,
        acc.n_sismos_mayores_m7,
        acc.n_sismos_tsunami,
        acc.magnitud_maxima,
        acc.prof_media_medida_km,
        -- Score multivariado ponderado
        (
            (ln(acc.energia_acumulada_pj + 1.0) * 15.0) +
            (acc.n_sismos_fuertes_m55 * 0.8) +
            (acc.n_sismos_mayores_m7 * 12.0) +
            (acc.n_sismos_tsunami * 4.0) +
            (acc.magnitud_maxima * 5.0)
        ) AS score_amenaza_crudo
    FROM auditoria_compacidad_cluster acc
)
SELECT
    ea.cluster_id,
    ea.centroide_lat,
    ea.centroide_lon,
    ea.total_sismos,
    ea.radio_medio_km,
    ea.radio_p95_km,
    ea.energia_acumulada_pj,
    ea.n_sismos_fuertes_m55,
    ea.n_sismos_mayores_m7,
    ea.n_sismos_tsunami,
    ea.magnitud_maxima,
    ea.prof_media_medida_km,
    round(ea.score_amenaza_crudo::numeric, 1) AS score_amenaza,
    -- Normalización acumulada CUME_DIST()
    round(cume_dist() OVER (ORDER BY ea.score_amenaza_crudo ASC)::numeric, 4) AS cume_dist_amenaza,
    CASE
        WHEN cume_dist() OVER (ORDER BY ea.score_amenaza_crudo ASC) >= 0.80 THEN 'Nivel Crítico (Top 20% Global)'
        WHEN cume_dist() OVER (ORDER BY ea.score_amenaza_crudo ASC) >= 0.50 THEN 'Nivel Alto'
        WHEN cume_dist() OVER (ORDER BY ea.score_amenaza_crudo ASC) >= 0.25 THEN 'Nivel Moderado'
        ELSE 'Nivel Bajo'
    END AS clasificacion_amenaza_sismica
FROM evaluacion_amenaza ea
ORDER BY ea.score_amenaza_crudo DESC;

