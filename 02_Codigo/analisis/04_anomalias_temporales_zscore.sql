-- ============================================================================
-- Capa 4: Consultas Analíticas Avanzadas - Monitor Sísmico Global
-- Archivo: 04_anomalias_temporales_zscore.sql
--
-- Pregunta de Negocio / Científica:
-- ¿Cuáles regiones exhiben picos anómalos o enjambres sísmicos extremos en su
-- serie temporal mensual (Z-Score >= 2.0 y variación porcentual intermensual
-- brusca respecto al mes previo), y cuál es el grado de concordancia y fidelidad
-- entre el cálculo directo sobre la tabla de hechos y la tabla pre-agregada sismos.agg_region_mes?
--
-- Fundamento Estadístico y Metodológico:
-- 1. Estandarización y Puntuación Z (Z-Score):
--    - Z = (X - mu) / sigma
--      donde X es el número de eventos en el mes para una región dada,
--      mu es la media de eventos mensuales de dicha región (AVG() OVER),
--      y sigma es la desviación estándar muestral mensual (STDDEV_SAMP() OVER).
--    - Identifica desviaciones estadísticas fuera del régimen de sismicidad habitual
--      (ej. secuencias mayores de réplicas en Turquía, Grecia, Taiwán o Filipinas).
-- 2. Variación Porcentual Intermensual (LAG):
--    - Delta% = ((N_t - N_{t-1}) / N_{t-1}) * 100
--    - Detecta aceleraciones abruptas en la tasa de sismicidad mensual.
-- 3. Auditoría de Conciliación de Datos (Fact vs. Aggregate):
--    - Comparación exhaustiva entre sismos.eventos (Capa 3 hechos) y sismos.agg_region_mes (Capa 3 derivados precalculados por Spark).
--    - Delta_conteo = n_eventos_hechos - n_eventos_agg debe ser 0 en toda partición procesada.
--
-- Invariantes Cumplidas:
-- - tipo_evento = 'earthquake'
-- - Intervalo temporal acotado a 4 años completos: [2022-01-01 00:00:00+00, 2026-01-01 00:00:00+00)
-- - Sin consultas a eventos_staging.
-- ============================================================================

WITH sismicidad_mensual_hechos AS (
    -- Paso 1: Agregación mensual a partir de la tabla de hechos particionada
    SELECT
        date_trunc('month', e.fecha_evento)::date AS mes,
        e.region,
        count(*) AS n_eventos_hechos,
        round(avg(e.magnitud)::numeric, 3) AS mag_prom_hechos,
        max(e.magnitud) AS mag_max_hechos,
        sum(e.energia_joules) AS energia_total_joules
    FROM sismos.eventos e
    WHERE e.tipo_evento = 'earthquake'
      AND e.fecha_evento >= '2022-01-01 00:00:00+00'
      AND e.fecha_evento < '2026-01-01 00:00:00+00'
      AND e.region IS NOT NULL
    GROUP BY date_trunc('month', e.fecha_evento)::date, e.region
),
auditoria_contraste_agregado AS (
    -- Paso 2: Conciliación cruzada con la tabla agregada sismos.agg_region_mes
    SELECT
        h.mes,
        h.region,
        h.n_eventos_hechos,
        COALESCE(a.n_eventos, 0) AS n_eventos_agregado,
        (h.n_eventos_hechos - COALESCE(a.n_eventos, 0)) AS discrepancia_conteo,
        h.mag_prom_hechos,
        a.magnitud_prom AS mag_prom_agregado,
        round(abs(h.mag_prom_hechos - COALESCE(a.magnitud_prom, 0))::numeric, 4) AS discrepancia_mag_prom,
        h.mag_max_hechos,
        h.energia_total_joules
    FROM sismicidad_mensual_hechos h
    LEFT JOIN sismos.agg_region_mes a
      ON h.region = a.region AND h.mes = a.mes
),
dinamica_temporal_zscore AS (
    -- Paso 3: Cálculo de variación intermensual (LAG) y Z-Score regional (AVG, STDDEV_SAMP)
    SELECT
        aca.mes,
        aca.region,
        aca.n_eventos_hechos,
        aca.n_eventos_agregado,
        aca.discrepancia_conteo,
        aca.mag_prom_hechos,
        aca.mag_max_hechos,
        round((aca.energia_total_joules / 1e15)::numeric, 2) AS energia_pj,
        -- Eventos del mes cronológicamente precedente dentro de la región
        LAG(aca.n_eventos_hechos) OVER w_region_tiempo AS n_eventos_mes_previo,
        -- Variación porcentual intermensual
        round((
            (aca.n_eventos_hechos - LAG(aca.n_eventos_hechos) OVER w_region_tiempo)::numeric
            / NULLIF(LAG(aca.n_eventos_hechos) OVER w_region_tiempo, 0)::numeric * 100.0
        ), 1) AS var_pct_intermensual,
        -- Parámetros de la distribución normal regional de 48 meses
        round(avg(aca.n_eventos_hechos) OVER w_region_total::numeric, 2) AS media_mensual_region,
        round(stddev_samp(aca.n_eventos_hechos) OVER w_region_total::numeric, 2) AS stddev_mensual_region,
        -- Z-Score estandarizado
        round((
            (aca.n_eventos_hechos - avg(aca.n_eventos_hechos) OVER w_region_total)
            / NULLIF(stddev_samp(aca.n_eventos_hechos) OVER w_region_total, 0)
        )::numeric, 2) AS z_score_actividad
    FROM auditoria_contraste_agregado aca
    WINDOW
        w_region_tiempo AS (PARTITION BY aca.region ORDER BY aca.mes),
        w_region_total  AS (PARTITION BY aca.region)
)
SELECT
    dtz.mes,
    dtz.region,
    dtz.n_eventos_hechos,
    dtz.n_eventos_mes_previo,
    dtz.var_pct_intermensual,
    dtz.media_mensual_region,
    dtz.stddev_mensual_region,
    dtz.z_score_actividad,
    dtz.discrepancia_conteo AS delta_auditoria_agg,
    dtz.mag_max_hechos,
    dtz.energia_pj,
    CASE
        WHEN dtz.z_score_actividad >= 3.0 THEN 'Anomalía Extrema (Z >= +3.0 Sigma)'
        WHEN dtz.z_score_actividad >= 2.0 THEN 'Anomalía Severa (+2.0 <= Z < +3.0 Sigma)'
        WHEN dtz.z_score_actividad >= 1.5 THEN 'Alerta Moderada (+1.5 <= Z < +2.0 Sigma)'
        WHEN dtz.z_score_actividad <= -2.0 THEN 'Calma Tectónica Atípica (Z <= -2.0 Sigma)'
        ELSE 'Actividad Sísmica Típica'
    END AS clasificacion_anomalia
FROM dinamica_temporal_zscore dtz
-- Filtrar anomalías estadísticamente significativas con al menos 20 eventos en el mes anómalo
WHERE dtz.z_score_actividad >= 2.0 AND dtz.n_eventos_hechos >= 20
ORDER BY dtz.z_score_actividad DESC, dtz.mes DESC
LIMIT 20;

