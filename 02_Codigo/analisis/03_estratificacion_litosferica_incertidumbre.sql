-- ============================================================================
-- Capa 4: Consultas Analíticas Avanzadas - Monitor Sísmico Global
-- Archivo: 03_estratificacion_litosferica_incertidumbre.sql
--
-- Pregunta de Negocio / Científica:
-- ¿Cómo varía la distribución de sismos, la calidad de la cobertura azimutal
-- (gap, nst, rms), la incertidumbre vertical (percentiles de error_profundidad_km)
-- y la propensión a generar tsunamis a lo largo de los distintos pisos litosféricos
-- de la zona de subducción de Wadati-Benioff?
--
-- Fundamento Físico y Sismológico:
-- 1. Zona de Wadati-Benioff y Estratificación Litosférica:
--    - En zonas de subducción, la placa oceánica fría se introduce bajo la litosfera
--      continental u oceánica adyacente, generando sismicidad a profundidades de hasta 700 km.
--    - Pisos sismotectónicos:
--      * Piso 1: Cortical Somera (< 35 km) - contacto interplaca sismogénico principal.
--      * Piso 2: Litosférica Superficial (35 - 70 km) - transición Moho / base de litosfera.
--      * Piso 3: Benioff Intermedia Alta (70 - 150 km) - metamorfismo y deshidratación de serpentinas.
--      * Piso 4: Benioff Intermedia Baja (150 - 300 km) - sismicidad intra-placa de la losa descendente.
--      * Piso 5: Manto Superior Profundo (300 - 500 km) - fragilidad por cizalla bajo confinamiento.
--      * Piso 6: Zona de Transición Ultra-profunda (>= 500 km) - cambios de fase mineral (olivino a espinela/ringwoodita).
-- 2. Calidad de Cobertura e Incertidumbre de Red:
--    - gap: Brecha azimutal sin estaciones (grados). Menor a 180° indica cobertura angular balanceada.
--    - nst: Número de estaciones que registraron fases sísmicas (P, S).
--    - rms: Error residual medio cuadrático de tiempos de viaje (segundos).
--    - error_profundidad_km: Dispersión vertical del modelo de inversión geofísica.
--      (Regla 9 de Calidad: Válido exclusivamente cuando profundidad_fija = false).
-- 3. Riesgo Tsunamigénico:
--    - Capacidad de deformación cosísmica del lecho oceánico: se restringe casi en su totalidad
--      a los pisos superficiales (< 70 km) combinados con magnitudes severas (M >= 6.5).
--
-- Invariantes Cumplidas:
-- - tipo_evento = 'earthquake'
-- - profundidad_fija = false (mandatorio: no incorporar profundidades prefijadas de 10 km)
-- - Intervalo temporal acotado a 4 años completos: [2022-01-01 00:00:00+00, 2026-01-01 00:00:00+00)
-- - Sin consultas a eventos_staging.
-- ============================================================================

WITH estratos_litosfericos AS (
    SELECT
        e.evento_id,
        e.fecha_evento,
        e.region,
        e.magnitud,
        e.profundidad_km,
        e.categoria_profundidad,
        e.gap,
        e.nst,
        e.rms,
        e.error_profundidad_km,
        e.tsunami,
        e.energia_joules,
        CASE
            WHEN e.profundidad_km < 35.0  THEN '1. Cortical Somera (< 35 km)'
            WHEN e.profundidad_km < 70.0  THEN '2. Litosférica Superficial (35 - 70 km)'
            WHEN e.profundidad_km < 150.0 THEN '3. Benioff Intermedia Alta (70 - 150 km)'
            WHEN e.profundidad_km < 300.0 THEN '4. Benioff Intermedia Baja (150 - 300 km)'
            WHEN e.profundidad_km < 500.0 THEN '5. Manto Superior Profundo (300 - 500 km)'
            ELSE                               '6. Zona de Transición Ultra-profunda (>= 500 km)'
        END AS piso_wadati_benioff
    FROM sismos.eventos e
    WHERE e.tipo_evento = 'earthquake'
      AND e.profundidad_fija = false
      AND e.fecha_evento >= '2022-01-01 00:00:00+00'
      AND e.fecha_evento < '2026-01-01 00:00:00+00'
)
SELECT
    el.piso_wadati_benioff,
    count(*) AS total_eventos,
    round(avg(el.profundidad_km)::numeric, 1) AS prof_media_km,
    round(avg(el.magnitud)::numeric, 2) AS mag_promedio,
    max(el.magnitud) AS mag_maxima,
    -- Cobertura y calidad geométrica de la red sismológica
    round(avg(el.gap)::numeric, 1) AS gap_azimutal_medio_grados,
    round(avg(el.nst)::numeric, 1) AS estaciones_nst_promedio,
    round(avg(el.rms)::numeric, 2) AS residuo_rms_medio_s,
    -- Distribución de incertidumbre vertical (Percentiles continuos P25, P50, P75, P90)
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY el.error_profundidad_km)::numeric, 2) AS err_prof_p25_km,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY el.error_profundidad_km)::numeric, 2) AS err_prof_mediana_km,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY el.error_profundidad_km)::numeric, 2) AS err_prof_p75_km,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY el.error_profundidad_km)::numeric, 2) AS err_prof_p90_km,
    -- Liberación energética
    round((sum(el.energia_joules) / 1e15)::numeric, 2) AS energia_petajoules,
    -- Indicadores de amenaza y riesgo tsunamigénico
    count(*) FILTER (WHERE el.tsunami) AS sismos_con_alerta_tsunami,
    count(*) FILTER (WHERE el.tsunami AND el.magnitud >= 7.0) AS tsunamis_destructivos_m7,
    round((100.0 * count(*) FILTER (WHERE el.tsunami) / NULLIF(count(*), 0))::numeric, 2) AS tasa_tsunami_pct,
    CASE
        WHEN min(el.profundidad_km) < 70.0 THEN 'Riesgo Tsunamigénico Primario (Deformación de Fondo Marino Posible)'
        ELSE 'Riesgo Tsunamigénico Nulo a Mínimo (Disipación Elástica Profunda)'
    END AS evaluacion_tsunami_litosfera
FROM estratos_litosfericos el
GROUP BY el.piso_wadati_benioff
ORDER BY min(el.profundidad_km);

