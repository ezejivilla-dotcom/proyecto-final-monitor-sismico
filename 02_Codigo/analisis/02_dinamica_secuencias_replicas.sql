-- ============================================================================
-- Capa 4: Consultas Analíticas Avanzadas - Monitor Sísmico Global
-- Archivo: 02_dinamica_secuencias_replicas.sql
--
-- Pregunta de Negocio / Científica:
-- ¿Cómo se comporta la dinámica espaciotemporal y la tasa de decaimiento temporal
-- de secuencias sísmicas y réplicas intra-clúster (tiempos inter-evento Delta-t,
-- salto vertical de hipocentros medidos y tendencia móvil de magnitud y profundidad)?
--
-- Fundamento Físico y Metodológico:
-- 1. Dispersión Espaciotemporal:
--    - Particionamiento por zonas tectónicas discretizadas (`cluster_id` de KMeans k=12).
--    - Tiempos inter-evento (Delta-t): Intervalo transcurrido entre el evento actual y
--      sus vecinos temporalmente contiguos (LAG hacia el antecesor, LEAD hacia el sucesor).
-- 2. Migración Hipocentral (Salto Vertical):
--    - Delta-z = Z_i - Z_{i-1} entre sismos consecutivos medidos en el mismo clúster.
--    - Permite detectar migración ascendente/descendente de fluidos o propagación de ruptura.
--    - OBLIGATORIO: profundidad_fija = false para evitar el sesgo artificial de los 10 km por defecto.
-- 3. Suavizado Causal por Ventana Móvil:
--    - Media móvil de 5 eventos con AVG() OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
--      para monitorear la tendencia de energía y profundidad descartando ruido local de alta frecuencia.
--
-- Invariantes Cumplidas:
-- - tipo_evento = 'earthquake'
-- - profundidad_fija = false (riguroso para análisis de hipocentros reales)
-- - Intervalo temporal acotado a 4 años completos: [2022-01-01 00:00:00+00, 2026-01-01 00:00:00+00)
-- - Sin consultas a eventos_staging.
-- ============================================================================

WITH secuencias_cluster AS (
    SELECT
        e.evento_id,
        e.fecha_evento,
        e.cluster_id,
        c.centro_lat AS cluster_lat,
        c.centro_lon AS cluster_lon,
        e.region,
        e.magnitud,
        e.profundidad_km,
        e.replica_probable,
        -- Funciones de ventana temporal contigua (LAG y LEAD)
        LAG(e.fecha_evento) OVER w_cluster AS fecha_evento_prev,
        LEAD(e.fecha_evento) OVER w_cluster AS fecha_evento_next,
        -- Tiempos inter-evento Delta-t (en horas)
        round((extract(epoch FROM (e.fecha_evento - LAG(e.fecha_evento) OVER w_cluster)) / 3600.0)::numeric, 2) AS delta_t_prev_horas,
        round((extract(epoch FROM (LEAD(e.fecha_evento) OVER w_cluster - e.fecha_evento)) / 3600.0)::numeric, 2) AS delta_t_next_horas,
        -- Salto vertical de profundidad real entre sismos sucesivos (Delta-z)
        round((e.profundidad_km - LAG(e.profundidad_km) OVER w_cluster)::numeric, 2) AS salto_vertical_prof_km,
        round(abs(e.profundidad_km - LAG(e.profundidad_km) OVER w_cluster)::numeric, 2) AS salto_vertical_abs_km,
        -- Medias móviles causales de 5 eventos (magnitud y profundidad)
        round(avg(e.magnitud) OVER (
            PARTITION BY e.cluster_id
            ORDER BY e.fecha_evento
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )::numeric, 2) AS media_movil_5_eventos_mag,
        round(avg(e.profundidad_km) OVER (
            PARTITION BY e.cluster_id
            ORDER BY e.fecha_evento
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        )::numeric, 2) AS media_movil_5_eventos_prof_km
    FROM sismos.eventos e
    INNER JOIN sismos.clusters c ON e.cluster_id = c.cluster_id
    WHERE e.tipo_evento = 'earthquake'
      AND e.profundidad_fija = false
      AND e.fecha_evento >= '2022-01-01 00:00:00+00'
      AND e.fecha_evento < '2026-01-01 00:00:00+00'
    WINDOW w_cluster AS (PARTITION BY e.cluster_id ORDER BY e.fecha_evento)
)
SELECT
    s.cluster_id,
    s.evento_id,
    s.fecha_evento,
    s.region,
    s.magnitud,
    s.media_movil_5_eventos_mag,
    s.profundidad_km,
    s.salto_vertical_prof_km,
    s.media_movil_5_eventos_prof_km,
    s.delta_t_prev_horas,
    s.delta_t_next_horas,
    s.replica_probable,
    CASE
        WHEN s.delta_t_prev_horas <= 1.0 THEN 'Enjambre Inmediato / Réplica Crítica (< 1h)'
        WHEN s.delta_t_prev_horas <= 12.0 THEN 'Secuencia Agrupada Rápida (1h - 12h)'
        WHEN s.delta_t_prev_horas <= 48.0 THEN 'Interacción Tectónica Tardía (12h - 48h)'
        ELSE 'Sismicidad Tectónica de Fondo (> 48h)'
    END AS regimen_secuencia,
    CASE
        WHEN s.salto_vertical_prof_km > 30.0 THEN 'Migración Hacia el Manto (+ >30 km)'
        WHEN s.salto_vertical_prof_km < -30.0 THEN 'Migración Ascendente Cortical (- >30 km)'
        ELSE 'Rango Isodepth Estable (+/- 30 km)'
    END AS dinamica_vertical
FROM secuencias_cluster s
WHERE s.delta_t_prev_horas IS NOT NULL
ORDER BY s.cluster_id, s.fecha_evento
LIMIT 25;

