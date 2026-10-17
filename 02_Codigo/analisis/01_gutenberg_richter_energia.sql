-- ============================================================================
-- Capa 4: Consultas Analíticas Avanzadas - Monitor Sísmico Global
-- Archivo: 01_gutenberg_richter_energia.sql
--
-- Pregunta de Negocio / Científica:
-- ¿Cuáles son los parámetros sismotectónicos de la ley de Gutenberg-Richter
-- (parámetro a, b-value y bondad de ajuste R²) y cuál es la distribución del
-- presupuesto acumulado de energía sísmica liberada (en Joules y Petajoules)
-- por región geográfica en el período 2022-2025?
--
-- Fundamento Físico y Matemático:
-- 1. Ley de Gutenberg-Richter: log10(N >= M) = a - b * M
--    - N(>= M): Frecuencia acumulada de eventos con magnitud mayor o igual a M.
--    - b-value: Tasa relativa entre sismos pequeños y grandes (típicamente ~1.0;
--      valores < 1.0 denotan zonas bajo alto estrés cortical o propensas a megasismos).
--    - a-value: Nivel de productividad sismotectónica regional (log10 del número acumulado proyectado a M=0).
--    - R²: Coeficiente de determinación lineal del ajuste por mínimos cuadrados ordinarios.
-- 2. Presupuesto Energético de Gutenberg-Richter:
--    - E = 10^(1.5 * M + 4.8) Joules (calculado en Spark y persistido en `energia_joules`).
--    - 1 Petajoule (PJ) = 10^15 Joules.
--
-- Invariantes Cumplidas:
-- - tipo_evento = 'earthquake'
-- - Intervalo temporal acotado a 4 años completos: [2022-01-01 00:00:00+00, 2026-01-01 00:00:00+00)
-- - Sin consultas a eventos_staging.
-- ============================================================================

WITH conteos_por_magnitud AS (
    -- Paso 1: Discretización por bins de magnitud (resolución 0.1) y conteo exacto por región
    SELECT
        e.region,
        round(e.magnitud::numeric, 1)::double precision AS mag_bin,
        count(*) AS n_exacto,
        sum(e.energia_joules) AS energia_bin_joules
    FROM sismos.eventos e
    WHERE e.tipo_evento = 'earthquake'
      AND e.fecha_evento >= '2022-01-01 00:00:00+00'
      AND e.fecha_evento < '2026-01-01 00:00:00+00'
      AND e.region IS NOT NULL
    GROUP BY e.region, round(e.magnitud::numeric, 1)
),
frecuencias_acumuladas AS (
    -- Paso 2: Cálculo de frecuencias acumuladas N(>= M) con función de ventana SUM() OVER
    -- Ordenando DESC por bin de magnitud para obtener el conteo acumulativo hacia sismos mayores
    SELECT
        c.region,
        c.mag_bin,
        c.n_exacto,
        c.energia_bin_joules,
        sum(c.n_exacto) OVER (
            PARTITION BY c.region
            ORDER BY c.mag_bin DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS n_acumulado
    FROM conteos_por_magnitud c
),
regresion_gutenberg_richter AS (
    -- Paso 3: Ajuste lineal mediante funciones agregadas estadísticas de PostgreSQL:
    --   Y = log10(n_acumulado)
    --   X = mag_bin
    --   Pendiente (slope) = -b  ==>  b = -REGR_SLOPE(Y, X)
    --   Intercepto = a          ==>  a = REGR_INTERCEPT(Y, X)
    --   R² = REGR_R2(Y, X)
    SELECT
        f.region,
        count(*) AS puntos_ajuste,
        sum(f.n_exacto) AS total_eventos,
        max(f.mag_bin) AS mag_maxima,
        sum(f.energia_bin_joules) AS energia_total_joules,
        -- log(numeric) en PostgreSQL calcula logaritmo base 10
        round((-regr_slope(log(f.n_acumulado::numeric)::double precision, f.mag_bin))::numeric, 3) AS b_value,
        round((regr_intercept(log(f.n_acumulado::numeric)::double precision, f.mag_bin))::numeric, 3) AS a_parametro,
        round((regr_r2(log(f.n_acumulado::numeric)::double precision, f.mag_bin))::numeric, 4) AS r2_ajuste
    FROM frecuencias_acumuladas f
    WHERE f.n_acumulado > 0
    GROUP BY f.region
    -- Umbral de significancia estadística: mínimo 8 bins de magnitud y al menos 50 sismos registrados
    HAVING count(*) >= 8 AND sum(f.n_exacto) >= 50
)
SELECT
    r.region,
    r.total_eventos,
    r.mag_maxima,
    r.b_value,
    r.a_parametro,
    r.r2_ajuste,
    round(r.energia_total_joules::numeric, 0) AS energia_total_joules,
    round((r.energia_total_joules / 1e15)::numeric, 2) AS energia_petajoules,
    round((100.0 * r.energia_total_joules / sum(r.energia_total_joules) OVER ())::numeric, 2) AS pct_energia_global_top,
    CASE
        WHEN r.b_value < 0.85 THEN 'Alto Estrés Tectónico / Propenso a Megasismos (b < 0.85)'
        WHEN r.b_value <= 1.15 THEN 'Régimen Tectónico Estándar (0.85 <= b <= 1.15)'
        ELSE 'Dominancia de Enjambres / Microsismicidad Relativa (b > 1.15)'
    END AS interpretacion_geofisica
FROM regresion_gutenberg_richter r
ORDER BY r.energia_total_joules DESC
LIMIT 20;

