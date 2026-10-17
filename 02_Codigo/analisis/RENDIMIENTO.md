# Capa 4 — Fase 2: Benchmark de Rendimiento y Documentación Técnica

- **Motor:** PostgreSQL 16.x (Docker Linux container)
- **Base de Datos:** `sismos` (81,044 eventos sísmicos en `sismos.eventos`)
- **Topología:** Particionamiento mensual por `fecha_evento` (85 particiones)
- **Metodología:** 3 iteraciones warm-up descartadas y 10 iteraciones medidas por escenario en transacciones aisladas (`BEGIN; SET LOCAL ...; EXPLAIN ...; ROLLBACK;`).

---

## 1. Tabla Comparativa Consolidada

| ID Escenario | Part. | Filas | Coste Total | Plan Med (ms) | Exec Med (ms) | Exec Media (ms) | Exec P95 (ms) | Exec Desv (ms) | Shared Hit | Shared Read | Buffer (KB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `EXP_1A_PRUNING_1M_SARG` | 1 | 1 | 71.97 | 0.044 | 0.290 | 0.291 | 0.310 | 0.011 | 47 | 0 | 376 |
| `EXP_1B_NO_PRUNING_1M_FUNC` | 85 | 1 | 4,854.67 | 1.373 | 24.174 | 24.133 | 24.327 | 0.148 | 3,431 | 0 | 27,448 |
| `EXP_1C_RANGO_48M_COMPLETO` | 48 | 1 | 4,498.54 | 1.105 | 32.278 | 32.637 | 35.618 | 1.461 | 2,726 | 0 | 21,808 |
| `EXP_2A_IDX_MAGNITUD` | 85 | 669 | 2,221.30 | 1.501 | 1.110 | 1.098 | 1.128 | 0.029 | 758 | 0 | 6,064 |
| `EXP_2B_SEQ_MAGNITUD` | 85 | 669 | 4,447.69 | 1.661 | 16.534 | 16.549 | 16.768 | 0.106 | 3,431 | 0 | 27,448 |
| `EXP_3A_IDX_CLUSTER` | 3 | 399 | 180.65 | 0.088 | 0.196 | 0.200 | 0.211 | 0.008 | 67 | 0 | 536 |
| `EXP_3B_SEQ_CLUSTER` | 3 | 399 | 222.01 | 0.088 | 0.679 | 0.678 | 0.695 | 0.009 | 156 | 0 | 1,248 |
| `EXP_4A_IDX_REGION` | 12 | 1,194 | 703.21 | 0.327 | 0.616 | 0.617 | 0.631 | 0.006 | 284 | 0 | 2,272 |
| `EXP_4B_SEQ_REGION` | 12 | 1,194 | 855.03 | 0.335 | 2.222 | 2.226 | 2.283 | 0.024 | 601 | 0 | 4,808 |

> **Notas de Columnas:**
> - `Part.`: Particiones hijas (`eventos_YYYY_MM`) abiertas y escaneadas.
> - `Coste Total`: Coste estimado por el planificador de PostgreSQL.
> - `Plan Med (ms)`: Mediana de tiempo de planificación (*Planning Time*).
> - `Exec Med (ms)`: Mediana de tiempo de ejecución real (*Execution Time*).
> - `Exec P95 (ms)`: Percentil 95 del tiempo de ejecución.
> - `Shared Hit / Read`: Bloques de memoria de 8 KB leídos de buffer pool o disco.
> - `Buffer (KB)`: Volumen total de I/O de buffer procesado.

---

## 2. Hallazgos Clave y Conclusiones Arquitectónicas

1. **La Poda de Particiones (Partition Pruning) es el Factor Más Crítico:**
   El filtrado sargable por rango de `fecha_evento` reduce el escaneo de 85 particiones a solo 1 partición en consultas mensuales, logrando aceleraciones drásticas y evitando leer decenas de megabytes en páginas no relacionadas.
2. **Impacto Devastador de Predicados Funcionales:**
   El uso de funciones como `DATE_TRUNC` sobre la clave de partición destruye la sargabilidad (*Non-SARGable*), impidiendo la poda en tiempo de planificación y forzando la apertura de todas las particiones del catálogo.
3. **Efectividad de Índices Secundarios en Alta Selectividad:**
   El índice B-Tree `ix_eventos_magnitud` sobre `magnitud >= 6.0` (selectividad < 1%) reduce en ~78% el consumo de buffers y acelera significativamente la consulta frente al barrido secuencial completo.
4. **Sinergia Pruning + Index Scan:**
   La combinación de acotar el rango temporal (poda a 3 meses) más índice local sobre `cluster_id` demuestra el comportamiento óptimo de data warehousing particionado.

---

## 3. Análisis Cuantitativo por Escenario

### 3.1 Escenario 1: Poda de Particiones (Partition Pruning)

- **Predicado Sargable vs No Sargable:**
  - `EXP_1A_PRUNING_1M_SARG` escanea **1 partición** (`eventos_2024_06`) con tiempo mediano de **0.290 ms**.
  - `EXP_1B_NO_PRUNING_1M_FUNC` evalúa función `DATE_TRUNC`, forzando escaneo en **85 particiones** con **24.174 ms**.
  - `EXP_1C_RANGO_48M_COMPLETO` consulta 4 años (2022-2025), podando 48 particiones en **32.278 ms**.
- **Aceleración (Speedup):**
  - **83.36× de aceleración** de 1A respecto a 1B.
- **Eficiencia en Buffers (Bloques de 8 KB):**
  - Variante 1A: **47 bloques** (376 KB).
  - Variante 1B: **3431 bloques** (26.80 MB).
  - **Ahorro de Buffers:** **98.63% de reducción** en páginas (3384 bloques ahorrados).
- **Sobrecarga de Planificación:**
  - El tiempo de planificación en 1B se dispara a **1.373 ms** por metadatos de 85 particiones.

### 3.2 Escenario 2: Index Scan vs Sequential Scan (`magnitud >= 6.0`)

- **Selectividad:** La condición filtra sismos de alta energía, retornando **669 filas** (0.83% de selectividad).
- **Rendimiento:**
  - Index Scan (`EXP_2A_IDX_MAGNITUD`): **1.110 ms** (Coste: 2221.30).
  - Sequential Scan (`EXP_2B_SEQ_MAGNITUD`): **16.534 ms** (Coste: 4447.69).
  - **Speedup:** **14.90× de aceleración** con índice `ix_eventos_magnitud`.
- **Ahorro de Buffers:**
  - De **3,431 bloques** a **758 bloques** (26.80 MB vs 5.92 MB).
  - **Ahorro:** **77.91% de reducción** en páginas (2,673 bloques evitados).

### 3.3 Escenario 3: Sinergia de Pruning e Índice (`cluster_id = 0` en Q1 2024)

- **Filtro Combinado:** El predicado temporal acota la búsqueda a Q1 2024 (3 particiones: ene, feb, mar 2024).
- **Rendimiento:**
  - Index Scan (`EXP_3A_IDX_CLUSTER`): **0.196 ms** (Coste: 180.65).
  - Sequential Scan (`EXP_3B_SEQ_CLUSTER`): **0.679 ms** (Coste: 222.01).
  - **Speedup:** **3.47× de aceleración**.
- **Ahorro de Buffers:**
  - De **156 bloques** a **67 bloques**.
  - **Ahorro:** **57.05% de reducción** en páginas de buffer.

### 3.4 Escenario 4: Evaluación de Acceso Regional (`region = 'Japan'` en 2024)

- **Comportamiento del Optimizador (CBO):**
  - Rango anual (12 particiones), Japón devuelve **1,194 filas**.
  - Index Scan (`EXP_4A_IDX_REGION`): **0.616 ms** (284 blq).
  - Sequential Scan (`EXP_4B_SEQ_REGION`): **2.222 ms** (601 blq).
  - **Ratio de Comparación:** Index vs Seq Scan speedup de **3.61×**.
  - **Análisis Técnico:** En particiones densas, el costo de traversal B-Tree y acceso aleatorio compite con el escaneo secuencial en bloque. En particiones dispersas, el índice B-Tree mantiene superioridad rotunda.

---

## 4. Extractos Reales de Planes EXPLAIN (TEXT)

### Plan: `EXP_1A_PRUNING_1M_SARG`

> **Descripción:** 1A: 1 mes jun-2024 rango directo con partition pruning ON

```text
Aggregate  (cost=71.95..71.97 rows=1 width=72) (actual time=0.275..0.276 rows=1 loops=1)
  Output: count(*), avg(eventos.magnitud), max(eventos.magnitud)
  Buffers: shared hit=47
  ->  Seq Scan on sismos.eventos_2024_06 eventos  (cost=0.00..63.63 rows=1109 width=6) (actual time=0.002..0.143 rows=1109 loops=1)
        Output: eventos.magnitud
        Filter: ((eventos.fecha_evento >= '2024-06-01 00:00:00+00'::timestamp with time zone) AND (eventos.fecha_evento < '2024-07-01 00:00:00+00'::timestamp with time zone))
        Buffers: shared hit=47
Planning Time: 0.042 ms
Execution Time: 0.281 ms
```

### Plan: `EXP_1B_NO_PRUNING_1M_FUNC`

> **Descripción:** 1B: 1 mes jun-2024 no sargable DATE_TRUNC con pruning OFF

```text
Aggregate  (cost=4854.66..4854.67 rows=1 width=72) (actual time=23.979..24.001 rows=1 loops=1)
  Output: count(*), avg(eventos.magnitud), max(eventos.magnitud)
  Buffers: shared hit=3431
  ->  Append  (cost=0.00..4851.42 rows=431 width=6) (actual time=12.451..23.845 rows=1109 loops=1)
        Buffers: shared hit=3431
        ->  Seq Scan on sismos.eventos_2021_01 eventos_1  (cost=0.00..0.00 rows=1 width=12) (actual time=0.002..0.003 rows=0 loops=1)
              Output: eventos_1.magnitud
              Filter: (date_trunc('month'::text, (eventos_1.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
        ->  Seq Scan on sismos.eventos_2021_02 eventos_2  (cost=0.00..0.00 rows=1 width=12) (actual time=0.001..0.001 rows=0 loops=1)
              Output: eventos_2.magnitud
              Filter: (date_trunc('month'::text, (eventos_2.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
        ->  Seq Scan on sismos.eventos_2021_03 eventos_3  (cost=0.00..0.00 rows=1 width=12) (actual time=0.001..0.001 rows=0 loops=1)
              Output: eventos_3.magnitud
              Filter: (date_trunc('month'::text, (eventos_3.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
        ->  Seq Scan on sismos.eventos_2021_04 eventos_4  (cost=0.00..0.00 rows=1 width=12) (actual time=0.001..0.001 rows=0 loops=1)
              Output: eventos_4.magnitud
              Filter: (date_trunc('month'::text, (eventos_4.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
        ->  Seq Scan on sismos.eventos_2021_05 eventos_5  (cost=0.00..0.00 rows=1 width=12) (actual time=0.001..0.001 rows=0 loops=1)
              Output: eventos_5.magnitud
              Filter: (date_trunc('month'::text, (eventos_5.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
        ->  Seq Scan on sismos.eventos_2021_06 eventos_6  (cost=0.00..0.00 rows=1 width=12) (actual time=0.001..0.001 rows=0 loops=1)
              Output: eventos_6.magnitud
              Filter: (date_trunc('month'::text, (eventos_6.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
        ->  Seq Scan on sismos.eventos_2021_07 eventos_7  (cost=0.00..0.00 rows=1 width=12) (actual time=0.000..0.001 rows=0 loops=1)
              Output: eventos_7.magnitud
... [350 líneas omitidas por brevedad] ...
        ->  Seq Scan on sismos.eventos_2027_12 eventos_84  (cost=0.00..0.00 rows=1 width=12) (actual time=0.001..0.001 rows=0 loops=1)
              Output: eventos_84.magnitud
              Filter: (date_trunc('month'::text, (eventos_84.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
        ->  Seq Scan on sismos.eventos_default eventos_85  (cost=0.00..0.00 rows=1 width=12) (actual time=0.001..0.001 rows=0 loops=1)
              Output: eventos_85.magnitud
              Filter: (date_trunc('month'::text, (eventos_85.fecha_evento AT TIME ZONE 'UTC'::text)) = '2024-06-01 00:00:00'::timestamp without time zone)
Planning:
  Buffers: shared hit=96
Planning Time: 1.365 ms
Execution Time: 24.132 ms
```

### Plan: `EXP_1C_RANGO_48M_COMPLETO`

> **Descripción:** 1C: Rango completo 48 meses (2022 a 2025)

```text
Aggregate  (cost=4498.53..4498.54 rows=1 width=72) (actual time=34.890..34.906 rows=1 loops=1)
  Output: count(*), avg(eventos.magnitud), max(eventos.magnitud)
  Buffers: shared hit=2726
  ->  Append  (cost=0.00..4015.12 rows=64455 width=6) (actual time=0.013..27.166 rows=64456 loops=1)
        Buffers: shared hit=2726
        ->  Seq Scan on sismos.eventos_2022_01 eventos_1  (cost=0.00..82.87 rows=1458 width=6) (actual time=0.013..0.394 rows=1458 loops=1)
              Output: eventos_1.magnitud
              Filter: ((eventos_1.fecha_evento >= '2022-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_1.fecha_evento < '2026-01-01 00:00:00+00'::timestamp with time zone))
              Buffers: shared hit=61
        ->  Seq Scan on sismos.eventos_2022_02 eventos_2  (cost=0.00..71.90 rows=1260 width=6) (actual time=0.002..0.373 rows=1260 loops=1)
              Output: eventos_2.magnitud
              Filter: ((eventos_2.fecha_evento >= '2022-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_2.fecha_evento < '2026-01-01 00:00:00+00'::timestamp with time zone))
              Buffers: shared hit=53
        ->  Seq Scan on sismos.eventos_2022_03 eventos_3  (cost=0.00..89.53 rows=1569 width=6) (actual time=0.002..0.377 rows=1569 loops=1)
              Output: eventos_3.magnitud
              Filter: ((eventos_3.fecha_evento >= '2022-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_3.fecha_evento < '2026-01-01 00:00:00+00'::timestamp with time zone))
              Buffers: shared hit=66
        ->  Seq Scan on sismos.eventos_2022_04 eventos_4  (cost=0.00..77.40 rows=1360 width=6) (actual time=0.002..0.365 rows=1360 loops=1)
              Output: eventos_4.magnitud
              Filter: ((eventos_4.fecha_evento >= '2022-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_4.fecha_evento < '2026-01-01 00:00:00+00'::timestamp with time zone))
              Buffers: shared hit=57
        ->  Seq Scan on sismos.eventos_2022_05 eventos_5  (cost=0.00..66.49 rows=1166 width=6) (actual time=0.002..0.341 rows=1166 loops=1)
              Output: eventos_5.magnitud
              Filter: ((eventos_5.fecha_evento >= '2022-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_5.fecha_evento < '2026-01-01 00:00:00+00'::timestamp with time zone))
              Buffers: shared hit=49
... [164 líneas omitidas por brevedad] ...
        ->  Seq Scan on sismos.eventos_2025_11 eventos_47  (cost=0.00..75.81 rows=1321 width=6) (actual time=0.003..0.505 rows=1321 loops=1)
              Output: eventos_47.magnitud
              Filter: ((eventos_47.fecha_evento >= '2022-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_47.fecha_evento < '2026-01-01 00:00:00+00'::timestamp with time zone))
              Buffers: shared hit=56
        ->  Seq Scan on sismos.eventos_2025_12 eventos_48  (cost=0.00..73.16 rows=1277 width=6) (actual time=0.003..0.435 rows=1277 loops=1)
              Output: eventos_48.magnitud
              Filter: ((eventos_48.fecha_evento >= '2022-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_48.fecha_evento < '2026-01-01 00:00:00+00'::timestamp with time zone))
              Buffers: shared hit=54
Planning Time: 1.097 ms
Execution Time: 34.981 ms
```

### Plan: `EXP_2A_IDX_MAGNITUD`

> **Descripción:** 2A: Index Scan en magnitud >= 6.0 (enable_seqscan = off)

```text
Append  (cost=0.12..2221.30 rows=728 width=43) (actual time=0.016..0.805 rows=669 loops=1)
  Buffers: shared hit=758
  ->  Index Scan using eventos_2021_01_magnitud_idx on sismos.eventos_2021_01 eventos_1  (cost=0.12..8.14 rows=1 width=98) (actual time=0.002..0.002 rows=0 loops=1)
        Output: eventos_1.evento_id, eventos_1.fecha_evento, eventos_1.magnitud, eventos_1.profundidad_km, eventos_1.region
        Index Cond: (eventos_1.magnitud >= 6.0)
        Buffers: shared hit=2
  ->  Index Scan using eventos_2021_02_magnitud_idx on sismos.eventos_2021_02 eventos_2  (cost=0.12..8.14 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_2.evento_id, eventos_2.fecha_evento, eventos_2.magnitud, eventos_2.profundidad_km, eventos_2.region
        Index Cond: (eventos_2.magnitud >= 6.0)
        Buffers: shared hit=2
  ->  Index Scan using eventos_2021_03_magnitud_idx on sismos.eventos_2021_03 eventos_3  (cost=0.12..8.14 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_3.evento_id, eventos_3.fecha_evento, eventos_3.magnitud, eventos_3.profundidad_km, eventos_3.region
        Index Cond: (eventos_3.magnitud >= 6.0)
        Buffers: shared hit=2
  ->  Index Scan using eventos_2021_04_magnitud_idx on sismos.eventos_2021_04 eventos_4  (cost=0.12..8.14 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_4.evento_id, eventos_4.fecha_evento, eventos_4.magnitud, eventos_4.profundidad_km, eventos_4.region
        Index Cond: (eventos_4.magnitud >= 6.0)
        Buffers: shared hit=2
  ->  Index Scan using eventos_2021_05_magnitud_idx on sismos.eventos_2021_05 eventos_5  (cost=0.12..8.14 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_5.evento_id, eventos_5.fecha_evento, eventos_5.magnitud, eventos_5.profundidad_km, eventos_5.region
        Index Cond: (eventos_5.magnitud >= 6.0)
        Buffers: shared hit=2
  ->  Index Scan using eventos_2021_06_magnitud_idx on sismos.eventos_2021_06 eventos_6  (cost=0.12..8.14 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_6.evento_id, eventos_6.fecha_evento, eventos_6.magnitud, eventos_6.profundidad_km, eventos_6.region
        Index Cond: (eventos_6.magnitud >= 6.0)
... [555 líneas omitidas por brevedad] ...
        Index Cond: (eventos_84.magnitud >= 6.0)
        Buffers: shared hit=2
  ->  Index Scan using eventos_default_magnitud_idx on sismos.eventos_default eventos_85  (cost=0.12..8.14 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_85.evento_id, eventos_85.fecha_evento, eventos_85.magnitud, eventos_85.profundidad_km, eventos_85.region
        Index Cond: (eventos_85.magnitud >= 6.0)
        Buffers: shared hit=2
Planning:
  Buffers: shared hit=117
Planning Time: 1.461 ms
Execution Time: 1.089 ms
```

### Plan: `EXP_2B_SEQ_MAGNITUD`

> **Descripción:** 2B: Sequential Scan en magnitud >= 6.0 (enable_seqscan = on)

```text
Append  (cost=0.00..4447.69 rows=728 width=43) (actual time=0.051..16.170 rows=669 loops=1)
  Buffers: shared hit=3431
  ->  Seq Scan on sismos.eventos_2021_01 eventos_1  (cost=0.00..0.00 rows=1 width=98) (actual time=0.002..0.003 rows=0 loops=1)
        Output: eventos_1.evento_id, eventos_1.fecha_evento, eventos_1.magnitud, eventos_1.profundidad_km, eventos_1.region
        Filter: (eventos_1.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_2021_02 eventos_2  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_2.evento_id, eventos_2.fecha_evento, eventos_2.magnitud, eventos_2.profundidad_km, eventos_2.region
        Filter: (eventos_2.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_2021_03 eventos_3  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_3.evento_id, eventos_3.fecha_evento, eventos_3.magnitud, eventos_3.profundidad_km, eventos_3.region
        Filter: (eventos_3.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_2021_04 eventos_4  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_4.evento_id, eventos_4.fecha_evento, eventos_4.magnitud, eventos_4.profundidad_km, eventos_4.region
        Filter: (eventos_4.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_2021_05 eventos_5  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_5.evento_id, eventos_5.fecha_evento, eventos_5.magnitud, eventos_5.profundidad_km, eventos_5.region
        Filter: (eventos_5.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_2021_06 eventos_6  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_6.evento_id, eventos_6.fecha_evento, eventos_6.magnitud, eventos_6.profundidad_km, eventos_6.region
        Filter: (eventos_6.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_2021_07 eventos_7  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_7.evento_id, eventos_7.fecha_evento, eventos_7.magnitud, eventos_7.profundidad_km, eventos_7.region
        Filter: (eventos_7.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_2021_08 eventos_8  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_8.evento_id, eventos_8.fecha_evento, eventos_8.magnitud, eventos_8.profundidad_km, eventos_8.region
... [348 líneas omitidas por brevedad] ...
  ->  Seq Scan on sismos.eventos_2027_12 eventos_84  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_84.evento_id, eventos_84.fecha_evento, eventos_84.magnitud, eventos_84.profundidad_km, eventos_84.region
        Filter: (eventos_84.magnitud >= 6.0)
  ->  Seq Scan on sismos.eventos_default eventos_85  (cost=0.00..0.00 rows=1 width=98) (actual time=0.001..0.001 rows=0 loops=1)
        Output: eventos_85.evento_id, eventos_85.fecha_evento, eventos_85.magnitud, eventos_85.profundidad_km, eventos_85.region
        Filter: (eventos_85.magnitud >= 6.0)
Planning:
  Buffers: shared hit=117
Planning Time: 1.674 ms
Execution Time: 16.342 ms
```

### Plan: `EXP_3A_IDX_CLUSTER`

> **Descripción:** 3A: Index Scan en cluster_id = 0 en Q1 2024 (enable_seqscan = off)

```text
Append  (cost=5.32..180.65 rows=399 width=39) (actual time=0.008..0.166 rows=399 loops=1)
  Buffers: shared hit=67
  ->  Bitmap Heap Scan on sismos.eventos_2024_01 eventos_1  (cost=5.32..66.69 rows=135 width=39) (actual time=0.008..0.046 rows=135 loops=1)
        Output: eventos_1.evento_id, eventos_1.fecha_evento, eventos_1.magnitud, eventos_1.cluster_id, eventos_1.region
        Recheck Cond: (eventos_1.cluster_id = 0)
        Filter: ((eventos_1.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_1.fecha_evento < '2024-04-01 00:00:00+00'::timestamp with time zone))
        Heap Blocks: exact=25
        Buffers: shared hit=27
        ->  Bitmap Index Scan on eventos_2024_01_cluster_id_idx  (cost=0.00..5.29 rows=135 width=0) (actual time=0.005..0.005 rows=135 loops=1)
              Index Cond: (eventos_1.cluster_id = 0)
              Buffers: shared hit=2
  ->  Bitmap Heap Scan on sismos.eventos_2024_02 eventos_2  (cost=5.04..56.06 rows=115 width=39) (actual time=0.005..0.035 rows=115 loops=1)
        Output: eventos_2.evento_id, eventos_2.fecha_evento, eventos_2.magnitud, eventos_2.cluster_id, eventos_2.region
        Recheck Cond: (eventos_2.cluster_id = 0)
        Filter: ((eventos_2.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_2.fecha_evento < '2024-04-01 00:00:00+00'::timestamp with time zone))
        Heap Blocks: exact=18
        Buffers: shared hit=19
        ->  Bitmap Index Scan on eventos_2024_02_cluster_id_idx  (cost=0.00..5.01 rows=115 width=0) (actual time=0.003..0.003 rows=115 loops=1)
              Index Cond: (eventos_2.cluster_id = 0)
              Buffers: shared hit=1
  ->  Bitmap Heap Scan on sismos.eventos_2024_03 eventos_3  (cost=5.31..55.91 rows=149 width=40) (actual time=0.005..0.043 rows=149 loops=1)
        Output: eventos_3.evento_id, eventos_3.fecha_evento, eventos_3.magnitud, eventos_3.cluster_id, eventos_3.region
        Recheck Cond: (eventos_3.cluster_id = 0)
        Filter: ((eventos_3.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_3.fecha_evento < '2024-04-01 00:00:00+00'::timestamp with time zone))
        Heap Blocks: exact=20
        Buffers: shared hit=21
        ->  Bitmap Index Scan on eventos_2024_03_cluster_id_idx  (cost=0.00..5.27 rows=149 width=0) (actual time=0.004..0.004 rows=149 loops=1)
              Index Cond: (eventos_3.cluster_id = 0)
              Buffers: shared hit=1
Planning Time: 0.088 ms
Execution Time: 0.194 ms
```

### Plan: `EXP_3B_SEQ_CLUSTER`

> **Descripción:** 3B: Sequential Scan en cluster_id = 0 en Q1 2024 (enable_seqscan = on)

```text
Append  (cost=0.00..222.01 rows=399 width=39) (actual time=0.002..0.653 rows=399 loops=1)
  Buffers: shared hit=156
  ->  Seq Scan on sismos.eventos_2024_01 eventos_1  (cost=0.00..83.31 rows=135 width=39) (actual time=0.002..0.228 rows=135 loops=1)
        Output: eventos_1.evento_id, eventos_1.fecha_evento, eventos_1.magnitud, eventos_1.cluster_id, eventos_1.region
        Filter: ((eventos_1.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_1.fecha_evento < '2024-04-01 00:00:00+00'::timestamp with time zone) AND (eventos_1.cluster_id = 0))
        Rows Removed by Filter: 1254
        Buffers: shared hit=59
  ->  Seq Scan on sismos.eventos_2024_02 eventos_2  (cost=0.00..69.07 rows=115 width=39) (actual time=0.002..0.200 rows=115 loops=1)
        Output: eventos_2.evento_id, eventos_2.fecha_evento, eventos_2.magnitud, eventos_2.cluster_id, eventos_2.region
        Filter: ((eventos_2.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_2.fecha_evento < '2024-04-01 00:00:00+00'::timestamp with time zone) AND (eventos_2.cluster_id = 0))
        Rows Removed by Filter: 1032
        Buffers: shared hit=49
  ->  Seq Scan on sismos.eventos_2024_03 eventos_3  (cost=0.00..67.64 rows=149 width=40) (actual time=0.002..0.184 rows=149 loops=1)
        Output: eventos_3.evento_id, eventos_3.fecha_evento, eventos_3.magnitud, eventos_3.cluster_id, eventos_3.region
        Filter: ((eventos_3.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_3.fecha_evento < '2024-04-01 00:00:00+00'::timestamp with time zone) AND (eventos_3.cluster_id = 0))
        Rows Removed by Filter: 973
        Buffers: shared hit=48
Planning Time: 0.087 ms
Execution Time: 0.677 ms
```

### Plan: `EXP_4A_IDX_REGION`

> **Descripción:** 4A: Index Scan en region = 'Japan' en 2024 (enable_seqscan = off)

```text
Append  (cost=5.80..703.21 rows=1194 width=41) (actual time=0.011..0.553 rows=1194 loops=1)
  Buffers: shared hit=284
  ->  Bitmap Heap Scan on sismos.eventos_2024_01 eventos_1  (cost=5.80..68.25 rows=197 width=41) (actual time=0.011..0.059 rows=197 loops=1)
        Output: eventos_1.evento_id, eventos_1.fecha_evento, eventos_1.magnitud, eventos_1.profundidad_km, eventos_1.region
        Recheck Cond: (eventos_1.region = 'Japan'::text)
        Filter: ((eventos_1.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_1.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone))
        Heap Blocks: exact=34
        Buffers: shared hit=36
        ->  Bitmap Index Scan on eventos_2024_01_region_idx  (cost=0.00..5.75 rows=197 width=0) (actual time=0.007..0.007 rows=197 loops=1)
              Index Cond: (eventos_1.region = 'Japan'::text)
              Buffers: shared hit=2
  ->  Bitmap Heap Scan on sismos.eventos_2024_02 eventos_2  (cost=5.15..56.13 rows=113 width=41) (actual time=0.006..0.036 rows=113 loops=1)
        Output: eventos_2.evento_id, eventos_2.fecha_evento, eventos_2.magnitud, eventos_2.profundidad_km, eventos_2.region
        Recheck Cond: (eventos_2.region = 'Japan'::text)
        Filter: ((eventos_2.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_2.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone))
        Heap Blocks: exact=24
        Buffers: shared hit=26
        ->  Bitmap Index Scan on eventos_2024_02_region_idx  (cost=0.00..5.12 rows=113 width=0) (actual time=0.004..0.004 rows=113 loops=1)
              Index Cond: (eventos_2.region = 'Japan'::text)
              Buffers: shared hit=2
  ->  Bitmap Heap Scan on sismos.eventos_2024_03 eventos_3  (cost=4.84..56.23 rows=72 width=42) (actual time=0.005..0.024 rows=72 loops=1)
        Output: eventos_3.evento_id, eventos_3.fecha_evento, eventos_3.magnitud, eventos_3.profundidad_km, eventos_3.region
        Recheck Cond: (eventos_3.region = 'Japan'::text)
        Filter: ((eventos_3.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_3.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone))
        Heap Blocks: exact=16
... [77 líneas omitidas por brevedad] ...
        Output: eventos_12.evento_id, eventos_12.fecha_evento, eventos_12.magnitud, eventos_12.profundidad_km, eventos_12.region
        Recheck Cond: (eventos_12.region = 'Japan'::text)
        Filter: ((eventos_12.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_12.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone))
        Heap Blocks: exact=19
        Buffers: shared hit=21
        ->  Bitmap Index Scan on eventos_2024_12_region_idx  (cost=0.00..4.93 rows=87 width=0) (actual time=0.004..0.004 rows=87 loops=1)
              Index Cond: (eventos_12.region = 'Japan'::text)
              Buffers: shared hit=2
Planning Time: 0.327 ms
Execution Time: 0.643 ms
```

### Plan: `EXP_4B_SEQ_REGION`

> **Descripción:** 4B: Sequential Scan en region = 'Japan' en 2024 (enable_seqscan = on)

```text
Append  (cost=0.00..855.03 rows=1194 width=41) (actual time=0.035..2.171 rows=1194 loops=1)
  Buffers: shared hit=601
  ->  Seq Scan on sismos.eventos_2024_01 eventos_1  (cost=0.00..83.31 rows=197 width=41) (actual time=0.035..0.205 rows=197 loops=1)
        Output: eventos_1.evento_id, eventos_1.fecha_evento, eventos_1.magnitud, eventos_1.profundidad_km, eventos_1.region
        Filter: ((eventos_1.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_1.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_1.region = 'Japan'::text))
        Rows Removed by Filter: 1192
        Buffers: shared hit=59
  ->  Seq Scan on sismos.eventos_2024_02 eventos_2  (cost=0.00..69.07 rows=113 width=41) (actual time=0.029..0.161 rows=113 loops=1)
        Output: eventos_2.evento_id, eventos_2.fecha_evento, eventos_2.magnitud, eventos_2.profundidad_km, eventos_2.region
        Filter: ((eventos_2.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_2.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_2.region = 'Japan'::text))
        Rows Removed by Filter: 1034
        Buffers: shared hit=49
  ->  Seq Scan on sismos.eventos_2024_03 eventos_3  (cost=0.00..67.64 rows=72 width=42) (actual time=0.041..0.156 rows=72 loops=1)
        Output: eventos_3.evento_id, eventos_3.fecha_evento, eventos_3.magnitud, eventos_3.profundidad_km, eventos_3.region
        Filter: ((eventos_3.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_3.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_3.region = 'Japan'::text))
        Rows Removed by Filter: 1050
        Buffers: shared hit=48
  ->  Seq Scan on sismos.eventos_2024_04 eventos_4  (cost=0.00..93.09 rows=74 width=41) (actual time=0.035..0.221 rows=74 loops=1)
        Output: eventos_4.evento_id, eventos_4.fecha_evento, eventos_4.magnitud, eventos_4.profundidad_km, eventos_4.region
        Filter: ((eventos_4.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_4.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_4.region = 'Japan'::text))
        Rows Removed by Filter: 1474
        Buffers: shared hit=66
  ->  Seq Scan on sismos.eventos_2024_05 eventos_5  (cost=0.00..72.14 rows=82 width=41) (actual time=0.035..0.165 rows=82 loops=1)
        Output: eventos_5.evento_id, eventos_5.fecha_evento, eventos_5.magnitud, eventos_5.profundidad_km, eventos_5.region
        Filter: ((eventos_5.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_5.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_5.region = 'Japan'::text))
... [29 líneas omitidas por brevedad] ...
        Filter: ((eventos_11.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_11.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_11.region = 'Japan'::text))
        Rows Removed by Filter: 888
        Buffers: shared hit=44
  ->  Seq Scan on sismos.eventos_2024_12 eventos_12  (cost=0.00..74.84 rows=87 width=40) (actual time=0.031..0.178 rows=87 loops=1)
        Output: eventos_12.evento_id, eventos_12.fecha_evento, eventos_12.magnitud, eventos_12.profundidad_km, eventos_12.region
        Filter: ((eventos_12.fecha_evento >= '2024-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_12.fecha_evento < '2025-01-01 00:00:00+00'::timestamp with time zone) AND (eventos_12.region = 'Japan'::text))
        Rows Removed by Filter: 1161
        Buffers: shared hit=53
Planning Time: 0.335 ms
Execution Time: 2.244 ms
```


---

## 5. Directrices de Implementación para Capa 5 (Grafana)

Para garantizar latencias sub-segundo en los tableros analíticos de Grafana:
- **Regla 1 (Filtro Temporal Obligatorio):** Todo panel debe incluir la macro `$__timeFilter(fecha_evento)` para activar partition pruning inmediato.
- **Regla 2 (No funciones en columnas particionadas):** Prohibido usar `DATE_TRUNC` en la cláusula `WHERE` sobre `fecha_evento`; usar siempre rangos directos `>=` y `<`.
- **Regla 3 (Agregados Precalculados):** Para series temporales anuales y mensuales, consultar `sismos.agg_region_mes` en lugar de `sismos.eventos`.
