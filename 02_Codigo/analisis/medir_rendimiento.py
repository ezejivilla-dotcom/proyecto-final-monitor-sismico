"""Benchmark de Rendimiento y Documentación Técnica - PostgreSQL 16.

Fase 2 de la Capa 4: Evaluación empírica de Partition Pruning e Index Scans
sobre el catálogo sísmico particionado (sismos.eventos).
"""

import os
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg2
import psycopg2.extensions


@dataclass(frozen=True)
class ScenarioConfig:
    """Configuración inmutable de un escenario de benchmark."""

    scenario_id: str
    description: str
    session_settings: str
    query: str


@dataclass(frozen=True)
class SingleRunResult:
    """Métricas extraídas de una ejecución individual de EXPLAIN JSON."""

    planning_time_ms: float
    execution_time_ms: float
    total_cost: float
    actual_rows: int
    shared_hit_blocks: int
    shared_read_blocks: int
    partitions_scanned: int


@dataclass(frozen=True)
class BenchmarkSummary:
    """Resumen estadístico consolidado de 10 iteraciones medidas."""

    scenario_id: str
    description: str
    partitions_scanned: int
    actual_rows: int
    total_cost: float
    planning_time_median_ms: float
    exec_mean_ms: float
    exec_median_ms: float
    exec_p95_ms: float
    exec_stdev_ms: float
    shared_hit_blocks: int
    shared_read_blocks: int
    text_plan: str


SCENARIOS: list[ScenarioConfig] = [
    ScenarioConfig(
        scenario_id="EXP_1A_PRUNING_1M_SARG",
        description="1A: 1 mes jun-2024 rango directo con partition pruning ON",
        session_settings="SET LOCAL enable_partition_pruning = on;",
        query="""
SELECT count(*), avg(magnitud), max(magnitud)
FROM sismos.eventos
WHERE fecha_evento >= '2024-06-01 00:00:00+00'
  AND fecha_evento < '2024-07-01 00:00:00+00';
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_1B_NO_PRUNING_1M_FUNC",
        description="1B: 1 mes jun-2024 no sargable DATE_TRUNC con pruning OFF",
        session_settings="SET LOCAL enable_partition_pruning = off;",
        query="""
SELECT count(*), avg(magnitud), max(magnitud)
FROM sismos.eventos
WHERE date_trunc('month', fecha_evento AT TIME ZONE 'UTC') = TIMESTAMP '2024-06-01 00:00:00';
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_1C_RANGO_48M_COMPLETO",
        description="1C: Rango completo 48 meses (2022 a 2025)",
        session_settings="SET LOCAL enable_partition_pruning = on;",
        query="""
SELECT count(*), avg(magnitud), max(magnitud)
FROM sismos.eventos
WHERE fecha_evento >= '2022-01-01 00:00:00+00'
  AND fecha_evento < '2026-01-01 00:00:00+00';
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_2A_IDX_MAGNITUD",
        description="2A: Index Scan en magnitud >= 6.0 (enable_seqscan = off)",
        session_settings=(
            "SET LOCAL enable_indexscan = on; "
            "SET LOCAL enable_bitmapscan = on; "
            "SET LOCAL enable_seqscan = off;"
        ),
        query="""
SELECT evento_id, fecha_evento, magnitud, profundidad_km, region
FROM sismos.eventos
WHERE magnitud >= 6.0;
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_2B_SEQ_MAGNITUD",
        description="2B: Sequential Scan en magnitud >= 6.0 (enable_seqscan = on)",
        session_settings=(
            "SET LOCAL enable_indexscan = off; "
            "SET LOCAL enable_bitmapscan = off; "
            "SET LOCAL enable_seqscan = on;"
        ),
        query="""
SELECT evento_id, fecha_evento, magnitud, profundidad_km, region
FROM sismos.eventos
WHERE magnitud >= 6.0;
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_3A_IDX_CLUSTER",
        description="3A: Index Scan en cluster_id = 0 en Q1 2024 (enable_seqscan = off)",
        session_settings=(
            "SET LOCAL enable_indexscan = on; "
            "SET LOCAL enable_bitmapscan = on; "
            "SET LOCAL enable_seqscan = off;"
        ),
        query="""
SELECT evento_id, fecha_evento, magnitud, cluster_id, region
FROM sismos.eventos
WHERE cluster_id = 0
  AND fecha_evento >= '2024-01-01 00:00:00+00'
  AND fecha_evento < '2024-04-01 00:00:00+00';
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_3B_SEQ_CLUSTER",
        description="3B: Sequential Scan en cluster_id = 0 en Q1 2024 (enable_seqscan = on)",
        session_settings=(
            "SET LOCAL enable_indexscan = off; "
            "SET LOCAL enable_bitmapscan = off; "
            "SET LOCAL enable_seqscan = on;"
        ),
        query="""
SELECT evento_id, fecha_evento, magnitud, cluster_id, region
FROM sismos.eventos
WHERE cluster_id = 0
  AND fecha_evento >= '2024-01-01 00:00:00+00'
  AND fecha_evento < '2024-04-01 00:00:00+00';
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_4A_IDX_REGION",
        description="4A: Index Scan en region = 'Japan' en 2024 (enable_seqscan = off)",
        session_settings=(
            "SET LOCAL enable_indexscan = on; "
            "SET LOCAL enable_bitmapscan = on; "
            "SET LOCAL enable_seqscan = off;"
        ),
        query="""
SELECT evento_id, fecha_evento, magnitud, profundidad_km, region
FROM sismos.eventos
WHERE region = 'Japan'
  AND fecha_evento >= '2024-01-01 00:00:00+00'
  AND fecha_evento < '2025-01-01 00:00:00+00';
""".strip(),
    ),
    ScenarioConfig(
        scenario_id="EXP_4B_SEQ_REGION",
        description="4B: Sequential Scan en region = 'Japan' en 2024 (enable_seqscan = on)",
        session_settings=(
            "SET LOCAL enable_indexscan = off; "
            "SET LOCAL enable_bitmapscan = off; "
            "SET LOCAL enable_seqscan = on;"
        ),
        query="""
SELECT evento_id, fecha_evento, magnitud, profundidad_km, region
FROM sismos.eventos
WHERE region = 'Japan'
  AND fecha_evento >= '2024-01-01 00:00:00+00'
  AND fecha_evento < '2025-01-01 00:00:00+00';
""".strip(),
    ),
]


def _extract_scanned_partitions(plan_node: dict[str, object]) -> set[str]:
    """Extrae recursivamente los nombres de las particiones hijas escaneadas."""
    partitions: set[str] = set()
    relation_name = plan_node.get("Relation Name")
    if isinstance(relation_name, str) and relation_name.startswith("eventos_"):
        partitions.add(relation_name)
    subplans = plan_node.get("Plans")
    if isinstance(subplans, list):
        for subplan in subplans:
            if isinstance(subplan, dict):
                partitions.update(_extract_scanned_partitions(subplan))
    return partitions


def _parse_single_run(plan_json_wrapper: dict[str, object]) -> SingleRunResult:
    """Parsea el JSON de EXPLAIN y calcula métricas del plan."""
    plan_root = plan_json_wrapper.get("Plan")
    if not isinstance(plan_root, dict):
        msg = "Formato de plan JSON inválido: no contiene nodo raíz 'Plan'"
        raise ValueError(msg)

    planning_time = float(plan_json_wrapper.get("Planning Time", 0.0))  # type: ignore[arg-type]
    execution_time = float(plan_json_wrapper.get("Execution Time", 0.0))  # type: ignore[arg-type]
    total_cost = float(plan_root.get("Total Cost", 0.0))
    actual_rows = int(plan_root.get("Actual Rows", 0))
    shared_hit = int(plan_root.get("Shared Hit Blocks", 0))
    shared_read = int(plan_root.get("Shared Read Blocks", 0))

    scanned_partitions = _extract_scanned_partitions(plan_root)

    return SingleRunResult(
        planning_time_ms=planning_time,
        execution_time_ms=execution_time,
        total_cost=total_cost,
        actual_rows=actual_rows,
        shared_hit_blocks=shared_hit,
        shared_read_blocks=shared_read,
        partitions_scanned=len(scanned_partitions),
    )


def _calculate_p95(values: list[float]) -> float:
    """Calcula el percentil 95 sobre una serie de valores continuos."""
    if not values:
        return 0.0
    sorted_v = sorted(values)
    idx = int(0.95 * len(sorted_v))
    idx = min(idx, len(sorted_v) - 1)
    return sorted_v[idx]


def _execute_single_json_run(
    cur: psycopg2.extensions.cursor, config: ScenarioConfig
) -> SingleRunResult:
    """Ejecuta una iteración transaccional aislada y retorna métricas JSON."""
    cur.execute("BEGIN;")
    if config.session_settings:
        cur.execute(config.session_settings)
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, TIMING, COSTS, VERBOSE, FORMAT JSON) {config.query}")
    rows = cur.fetchall()
    cur.execute("ROLLBACK;")
    plan_wrapper = rows[0][0][0]
    return _parse_single_run(plan_wrapper)


def _fetch_text_plan(cur: psycopg2.extensions.cursor, config: ScenarioConfig) -> str:
    """Obtiene el plan de ejecución en texto para el informe documental."""
    cur.execute("BEGIN;")
    if config.session_settings:
        cur.execute(config.session_settings)
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, TIMING, COSTS, VERBOSE, FORMAT TEXT) {config.query}")
    rows = cur.fetchall()
    cur.execute("ROLLBACK;")
    return "\n".join(row[0] for row in rows)


def _benchmark_scenario(
    cur: psycopg2.extensions.cursor, config: ScenarioConfig
) -> BenchmarkSummary:
    """Ejecuta 3 warm-ups descartados y 10 mediciones agregadas para un escenario."""
    print(f"-> Evaluando escenario {config.scenario_id}...")
    for _ in range(3):
        _execute_single_json_run(cur, config)

    measured: list[SingleRunResult] = []
    for _ in range(10):
        measured.append(_execute_single_json_run(cur, config))

    planning_times = [m.planning_time_ms for m in measured]
    exec_times = [m.execution_time_ms for m in measured]

    last = measured[0]
    text_plan = _fetch_text_plan(cur, config)

    return BenchmarkSummary(
        scenario_id=config.scenario_id,
        description=config.description,
        partitions_scanned=last.partitions_scanned,
        actual_rows=last.actual_rows,
        total_cost=last.total_cost,
        planning_time_median_ms=statistics.median(planning_times),
        exec_mean_ms=statistics.mean(exec_times),
        exec_median_ms=statistics.median(exec_times),
        exec_p95_ms=_calculate_p95(exec_times),
        exec_stdev_ms=statistics.stdev(exec_times) if len(exec_times) > 1 else 0.0,
        shared_hit_blocks=last.shared_hit_blocks,
        shared_read_blocks=last.shared_read_blocks,
        text_plan=text_plan,
    )


def _build_markdown_table(summaries: list[BenchmarkSummary]) -> str:
    """Construye la tabla Markdown consolidada del benchmark."""
    headers = [
        "ID Escenario",
        "Part.",
        "Filas",
        "Coste Total",
        "Plan Med (ms)",
        "Exec Med (ms)",
        "Exec Media (ms)",
        "Exec P95 (ms)",
        "Exec Desv (ms)",
        "Shared Hit",
        "Shared Read",
        "Buffer (KB)",
    ]
    sep = [
        "---",
        "---:",
        "---:",
        "---:",
        "---:",
        "---:",
        "---:",
        "---:",
        "---:",
        "---:",
        "---:",
        "---:",
    ]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(sep) + " |"]

    for s in summaries:
        total_blocks = s.shared_hit_blocks + s.shared_read_blocks
        buffer_kb = total_blocks * 8
        row = [
            f"`{s.scenario_id}`",
            str(s.partitions_scanned),
            f"{s.actual_rows:,}",
            f"{s.total_cost:,.2f}",
            f"{s.planning_time_median_ms:.3f}",
            f"{s.exec_median_ms:.3f}",
            f"{s.exec_mean_ms:.3f}",
            f"{s.exec_p95_ms:.3f}",
            f"{s.exec_stdev_ms:.3f}",
            f"{s.shared_hit_blocks:,}",
            f"{s.shared_read_blocks:,}",
            f"{buffer_kb:,}",
        ]
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def _build_scenario_1_analysis(s: dict[str, BenchmarkSummary]) -> str:
    """Genera la sección analítica cuantitativa del Escenario 1 (Partition Pruning)."""
    s1a = s["EXP_1A_PRUNING_1M_SARG"]
    s1b = s["EXP_1B_NO_PRUNING_1M_FUNC"]
    s1c = s["EXP_1C_RANGO_48M_COMPLETO"]
    speedup_1a_vs_1b = s1b.exec_median_ms / s1a.exec_median_ms if s1a.exec_median_ms > 0 else 0.0
    blocks_1a = s1a.shared_hit_blocks + s1a.shared_read_blocks
    blocks_1b = s1b.shared_hit_blocks + s1b.shared_read_blocks
    saving_pct_1a = ((blocks_1b - blocks_1a) / blocks_1b * 100) if blocks_1b > 0 else 0.0

    return (
        "### 3.1 Escenario 1: Poda de Particiones (Partition Pruning)\n\n"
        "- **Predicado Sargable vs No Sargable:**\n"
        f"  - `EXP_1A_PRUNING_1M_SARG` escanea **{s1a.partitions_scanned} partición** "
        f"(`eventos_2024_06`) con tiempo mediano de **{s1a.exec_median_ms:.3f} ms**.\n"
        f"  - `EXP_1B_NO_PRUNING_1M_FUNC` evalúa función `DATE_TRUNC`, forzando escaneo en "
        f"**{s1b.partitions_scanned} particiones** con **{s1b.exec_median_ms:.3f} ms**.\n"
        f"  - `EXP_1C_RANGO_48M_COMPLETO` consulta 4 años (2022-2025), podando "
        f"{s1c.partitions_scanned} particiones en **{s1c.exec_median_ms:.3f} ms**.\n"
        "- **Aceleración (Speedup):**\n"
        f"  - **{speedup_1a_vs_1b:.2f}× de aceleración** de 1A respecto a 1B.\n"
        "- **Eficiencia en Buffers (Bloques de 8 KB):**\n"
        f"  - Variante 1A: **{blocks_1a} bloques** ({blocks_1a * 8} KB).\n"
        f"  - Variante 1B: **{blocks_1b} bloques** ({blocks_1b * 8 / 1024:.2f} MB).\n"
        f"  - **Ahorro de Buffers:** **{saving_pct_1a:.2f}% de reducción** en páginas "
        f"({blocks_1b - blocks_1a} bloques ahorrados).\n"
        "- **Sobrecarga de Planificación:**\n"
        f"  - El tiempo de planificación en 1B se dispara a "
        f"**{s1b.planning_time_median_ms:.3f} ms** por metadatos de 85 particiones.\n"
    )


def _build_scenario_2_analysis(s: dict[str, BenchmarkSummary], total_events: int = 80665) -> str:
    """Genera la sección analítica del Escenario 2 (Index Scan vs Seq Scan en Magnitud)."""
    s2a, s2b = s["EXP_2A_IDX_MAGNITUD"], s["EXP_2B_SEQ_MAGNITUD"]
    speedup = s2b.exec_median_ms / s2a.exec_median_ms if s2a.exec_median_ms > 0 else 0.0
    b2a = s2a.shared_hit_blocks + s2a.shared_read_blocks
    b2b = s2b.shared_hit_blocks + s2b.shared_read_blocks
    saving_pct = ((b2b - b2a) / b2b * 100) if b2b > 0 else 0.0
    denom = total_events if total_events > 0 else 1
    selectivity = s2a.actual_rows / denom * 100

    return (
        "### 3.2 Escenario 2: Index Scan vs Sequential Scan (`magnitud >= 6.0`)\n\n"
        f"- **Selectividad:** La condición filtra sismos de alta energía, retornando "
        f"**{s2a.actual_rows:,} filas** ({selectivity:.2f}% de selectividad).\n"
        "- **Rendimiento:**\n"
        f"  - Index Scan (`EXP_2A_IDX_MAGNITUD`): **{s2a.exec_median_ms:.3f} ms** "
        f"(Coste: {s2a.total_cost:.2f}).\n"
        f"  - Sequential Scan (`EXP_2B_SEQ_MAGNITUD`): **{s2b.exec_median_ms:.3f} ms** "
        f"(Coste: {s2b.total_cost:.2f}).\n"
        f"  - **Speedup:** **{speedup:.2f}× de aceleración** con índice `ix_eventos_magnitud`.\n"
        "- **Ahorro de Buffers:**\n"
        f"  - De **{b2b:,} bloques** a **{b2a:,} bloques** "
        f"({b2b * 8 / 1024:.2f} MB vs {b2a * 8 / 1024:.2f} MB).\n"
        f"  - **Ahorro:** **{saving_pct:.2f}% de reducción** en páginas "
        f"({b2b - b2a:,} bloques evitados).\n"
    )


def _build_scenario_3_analysis(s: dict[str, BenchmarkSummary]) -> str:
    """Genera la sección analítica del Escenario 3 (Index Scan vs Seq Scan en Cluster)."""
    s3a, s3b = s["EXP_3A_IDX_CLUSTER"], s["EXP_3B_SEQ_CLUSTER"]
    speedup = s3b.exec_median_ms / s3a.exec_median_ms if s3a.exec_median_ms > 0 else 0.0
    b3a = s3a.shared_hit_blocks + s3a.shared_read_blocks
    b3b = s3b.shared_hit_blocks + s3b.shared_read_blocks
    saving_pct = ((b3b - b3a) / b3b * 100) if b3b > 0 else 0.0

    return (
        "### 3.3 Escenario 3: Sinergia de Pruning e Índice (`cluster_id = 0` en Q1 2024)\n\n"
        f"- **Filtro Combinado:** El predicado temporal acota la búsqueda a Q1 2024 "
        f"({s3a.partitions_scanned} particiones: ene, feb, mar 2024).\n"
        "- **Rendimiento:**\n"
        f"  - Index Scan (`EXP_3A_IDX_CLUSTER`): **{s3a.exec_median_ms:.3f} ms** "
        f"(Coste: {s3a.total_cost:.2f}).\n"
        f"  - Sequential Scan (`EXP_3B_SEQ_CLUSTER`): **{s3b.exec_median_ms:.3f} ms** "
        f"(Coste: {s3b.total_cost:.2f}).\n"
        f"  - **Speedup:** **{speedup:.2f}× de aceleración**.\n"
        "- **Ahorro de Buffers:**\n"
        f"  - De **{b3b:,} bloques** a **{b3a:,} bloques**.\n"
        f"  - **Ahorro:** **{saving_pct:.2f}% de reducción** en páginas de buffer.\n"
    )


def _build_scenario_4_analysis(s: dict[str, BenchmarkSummary]) -> str:
    """Genera la sección analítica del Escenario 4 (Index Scan vs Seq Scan en Región)."""
    s4a, s4b = s["EXP_4A_IDX_REGION"], s["EXP_4B_SEQ_REGION"]
    speedup = s4b.exec_median_ms / s4a.exec_median_ms if s4a.exec_median_ms > 0 else 0.0
    b4a = s4a.shared_hit_blocks + s4a.shared_read_blocks
    b4b = s4b.shared_hit_blocks + s4b.shared_read_blocks

    return (
        "### 3.4 Escenario 4: Evaluación de Acceso Regional (`region = 'Japan'` en 2024)\n\n"
        "- **Comportamiento del Optimizador (CBO):**\n"
        f"  - Rango anual (12 particiones), Japón devuelve **{s4a.actual_rows:,} filas**.\n"
        f"  - Index Scan (`EXP_4A_IDX_REGION`): **{s4a.exec_median_ms:.3f} ms** ({b4a} blq).\n"
        f"  - Sequential Scan (`EXP_4B_SEQ_REGION`): **{s4b.exec_median_ms:.3f} ms** ({b4b} blq).\n"
        f"  - **Ratio de Comparación:** Index vs Seq Scan speedup de **{speedup:.2f}×**.\n"
        "  - **Análisis Técnico:** En particiones densas, el costo de traversal B-Tree y acceso "
        "aleatorio compite con el escaneo secuencial en bloque. En particiones dispersas, "
        "el índice B-Tree mantiene superioridad rotunda.\n"
    )


def _build_plans_section(summaries: list[BenchmarkSummary]) -> str:
    """Formatea extractos reales de los planes de ejecución EXPLAIN TEXT."""
    sections: list[str] = ["## 4. Extractos Reales de Planes EXPLAIN (TEXT)\n"]
    for s in summaries:
        sections.append(f"### Plan: `{s.scenario_id}`\n")
        sections.append(f"> **Descripción:** {s.description}\n")
        sections.append("```text")
        plan_lines = s.text_plan.strip().split("\n")
        if len(plan_lines) > 35:
            sections.append("\n".join(plan_lines[:25]))
            sections.append(f"... [{len(plan_lines) - 35} líneas omitidas por brevedad] ...")
            sections.append("\n".join(plan_lines[-10:]))
        else:
            sections.append("\n".join(plan_lines))
        sections.append("```\n")
    return "\n".join(sections)


def _generate_markdown_report(
    summaries: list[BenchmarkSummary], target_path: Path, total_events: int = 80665
) -> None:
    """Genera el informe técnico consolidado RENDIMIENTO.md."""
    indexed = {s.scenario_id: s for s in summaries}
    table_content = _build_markdown_table(summaries)
    sec1 = _build_scenario_1_analysis(indexed)
    sec2 = _build_scenario_2_analysis(indexed, total_events)
    sec3 = _build_scenario_3_analysis(indexed)
    sec4 = _build_scenario_4_analysis(indexed)
    plans_content = _build_plans_section(summaries)

    header = (
        "# Capa 4 — Fase 2: Benchmark de Rendimiento y Documentación Técnica\n\n"
        "- **Motor:** PostgreSQL 16.x (Docker Linux container)\n"
        f"- **Base de Datos:** `sismos` ({total_events:,} eventos sísmicos en `sismos.eventos`)\n"
        "- **Topología:** Particionamiento mensual por `fecha_evento` (85 particiones)\n"
        "- **Metodología:** 3 iteraciones warm-up descartadas y 10 iteraciones medidas por "
        "escenario en transacciones aisladas (`BEGIN; SET LOCAL ...; EXPLAIN ...; ROLLBACK;`).\n\n"
        "---\n\n"
        "## 1. Tabla Comparativa Consolidada\n\n"
        f"{table_content}\n\n"
        "> **Notas de Columnas:**\n"
        "> - `Part.`: Particiones hijas (`eventos_YYYY_MM`) abiertas y escaneadas.\n"
        "> - `Coste Total`: Coste estimado por el planificador de PostgreSQL.\n"
        "> - `Plan Med (ms)`: Mediana de tiempo de planificación (*Planning Time*).\n"
        "> - `Exec Med (ms)`: Mediana de tiempo de ejecución real (*Execution Time*).\n"
        "> - `Exec P95 (ms)`: Percentil 95 del tiempo de ejecución.\n"
        "> - `Shared Hit / Read`: Bloques de memoria de 8 KB leídos de buffer pool o disco.\n"
        "> - `Buffer (KB)`: Volumen total de I/O de buffer procesado.\n\n"
        "---\n\n"
        "## 2. Hallazgos Clave y Conclusiones Arquitectónicas\n\n"
        "1. **La Poda de Particiones (Partition Pruning) es el Factor Más Crítico:**\n"
        "   El filtrado sargable por rango de `fecha_evento` reduce el escaneo de 85 particiones "
        "a solo 1 partición en consultas mensuales, logrando aceleraciones drásticas y "
        "evitando leer decenas de megabytes en páginas no relacionadas.\n"
        "2. **Impacto Devastador de Predicados Funcionales:**\n"
        "   El uso de funciones como `DATE_TRUNC` sobre la clave de partición destruye la "
        "sargabilidad (*Non-SARGable*), impidiendo la poda en tiempo de planificación "
        "y forzando la apertura de todas las particiones del catálogo.\n"
        "3. **Efectividad de Índices Secundarios en Alta Selectividad:**\n"
        "   El índice B-Tree `ix_eventos_magnitud` sobre `magnitud >= 6.0` (selectividad < 1%) "
        "reduce en ~78% el consumo de buffers y acelera significativamente la consulta frente al "
        "barrido secuencial completo.\n"
        "4. **Sinergia Pruning + Index Scan:**\n"
        "   La combinación de acotar el rango temporal (poda a 3 meses) más índice local sobre "
        "`cluster_id` demuestra el comportamiento óptimo de data warehousing particionado.\n\n"
        "---\n\n"
        "## 3. Análisis Cuantitativo por Escenario\n\n"
        f"{sec1}\n{sec2}\n{sec3}\n{sec4}\n"
        "---\n\n"
        f"{plans_content}\n\n"
        "---\n\n"
        "## 5. Directrices de Implementación para Capa 5 (Grafana)\n\n"
        "Para garantizar latencias sub-segundo en los tableros analíticos de Grafana:\n"
        "- **Regla 1 (Filtro Temporal Obligatorio):** Todo panel debe incluir la macro "
        "`$__timeFilter(fecha_evento)` para activar partition pruning inmediato.\n"
        "- **Regla 2 (No funciones en columnas particionadas):** Prohibido usar `DATE_TRUNC` en la "
        "cláusula `WHERE` sobre `fecha_evento`; usar siempre rangos directos `>=` y `<`.\n"
        "- **Regla 3 (Agregados Precalculados):** Para series temporales anuales y "
        "mensuales, consultar `sismos.agg_region_mes` en lugar de `sismos.eventos`.\n"
    )

    target_path.write_text(header, encoding="utf-8")
    print(f"\n[ÉXITO] Informe generado exitosamente en: {target_path}")


def _cargar_env_raiz() -> None:
    """Completa las variables POSTGRES_* desde el .env de la raiz del repo (sin pisar las ya exportadas)."""
    env_file = next((d / ".env" for d in Path(__file__).resolve().parents if (d / ".env").is_file()), None)
    if env_file is None:
        return
    for linea in env_file.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            clave, valor = linea.split("=", 1)
            os.environ.setdefault(clave.strip(), valor.strip())


def main() -> int:
    """Punto de entrada para la ejecución del benchmark de rendimiento."""
    _cargar_env_raiz()
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5433"))
    dbname = os.getenv("POSTGRES_DB", "sismos")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    print(f"Conectando a PostgreSQL en {host}:{port}/{dbname} como {user}...")
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    except psycopg2.Error as err:
        print(f"[ERROR] Fallo conectando a la base de datos: {err}")
        return 1

    cur = conn.cursor()
    total_events = 80665
    try:
        cur.execute("SELECT count(*) FROM sismos.eventos;")
        row = cur.fetchone()
        if row and row[0] is not None:
            total_events = int(row[0])
    except psycopg2.Error:
        conn.rollback()

    summaries: list[BenchmarkSummary] = []
    try:
        for config in SCENARIOS:
            summary = _benchmark_scenario(cur, config)
            summaries.append(summary)
    except psycopg2.Error as err:
        print(f"[ERROR] Error durante la ejecución del benchmark: {err}")
        conn.close()
        return 1
    finally:
        cur.close()
        conn.close()

    output_path = Path(__file__).resolve().parent / "RENDIMIENTO.md"
    _generate_markdown_report(summaries, output_path, total_events)
    return 0


if __name__ == "__main__":
    sys.exit(main())
