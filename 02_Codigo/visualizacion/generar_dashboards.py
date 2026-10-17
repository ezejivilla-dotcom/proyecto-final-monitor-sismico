"""Generador automatizado de dashboards aprovisionables para Grafana (Capa 5).

Genera las especificaciones JSON oficiales (schemaVersion 39) para:
1. Dashboard de Negocio y KMeans: 01_sismos_kmeans_negocio.json
2. Dashboard de Monitoreo de Pipeline: 02_sismos_salud_pipeline.json
"""

import json
from pathlib import Path
from typing import TypeAlias

JsonDict: TypeAlias = dict[str, object]

DATASOURCE_UID = "postgres-sismos"
DATASOURCE_TYPE = "postgres"


def _ds_ref() -> JsonDict:
    """Retorna la referencia canónica al datasource PostgreSQL."""
    return {"type": DATASOURCE_TYPE, "uid": DATASOURCE_UID}


def _build_target(query: str, ref_id: str, format_type: str = "table") -> JsonDict:
    """Construye un target de consulta SQL para el plugin PostgreSQL de Grafana."""
    return {
        "datasource": _ds_ref(),
        "editorMode": "code",
        "format": format_type,
        "rawQuery": True,
        "rawSql": query.strip(),
        "refId": ref_id,
    }


def _build_stat_panel(
    panel_id: int,
    title: str,
    query: str,
    pos: tuple[int, int, int, int],
    color: str = "green",
) -> JsonDict:
    """Construye un panel tipo stat para métricas clave individuales."""
    x, y, w, h = pos
    return {
        "id": panel_id,
        "title": title,
        "type": "stat",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": _ds_ref(),
        "targets": [_build_target(query, "A")],
        "options": {
            "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto",
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "fixed", "fixedColor": color},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": color, "value": None}]},
            },
            "overrides": [],
        },
    }


def _build_geomap_panel(
    panel_id: int,
    title: str,
    query: str,
    pos: tuple[int, int, int, int],
) -> JsonDict:
    """Construye un panel geomap con capa de marcadores epicentrales."""
    x, y, w, h = pos
    layer_config: JsonDict = {
        "style": {
            "size": {"field": "magnitud", "min": 2, "max": 14},
            "color": {"field": "magnitud"},
            "opacity": 0.85,
            "symbol": {"mode": "fixed", "fixed": "circle"},
        },
        "showLegend": True,
    }
    return {
        "id": panel_id,
        "title": title,
        "type": "geomap",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": _ds_ref(),
        "targets": [_build_target(query, "A")],
        "options": {
            "view": {"id": "zero", "lat": 15, "lon": 0, "zoom": 1.6},
            "basemap": {"type": "osm-standard", "config": {}},
            "layers": [
                {
                    "type": "markers",
                    "name": "Magnitud",
                    "config": layer_config,
                    "location": {
                        "mode": "coords",
                        "latitude": "latitud",
                        "longitude": "longitud",
                    },
                    "tooltip": {"mode": "details"},
                }
            ],
            "controls": {
                "showZoom": True,
                "mouseWheelZoom": True,
                "showAttribution": True,
                "showScale": True,
            },
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "continuous-GrYlRd"},
                "min": 4.0,
                "max": 8.0,
                "custom": {},
            },
            "overrides": [],
        },
    }


def _build_timeseries_panel(
    panel_id: int,
    title: str,
    targets: list[tuple[str, str]],
    pos: tuple[int, int, int, int],
    overrides: list[JsonDict] | None = None,
) -> JsonDict:
    """Construye un panel timeseries con soporte para series múltiples y doble eje Y."""
    x, y, w, h = pos
    return {
        "id": panel_id,
        "title": title,
        "type": "timeseries",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": _ds_ref(),
        "targets": [_build_target(q, ref) for q, ref in targets],
        "options": {
            "tooltip": {"mode": "multi", "sort": "desc"},
            "legend": {
                "showLegend": True,
                "displayMode": "table",
                "placement": "bottom",
                "calcs": ["sum", "max", "mean"],
            },
        },
        "fieldConfig": {
            "defaults": {"custom": {"drawStyle": "line", "lineInterpolation": "smooth"}},
            "overrides": overrides or [],
        },
    }


def _build_xychart_panel(
    panel_id: int,
    title: str,
    query: str,
    pos: tuple[int, int, int, int],
    desc: str = "",
) -> JsonDict:
    """Construye un panel de dispersión xychart para comparar dos dimensiones numéricas."""
    x, y, w, h = pos
    return {
        "id": panel_id,
        "title": title,
        "description": desc,
        "type": "xychart",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": _ds_ref(),
        "targets": [_build_target(query, "A")],
        "options": {
            "seriesMapping": "auto",
            "dims": {"x": "Profundidad Real (km)"},
            "series": [{"frame": 0, "x": "Profundidad Real (km)", "y": "Magnitud"}],
        },
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "show": "points",
                    "pointSize": {"fixed": 5},
                    "pointColor": {"mode": "palette-classic"},
                    "pointStrokeWidth": 1,
                }
            },
            "overrides": [],
        },
    }


def _build_barchart_panel(
    panel_id: int,
    title: str,
    query: str,
    pos: tuple[int, int, int, int],
    x_field: str = "Componente",
    stacking: str = "none",
) -> JsonDict:
    """Construye un panel barchart categórico."""
    x, y, w, h = pos
    return {
        "id": panel_id,
        "title": title,
        "type": "barchart",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": _ds_ref(),
        "targets": [_build_target(query, "A")],
        "options": {
            "orientation": "auto",
            "barWidth": 0.75,
            "groupWidth": 0.7,
            "stacking": stacking,
            "showValue": "auto",
            "xField": x_field,
            "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
        },
        "fieldConfig": {"defaults": {}, "overrides": []},
    }


def _build_table_panel(
    panel_id: int,
    title: str,
    query: str,
    pos: tuple[int, int, int, int],
    desc: str = "",
) -> JsonDict:
    """Construye un panel table para visualización estructurada y auditoría."""
    x, y, w, h = pos
    return {
        "id": panel_id,
        "title": title,
        "description": desc,
        "type": "table",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": _ds_ref(),
        "targets": [_build_target(query, "A")],
        "options": {
            "showHeader": True,
            "sortBy": [],
            "footer": {"show": False, "enablePagination": True},
        },
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": []},
    }


def _build_piechart_panel(
    panel_id: int,
    title: str,
    query: str,
    pos: tuple[int, int, int, int],
) -> JsonDict:
    """Construye un panel piechart para distribución proporcional."""
    x, y, w, h = pos
    return {
        "id": panel_id,
        "title": title,
        "type": "piechart",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": _ds_ref(),
        "targets": [_build_target(query, "A")],
        "options": {
            "reduceOptions": {"values": True, "calcs": [], "fields": ""},
            "pieType": "donut",
            "displayLabels": ["percent", "name"],
            "legend": {
                "showLegend": True,
                "displayMode": "table",
                "placement": "right",
                "values": ["percent", "value"],
            },
        },
        "fieldConfig": {"defaults": {}, "overrides": []},
    }


def _build_negocio_kpis() -> list[JsonDict]:
    """Genera la fila superior de 4 paneles KPI de negocio."""
    where_base = (
        "WHERE $__timeFilter(fecha_evento) "
        "AND tipo_evento = 'earthquake' "
        "AND magnitud >= ${rango_magnitud} "
        "AND region IN (${region:sqlstring}) "
        "AND cluster_id::text IN (${cluster:sqlstring})"
    )
    kpis = [
        (
            1,
            "Total Sismos Filtrados",
            f"SELECT count(*) AS total_sismos FROM sismos.eventos {where_base};",
            (0, 0, 6, 4),
            "#73BF69",
        ),
        (
            2,
            "Magnitud Máxima",
            f"SELECT COALESCE(max(magnitud), 0) AS max_mag FROM sismos.eventos {where_base};",
            (6, 0, 6, 4),
            "#F2495C",
        ),
        (
            3,
            "Profundidad Real Media (km)",
            (
                f"SELECT ROUND(COALESCE(avg(profundidad_km), 0)::numeric, 1) AS prof_media "
                f"FROM sismos.eventos {where_base} AND profundidad_fija = false;"
            ),
            (12, 0, 6, 4),
            "#5794F2",
        ),
        (
            4,
            "Total Tsunamis Asociados",
            f"SELECT count(*) AS tsunamis FROM sismos.eventos {where_base} AND tsunami = true;",
            (18, 0, 6, 4),
            "#FF9830",
        ),
    ]
    return [_build_stat_panel(pid, title, q, pos, color) for pid, title, q, pos, color in kpis]


def _build_negocio_spatial_panels(where_base: str) -> list[JsonDict]:
    """Construye los paneles geoespacial (geomap) y de dispersión (xychart)."""
    geomap_sql = (
        "SELECT fecha_evento AS time, latitud, longitud, magnitud, "
        f"cluster_id, lugar, profundidad_km FROM sismos.eventos {where_base} "
        "ORDER BY fecha_evento DESC LIMIT 5000;"
    )
    p_geomap = _build_geomap_panel(
        5,
        "Mapa Geoespacial de Epicentros (KMeans)",
        geomap_sql,
        (0, 4, 14, 10),
    )

    xy_sql = (
        'SELECT profundidad_km AS "Profundidad Real (km)", magnitud AS "Magnitud" '
        f"FROM sismos.eventos {where_base} AND profundidad_fija = false "
        "ORDER BY fecha_evento DESC LIMIT 3000;"
    )
    desc_xy = (
        "Filtra estrictamente sismos naturales y profundidades medidas (profundidad_fija=false)"
    )
    p_xy = _build_xychart_panel(
        6,
        "Dispersión Magnitud vs Profundidad Real",
        xy_sql,
        (14, 4, 10, 10),
        desc_xy,
    )
    return [p_geomap, p_xy]


def _build_negocio_temporal_panel(where_base: str) -> JsonDict:
    """Construye el panel timeseries con frecuencia semanal y log10 de energía."""
    ts_sql = (
        "SELECT date_trunc('week', fecha_evento) AS time, "
        'count(*) AS "Frecuencia Semanal", '
        "ROUND(COALESCE(LOG(NULLIF(SUM(energia_joules), 0)), 0)::numeric, 2) "
        f'AS "Log10 Energía (J)" FROM sismos.eventos {where_base} '
        "GROUP BY 1 ORDER BY 1;"
    )
    ts_overrides: list[JsonDict] = [
        {
            "matcher": {"id": "byName", "options": "Log10 Energía (J)"},
            "properties": [
                {"id": "custom.axisPlacement", "value": "right"},
                {"id": "custom.axisLabel", "value": "Log10 Energía (Joules)"},
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FF9830"}},
            ],
        },
        {
            "matcher": {"id": "byName", "options": "Frecuencia Semanal"},
            "properties": [
                {"id": "custom.axisPlacement", "value": "left"},
                {"id": "custom.axisLabel", "value": "Eventos / Semana"},
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794F2"}},
            ],
        },
    ]
    return _build_timeseries_panel(
        7,
        "Serie Temporal: Frecuencia Semanal y Energía Acumulada (Log10 Joules)",
        [(ts_sql, "A")],
        (0, 14, 24, 8),
        ts_overrides,
    )


def _build_negocio_cluster_panels() -> list[JsonDict]:
    """Construye los paneles comparativos de clústeres KMeans (barchart y table)."""
    bar_cluster_sql = (
        "SELECT 'Cluster ' || c.cluster_id::text AS \"Cluster\", "
        'c.n_eventos AS "Eventos Modelo", count(e.evento_id) AS "Eventos Filtrados" '
        "FROM sismos.clusters c LEFT JOIN sismos.eventos e "
        "ON c.cluster_id = e.cluster_id AND $__timeFilter(e.fecha_evento) "
        "AND e.tipo_evento = 'earthquake' AND e.magnitud >= ${rango_magnitud} "
        "AND e.region IN (${region:sqlstring}) "
        "WHERE c.cluster_id::text IN (${cluster:sqlstring}) "
        "GROUP BY c.cluster_id, c.n_eventos ORDER BY c.cluster_id ASC;"
    )
    p_bar = _build_barchart_panel(
        8,
        "Comparativa de Clústeres KMeans (Modelo vs Filtrados)",
        bar_cluster_sql,
        (0, 22, 10, 8),
        x_field="Cluster",
    )

    tbl_cluster_sql = (
        'SELECT c.cluster_id AS "Cluster ID", '
        'ROUND(c.centro_lat::numeric, 2) AS "Centroide Lat", '
        'ROUND(c.centro_lon::numeric, 2) AS "Centroide Lon", '
        'c.n_eventos AS "Total Base", count(e.evento_id) AS "Total Filtrado", '
        'ROUND(c.magnitud_prom::numeric, 2) AS "Mag Prom Base", '
        'ROUND(COALESCE(avg(e.magnitud), 0)::numeric, 2) AS "Mag Prom Filtrada", '
        "ROUND(COALESCE(avg(e.profundidad_km) "
        "FILTER (WHERE NOT e.profundidad_fija), 0)::numeric, 1) "
        'AS "Prof Real Prom (km)", '
        'count(e.evento_id) FILTER (WHERE e.tsunami) AS "Tsunamis" '
        "FROM sismos.clusters c LEFT JOIN sismos.eventos e "
        "ON c.cluster_id = e.cluster_id AND $__timeFilter(e.fecha_evento) "
        "AND e.tipo_evento = 'earthquake' AND e.magnitud >= ${rango_magnitud} "
        "AND e.region IN (${region:sqlstring}) "
        "WHERE c.cluster_id::text IN (${cluster:sqlstring}) "
        "GROUP BY c.cluster_id, c.centro_lat, c.centro_lon, c.n_eventos, c.magnitud_prom "
        "ORDER BY c.cluster_id ASC;"
    )
    p_tbl = _build_table_panel(
        9,
        "Auditoría Detallada de Clústeres KMeans",
        tbl_cluster_sql,
        (10, 22, 14, 8),
        "Contraste entre sismos.clusters y agregaciones reales de sismos.eventos",
    )
    return [p_bar, p_tbl]


def _build_negocio_panels() -> list[JsonDict]:
    """Genera la lista completa de paneles del dashboard de negocio."""
    where_base = (
        "WHERE $__timeFilter(fecha_evento) "
        "AND tipo_evento = 'earthquake' "
        "AND magnitud >= ${rango_magnitud} "
        "AND region IN (${region:sqlstring}) "
        "AND cluster_id::text IN (${cluster:sqlstring})"
    )
    return [
        *_build_negocio_kpis(),
        *_build_negocio_spatial_panels(where_base),
        _build_negocio_temporal_panel(where_base),
        *_build_negocio_cluster_panels(),
    ]


def _build_query_variable(name: str, label: str, sql: str) -> JsonDict:
    """Construye una variable de plantilla tipo query compatible con Grafana 10/11+."""
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": _ds_ref(),
        "definition": sql,
        "query": sql,
        "multi": True,
        "includeAll": True,
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "refresh": 1,
        "sort": 1,
        "skipUrlSync": False,
    }


def _build_negocio_variables() -> list[JsonDict]:
    """Genera las variables de plantilla requeridas para el dashboard de negocio."""
    region_query = (
        "SELECT DISTINCT region FROM sismos.eventos "
        "WHERE tipo_evento = 'earthquake' AND region IS NOT NULL "
        "ORDER BY region ASC;"
    )
    cluster_query = "SELECT cluster_id::text FROM sismos.clusters ORDER BY cluster_id ASC;"
    return [
        _build_query_variable("region", "Región Sísmica", region_query),
        _build_query_variable("cluster", "Clúster KMeans", cluster_query),
        {
            "name": "rango_magnitud",
            "label": "Magnitud Mínima",
            "type": "custom",
            "query": "4.0, 4.5, 5.0, 5.5, 6.0, 7.0",
            "current": {"selected": True, "text": "4.0", "value": "4.0"},
            "options": [
                {"selected": True, "text": "4.0", "value": "4.0"},
                {"selected": False, "text": "4.5", "value": "4.5"},
                {"selected": False, "text": "5.0", "value": "5.0"},
                {"selected": False, "text": "5.5", "value": "5.5"},
                {"selected": False, "text": "6.0", "value": "6.0"},
                {"selected": False, "text": "7.0", "value": "7.0"},
            ],
            "multi": False,
            "includeAll": False,
            "skipUrlSync": False,
        },
    ]


def build_negocio_dashboard() -> JsonDict:
    """Construye el dashboard completo de negocio y KMeans."""
    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": [],
        "liveNow": False,
        "panels": _build_negocio_panels(),
        "refresh": "1m",
        "schemaVersion": 39,
        "tags": ["sismos", "negocio", "kmeans"],
        "templating": {"list": _build_negocio_variables()},
        "time": {
            "from": "2022-01-01T00:00:00.000Z",
            "to": "2025-12-31T23:59:59.000Z",
        },
        "timepicker": {
            "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"]
        },
        "timezone": "utc",
        "title": "1. Análisis Sismológico y KMeans - Negocio",
        "uid": "01-sismos-kmeans-negocio",
        "version": 1,
        "weekStart": "",
    }


def _build_salud_kpis() -> list[JsonDict]:
    """Genera la fila superior de 4 paneles KPI operacionales."""
    kpis = [
        (
            1,
            "Eventos en la Base (última corrida Spark)",
            "SELECT COALESCE((SELECT registros_ok FROM sismos.metricas_pipeline "
            "WHERE componente = 'spark' AND $__timeFilter(ts) ORDER BY ts DESC LIMIT 1), 0) AS val;",
            (0, 0, 6, 4),
            "#73BF69",
        ),
        (
            2,
            "Latencia Media NiFi Feed (ms)",
            "SELECT ROUND(COALESCE(avg(latencia_ms_prom), 0)::numeric, 1) AS val "
            "FROM sismos.metricas_pipeline WHERE $__timeFilter(ts) "
            "AND componente = 'nifi_feed' AND latencia_ms_prom IS NOT NULL;",
            (6, 0, 6, 4),
            "#5794F2",
        ),
        (
            3,
            "Duración Media Spark (s)",
            "SELECT ROUND(COALESCE(avg(duracion_s), 0)::numeric, 1) AS val "
            "FROM sismos.metricas_pipeline WHERE $__timeFilter(ts) "
            "AND componente = 'spark' AND duracion_s IS NOT NULL;",
            (12, 0, 6, 4),
            "#FF9830",
        ),
        (
            4,
            "Registros Rechazados por Calidad",
            "SELECT COALESCE(SUM(cantidad) FILTER (WHERE motivo <> 'fuera_de_alcance_magnitud'), 0) AS val "
            "FROM sismos.rechazados WHERE $__timeFilter(ts);",
            (18, 0, 6, 4),
            "#F2495C",
        ),
    ]
    return [_build_stat_panel(pid, title, q, pos, color) for pid, title, q, pos, color in kpis]


def _build_salud_volumen_panels() -> list[JsonDict]:
    """Construye los paneles de throughput temporal y volumen acumulado por componente."""
    tp_sql = (
        "SELECT ts AS time, "
        "SUM(CASE WHEN componente = 'spark' THEN registros_in END) "
        'AS "Spark (In)", '
        "SUM(CASE WHEN componente = 'nifi_feed' THEN registros_in END) "
        'AS "NiFi Feed (In)", '
        "SUM(CASE WHEN componente = 'nifi_historico' THEN registros_in END) "
        'AS "NiFi Histórico (In)" '
        "FROM sismos.metricas_pipeline "
        "WHERE $__timeFilter(ts) "
        "GROUP BY ts "
        "ORDER BY ts ASC;"
    )
    p_tp = _build_timeseries_panel(
        5,
        "Throughput Temporal por Componente (Registros In)",
        [(tp_sql, "A")],
        (0, 4, 14, 8),
    )

    # Spark relee toda la zona raw en cada corrida y NiFi reporta ventanas móviles de 5 min: sumar lecturas
    # sobrecontaría. Se muestra la última lectura de cada componente en el rango.
    vol_sql = (
        'SELECT DISTINCT ON (componente) componente AS "Componente", '
        'registros_in AS "Registros In", registros_ok AS "Registros OK" '
        "FROM sismos.metricas_pipeline WHERE $__timeFilter(ts) "
        "ORDER BY componente, ts DESC;"
    )
    p_vol = _build_barchart_panel(
        6,
        "Última Lectura por Componente (In vs OK)",
        vol_sql,
        (14, 4, 10, 8),
        x_field="Componente",
    )
    return [p_tp, p_vol]


def _build_salud_eficiencia_panels() -> list[JsonDict]:
    """Construye los paneles de tasas y balance de eficiencia operativa."""
    tasas_sql = (
        'SELECT DISTINCT ON (componente) componente AS "Componente", '
        'registros_ok AS "Registros OK", registros_duplicados AS "Duplicados", registros_error AS "Errores" '
        "FROM sismos.metricas_pipeline WHERE $__timeFilter(ts) "
        "ORDER BY componente, ts DESC;"
    )
    p_tasas = _build_barchart_panel(
        7,
        "Distribución (última lectura): OK vs Duplicados vs Errores",
        tasas_sql,
        (0, 12, 10, 8),
        x_field="Componente",
        stacking="normal",
    )

    bal_sql = (
        'SELECT DISTINCT ON (componente) componente AS "Componente", '
        'registros_in AS "Total In", registros_ok AS "Total OK", '
        'registros_duplicados AS "Total Duplicados", registros_error AS "Total Errores", '
        'ROUND(registros_ok::numeric / NULLIF(registros_in, 0) * 100, 2) AS "% Éxito", '
        'ROUND(registros_duplicados::numeric / NULLIF(registros_in, 0) * 100, 2) AS "% Duplicados", '
        'ROUND(registros_error::numeric / NULLIF(registros_in, 0) * 100, 2) AS "% Error" '
        "FROM sismos.metricas_pipeline WHERE $__timeFilter(ts) "
        "ORDER BY componente, ts DESC;"
    )
    p_bal = _build_table_panel(
        8,
        "Tasas de Éxito, Duplicados y Errores (última lectura)",
        bal_sql,
        (10, 12, 14, 8),
        "Última lectura de nifi_feed (ventana móvil de 5 min), nifi_historico y spark (corrida completa)",
    )
    return [p_tasas, p_bal]


def _build_salud_rendimiento_descartes_panels() -> list[JsonDict]:
    """Construye los paneles de latencia, duración y auditoría de descartes."""
    lat_sql = (
        'SELECT ts AS time, latencia_ms_prom AS "Latencia NiFi (ms)" '
        "FROM sismos.metricas_pipeline WHERE $__timeFilter(ts) "
        "AND latencia_ms_prom IS NOT NULL ORDER BY ts ASC;"
    )
    dur_sql = (
        'SELECT ts AS time, duracion_s AS "Duración Spark (s)" '
        "FROM sismos.metricas_pipeline WHERE $__timeFilter(ts) "
        "AND componente = 'spark' AND duracion_s IS NOT NULL ORDER BY ts ASC;"
    )
    lat_overrides: list[JsonDict] = [
        {
            "matcher": {"id": "byName", "options": "Latencia NiFi (ms)"},
            "properties": [
                {"id": "custom.axisPlacement", "value": "left"},
                {"id": "custom.axisLabel", "value": "Latencia NiFi (ms)"},
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794F2"}},
            ],
        },
        {
            "matcher": {"id": "byName", "options": "Duración Spark (s)"},
            "properties": [
                {"id": "custom.axisPlacement", "value": "right"},
                {"id": "custom.axisLabel", "value": "Duración Spark (s)"},
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "#FF9830"}},
            ],
        },
    ]
    p_lat = _build_timeseries_panel(
        9,
        "Latencia Promedio NiFi (ms) y Duración Spark (s)",
        [(lat_sql, "A"), (dur_sql, "B")],
        (0, 20, 14, 8),
        lat_overrides,
    )

    pie_sql = (
        'SELECT motivo AS "Motivo", SUM(cantidad) AS "Cantidad" '
        "FROM sismos.rechazados WHERE $__timeFilter(ts) "
        'GROUP BY motivo ORDER BY "Cantidad" DESC;'
    )
    p_pie = _build_piechart_panel(
        10,
        "Distribución de Motivos de Descarte (eventos)",
        pie_sql,
        (14, 20, 10, 8),
    )

    rej_sql = (
        'SELECT ts AS "Timestamp", fuente AS "Fuente", '
        'motivo AS "Motivo", cantidad AS "Eventos", detalle AS "Detalle Técnico" '
        "FROM sismos.rechazados WHERE $__timeFilter(ts) "
        "ORDER BY ts DESC LIMIT 500;"
    )
    p_rej = _build_table_panel(
        11,
        "Auditoría de Registros Rechazados (sismos.rechazados)",
        rej_sql,
        (0, 28, 24, 8),
        "Detalle de motivos de descarte capturados durante la limpieza de datos",
    )
    return [p_lat, p_pie, p_rej]


def _build_salud_pipeline_panels() -> list[JsonDict]:
    """Genera la lista completa de paneles del dashboard operacional."""
    return [
        *_build_salud_kpis(),
        *_build_salud_volumen_panels(),
        *_build_salud_eficiencia_panels(),
        *_build_salud_rendimiento_descartes_panels(),
    ]


def build_salud_pipeline_dashboard() -> JsonDict:
    """Construye el dashboard completo de salud operacional del pipeline."""
    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": [],
        "liveNow": False,
        "panels": _build_salud_pipeline_panels(),
        "refresh": "30s",
        "schemaVersion": 39,
        "tags": ["sismos", "salud", "pipeline", "operacional"],
        "templating": {"list": []},
        "time": {"from": "now-90d", "to": "now"},
        "timepicker": {
            "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"]
        },
        "timezone": "utc",
        "title": "2. Salud Operacional del Pipeline - Monitoreo",
        "uid": "02-sismos-salud-pipeline",
        "version": 1,
        "weekStart": "",
    }


def _save_json(target_path: Path, payload: JsonDict) -> None:
    """Serializa y almacena un dashboard en disco en formato JSON legible."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    target_path.write_text(text, encoding="utf-8")
    print(f"[OK] Generado: {target_path} ({len(text)} bytes)")


def main() -> int:
    """Punto de entrada para la generación de dashboards de visualización."""
    base_dir = Path(__file__).resolve().parent / "dashboards"
    negocio_file = base_dir / "01_sismos_kmeans_negocio.json"
    salud_file = base_dir / "02_sismos_salud_pipeline.json"

    print("--- Generando Dashboards de Grafana (schemaVersion 39) ---")
    _save_json(negocio_file, build_negocio_dashboard())
    _save_json(salud_file, build_salud_pipeline_dashboard())
    print("[EXITO] Generación de dashboards completada satisfactoriamente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
