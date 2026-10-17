r"""
Construye el pipeline "Pipeline Sismos USGS" en Apache NiFi 2.7.2 usando la API REST.

Estructura (Process Groups dentro de un PG contenedor):
    1-Ingesta  ->  2-Validacion  ->  3-Procesamiento  ->  4-Salida
         \               \                  \                 \
          +---------------+------------------+-----------------+--> 5-Manejo de Errores

Uso:  python3 construir_pipeline.py            (borra el pipeline anterior si existe y lo recrea)
Lee credenciales de la raíz del repo: .env  (NIFI_USER / NIFI_PASSWORD / NIFI_PORT).
"""
import time
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings()

BASE = Path(__file__).resolve().parent.parent          # 02_Codigo/ingesta
RAIZ = BASE.parents[1]                                  # raíz del repo (.env, data/)
ENV: dict[str, str] = {}
env_file = RAIZ / ".env"
if env_file.exists():
    for linea in env_file.read_text().splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            ENV[k.strip()] = v.strip()
API = f"https://localhost:{ENV.get('NIFI_PORT', '8443')}/nifi-api"
NOMBRE_PIPELINE = "Pipeline Sismos USGS"
V = "2.7.2"

S = requests.Session()
S.verify = False


# ---------------------------------------------------------------- utilidades REST
def login():
    r = S.post(f"{API}/access/token", data={"username": ENV["NIFI_USER"], "password": ENV["NIFI_PASSWORD"]})
    r.raise_for_status()
    S.headers["Authorization"] = f"Bearer {r.text}"


def get(path):
    r = S.get(f"{API}{path}")
    r.raise_for_status()
    return r.json()


def send(method, path, body=None, **params):
    r = S.request(method, f"{API}{path}", json=body, params=params)
    if not r.ok:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json() if r.text else {}


def rev(version=0):
    return {"revision": {"version": version}}


def bundle(artifact):
    return {"group": "org.apache.nifi", "artifact": artifact, "version": V}


def pos(x, y):
    return {"x": x, "y": y}


# ---------------------------------------------------------------- constructores
def process_group(parent, nombre, x, y, comentario="", contexto=None):
    comp = {"name": nombre, "position": pos(x, y), "comments": comentario}
    if contexto:
        comp["parameterContext"] = {"id": contexto}
    return send("POST", f"/process-groups/{parent}/process-groups", rev() | {"component": comp})["id"]


def processor(pg, tipo, nombre, x, y, props=None, auto=(), periodo=None, artifact="nifi-standard-nar",
              comentario="", penalizacion=None):
    paquete = {"nifi-standard-nar": "org.apache.nifi.processors.standard.",
               "nifi-update-attribute-nar": "org.apache.nifi.processors.attributes.",
               "nifi-jolt-nar": "org.apache.nifi.processors.jolt."}[artifact]
    cfg = {"properties": props or {}, "autoTerminatedRelationships": list(auto), "comments": comentario}
    if periodo:
        cfg["schedulingPeriod"] = periodo
    if penalizacion:
        cfg["penaltyDuration"] = penalizacion
    body = rev() | {"component": {"type": paquete + tipo, "bundle": bundle(artifact), "name": nombre,
                                  "position": pos(x, y), "config": cfg}}
    return send("POST", f"/process-groups/{pg}/processors", body)["id"]


def puerto(pg, tipo, nombre, x, y):
    ruta = "input-ports" if tipo == "in" else "output-ports"
    return send("POST", f"/process-groups/{pg}/{ruta}", rev() | {"component": {"name": nombre, "position": pos(x, y)}})["id"]


TIPOS_EXTREMO = {"p": "PROCESSOR", "in": "INPUT_PORT", "out": "OUTPUT_PORT", "f": "FUNNEL"}


def conectar(pg, origen, destino, rels=(), nombre="", bp_objetos=10000, bp_tamano="1 GB", dobleces=None):
    """origen/destino = (tipo, id, group_id). Para puertos de PG hijos, group_id es el del hijo."""
    def extremo(e):
        return {"id": e[1], "groupId": e[2], "type": TIPOS_EXTREMO[e[0]]}

    comp = {"name": nombre, "source": extremo(origen), "destination": extremo(destino),
            "selectedRelationships": list(rels),
            "backPressureObjectThreshold": bp_objetos, "backPressureDataSizeThreshold": bp_tamano}
    if dobleces:
        comp["bends"] = [pos(*b) for b in dobleces]
    return send("POST", f"/process-groups/{pg}/connections", rev() | {"component": comp})["id"]


def servicio(pg, tipo, nombre, props, artifact, comentario=""):
    body = rev() | {"component": {"type": tipo, "bundle": bundle(artifact), "name": nombre,
                                  "properties": props, "comments": comentario}}
    return send("POST", f"/process-groups/{pg}/controller-services", body)["id"]


# ---------------------------------------------------------------- limpieza
def borrar_pipeline_anterior(raiz, nombre=NOMBRE_PIPELINE, nombre_ctx="parametros-sismos"):
    for pg in get(f"/process-groups/{raiz}/process-groups")["processGroups"]:
        if pg["component"]["name"] != nombre:
            continue
        pid = pg["id"]
        print(f"Borrando pipeline anterior {pid} ...")
        send("PUT", f"/flow/process-groups/{pid}", {"id": pid, "state": "STOPPED"})
        send("PUT", f"/flow/process-groups/{pid}/controller-services", {"id": pid, "state": "DISABLED"})
        time.sleep(3)
        req = send("POST", f"/process-groups/{pid}/empty-all-connections-requests")
        time.sleep(2)
        S.delete(f"{API}/process-groups/{pid}/empty-all-connections-requests/{req['dropRequest']['id']}")
        actual = get(f"/process-groups/{pid}")
        send("DELETE", f"/process-groups/{pid}", version=actual["revision"]["version"])
    for ctx in get("/flow/parameter-contexts")["parameterContexts"]:
        if ctx["component"]["name"] == nombre_ctx:
            send("DELETE", f"/parameter-contexts/{ctx['id']}", version=ctx["revision"]["version"])


# ================================================================ PIPELINE
def construir():
    for d in ("entrada", "salida", "errores", "alertas"):      # carpetas montadas en el contenedor
        (RAIZ / "data" / "raw" / d).mkdir(parents=True, exist_ok=True)
    login()
    raiz = get("/flow/process-groups/root")["processGroupFlow"]["id"]
    borrar_pipeline_anterior(raiz)

    # ---------- Parameter Context: umbrales y rutas parametrizados (no "hardcodeados")
    params = {
        "usgs_feed_url": ("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
                          "Feed GeoJSON de USGS (sismos de las últimas 24 h, se actualiza cada minuto)"),
        "intervalo_consulta": ("5 min", "Cada cuánto InvokeHTTP consulta la API"),
        "umbral_fuerte": ("4.5", "Magnitud >= este valor -> categoría FUERTE (genera alerta)"),
        "umbral_moderado": ("2.5", "Magnitud >= este valor y < umbral_fuerte -> MODERADO"),
        "dir_entrada": ("/opt/nifi/datos/entrada", "Carpeta vigilada por GetFile (lotes de prueba)"),
        "dir_salida": ("/opt/nifi/datos/salida", "Raíz de las salidas procesadas"),
        "dir_alertas": ("/opt/nifi/datos/alertas", "Alertas de sismos fuertes / tsunami"),
        "dir_errores": ("/opt/nifi/datos/errores", "Registros rechazados o con fallas"),
    }
    ctx = send("POST", "/parameter-contexts", rev() | {"component": {
        "name": "parametros-sismos",
        "description": "Umbrales de clasificación y rutas del pipeline de sismos",
        "parameters": [{"parameter": {"name": k, "value": v, "description": d, "sensitive": False}}
                       for k, (v, d) in params.items()]}})["id"]

    pipe = process_group(raiz, NOMBRE_PIPELINE, 0, 0, contexto=ctx,
                         comentario="Ingesta en vivo de sismos (API USGS) + lotes de archivos; validación, "
                                    "clasificación por magnitud, enriquecimiento, almacenamiento y alertas.")

    # ---------- Controller Services (definidos en el PG contenedor -> visibles para todos los hijos)
    esquema = (BASE / "config" / "esquema_sismo.avsc").read_text()
    lector = servicio(pipe, "org.apache.nifi.json.JsonTreeReader", "Lector JSON Sismo (esquema Avro)",
                      {"Schema Access Strategy": "schema-text-property", "Schema Text": esquema},
                      "nifi-record-serialization-services-nar",
                      "Lee los sismos ya normalizados por Jolt usando un esquema Avro explícito")
    escritor_csv = servicio(pipe, "org.apache.nifi.csv.CSVRecordSetWriter", "Escritor CSV consolidado",
                            {"Schema Access Strategy": "inherit-record-schema", "Include Header Line": "true"},
                            "nifi-record-serialization-services-nar",
                            "Escribe los lotes consolidados por categoría en CSV")
    servicio(
        pipe,
        "org.apache.nifi.distributed.cache.server.map.MapCacheServer",
        "Servidor de Caché (deduplicación)",
        {"Port": "4557", "Maximum Cache Entries": "50000", "Eviction Strategy": "First In, First Out"},
        "nifi-distributed-cache-services-nar",
        "Caché en memoria para recordar qué eventos ya se procesaron",
    )
    cache_cli = servicio(pipe, "org.apache.nifi.distributed.cache.client.MapCacheClientService",
                         "Cliente de Caché (deduplicación)",
                         {"Server Hostname": "localhost", "Server Port": "4557"},
                         "nifi-distributed-cache-services-nar",
                         "Cliente que usa DetectDuplicate para consultar el caché")

    # ---------- Process Groups lógicos
    g_ing = process_group(pipe, "1 - Ingesta", 0, 0, "Fuentes: API REST USGS (InvokeHTTP) y archivos (GetFile)", ctx)
    g_val = process_group(pipe, "2 - Validación", 520, 0, "Extracción de campos, reglas de calidad y deduplicación", ctx)
    g_pro = process_group(pipe, "3 - Procesamiento", 1040, 0, "Clasificación por magnitud y transformación Jolt", ctx)
    g_sal = process_group(pipe, "4 - Salida", 1560, 0, "Consolidados CSV por categoría (zona raw para Spark) y alertas", ctx)
    g_err = process_group(pipe, "5 - Manejo de Errores", 780, 420, "Registro (LogAttribute) y archivo de rechazos", ctx)

    # ======================= 1 - INGESTA
    api = processor(g_ing, "InvokeHTTP", "Consultar API USGS", 0, 0, {
        "HTTP Method": "GET", "HTTP URL": "#{usgs_feed_url}",
        "Connection Timeout": "10 secs", "Socket Read Timeout": "30 secs",
        "Request User-Agent": "NiFi-Practica-INTEC/1.0"},
        auto=["Original"], periodo="#{intervalo_consulta}",
        comentario="Ingesta en vivo: descarga el FeatureCollection GeoJSON de sismos de USGS")
    archivo = processor(g_ing, "GetFile", "Leer lotes de archivos", 480, 0, {
        "Input Directory": "#{dir_entrada}", "File Filter": r".*\.(json|geojson)",
        "Keep Source File": "false", "Polling Interval": "10 sec"}, periodo="10 sec",
        comentario="Ingesta por archivos: lotes de prueba con casos edge dejados en datos/entrada")
    etiqueta = processor(g_ing, "UpdateAttribute", "Etiquetar fuente y lote", 240, 260, {
        "fuente": "${invokehttp.request.url:isEmpty():ifElse('archivo_local','api_usgs')}",
        "lote.id": "${uuid}",
        "lote.recibido": "${now():format('yyyy-MM-dd HH:mm:ss')}"},
        artifact="nifi-update-attribute-nar")
    split = processor(g_ing, "SplitJson", "Separar eventos (features)", 240, 500, {
        "JsonPath Expression": "$.features[*]"}, auto=["original"],
        comentario="Un FlowFile por sismo")
    err_ing = processor(g_ing, "UpdateAttribute", "Marcar error de ingesta", 760, 500, {
        "error.etapa": "ingesta",
        "error.motivo": "${invokehttp.status.code:isEmpty():ifElse('json_malformado', "
                        "${invokehttp.status.code:prepend('http_')})}"},
        artifact="nifi-update-attribute-nar")
    out_ing = puerto(g_ing, "out", "eventos", 240, 760)
    outerr_ing = puerto(g_ing, "out", "errores", 760, 760)

    conectar(g_ing, ("p", api, g_ing), ("p", etiqueta, g_ing), ["Response"])
    conectar(g_ing, ("p", archivo, g_ing), ("p", etiqueta, g_ing), ["success"])
    conectar(g_ing, ("p", etiqueta, g_ing), ("p", split, g_ing), ["success"])
    conectar(g_ing, ("p", split, g_ing), ("out", out_ing, g_ing), ["split"], "eventos individuales")
    conectar(g_ing, ("p", api, g_ing), ("p", err_ing, g_ing), ["Failure", "No Retry", "Retry"],
             dobleces=[(1060, 40)])
    conectar(g_ing, ("p", split, g_ing), ("p", err_ing, g_ing), ["failure"])
    conectar(g_ing, ("p", err_ing, g_ing), ("out", outerr_ing, g_ing), ["success"])

    # ======================= 2 - VALIDACION
    in_val = puerto(g_val, "in", "eventos", 240, 0)
    extraer = processor(g_val, "EvaluateJsonPath", "Extraer campos clave", 160, 160, {
        "Destination": "flowfile-attribute", "Return Type": "auto-detect", "Path Not Found Behavior": "ignore",
        "sismo.id": "$.id",
        "sismo.mag": "$.properties.mag",
        "sismo.tipo": "$.properties.type",
        "sismo.lugar": "$.properties.place",
        "sismo.tiempo": "$.properties.time",
        "sismo.actualizado": "$.properties.updated",
        "sismo.estado": "$.properties.status",
        "sismo.tsunami": "$.properties.tsunami",
        "sismo.longitud": "$.geometry.coordinates[0]",
        "sismo.latitud": "$.geometry.coordinates[1]",
        "sismo.profundidad": "$.geometry.coordinates[2]"},
        comentario="Lleva los campos del JSON a atributos para poder validarlos y enrutarlos")
    num = "^-?[0-9]+([.][0-9]+)?$"
    reglas = processor(g_val, "RouteOnAttribute", "Validar reglas de calidad", 160, 400, {
        "Routing Strategy": "Route to Property name",
        "id_faltante": "${sismo.id:trim():isEmpty()}",
        "magnitud_invalida": "${sismo.mag:matches('" + num + "'):not()}",
        "magnitud_fuera_de_rango": "${sismo.mag:toDecimal():lt(-2):or(${sismo.mag:toDecimal():gt(10)})}",
        "tiempo_invalido": "${sismo.tiempo:matches('^[0-9]{10,13}$'):not()}",
        "coordenadas_invalidas": "${sismo.latitud:matches('" + num + "'):not():or("
                                 "${sismo.longitud:matches('" + num + "'):not()})}",
        "tipo_faltante": "${sismo.tipo:trim():isEmpty()}"},
        comentario="unmatched = cumple TODAS las reglas (registro válido)")
    dedup = processor(g_val, "DetectDuplicate", "Descartar duplicados", 160, 640, {
        "Cache Entry Identifier": "${sismo.id}-${sismo.actualizado}",
        "FlowFile Description": "${sismo.lugar}",
        "Age Off Duration": "24 hours",
        "Distributed Cache Service": cache_cli},
        comentario="La API devuelve las últimas 24 h en cada consulta: se descarta lo ya procesado "
                   "(misma id + misma fecha de actualización)")
    log_dup = processor(g_val, "LogMessage", "Registrar duplicado", 620, 640, {
        "Log Level": "debug", "Log Message": "Duplicado descartado: ${sismo.id} (${sismo.lugar})"},
        auto=["success"])
    rechazo = processor(g_val, "UpdateAttribute", "Marcar rechazo de validación", 700, 400, {
        "error.etapa": "validacion",
        "error.motivo": "${RouteOnAttribute.Route:replaceNull('falla_tecnica'):replace('unmatched','falla_tecnica')}"},
        artifact="nifi-update-attribute-nar")
    out_val = puerto(g_val, "out", "validos", 240, 880)
    outerr_val = puerto(g_val, "out", "errores", 780, 880)

    invalidas = ["id_faltante", "magnitud_invalida", "magnitud_fuera_de_rango", "tiempo_invalido",
                 "coordenadas_invalidas", "tipo_faltante"]
    conectar(g_val, ("in", in_val, g_val), ("p", extraer, g_val))
    conectar(g_val, ("p", extraer, g_val), ("p", reglas, g_val), ["matched", "unmatched"])
    conectar(g_val, ("p", reglas, g_val), ("p", dedup, g_val), ["unmatched"], "válidos")
    conectar(g_val, ("p", reglas, g_val), ("p", rechazo, g_val), invalidas, "rechazados")
    conectar(g_val, ("p", extraer, g_val), ("p", rechazo, g_val), ["failure"], dobleces=[(1000, 200)])
    conectar(g_val, ("p", dedup, g_val), ("p", log_dup, g_val), ["duplicate"])
    conectar(g_val, ("p", dedup, g_val), ("p", rechazo, g_val), ["failure"])
    conectar(g_val, ("p", dedup, g_val), ("out", out_val, g_val), ["non-duplicate"])
    conectar(g_val, ("p", rechazo, g_val), ("out", outerr_val, g_val), ["success"])

    # ======================= 3 - PROCESAMIENTO
    in_pro = puerto(g_pro, "in", "validos", 520, 0)
    mag = "${sismo.mag:toDecimal()"
    clasificar = processor(g_pro, "RouteOnAttribute", "Clasificar por magnitud", 440, 160, {
        "Routing Strategy": "Route to Property name",
        "fuerte": mag + ":ge(#{umbral_fuerte})}",
        "moderado": mag + ":ge(#{umbral_moderado}):and(" + mag + ":lt(#{umbral_fuerte})})}",
        "menor": mag + ":lt(#{umbral_moderado})}"},
        comentario="3 rutas: fuerte (>= 4.5), moderado (2.5 a < 4.5), menor (< 2.5)")
    cats = {
        "fuerte": ("ROJO", "1", 0),
        "moderado": ("AMARILLO", "2", 440),
        "menor": ("VERDE", "3", 880),
    }
    ua_cat = {}
    for cat, (nivel, prio, x) in cats.items():
        ua_cat[cat] = processor(g_pro, "UpdateAttribute", f"Categoría {cat}", x, 400, {
            "categoria": cat, "nivel_alerta": nivel, "prioridad": prio},
            artifact="nifi-update-attribute-nar")
    enriquecer = processor(g_pro, "UpdateAttribute", "Enriquecer metadatos", 440, 640, {
        "fecha_evento_utc": "${sismo.tiempo:toNumber():format(\"yyyy-MM-dd'T'HH:mm:ss'Z'\", 'GMT')}",
        "timestamp_procesamiento": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\")}",
        "origen": "${sismo.tipo:equals('earthquake'):ifElse('natural','antropogenico')}",
        "tipo_dir": "${sismo.tipo:replaceAll('[ ]+','_')}",
        "filename": "${sismo.id}.json"},
        artifact="nifi-update-attribute-nar",
        comentario="Fecha legible, sello de procesamiento, origen natural/antropogénico, nombre de archivo")
    jolt = processor(g_pro, "JoltTransformJSON", "Normalizar y enriquecer (Jolt)", 440, 880, {
        "Jolt Transform": "jolt-transform-chain",
        "Jolt Specification": (BASE / "config" / "jolt_sismo.json").read_text(),
        "Pretty Print": "true"}, artifact="nifi-jolt-nar",
        comentario="shift: aplana GeoJSON a registro plano en español; default: agrega categoría y "
                   "timestamp de procesamiento")
    err_pro = processor(g_pro, "UpdateAttribute", "Marcar error de procesamiento", 1000, 880, {
        "error.etapa": "procesamiento",
        "error.motivo": "${RouteOnAttribute.Route:equals('unmatched'):ifElse('sin_categoria','falla_transformacion')}"},
        artifact="nifi-update-attribute-nar")
    out_pro = puerto(g_pro, "out", "procesados", 520, 1120)
    outerr_pro = puerto(g_pro, "out", "errores", 1080, 1120)

    conectar(g_pro, ("in", in_pro, g_pro), ("p", clasificar, g_pro))
    for cat in cats:
        conectar(g_pro, ("p", clasificar, g_pro), ("p", ua_cat[cat], g_pro), [cat], cat)
        conectar(g_pro, ("p", ua_cat[cat], g_pro), ("p", enriquecer, g_pro), ["success"])
    conectar(g_pro, ("p", clasificar, g_pro), ("p", err_pro, g_pro), ["unmatched"], dobleces=[(1380, 200)])
    conectar(g_pro, ("p", enriquecer, g_pro), ("p", jolt, g_pro), ["success"])
    conectar(g_pro, ("p", jolt, g_pro), ("p", err_pro, g_pro), ["failure"])
    conectar(g_pro, ("p", jolt, g_pro), ("out", out_pro, g_pro), ["success"])
    conectar(g_pro, ("p", err_pro, g_pro), ("out", outerr_pro, g_pro), ["success"])

    # ======================= 4 - SALIDA
    in_sal = puerto(g_sal, "in", "procesados", 520, 0)
    consolidar = processor(g_sal, "MergeRecord", "Consolidar lote por categoría", 440, 200, {
        "Record Reader": lector, "Record Writer": escritor_csv,
        "Merge Strategy": "Bin-Packing Algorithm", "Correlation Attribute Name": "categoria",
        "Minimum Number of Records": "500", "Maximum Number of Records": "5000",
        "Max Bin Age": "2 min"}, auto=["original"],
        comentario="Junta los sismos de cada categoría en un CSV (cierra el lote cada 2 min o a los 5000)")
    nombrar_csv = processor(g_sal, "UpdateAttribute", "Nombrar CSV consolidado", 440, 440, {
        "filename": "sismos_${categoria}_${now():format('yyyyMMdd_HHmmss')}.csv"},
        artifact="nifi-update-attribute-nar")
    guardar_csv = processor(g_sal, "PutFile", "Guardar consolidado CSV", 440, 680, {
        "Directory": "#{dir_salida}/consolidado", "Conflict Resolution Strategy": "replace"},
        auto=["success"])
    es_alerta = processor(g_sal, "RouteOnAttribute", "¿Requiere alerta?", 880, 200, {
        "Routing Strategy": "Route to Property name",
        "alerta": "${categoria:equals('fuerte'):or(${sismo.tsunami:equals('1')})}"},
        auto=["unmatched"], comentario="Alerta si magnitud >= umbral_fuerte o si USGS marca tsunami")
    log_alerta = processor(g_sal, "LogMessage", "Emitir alerta (log WARN)", 880, 440, {
        "Log Level": "warn", "Log Prefix": "[ALERTA SISMICA] ",
        "Log Message": "M${sismo.mag} ${sismo.lugar} | ${fecha_evento_utc} | prof ${sismo.profundidad} km | "
                       "tsunami=${sismo.tsunami} | ${sismo.id}"})
    json_alerta = processor(g_sal, "AttributesToJSON", "Construir aviso de alerta", 880, 680, {
        "Attributes List": "sismo.id,sismo.mag,sismo.lugar,fecha_evento_utc,nivel_alerta,sismo.tsunami,"
                           "sismo.profundidad,sismo.latitud,sismo.longitud,sismo.estado,timestamp_procesamiento",
        "Destination": "flowfile-content", "Include Core Attributes": "false", "Pretty Print": "true"})
    guardar_alerta = processor(g_sal, "PutFile", "Guardar alerta", 880, 920, {
        "Directory": "#{dir_alertas}", "Conflict Resolution Strategy": "replace"}, auto=["success"])
    err_sal = processor(g_sal, "UpdateAttribute", "Marcar error de salida", 1320, 680, {
        "error.etapa": "salida", "error.motivo": "falla_escritura"},
        artifact="nifi-update-attribute-nar")
    outerr_sal = puerto(g_sal, "out", "errores", 1400, 920)

    conectar(g_sal, ("in", in_sal, g_sal), ("p", consolidar, g_sal))
    conectar(g_sal, ("in", in_sal, g_sal), ("p", es_alerta, g_sal))
    conectar(g_sal, ("p", consolidar, g_sal), ("p", nombrar_csv, g_sal), ["merged"])
    conectar(g_sal, ("p", nombrar_csv, g_sal), ("p", guardar_csv, g_sal), ["success"])
    conectar(g_sal, ("p", es_alerta, g_sal), ("p", log_alerta, g_sal), ["alerta"], "alerta")
    conectar(g_sal, ("p", log_alerta, g_sal), ("p", json_alerta, g_sal), ["success"])
    conectar(g_sal, ("p", json_alerta, g_sal), ("p", guardar_alerta, g_sal), ["success"])
    for p in (guardar_csv, guardar_alerta, consolidar, json_alerta):
        conectar(g_sal, ("p", p, g_sal), ("p", err_sal, g_sal), ["failure"])
    conectar(g_sal, ("p", err_sal, g_sal), ("out", outerr_sal, g_sal), ["success"])

    # ======================= 5 - MANEJO DE ERRORES
    in_err = puerto(g_err, "in", "errores", 240, 0)
    sellar = processor(g_err, "UpdateAttribute", "Sellar error", 160, 160, {
        "error.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss')}",
        "filename": "${error.etapa}_${error.motivo}_${uuid}.json"},
        artifact="nifi-update-attribute-nar")
    log_err = processor(g_err, "LogAttribute", "Registrar error (LogAttribute)", 160, 400, {
        "Log Level": "warn", "Log Payload": "true", "Log Prefix": "ERROR-PIPELINE",
        "Attributes to Log Regular Expression": "error\\..*|sismo\\.id|fuente|filename|lote\\.id"})
    guardar_err = processor(g_err, "PutFile", "Archivar rechazos", 160, 640, {
        "Directory": "#{dir_errores}/${error.etapa}/${error.motivo}",
        "Conflict Resolution Strategy": "replace"}, auto=["success"])
    critico = processor(g_err, "LogMessage", "Falla crítica al archivar", 620, 640, {
        "Log Level": "error", "Log Message": "No se pudo archivar el error ${filename}"}, auto=["success"])

    conectar(g_err, ("in", in_err, g_err), ("p", sellar, g_err))
    conectar(g_err, ("p", sellar, g_err), ("p", log_err, g_err), ["success"])
    conectar(g_err, ("p", log_err, g_err), ("p", guardar_err, g_err), ["success"])
    conectar(g_err, ("p", guardar_err, g_err), ("p", critico, g_err), ["failure"])

    # ======================= CONEXIONES ENTRE GRUPOS (con backpressure explícito)
    conectar(pipe, ("out", out_ing, g_ing), ("in", in_val, g_val), nombre="ingesta -> validación",
             bp_objetos=10000, bp_tamano="1 GB")
    conectar(pipe, ("out", out_val, g_val), ("in", in_pro, g_pro), nombre="validación -> procesamiento",
             bp_objetos=10000, bp_tamano="1 GB")
    conectar(pipe, ("out", out_pro, g_pro), ("in", in_sal, g_sal), nombre="procesamiento -> salida",
             bp_objetos=5000, bp_tamano="500 MB")
    for g, o in ((g_ing, outerr_ing), (g_val, outerr_val), (g_pro, outerr_pro), (g_sal, outerr_sal)):
        conectar(pipe, ("out", o, g), ("in", in_err, g_err), nombre="errores",
                 bp_objetos=2000, bp_tamano="100 MB")

    # ---------- habilitar servicios
    send("PUT", f"/flow/process-groups/{pipe}/controller-services", {"id": pipe, "state": "ENABLED"})
    time.sleep(3)
    for cs in get(f"/flow/process-groups/{pipe}/controller-services")["controllerServices"]:
        c = cs["component"]
        print(f"  servicio {c['name']:<38} {c['state']}  {c.get('validationErrors') or ''}")

    invalidos = [p for p in get(f"/process-groups/{pipe}/processors?includeDescendantGroups=true")["processors"]
                 if p["component"].get("validationErrors")]
    for p in invalidos:
        print("  INVALIDO:", p["component"]["name"], p["component"]["validationErrors"])
    print(f"\nPipeline creado: {pipe}  ({'sin' if not invalidos else len(invalidos)} processors inválidos)")
    return pipe


if __name__ == "__main__":
    construir()
