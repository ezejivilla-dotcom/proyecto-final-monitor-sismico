r"""
Construye el pipeline "Pipeline Historico USGS" (fuente 2: carga por lotes) en NiFi vía API REST.

Descarga el catálogo histórico del USGS (API FDSN) en ventanas mensuales, DOS consultas por ventana:

    GenerateFlowFile -> SplitText -> EvaluateJsonPath -> ControlRate --+--> InvokeHTTP (format=csv)
        -> ¿vacío? -> ValidateRecord (esquema Avro) -> PutFile  data/raw/historico/
                          \-> (invalid / fallas HTTP) -> PutFile data/raw/errores/historico/
                                                                   |
                                                                   +--> InvokeHTTP (format=geojson)
        -> Jolt (aplana FeatureCollection) -> ConvertRecord JSON->CSV -> PutFile data/raw/historico_impacto/

El CSV trae calidad de la medición (nst, gap, errores...); el GeoJSON es el único que trae impacto humano
(felt, cdi, mmi, alert PAGER, tsunami, sig). Spark los une por evento_id.

Rate limiting: ControlRate deja pasar 1 ventana cada 5 s (respeta la API pública del USGS).
Idempotente: borra y recrea el pipeline. Requiere haber levantado NiFi (docker compose up -d nifi).

Uso:  python3 construir_historico.py [anios=5] [magnitud_minima=4.0]
"""
import json
import sys
import time
from datetime import date, timedelta

import construir_pipeline as base
from construir_pipeline import (BASE, RAIZ, conectar, get, login, process_group, processor, puerto, rev, send,
                                servicio)

NOMBRE = "Pipeline Historico USGS"
CTX = "parametros-historico"


def ventanas_mensuales(anios):
    """Ventanas mensuales desde hace `anios` anios hasta HOY inclusive (el mes en curso es parcial).
    El fin de cada ventana es exclusivo en la API FDSN, por eso la ultima termina manana."""
    hoy = date.today()
    manana = hoy + timedelta(days=1)
    d = date(hoy.year - anios, hoy.month, 1)
    lista = []
    while d <= hoy:
        sig = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
        lista.append({"inicio": d.isoformat(), "fin": min(sig, manana).isoformat()})
        d = sig
    return lista


def construir(anios=5, mag_min="4.0"):
    for d in ("historico", "historico_impacto", "errores"):
        (RAIZ / "data" / "raw" / d).mkdir(parents=True, exist_ok=True)
    login()
    raiz = get("/flow/process-groups/root")["processGroupFlow"]["id"]
    base.borrar_pipeline_anterior(raiz, NOMBRE, CTX)

    params = {
        "fdsn_url": ("https://earthquake.usgs.gov/fdsnws/event/1/query", "Endpoint FDSN del catálogo USGS"),
        "hist_mag_min": (str(mag_min), "Magnitud mínima a descargar (limita el volumen)"),
        "hist_ritmo": ("5 secs", "Rate limiting: una ventana mensual cada N segundos"),
        "dir_historico": ("/opt/nifi/datos/historico", "Zona raw: CSV validados del catálogo"),
        "dir_impacto": ("/opt/nifi/datos/historico_impacto", "Zona raw: campos de impacto (felt, cdi, mmi, alert...)"),
        "dir_errores": ("/opt/nifi/datos/errores", "Registros/ventanas rechazadas"),
    }
    ctx = send("POST", "/parameter-contexts", rev() | {"component": {
        "name": CTX, "description": "Parámetros de la carga histórica USGS",
        "parameters": [{"parameter": {"name": k, "value": v, "description": d, "sensitive": False}}
                       for k, (v, d) in params.items()]}})["id"]
    pipe = process_group(raiz, NOMBRE, 0, 700, contexto=ctx,
                         comentario="Fuente 2 (batch): catálogo histórico USGS por ventanas mensuales, "
                                    "con rate limiting y validación por esquema")

    esquema = (BASE / "config" / "esquema_historico.avsc").read_text()
    lector = servicio(pipe, "org.apache.nifi.csv.CSVReader", "Lector CSV catálogo USGS",
                      {"Schema Access Strategy": "schema-text-property", "Schema Text": esquema,
                       "Treat First Line as Header": "true"},
                      "nifi-record-serialization-services-nar",
                      "Lee el CSV del catálogo con el esquema Avro explícito (tipos y obligatorios)")
    escritor = servicio(pipe, "org.apache.nifi.csv.CSVRecordSetWriter", "Escritor CSV catálogo",
                        {"Schema Access Strategy": "inherit-record-schema", "Include Header Line": "true"},
                        "nifi-record-serialization-services-nar", "Escribe CSV con cabecera")

    lector_json = servicio(pipe, "org.apache.nifi.json.JsonTreeReader", "Lector JSON impacto",
                           {"Schema Access Strategy": "schema-text-property",
                            "Schema Text": (BASE / "config" / "esquema_impacto.avsc").read_text()},
                           "nifi-record-serialization-services-nar", "Lee el arreglo aplanado por Jolt")

    texto = "\n".join(json.dumps(v) for v in ventanas_mensuales(anios))
    gen = processor(pipe, "GenerateFlowFile", "Generar calendario de ventanas", 0, 0, {
        "Custom Text": texto}, periodo="1 day",
        comentario="Una línea JSON por ventana mensual. Se dispara al iniciar y luego cada 24 h")
    split = processor(pipe, "SplitText", "Una ventana por FlowFile", 0, 160, {
        "Line Split Count": "1", "Remove Trailing Newlines": "true"}, auto=["original"])
    ext = processor(pipe, "EvaluateJsonPath", "Leer inicio y fin", 0, 320, {
        "Destination": "flowfile-attribute", "rango.inicio": "$.inicio", "rango.fin": "$.fin"})
    ritmo = processor(pipe, "ControlRate", "Limitar ritmo (rate limiting)", 0, 480, {
        "Rate Control Criteria": "flowfile count", "Maximum Rate": "1", "Time Duration": "#{hist_ritmo}"},
        comentario="Una consulta a la API cada 5 s: evita saturar el servicio público")
    http = processor(pipe, "InvokeHTTP", "Descargar CSV del catálogo", 0, 640, {
        "HTTP Method": "GET",
        "HTTP URL": "#{fdsn_url}?format=csv&starttime=${rango.inicio}&endtime=${rango.fin}"
                    "&minmagnitude=#{hist_mag_min}&orderby=time-asc",
        "Connection Timeout": "15 secs", "Socket Read Timeout": "120 secs",
        "Request User-Agent": "Sismos-Proyecto-INTEC/1.0"}, auto=["Original"])
    vacio = processor(pipe, "RouteOnAttribute", "¿Ventana sin datos?", 0, 800, {
        "Routing Strategy": "Route to Property name",
        "vacia": "${invokehttp.status.code:equals('204'):or(${fileSize:lt(100)})}"},
        auto=["vacia"], comentario="El USGS responde 204/vacío cuando no hay eventos en la ventana")
    valida = processor(pipe, "ValidateRecord", "Validar registros (esquema Avro)", 0, 960, {
        "Record Reader": lector, "Record Writer": escritor,
        "Allow Extra Fields": "true", "Strict Type Checking": "true"},
        comentario="valid -> zona raw; invalid -> errores (tipos incorrectos o campos obligatorios vacíos)")
    nombrar = processor(pipe, "UpdateAttribute", "Nombrar archivo", 0, 1120, {
        "filename": "catalogo_${rango.inicio:substring(0, 7)}.csv"},
        artifact="nifi-update-attribute-nar")
    guardar = processor(pipe, "PutFile", "Guardar en zona raw", 0, 1280, {
        "Directory": "#{dir_historico}", "Conflict Resolution Strategy": "replace"}, auto=["success"])
    marcar = processor(pipe, "UpdateAttribute", "Marcar rechazo", 600, 960, {
        "error.etapa": "historico",
        "error.motivo": "${invokehttp.status.code:isEmpty():ifElse('registros_invalidos',"
                        "${invokehttp.status.code:prepend('http_')})}",
        "filename": "${rango.inicio}_${error.motivo}_${uuid}.csv"},
        artifact="nifi-update-attribute-nar")
    err = processor(pipe, "PutFile", "Archivar rechazos", 600, 1280, {
        "Directory": "#{dir_errores}/historico/${error.motivo}", "Conflict Resolution Strategy": "replace",
        "Create Missing Directories": "true"}, auto=["success", "failure"])

    # ---- rama 2: impacto humano (GeoJSON)
    http_geo = processor(pipe, "InvokeHTTP", "Descargar GeoJSON (impacto)", 1200, 640, {
        "HTTP Method": "GET",
        "HTTP URL": "#{fdsn_url}?format=geojson&starttime=${rango.inicio}&endtime=${rango.fin}"
                    "&minmagnitude=#{hist_mag_min}&orderby=time-asc",
        "Connection Timeout": "15 secs", "Socket Read Timeout": "120 secs",
        "Request User-Agent": "Sismos-Proyecto-INTEC/1.0"}, auto=["Original"],
        comentario="Mismas ventanas que el CSV; el GeoJSON incluye felt, cdi, mmi, alert, tsunami, sig")
    jolt = processor(pipe, "JoltTransformJSON", "Aplanar FeatureCollection", 1200, 800, {
        "Jolt Transform": "jolt-transform-chain",
        "Jolt Specification": (BASE / "config" / "jolt_impacto.json").read_text()}, artifact="nifi-jolt-nar",
        comentario="features[*] -> arreglo de registros planos {evento_id, felt, cdi, mmi, ...}")
    conv = processor(pipe, "ConvertRecord", "JSON a CSV (esquema impacto)", 1200, 960, {
        "Record Reader": lector_json, "Record Writer": escritor})
    nombrar_geo = processor(pipe, "UpdateAttribute", "Nombrar archivo impacto", 1200, 1120, {
        "filename": "impacto_${rango.inicio:substring(0, 7)}.csv"}, artifact="nifi-update-attribute-nar")
    guardar_geo = processor(pipe, "PutFile", "Guardar impacto en zona raw", 1200, 1280, {
        "Directory": "#{dir_impacto}", "Conflict Resolution Strategy": "replace"}, auto=["success"])

    conectar(pipe, ("p", ritmo, pipe), ("p", http_geo, pipe), ["success"], bp_objetos=100, dobleces=[(600, 560)])
    conectar(pipe, ("p", http_geo, pipe), ("p", jolt, pipe), ["Response"])
    conectar(pipe, ("p", jolt, pipe), ("p", conv, pipe), ["success"])
    conectar(pipe, ("p", conv, pipe), ("p", nombrar_geo, pipe), ["success"])
    conectar(pipe, ("p", nombrar_geo, pipe), ("p", guardar_geo, pipe), ["success"])
    for origen, rels in ((http_geo, ["Failure", "No Retry", "Retry"]), (jolt, ["failure"]), (conv, ["failure"]),
                         (guardar_geo, ["failure"])):
        conectar(pipe, ("p", origen, pipe), ("p", marcar, pipe), rels, dobleces=[(900, 1000)])

    conectar(pipe, ("p", gen, pipe), ("p", split, pipe), ["success"])
    conectar(pipe, ("p", split, pipe), ("p", ext, pipe), ["splits"])
    conectar(pipe, ("p", ext, pipe), ("p", ritmo, pipe), ["matched"], bp_objetos=100)
    conectar(pipe, ("p", ritmo, pipe), ("p", http, pipe), ["success"], bp_objetos=100)
    for origen, rels in ((split, ["failure"]), (ext, ["failure", "unmatched"]), (ritmo, ["failure"])):
        conectar(pipe, ("p", origen, pipe), ("p", marcar, pipe), rels, dobleces=[(900, 300)])
    conectar(pipe, ("p", http, pipe), ("p", vacio, pipe), ["Response"])
    conectar(pipe, ("p", http, pipe), ("p", marcar, pipe), ["Failure", "No Retry", "Retry"], dobleces=[(900, 700)])
    conectar(pipe, ("p", vacio, pipe), ("p", valida, pipe), ["unmatched"])
    conectar(pipe, ("p", valida, pipe), ("p", nombrar, pipe), ["valid"])
    conectar(pipe, ("p", valida, pipe), ("p", marcar, pipe), ["invalid", "failure"])
    conectar(pipe, ("p", nombrar, pipe), ("p", guardar, pipe), ["success"])
    conectar(pipe, ("p", marcar, pipe), ("p", err, pipe), ["success"])
    conectar(pipe, ("p", guardar, pipe), ("p", marcar, pipe), ["failure"], dobleces=[(900, 1400)])

    send("PUT", f"/flow/process-groups/{pipe}/controller-services", {"id": pipe, "state": "ENABLED"})
    time.sleep(3)
    invalidos = [p for p in get(f"/process-groups/{pipe}/processors")["processors"]
                 if p["component"].get("validationErrors")]
    for p in invalidos:
        print("  INVALIDO:", p["component"]["name"], p["component"]["validationErrors"])
    print(f"Pipeline histórico creado: {pipe}  ({len(ventanas_mensuales(anios))} ventanas, "
          f"{len(invalidos)} processors inválidos)")
    return pipe


if __name__ == "__main__":
    a = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    m = sys.argv[2] if len(sys.argv) > 2 else "4.0"
    construir(a, m)
