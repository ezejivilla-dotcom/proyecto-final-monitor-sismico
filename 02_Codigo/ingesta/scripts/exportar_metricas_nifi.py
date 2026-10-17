"""
Exporta metricas operacionales de NiFi a PostgreSQL (tabla sismos.metricas_pipeline) para el dashboard
operacional. NiFi entrega contadores en una ventana movil de 5 minutos, por eso cada ejecucion guarda
UNA fila por pipeline que representa los ultimos 5 min; el modo --loop la repite cada 5 min.

    python3 exportar_metricas_nifi.py            # una sola lectura
    python3 exportar_metricas_nifi.py --loop     # cada 5 minutos, hasta Ctrl-C
"""
import subprocess
import sys
import time
from datetime import datetime

from nifi_api import RAIZ, ENV, get

PG_CONT = "sismos-postgres"


def grupo(nombre):
    return next(g["id"] for g in get("/process-groups/root/process-groups")["processGroups"]
                if g["component"]["name"] == nombre)


def snap_pg(pid):
    return get(f"/flow/process-groups/{pid}/status?recursive=true")["processGroupStatus"]["aggregateSnapshot"]


def feed():
    s = snap_pg(grupo("Pipeline Sismos USGS"))
    hijos = {h["processGroupStatusSnapshot"]["name"]: h["processGroupStatusSnapshot"]
             for h in s["processGroupStatusSnapshots"]}
    entrada = hijos["1 - Ingesta"]["flowFilesOut"]
    validos = hijos["2 - Validación"]["flowFilesOut"]
    errores = hijos["5 - Manejo de Errores"]["flowFilesIn"]
    dup = max(entrada - validos - errores, 0)
    lat = s["processingNanos"] / 1e6 / max(s["flowFilesTransferred"], 1)
    return ("nifi_feed", entrada, validos, dup, errores, lat)


def historico():
    pid = grupo("Pipeline Historico USGS")
    s = snap_pg(pid)
    snapshots = s.get("processorStatusSnapshots") or []
    p = {y["processorStatusSnapshot"]["name"]: y["processorStatusSnapshot"] for y in snapshots}
    recibidas = p["Descargar CSV del catálogo"]["flowFilesOut"]
    ok = p["Guardar en zona raw"]["flowFilesIn"]
    err = p["Archivar rechazos"]["flowFilesIn"]
    lat = s["processingNanos"] / 1e6 / max(s["flowFilesTransferred"], 1)
    return ("nifi_historico", recibidas, ok, 0, err, lat)


def insertar(fila):
    comp, rin, rok, rdup, rerr, lat = fila
    corrida = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql = ("INSERT INTO sismos.metricas_pipeline (componente, corrida_id, registros_in, registros_ok, "
           f"registros_duplicados, registros_error, latencia_ms_prom) VALUES ('{comp}','{corrida}',{rin},{rok},"
           f"{rdup},{rerr},{lat:.3f});")
    subprocess.run(["docker", "exec", "-i", PG_CONT, "psql", "-U", ENV["POSTGRES_USER"], "-d", ENV["POSTGRES_DB"],
                    "-q", "-c", sql], check=True)
    print(f"{datetime.now():%H:%M:%S} {comp:<15} in={rin} ok={rok} dup={rdup} err={rerr} lat={lat:.2f} ms")


if __name__ == "__main__":
    while True:
        for f in (feed, historico):
            try:
                fila = f()
                if fila[0] == "nifi_feed" or fila[1] > 0:
                    insertar(fila)
            except Exception as e:                      # no tumbar el loop por una lectura fallida
                print("aviso:", f.__name__, e)
        if "--loop" not in sys.argv:
            break
        time.sleep(300)
