"""
Genera lotes de prueba (formato GeoJSON idéntico al de USGS) para ejercitar la validación.

Toma sismos REALES descargados de la API y les inyecta defectos controlados, más
casos frontera de magnitud. Los archivos se escriben en pruebas/ ; para procesarlos
se copian a data/raw/entrada/ (GetFile los consume).
"""
import copy, json
import requests
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[3]
OUT = RAIZ / "data" / "pruebas"     # luego copiar a data/raw/entrada/ para que GetFile los consuma
OUT.mkdir(parents=True, exist_ok=True)
URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
real = requests.get(URL, timeout=30).json()["features"]


def variante(i, sufijo, **cambios):
    f = copy.deepcopy(real[i])
    f["id"] = f"{f['id']}-{sufijo}"
    for ruta, valor in cambios.items():
        nodo, *resto = ruta.split(".")
        destino = f if not resto else f[nodo]
        clave = resto[0] if resto else nodo
        if valor is ...:
            destino.pop(clave, None)
        else:
            destino[clave] = valor
    return f


def lote(nombre, features):
    (OUT / nombre).write_text(json.dumps({"type": "FeatureCollection", "features": features}, indent=2))


# Lote 1: casos frontera VÁLIDOS (clasificación exacta en los umbrales)
lote("lote_01_fronteras.geojson", [
    variante(0, "FRONT-2.49", **{"properties.mag": 2.49}),   # -> menor
    variante(1, "FRONT-2.50", **{"properties.mag": 2.5}),    # -> moderado
    variante(2, "FRONT-4.49", **{"properties.mag": 4.49}),   # -> moderado
    variante(3, "FRONT-4.50", **{"properties.mag": 4.5}),    # -> fuerte (alerta)
    variante(4, "NEG-0.80", **{"properties.mag": -0.8}),     # microsismo real: válido -> menor
    variante(5, "TSUNAMI", **{"properties.mag": 3.1, "properties.tsunami": 1}),  # moderado + alerta tsunami
])
# Lote 2: registros INVÁLIDOS (cada uno con un solo defecto)
lote("lote_02_invalidos.geojson", [
    variante(6, "SIN-MAG", **{"properties.mag": ...}),
    variante(7, "MAG-NULL", **{"properties.mag": None}),
    variante(8, "MAG-TEXTO", **{"properties.mag": "fuerte"}),
    variante(9, "MAG-15", **{"properties.mag": 15.2}),
    variante(10, "SIN-TIEMPO", **{"properties.time": ...}),
    variante(11, "SIN-TIPO", **{"properties.type": ""}),
    variante(12, "SIN-COORD", **{"geometry": {"type": "Point", "coordinates": []}}),
    {k: v for k, v in real[13].items() if k != "id"},                         # sin id
])
# Lote 3: duplicados exactos de eventos reales (deben descartarse en DetectDuplicate)
lote("lote_03_duplicados.geojson", [real[20], real[20], real[21]])
# Lote 4: archivo con JSON mal formado (falla SplitJson -> errores de ingesta)
(OUT / "lote_04_malformado.geojson").write_text('{"type": "FeatureCollection", "features": [ {"id": "x1", ')
print("Lotes de prueba generados en", OUT)
for p in sorted(OUT.iterdir()):
    print("  ", p.name)
