"""Utilidad mínima: login + get/put contra la API de NiFi (usa .env de la raíz del repo)."""
from pathlib import Path
from typing import Any

import requests
import urllib3

urllib3.disable_warnings()

RAIZ = Path(__file__).resolve().parents[3]  # raíz del repo
ENV: dict[str, str] = {}
env_file = RAIZ / ".env"
if env_file.exists():
    for linea in env_file.read_text().splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            ENV[k.strip()] = v.strip()

API = f"https://localhost:{ENV.get('NIFI_PORT', '8443')}/nifi-api"
S = requests.Session()
S.verify = False

if "NIFI_USER" in ENV and "NIFI_PASSWORD" in ENV:
    try:
        r_auth = S.post(
            f"{API}/access/token",
            data={"username": ENV["NIFI_USER"], "password": ENV["NIFI_PASSWORD"]},
            timeout=10,
        )
        if r_auth.ok:
            S.headers["Authorization"] = f"Bearer {r_auth.text}"
    except requests.RequestException:
        pass


def get(p: str) -> Any:
    return S.get(API + p).json()


def put(p: str, b: dict[str, Any]) -> Any:
    r = S.put(API + p, json=b)
    r.raise_for_status()
    return r.json()


def pipeline_id() -> str:
    raiz = get("/flow/process-groups/root")["processGroupFlow"]["id"]
    return next(
        g["id"]
        for g in get(f"/process-groups/{raiz}/process-groups")["processGroups"]
        if g["component"]["name"] == "Pipeline Sismos USGS"
    )
