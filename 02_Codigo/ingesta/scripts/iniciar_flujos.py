"""Arranca (RUNNING) los dos pipelines de NiFi. Con --detener los para."""
import sys
from nifi_api import S, API, get

estado = "STOPPED" if "--detener" in sys.argv else "RUNNING"
for g in get("/process-groups/root/process-groups")["processGroups"]:
    if g["component"]["name"].startswith("Pipeline"):
        r = S.put(f"{API}/flow/process-groups/{g['id']}", json={"id": g["id"], "state": estado})
        print(f"{g['component']['name']:<28} -> {estado} ({r.status_code})")
