# Capa 1 — Ingesta (Apache NiFi 2.7.2)

Dos flujos construidos por **API REST** (los scripts son idempotentes: borran y recrean el flujo):

| Flujo | Script | Fuente | Qué hace |
|---|---|---|---|
| `Pipeline Sismos USGS` | `scripts/construir_pipeline.py` | API GeoJSON USGS (cada 5 min) + carpeta `data/raw/entrada` | Valida (6 reglas), deduplica (caché), clasifica por magnitud, enriquece (Jolt), consolida CSV por categoría, genera alertas, aísla errores. Backpressure explícito entre grupos |
| `Pipeline Historico USGS` | `scripts/construir_historico.py [anios] [mag_min]` | API FDSN: CSV + GeoJSON | Ventanas mensuales hasta hoy (la última es parcial; re-ejecutar para ponerse al día). **2 consultas por ventana**: CSV (calidad de la medición, `ValidateRecord` con esquema Avro) y GeoJSON (impacto humano, aplanado con Jolt y convertido a CSV). **Rate limiting** 1 ventana / 5 s, rechazos aparte |

Salidas en `data/raw/`: `salida/consolidado/` (feed), `historico/` (CSV), `historico_impacto/` (GeoJSON→CSV), `alertas/`, `errores/`.

Esquemas Avro en `config/`: `esquema_sismo.avsc` (feed), `esquema_historico.avsc` (CSV), `esquema_impacto.avsc` (GeoJSON aplanado). Specs Jolt: `jolt_sismo.json`, `jolt_impacto.json`.

## Operación

```bash
cd 02_Codigo/ingesta/scripts
python3 construir_pipeline.py          # crea el flujo (parado)
python3 construir_historico.py
python3 iniciar_flujos.py              # arranca ambos
python3 exportar_metricas_nifi.py --loop   # métricas -> sismos.metricas_pipeline (cada 5 min)
python3 generar_casos_prueba.py        # lotes con defectos para probar la validación (-> data/pruebas)
```

`flujo/*.json` son las *Flow Definition* exportadas (NiFi 2.x ya no usa templates XML).

## Capturas de evidencia

`scripts/capturar_evidencias.mjs` genera las capturas de la interfaz de NiFi (`03_Visualizaciones/ingesta/`): autentica con el `.env`, navega los dos flujos, abre los diálogos de configuración y fotografía cada evidencia. Requiere NiFi arriba con los flujos corriendo, Chrome y Node ≥ 22 (sin dependencias; usa `cdp.mjs`).

```bash
node 02_Codigo/ingesta/scripts/capturar_evidencias.mjs
```
