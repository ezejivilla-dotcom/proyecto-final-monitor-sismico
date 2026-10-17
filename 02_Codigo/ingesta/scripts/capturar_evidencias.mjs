// Capturas de evidencia de la capa de ingesta (interfaz de Apache NiFi) para 03_Visualizaciones/ingesta/.
//
// Requisitos: NiFi arriba con los dos flujos construidos y corriendo (para que haya estadísticas y linaje),
// Chrome instalado y Node >= 22. No necesita dependencias: habla con Chrome por el protocolo DevTools (cdp.mjs).
//
//   node capturar_evidencias.mjs [carpeta_salida]
//
// Autentica con la sesión de NiFi (usuario y clave del .env de la raíz del repo), navega, abre los diálogos de
// configuración y guarda una captura por evidencia.
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { lanzarChrome, sleep } from './cdp.mjs';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'; // certificado autofirmado de NiFi
const AQUI = dirname(fileURLToPath(import.meta.url));
const RAIZ = resolve(AQUI, '../../..');
const SALIDA = resolve(process.argv[2] || join(RAIZ, '03_Visualizaciones/ingesta'));
const env = Object.fromEntries(readFileSync(join(RAIZ, '.env'), 'utf8').split('\n')
  .filter((l) => l.includes('=') && !l.startsWith('#')).map((l) => [l.split('=')[0].trim(), l.slice(l.indexOf('=') + 1).trim()]));
const NIFI = `https://localhost:${env.NIFI_PORT || 8443}`;
const UI = `${NIFI}/nifi/#/`;

// ------------------------------------------------------------------ API REST (solo para localizar IDs)
const tok = await (await fetch(`${NIFI}/nifi-api/access/token`, {
  method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  body: new URLSearchParams({ username: env.NIFI_USER, password: env.NIFI_PASSWORD }) })).text();
const api = async (ruta, opciones = {}) => (await fetch(NIFI + '/nifi-api' + ruta, { ...opciones, headers: { Authorization: 'Bearer ' + tok, 'Content-Type': 'application/json' } })).json();

const raiz = (await api('/flow/process-groups/root')).processGroupFlow.id;
const grupos = Object.fromEntries((await api(`/process-groups/${raiz}/process-groups`)).processGroups.map((g) => [g.component.name, g.id]));
const feed = grupos['Pipeline Sismos USGS'];
const historico = grupos['Pipeline Historico USGS'];
const sub = Object.fromEntries((await api(`/process-groups/${feed}/process-groups`)).processGroups.map((g) => [g.component.name, g.id]));
const procs = async (pg) => Object.fromEntries((await api(`/process-groups/${pg}/processors`)).processors.map((p) => [p.component.name, p.id]));
const pVal = await procs(sub['2 - Validación']);
const pHist = await procs(historico);
const pSal = await procs(sub['4 - Salida']);
const conexFeed = (await api(`/process-groups/${feed}/connections`)).connections;
const conIngVal = conexFeed.find((c) => (c.component.name || '').startsWith('ingesta')).id;

// FlowFile con linaje completo: uno que llegó hasta "Guardar alerta" (RECEIVE → … → SEND)
async function flowfileConLinaje() {
  const q = (await api('/provenance', { method: 'POST', body: JSON.stringify({ provenance: { request: { maxResults: 50, searchTerms: { ProcessorID: { value: pSal['Guardar alerta'], inverse: false } } } } }) })).provenance;
  for (let i = 0; i < 30; i++) { const r = (await api(`/provenance/${q.id}`)).provenance; if (r.finished) { await api(`/provenance/${q.id}`, { method: 'DELETE' }); return r.results.provenanceEvents[0]?.flowFileUuid; } await sleep(1000); }
}
const ffLinaje = await flowfileConLinaje();

// ------------------------------------------------------------------ navegador
const c = await lanzarChrome(9224);
const out = (n) => join(SALIDA, n);
const log = (m) => console.log('  ' + m);

async function lienzo(pgId, { fit = true, w = 1413, h = 800, dsf = 1, alejar = 1 } = {}) {
  await c.tam(w, h, dsf);
  await c.ir(UI + 'process-groups/' + pgId, 5000);
  if (fit) { await c.clic(112, 179); await sleep(1200); }          // "ajustar a pantalla"
  for (let i = 0; i < alejar && fit; i++) { await c.clic(66, 179); await sleep(700); }   // un nivel menos: el grafo queda centrado y con margen
  await c.clic(271, 389); await sleep(300);                         // colapsar panel Operation
  await c.clic(271, 137); await sleep(600);                         // colapsar panel Navigation
  if (fit && !alejar) { await c.arrastrar(w - 160, h - 120, w - 100, h - 90); await sleep(600); } // sin zoom-out: desplazar para que los iconos laterales no tapen
}
async function centro(id) {
  return c.js(`(()=>{const e=document.getElementById('id-${id}'); if(!e) return null; const r=e.getBoundingClientRect(); return {x:r.x+r.width/2,y:r.y+22}})()`);
}
async function abrirDialogo(id, tabTexto, alto = 1040) {
  const p = await centro(id); if (!p) throw new Error('componente no visible: ' + id);
  await c.clic(p.x, p.y, 2); await sleep(1800);
  if (tabTexto) { await c.js(`(()=>{const t=[...document.querySelectorAll('[role=tab]')].find(t=>/${tabTexto}/i.test(t.innerText)); if(t) t.click()})()`); await sleep(1000); }
  await c.tam(1413, alto); await sleep(600);
}

try {
  await c.cmd('Network.setCookie', { name: '__Secure-Authorization-Bearer', value: tok, domain: 'localhost', path: '/', secure: true, httpOnly: true, sameSite: 'Strict' });

  log('01 NiFi funcional (lienzo raíz con los dos pipelines)');
  await lienzo(raiz); await c.foto(out('01_nifi_funcional.png'));

  log('02 Pipeline de sismos en vivo: vista general');
  await lienzo(feed, { w: 1700, alejar: 0 }); await c.foto(out('02_feed_vista_general.png'));

  log('03 Grupo 2 - Validación');
  await lienzo(sub['2 - Validación'], { h: 1040 }); await c.foto(out('03_feed_pg2_validacion.png'));

  log('04 Configuración de las reglas de calidad (RouteOnAttribute)');
  await lienzo(sub['2 - Validación'], { fit: false }); await abrirDialogo(pVal['Validar reglas de calidad'], 'propert'); await c.foto(out('04_feed_config_reglas.png'));

  log('05 Backpressure entre grupos (conexión ingesta → validación)');
  await lienzo(feed);
  const cx = await c.js(`(()=>{const e=document.getElementById('id-${conIngVal}'); if(!e) return null; const r=e.getBoundingClientRect(); return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
  if (!cx) throw new Error('conexión no visible');
  await c.clic(cx.x, cx.y, 2); await sleep(1800);
  await c.js(`(()=>{const t=[...document.querySelectorAll('[role=tab]')].find(t=>/setting/i.test(t.innerText)); if(t) t.click()})()`); await sleep(1000);
  await c.tam(1413, 1040); await sleep(600); await c.foto(out('05_feed_backpressure.png'));

  log('06 Grupo 4 - Salida (consolidado CSV y alertas)');
  await lienzo(sub['4 - Salida'], { h: 1040 }); await c.foto(out('06_feed_pg4_salida.png'));

  log('07 Grupo 5 - Manejo de errores');
  await lienzo(sub['5 - Manejo de Errores'], { h: 1040 }); await c.foto(out('07_feed_pg5_errores.png'));

  log('08 Pipeline histórico: vista general (ramas CSV y GeoJSON)');
  await lienzo(historico, { w: 1800, h: 1000, dsf: 2, alejar: 0 }); await c.foto(out('08_historico_vista_general.png'));

  log('09 Rate limiting (ControlRate)');
  await lienzo(historico); await abrirDialogo(pHist['Limitar ritmo (rate limiting)'], 'propert'); await c.foto(out('09_historico_rate_limiting.png'));

  log('10 Validación por esquema Avro (ValidateRecord)');
  await lienzo(historico); await abrirDialogo(pHist['Validar registros (esquema Avro)'], 'propert'); await c.foto(out('10_historico_validaterecord.png'));

  log('11 Controller Services del pipeline en vivo');
  await c.tam(1413, 1040); await c.ir(UI + 'process-groups/' + feed + '/controller-services', 4500); await c.foto(out('11_controller_services.png'));

  log('12 Estadísticas (Summary)');
  await c.tam(1413, 1000); await c.ir(UI + 'summary', 5000); await c.foto(out('12_estadisticas_summary.png'));

  log('13 Eventos de Provenance de un FlowFile');
  await c.tam(1405, 1033); await c.ir(UI + 'provenance?flowFileUuid=' + ffLinaje, 6000); await c.foto(out('13_provenance_eventos.png'));

  log('14 Grafo de linaje del FlowFile');
  const bm = await c.js(`(()=>{const b=document.querySelector('tbody tr button, tr.mat-mdc-row button'); if(!b) return null; const r=b.getBoundingClientRect(); return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
  await c.clic(bm.x, bm.y); await sleep(800);
  await c.js(`(()=>{const i=[...document.querySelectorAll('[role=menuitem]')].find(i=>/lineage/i.test(i.innerText)); i.click()})()`);
  await sleep(6000); await c.foto(out('14_provenance_linaje.png'));

  log('15 Archivos de salida en data/raw');
  await c.tam(1501, 935);
  const fmt = (d) => d.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
  const tam = (n) => (n < 1024 ? `${n} bytes` : n < 1048576 ? `${Math.round(n / 1024)} KB` : `${(n / 1048576).toFixed(1)} MB`);
  const filas = [];
  const carpeta = (ruta, nivel, max = 6) => {
    let ents; try { ents = readdirSync(ruta, { withFileTypes: true }).filter((e) => !e.name.startsWith('.')); } catch { return; }
    ents.sort((a, b) => (b.isDirectory() - a.isDirectory()) || a.name.localeCompare(b.name));
    let n = 0;
    for (const e of ents) {
      const p = join(ruta, e.name); const st = statSync(p);
      if (e.isDirectory()) { filas.push({ nivel, nombre: e.name, dir: true, fecha: fmt(st.mtime), tam: '--', tipo: 'Folder' }); carpeta(p, nivel + 1, max); }
      else if (n++ < max) filas.push({ nivel, nombre: e.name, dir: false, fecha: fmt(st.mtime), tam: tam(st.size), tipo: e.name.endsWith('.csv') ? 'Comma…et (.csv)' : 'JSON' });
      else if (n === max + 1) filas.push({ nivel, nombre: `… ${ents.filter((x) => !x.isDirectory()).length - max} archivos más`, dir: false, fecha: '', tam: '', tipo: '', gris: true });
    }
  };
  const base = join(RAIZ, 'data/raw');
  for (const d of ['alertas', 'errores', 'historico', 'historico_impacto', 'salida']) {
    const st = statSync(join(base, d)); filas.push({ nivel: 0, nombre: d, dir: true, fecha: fmt(st.mtime), tam: '--', tipo: 'Folder' }); carpeta(join(base, d), 1, d === 'alertas' ? 8 : 5);
  }
  const html = `<html><body style="margin:0;background:#1e1e1e;color:#e8e8e8;font:13px -apple-system,Helvetica,Arial"><table style="width:100%;border-collapse:collapse">
  <tr style="color:#aaa;border-bottom:1px solid #333"><th style="text-align:left;padding:6px 10px;width:60%">Name</th><th style="text-align:left">Date Modified</th><th style="text-align:right;padding-right:20px">Size</th><th style="text-align:left">Kind</th></tr>
  ${filas.map((f) => `<tr style="height:20px;${f.gris ? 'color:#888' : ''}"><td style="padding-left:${10 + f.nivel * 16}px;white-space:nowrap">${f.dir ? '▾ 📁' : '&nbsp;&nbsp;&nbsp;📄'} ${f.nombre}</td><td style="color:#bbb">${f.fecha}</td><td style="text-align:right;padding-right:20px;color:#bbb">${f.tam}</td><td style="color:#bbb">${f.tipo}</td></tr>`).join('')}</table></body></html>`;
  await c.ir('data:text/html;charset=utf-8,' + encodeURIComponent(html), 800); await c.foto(out('15_archivos_salida.png'));
  console.log('Listo →', SALIDA);
} finally { await c.cerrar(); }
