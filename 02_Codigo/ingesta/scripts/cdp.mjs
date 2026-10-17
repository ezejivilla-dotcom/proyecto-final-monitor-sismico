// Utilidad mínima para controlar Chrome por el protocolo DevTools (sin dependencias; requiere Node >= 22).
// La usa capturar_evidencias.mjs para sacar capturas reproducibles de la interfaz de NiFi.
import { spawn } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

export const CHROME = process.env.CHROME_BIN || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export { sleep };

export async function lanzarChrome(puerto = 9223) {
  const proc = spawn(CHROME, [
    '--headless=new', `--remote-debugging-port=${puerto}`, '--ignore-certificate-errors', '--hide-scrollbars',
    '--force-device-scale-factor=1', '--no-first-run', '--no-default-browser-check',
    `--user-data-dir=/tmp/chrome-cdp-${puerto}`, 'about:blank',
  ], { stdio: 'ignore' });
  for (let i = 0; i < 50; i++) {
    try { const r = await fetch(`http://127.0.0.1:${puerto}/json/version`); if (r.ok) break; } catch { /* aún no */ }
    await sleep(200);
  }
  const lista = await (await fetch(`http://127.0.0.1:${puerto}/json/list`)).json();
  const pagina = lista.find((t) => t.type === 'page');
  const ws = new WebSocket(pagina.webSocketDebuggerUrl);
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
  let id = 0; const pend = new Map();
  ws.onmessage = (m) => {
    const d = JSON.parse(m.data);
    if (d.id && pend.has(d.id)) { const { res, rej } = pend.get(d.id); pend.delete(d.id); d.error ? rej(new Error(JSON.stringify(d.error))) : res(d.result); }
  };
  const cmd = (method, params = {}) => new Promise((res, rej) => { const i = ++id; pend.set(i, { res, rej }); ws.send(JSON.stringify({ id: i, method, params })); });

  const api = {
    cmd,
    async ir(url, espera = 3000) { await cmd('Page.navigate', { url }); await sleep(espera); },
    async js(expr) {
      const r = await cmd('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true });
      if (r.exceptionDetails) throw new Error(r.exceptionDetails.text + ' ' + (r.exceptionDetails.exception?.description || ''));
      return r.result.value;
    },
    async tam(w, h, dsf = 1) { await cmd('Emulation.setDeviceMetricsOverride', { width: w, height: h, deviceScaleFactor: dsf, mobile: false }); },
    async arrastrar(x1, y1, x2, y2, pasos = 8) {
      await cmd('Input.dispatchMouseEvent', { type: 'mouseMoved', x: x1, y: y1 });
      await cmd('Input.dispatchMouseEvent', { type: 'mousePressed', x: x1, y: y1, button: 'left', clickCount: 1 });
      for (let i = 1; i <= pasos; i++) await cmd('Input.dispatchMouseEvent', { type: 'mouseMoved', x: x1 + ((x2 - x1) * i) / pasos, y: y1 + ((y2 - y1) * i) / pasos, buttons: 1 });
      await cmd('Input.dispatchMouseEvent', { type: 'mouseReleased', x: x2, y: y2, button: 'left', clickCount: 1 });
    },
    async foto(ruta) {
      const { data } = await cmd('Page.captureScreenshot', { format: 'png' });
      mkdirSync(dirname(ruta), { recursive: true }); writeFileSync(ruta, Buffer.from(data, 'base64')); return ruta;
    },
    async clic(x, y, veces = 1) {
      await cmd('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
      await cmd('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: veces });
      await cmd('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: veces });
    },
    async cerrar() { try { await cmd('Browser.close'); } catch { /* ya cerrado */ } ws.close(); proc.kill(); },
  };
  await cmd('Page.enable'); await cmd('Network.enable'); await cmd('Runtime.enable');
  return api;
}
