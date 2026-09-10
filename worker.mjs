import WebSocket from 'ws';
import crypto from 'node:crypto';
import http from 'node:http';

const WS_URL = 'wss://contract.mexc.com/edge';
const API_KEY = process.env.MEXC_API_KEY;
const API_SECRET = process.env.MEXC_API_SECRET;
const VORLEN_URL = process.env.VORLEN_URL || 'https://vorlen-ai-trader-zsi4ny.v2.appdeploy.ai';
const RECONNECT_MS = 5000;
const PORT = Number(process.env.PORT || 10000);

http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ service: 'vorlen-mexc-private-worker', status: 'ok' }));
    return;
  }
  res.writeHead(404);
  res.end();
}).listen(PORT, '0.0.0.0', () => {
  console.log(JSON.stringify({ event: 'health_server_ready', port: PORT, at: new Date().toISOString() }));
});

if (!API_KEY || !API_SECRET) {
  console.error('MEXC_API_KEY and MEXC_API_SECRET are required');
  process.exit(1);
}

function signature(apiKey, reqTime, secret) {
  return crypto.createHmac('sha256', secret).update(`${apiKey}${reqTime}`).digest('hex');
}

function workerSignature(ts, events) {
  const payload = JSON.stringify(events);
  return crypto.createHmac('sha256', API_SECRET).update(`${API_KEY}${ts}${payload}`).digest('hex');
}

async function forwardEvents(events) {
  if (!events?.length) return;
  const ts = String(Date.now());
  const body = { key: API_KEY, ts, sig: workerSignature(ts, events), events };
  try {
    const r = await fetch(`${VORLEN_URL}/api/private-worker/events`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) console.error(JSON.stringify({ event: 'forward_failed', status: r.status }));
    else console.log(JSON.stringify({ event: 'forwarded', count: events.length, at: new Date().toISOString() }));
  } catch (e) {
    console.error(JSON.stringify({ event: 'forward_error', message: e?.message || 'unknown' }));
  }
}

function loginMessage() {
  const reqTime = Date.now().toString();
  return JSON.stringify({
    method: 'login', subscribe: false,
    param: { apiKey: API_KEY, reqTime, signature: signature(API_KEY, reqTime, API_SECRET) }
  });
}

function filterMessage() {
  return JSON.stringify({ method: 'personal.filter', param: { filters: [
    { filter: 'order' }, { filter: 'order.deal' }, { filter: 'position' }
  ] } });
}

function connect() {
  const ws = new WebSocket(WS_URL);
  let loggedIn = false;
  let pingTimer;
  let closedByUs = false;

  ws.on('open', () => {
    console.log(JSON.stringify({ event: 'connected', at: new Date().toISOString() }));
    ws.send(loginMessage());
    pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'ping' }));
    }, 10000);
  });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.channel === 'rs.login') {
      if (msg.data === 'success') {
        loggedIn = true;
        const event = { channel: 'worker.status', data: { status: 'AUTHENTICATED' }, ts: Date.now() };
        console.log(JSON.stringify({ event: 'authenticated', at: new Date().toISOString() }));
        void forwardEvents([event]);
        ws.send(filterMessage());
      } else {
        console.error(JSON.stringify({ event: 'authentication_failed', channel: msg.channel, dataType: typeof msg.data, data: msg.data }));
        void forwardEvents([{ channel: 'worker.status', data: { status: 'AUTHENTICATION_FAILED' }, ts: Date.now() }]);
        closedByUs = true;
        ws.close();
      }
      return;
    }

    if (msg.channel === 'rs.error') {
      console.error(JSON.stringify({ event: 'authentication_or_protocol_error', data: msg.data }));
      void forwardEvents([{ channel: 'worker.status', data: { status: 'PROTOCOL_ERROR', detail: msg.data }, ts: Date.now() }]);
      return;
    }

    if (msg.channel === 'push.personal.order' || msg.channel === 'push.personal.order.deal' || msg.channel === 'push.personal.position') {
      const event = { channel: msg.channel, data: msg.data, ts: msg.ts || Date.now() };
      console.log(JSON.stringify({ event: 'private_event', channel: msg.channel, data: msg.data, at: new Date().toISOString() }));
      void forwardEvents([event]);
    }
  });

  ws.on('error', err => console.error(JSON.stringify({ event: 'ws_error', message: err.message })));
  ws.on('close', (code, reason) => {
    clearInterval(pingTimer);
    console.error(JSON.stringify({ event: 'disconnected', code, reason: reason?.toString() || '', authenticated: loggedIn, reconnectInMs: RECONNECT_MS }));
    void forwardEvents([{ channel: 'worker.status', data: { status: 'DISCONNECTED', code, authenticated: loggedIn }, ts: Date.now() }]);
    if (!closedByUs) setTimeout(connect, RECONNECT_MS);
  });
}

connect();
