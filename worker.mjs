import WebSocket from 'ws';
import crypto from 'node:crypto';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { URL } from 'node:url';

const SERVICE = 'aurevix-elite-independent-runtime';
const WS_URL = 'wss://contract.mexc.com/edge';
const API_KEY = process.env.MEXC_API_KEY;
const API_SECRET = process.env.MEXC_API_SECRET;
const PORT = Number(process.env.PORT || 10000);
const STATE_FILE = process.env.RUNTIME_STATE_FILE || '/tmp/aurevix-runtime-state.json';
const LEASE_DIR = process.env.RUNTIME_LEASE_DIR || '/tmp/aurevix-runtime-lease';
const LEASE_OWNER_FILE = path.join(LEASE_DIR, 'owner.json');
const LEASE_MS = 90000;
const HEARTBEAT_MS = 15000;
const RECONNECT_MS = 5000;
const ENGINE_CYCLE_MS = 5000;
const LIVE_ENABLED = String(process.env.AUREVIX_LIVE_TRADING || '').trim().toUpperCase() === 'ENABLED';
const runtimeId = `runtime-${crypto.randomUUID()}`;
let state = loadState();
let lastMexcEventAt = null;
let lastMexcCallAt = null;
let lastError = null;
let connected = false;
let authenticated = false;
let ws = null;
let closedByUs = false;
let stopping = false;
let leaseHeld = false;

function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); }
  catch { return { service: SERVICE, state: 'STARTING', sequence: 0, ordersSent: 0, positionsModified: 0 }; }
}
function persist() {
  const next = { ...state, runtimeId, updatedAt: new Date().toISOString(), liveEnabled: LIVE_ENABLED, ordersSent: 0, positionsModified: 0 };
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });
  const temp = `${STATE_FILE}.${runtimeId}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(next));
  fs.renameSync(temp, STATE_FILE);
  state = next;
}
function audit(event, extra = {}) {
  const safe = { event, service: SERVICE, runtimeId, at: new Date().toISOString(), liveEnabled: LIVE_ENABLED, ordersSent: 0, positionsModified: 0, ...extra };
  console.log(JSON.stringify(safe));
}
function acquireLease() {
  const now = Date.now();
  try {
    fs.mkdirSync(LEASE_DIR);
    const record = { key: 'EXECUTION_AUTHORITY', runtimeId, expiresAt: now + LEASE_MS, updatedAt: new Date(now).toISOString() };
    fs.writeFileSync(LEASE_OWNER_FILE, JSON.stringify(record), { flag: 'wx' });
    leaseHeld = true;
    audit('execution_authority_acquired', { expiresAt: record.expiresAt });
    return true;
  } catch (e) {
    try {
      const old = JSON.parse(fs.readFileSync(LEASE_OWNER_FILE, 'utf8'));
      if (old.runtimeId === runtimeId && old.expiresAt > now) {
        leaseHeld = true;
        return true;
      }
      if (old.expiresAt <= now) {
        audit('execution_authority_expired_owner', { ownerRuntimeId: old.runtimeId });
      } else {
        audit('execution_authority_blocked', { ownerRuntimeId: old.runtimeId, expiresAt: old.expiresAt });
      }
    } catch (readError) {
      audit('execution_authority_error', { message: readError instanceof Error ? readError.message : 'unknown' });
    }
    leaseHeld = false;
    return false;
  }
}
function renewLease() {
  if (!leaseHeld) return acquireLease();
  try {
    const current = JSON.parse(fs.readFileSync(LEASE_OWNER_FILE, 'utf8'));
    if (current.runtimeId !== runtimeId || current.expiresAt <= Date.now()) {
      leaseHeld = false;
      audit('execution_authority_lost', { ownerRuntimeId: current.runtimeId });
      return false;
    }
    current.expiresAt = Date.now() + LEASE_MS;
    current.updatedAt = new Date().toISOString();
    const temp = `${LEASE_OWNER_FILE}.${runtimeId}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(current));
    fs.renameSync(temp, LEASE_OWNER_FILE);
    return true;
  } catch (e) {
    leaseHeld = false;
    audit('execution_authority_renewal_failed', { message: e instanceof Error ? e.message : 'unknown' });
    return false;
  }
}
function releaseLease() {
  try {
    const current = JSON.parse(fs.readFileSync(LEASE_OWNER_FILE, 'utf8'));
    if (current.runtimeId === runtimeId) {
      fs.unlinkSync(LEASE_OWNER_FILE);
      fs.rmdirSync(LEASE_DIR);
    }
  } catch {}
  leaseHeld = false;
}
function signature(apiKey, reqTime, secret) {
  return crypto.createHmac('sha256', secret).update(`${apiKey}${reqTime}`).digest('hex');
}
function loginMessage() {
  const reqTime = Date.now().toString();
  return JSON.stringify({ method: 'login', subscribe: false, param: { apiKey: API_KEY, reqTime, signature: signature(API_KEY, reqTime, API_SECRET) } });
}
function filterMessage() {
  return JSON.stringify({ method: 'personal.filter', param: { filters: [{ filter: 'order' }, { filter: 'order.deal' }, { filter: 'position' }] } });
}
function connect() {
  if (stopping) return;
  ws = new WebSocket(WS_URL);
  connected = false;
  authenticated = false;
  let pingTimer;
  closedByUs = false;
  ws.on('open', () => {
    connected = true;
    audit('mexc_connected');
    ws.send(loginMessage());
    pingTimer = setInterval(() => {
      if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'ping' }));
    }, 10000);
  });
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    lastMexcEventAt = Date.now();
    if (msg.channel === 'rs.login') {
      authenticated = msg.data === 'success';
      audit(authenticated ? 'mexc_authenticated' : 'mexc_authentication_failed', { dataType: typeof msg.data });
      if (authenticated) ws.send(filterMessage()); else { closedByUs = true; ws.close(); }
      return;
    }
    if (msg.channel === 'rs.error') audit('mexc_protocol_error', { dataType: typeof msg.data });
    if (msg.channel?.startsWith('push.personal.')) audit('mexc_private_event', { channel: msg.channel });
  });
  ws.on('error', err => { lastError = err.message; audit('websocket_error', { message: err.message }); });
  ws.on('close', (code, reason) => {
    clearInterval(pingTimer);
    connected = false;
    authenticated = false;
    audit('mexc_disconnected', { code, reason: reason?.toString() || '', reconnectInMs: RECONNECT_MS });
    if (!closedByUs && !stopping) setTimeout(connect, RECONNECT_MS);
  });
}
function engineCycle() {
  const lease = renewLease();
  lastMexcCallAt = Date.now();
  if (!lease) {
    state = { ...state, state: 'SAFE_STATE', lastError: 'EXECUTION_AUTHORITY_UNKNOWN' };
    persist();
    return;
  }
  if (!authenticated) {
    state = { ...state, state: 'RECOVERING', lastError: connected ? 'MEXC_AUTH_PENDING' : 'MEXC_DISCONNECTED' };
    persist();
    return;
  }
  state = { ...state, state: LIVE_ENABLED ? 'WAITING_FOR_APP_ENGINE_AUTHORITY' : 'PAPER_RUNTIME', lastError: null, sequence: (state.sequence || 0) + 1 };
  persist();
}
function status() {
  return { service: SERVICE, runtimeId, state: state.state, connected, authenticated, leaseHeld, liveEnabled: LIVE_ENABLED, ordersSent: 0, positionsModified: 0, lastMexcEventAt, lastMexcCallAt, lastError, updatedAt: state.updatedAt || null };
}
const server = http.createServer((req, res) => {
  const u = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  if (u.pathname === '/' || u.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify(status()));
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: 'NOT_FOUND' }));
});
server.listen(PORT, '0.0.0.0', () => audit('health_server_ready', { port: PORT }));

async function main() {
  if (!API_KEY || !API_SECRET) {
    audit('startup_blocked', { reason: 'MEXC credentials are not configured' });
    process.exitCode = 1;
    return;
  }
  if (!acquireLease()) {
    state = { ...state, state: 'SAFE_STATE', lastError: 'EXECUTION_AUTHORITY_UNKNOWN' };
    persist();
    audit('runtime_start_blocked', { reason: 'Another runtime owns the local atomic lease' });
    return;
  }
  persist();
  audit('runtime_started', { leaseHeld, mode: LIVE_ENABLED ? 'LIVE_LOCKED_PENDING_AUTHORITY' : 'LIVE_OFF' });
  connect();
  const timer = setInterval(engineCycle, ENGINE_CYCLE_MS);
  const heartbeat = setInterval(() => { persist(); audit('heartbeat', { state: status().state, authenticated }); }, HEARTBEAT_MS);
  const shutdown = signal => {
    if (stopping) return;
    stopping = true;
    clearInterval(timer);
    clearInterval(heartbeat);
    closedByUs = true;
    try { ws?.close(); } catch {}
    releaseLease();
    state = { ...state, state: 'STOPPED' };
    persist();
    audit('graceful_shutdown', { signal });
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('uncaughtException', err => { lastError = err.message; audit('uncaught_exception', { message: err.message }); process.exit(1); });
  process.on('unhandledRejection', err => { lastError = err instanceof Error ? err.message : 'unhandled rejection'; audit('unhandled_rejection', { message: lastError }); process.exit(1); });
}
main();
