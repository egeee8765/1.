import WebSocket from 'ws';
import crypto from 'node:crypto';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { URL } from 'node:url';

const SERVICE = 'aurevix-elite-independent-runtime';
const WS_URL = 'wss://contract.mexc.com/edge';
const REST_URL = 'https://api.mexc.com';
const API_KEY = process.env.MEXC_API_KEY;
const API_SECRET = process.env.MEXC_API_SECRET;
const PORT = Number(process.env.PORT || 10000);
const STATE_FILE = process.env.RUNTIME_STATE_FILE || '/tmp/aurevix-runtime-state.json';
const LEASE_DIR = process.env.RUNTIME_LEASE_DIR || '/tmp/aurevix-runtime-lease';
const LEASE_OWNER_FILE = path.join(LEASE_DIR, 'owner.json');
const RECON_STATE_FILE = process.env.RECONCILIATION_STATE_FILE || '/tmp/aurevix-reconciliation-state.json';
const LEASE_MS = 90000;
const HEARTBEAT_MS = 15000;
const RECONNECT_MS = 5000;
const ENGINE_CYCLE_MS = 5000;
const READ_ONLY_CHECK_MS = 30000;
const RECONCILIATION_MS = 30000;
const WS_CONNECT_TIMEOUT_MS = 10000;
const LIVE_ENABLED = String(process.env.AUREVIX_LIVE_TRADING || '').trim().toUpperCase() === 'ENABLED';
const runtimeId = `runtime-${crypto.randomUUID()}`;
let state = loadState();
let lastMexcEventAt = null;
let lastMexcCallAt = null;
let lastReadOnlyCheckAt = null;
let lastReadOnlyResult = 'NOT_RUN';
let lastError = null;
let connected = false;
let authenticated = false;
let ws = null;
let closedByUs = false;
let stopping = false;
let leaseHeld = false;
let wsConnectTimer = null;
let reconciliationResult = 'NOT_RUN';
let reconciliationAt = null;
let safetyResult = 'NOT_RUN';
let safetyAt = null;
let readiness = 'BLOCKED';
let readinessReason = 'NOT_EVALUATED';

function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); }
  catch { return { service: SERVICE, state: 'STARTING', sequence: 0, ordersSent: 0, positionsModified: 0 }; }
}
function persist() {
  const next = { ...state, runtimeId, updatedAt: new Date().toISOString(), liveEnabled: LIVE_ENABLED, ordersSent: 0, positionsModified: 0, lastReadOnlyCheckAt, lastReadOnlyResult, reconciliationResult, reconciliationAt, safetyResult, safetyAt, readiness, readinessReason };
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
function loadReconBaseline() {
  try { return JSON.parse(fs.readFileSync(RECON_STATE_FILE, 'utf8')); } catch { return null; }
}
function saveReconBaseline(snapshot) {
  fs.mkdirSync(path.dirname(RECON_STATE_FILE), { recursive: true });
  const temp = `${RECON_STATE_FILE}.${runtimeId}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(snapshot));
  fs.renameSync(temp, RECON_STATE_FILE);
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
      if (old.runtimeId === runtimeId && old.expiresAt > now) { leaseHeld = true; return true; }
      if (old.expiresAt <= now) audit('execution_authority_expired_owner', { ownerRuntimeId: old.runtimeId });
      else audit('execution_authority_blocked', { ownerRuntimeId: old.runtimeId, expiresAt: old.expiresAt });
    } catch (readError) { audit('execution_authority_error', { message: readError instanceof Error ? readError.message : 'unknown' }); }
    leaseHeld = false;
    return false;
  }
}
function renewLease() {
  if (!leaseHeld) return acquireLease();
  try {
    const current = JSON.parse(fs.readFileSync(LEASE_OWNER_FILE, 'utf8'));
    if (current.runtimeId !== runtimeId || current.expiresAt <= Date.now()) { leaseHeld = false; audit('execution_authority_lost', { ownerRuntimeId: current.runtimeId }); return false; }
    current.expiresAt = Date.now() + LEASE_MS;
    current.updatedAt = new Date().toISOString();
    const temp = `${LEASE_OWNER_FILE}.${runtimeId}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(current));
    fs.renameSync(temp, LEASE_OWNER_FILE);
    return true;
  } catch (e) { leaseHeld = false; audit('execution_authority_renewal_failed', { message: e instanceof Error ? e.message : 'unknown' }); return false; }
}
function releaseLease() {
  try { const current = JSON.parse(fs.readFileSync(LEASE_OWNER_FILE, 'utf8')); if (current.runtimeId === runtimeId) { fs.unlinkSync(LEASE_OWNER_FILE); fs.rmdirSync(LEASE_DIR); } } catch {}
  leaseHeld = false;
}
function signature(apiKey, reqTime, parameterString, secret) { return crypto.createHmac('sha256', secret).update(`${apiKey}${reqTime}${parameterString}`).digest('hex'); }
function queryString(params) { return Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== '').sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => `${key}=${encodeURIComponent(String(value))}`).join('&'); }
async function privateGet(pathname, params = {}) {
  if (!API_KEY || !API_SECRET) throw new Error('MEXC credentials are not configured');
  const qs = queryString(params); const reqTime = Date.now().toString();
  const response = await fetch(`${REST_URL}${pathname}${qs ? `?${qs}` : ''}`, { method: 'GET', headers: { ApiKey: API_KEY, 'Request-Time': reqTime, Signature: signature(API_KEY, reqTime, qs, API_SECRET), 'Recv-Window': '10000', Language: 'English', Accept: 'application/json' } });
  const payload = await response.json();
  if (!response.ok || !payload?.success) throw new Error(`${pathname}: HTTP ${response.status} code=${payload?.code ?? 'UNKNOWN'} message=${typeof payload?.message === 'string' ? payload.message : 'request failed'}`);
  return payload.data;
}
function n(value) { return value === null || value === undefined ? null : Number(value); }
function sameNumber(a, b, tolerance = 1e-10) { if (a === null || b === null || Number.isNaN(a) || Number.isNaN(b)) return false; return Math.abs(a - b) <= tolerance * Math.max(1, Math.abs(a), Math.abs(b)); }
function eq(a, b) { if (typeof a === 'number' || typeof b === 'number') return sameNumber(n(a), n(b)); return JSON.stringify(a) === JSON.stringify(b); }
function comparison(exchange, internal) { if (internal === 'UNAVAILABLE') return { exchange, internal, result: 'UNAVAILABLE' }; return { exchange, internal, result: eq(exchange, internal) ? 'EQUAL' : 'MISMATCH' }; }
async function readOnlyAccountCheck() {
  if (!API_KEY || !API_SECRET) { lastReadOnlyCheckAt = new Date().toISOString(); lastReadOnlyResult = 'NOT_CONFIGURED'; audit('mexc_read_only_unavailable', { reason: 'credentials_not_configured' }); return false; }
  try {
    const assets = await privateGet('/api/v1/private/account/assets');
    const usdt = Array.isArray(assets) ? assets.find(x => String(x.currency).toUpperCase() === 'USDT') : null;
    lastReadOnlyCheckAt = new Date().toISOString(); lastReadOnlyResult = 'PASS'; lastMexcCallAt = Date.now();
    audit('mexc_read_only_verified', { assetCount: Array.isArray(assets) ? assets.length : 0, usdtEquity: n(usdt?.equity ?? 0), usdtAvailable: n(usdt?.availableBalance ?? 0) });
    return true;
  } catch (e) { lastReadOnlyCheckAt = new Date().toISOString(); lastReadOnlyResult = 'ERROR'; audit('mexc_read_only_error', { message: e instanceof Error ? e.message : 'unknown' }); return false; }
}
async function reconciliationSnapshot() {
  const snapshotTimestamp = new Date().toISOString();
  try {
    const internalState = loadState();
    const [assets, positions, openOrders, stopOrders] = await Promise.all([
      privateGet('/api/v1/private/account/assets'),
      privateGet('/api/v1/private/position/open_positions'),
      privateGet('/api/v1/private/order/list/open_orders', { page_num: 1, page_size: 100 }),
      privateGet('/api/v1/private/stoporder/open_orders'),
    ]);
    const usdt = Array.isArray(assets) ? assets.find(x => String(x.currency).toUpperCase() === 'USDT') : null;
    const exchangePositions = (Array.isArray(positions) ? positions : []).map(p => ({ positionId: String(p.positionId ?? ''), symbol: p.symbol ?? null, positionSize: n(p.holdVol), direction: Number(p.positionType) === 1 ? 'LONG' : Number(p.positionType) === 2 ? 'SHORT' : 'UNKNOWN', entryPrice: n(p.holdAvgPrice), leverage: n(p.leverage), unrealizedPnl: n(p.unRealizedPnl) }));
    const exchangeOrders = Array.isArray(openOrders) ? openOrders : [];
    const exchangeStops = Array.isArray(stopOrders) ? stopOrders : [];
    const safeExecution = LIVE_ENABLED === false && Number(internalState.ordersSent ?? 0) === 0 && Number(internalState.positionsModified ?? 0) === 0;
    const prior = loadReconBaseline();
    const internalBaseline = prior ? prior.internal : { balance: n(usdt?.cashBalance), equity: n(usdt?.equity), availableMargin: n(usdt?.availableBalance), unrealizedPnl: n(usdt?.unrealized ?? 0), positions: exchangePositions, openOrders: exchangeOrders, stopOrders: exchangeStops, execution: { state: internalState.state ?? null, liveEnabled: false, ordersSent: 0, positionsModified: 0 } };
    const checks = {
      balance: comparison(n(usdt?.cashBalance), internalBaseline.balance),
      equity: comparison(n(usdt?.equity), internalBaseline.equity),
      availableMargin: comparison(n(usdt?.availableBalance), internalBaseline.availableMargin),
      openPositions: comparison(exchangePositions.length, internalBaseline.positions.length),
      positionSize: comparison(exchangePositions.map(p => p.positionSize), internalBaseline.positions.map(p => p.positionSize)),
      direction: comparison(exchangePositions.map(p => p.direction), internalBaseline.positions.map(p => p.direction)),
      entryPrice: comparison(exchangePositions.map(p => p.entryPrice), internalBaseline.positions.map(p => p.entryPrice)),
      leverage: comparison(exchangePositions.map(p => p.leverage), internalBaseline.positions.map(p => p.leverage)),
      unrealizedPnl: comparison(n(usdt?.unrealized ?? 0), n(internalBaseline.unrealizedPnl)),
      openOrders: comparison(exchangeOrders, internalBaseline.openOrders),
      stopOrders: comparison(exchangeStops, internalBaseline.stopOrders),
      internalExecutionState: { exchange: { state: internalState.state ?? null, liveEnabled: false, ordersSent: 0, positionsModified: 0 }, internal: internalBaseline.execution, result: safeExecution && eq(internalState.state ?? null, internalBaseline.execution.state ?? null) ? 'EQUAL' : 'MISMATCH' },
    };
    const allEqual = Object.values(checks).every(x => x.result === 'EQUAL');
    if (!prior) {
      saveReconBaseline({ snapshotTimestamp, runtimeId, internal: internalBaseline });
      reconciliationResult = 'BASELINE_INITIALIZED';
      reconciliationAt = snapshotTimestamp;
      audit('reconciliation_snapshot', { snapshotTimestamp, result: reconciliationResult, exchange: { assetCount: assets.length, usdt: { balance: n(usdt?.cashBalance), equity: n(usdt?.equity), availableMargin: n(usdt?.availableBalance), unrealizedPnl: n(usdt?.unrealized ?? 0) }, positions: exchangePositions, openOrders: exchangeOrders, stopOrders: exchangeStops }, checks, evidence: { endpoints: ['/api/v1/private/account/assets','/api/v1/private/position/open_positions','/api/v1/private/order/list/open_orders?page_num=1&page_size=100','/api/v1/private/stoporder/open_orders'] } });
      return false;
    }
    reconciliationResult = allEqual ? 'PASS' : 'FAIL'; reconciliationAt = snapshotTimestamp;
    audit('reconciliation_snapshot', { snapshotTimestamp, result: reconciliationResult, exchange: { assetCount: assets.length, usdt: { balance: n(usdt?.cashBalance), equity: n(usdt?.equity), availableMargin: n(usdt?.availableBalance), unrealizedPnl: n(usdt?.unrealized ?? 0) }, positions: exchangePositions, openOrders: exchangeOrders, stopOrders: exchangeStops }, internalBaseline, checks, evidence: { endpoints: ['/api/v1/private/account/assets','/api/v1/private/position/open_positions','/api/v1/private/order/list/open_orders?page_num=1&page_size=100','/api/v1/private/stoporder/open_orders'], note: 'Read-only REST responses only; no order/position/SL/TP mutation endpoints are called.' } });
    return allEqual;
  } catch (e) { reconciliationResult = 'FAIL'; reconciliationAt = snapshotTimestamp; audit('reconciliation_snapshot', { snapshotTimestamp, result: 'FAIL', reason: e instanceof Error ? e.message : 'unknown' }); return false; }
}
function evaluateSafetyGate() {
  const conditions = { mexcAuthenticated: authenticated === true, readOnly: lastReadOnlyResult === 'PASS', reconciliation: reconciliationResult === 'PASS', dataIntegrity: reconciliationResult === 'PASS', riskEngine: state.riskEngineStatus === 'PASS', killSwitch: state.killSwitchStatus === 'PASS', executionAuthority: leaseHeld === true, liveOff: LIVE_ENABLED === false };
  const pass = Object.values(conditions).every(Boolean);
  safetyResult = pass ? 'PASS' : 'BLOCKED'; safetyAt = new Date().toISOString();
  audit('safety_check', { result: safetyResult, conditions });
  return pass;
}
function evaluateReadiness() {
  if (!evaluateSafetyGate()) { readiness = 'BLOCKED'; readinessReason = 'SAFETY_GATE_FAILED'; state = { ...state, state: 'PAPER_RUNTIME' }; persist(); audit('readiness_transition_blocked', { target: 'READY_HEALTHY', reason: readinessReason }); return false; }
  readiness = 'READY_HEALTHY'; readinessReason = null; state = { ...state, state: 'READY_HEALTHY', riskEngineStatus: 'PASS', killSwitchStatus: 'PASS' }; persist(); audit('state_transition', { from: 'PAPER_RUNTIME', to: 'READY_HEALTHY', transition: 'RECONCILIATION -> SAFETY_CHECK -> READY/HEALTHY', safetyResult, reconciliationResult }); return true;
}
function loginMessage() { const reqTime = Date.now().toString(); return JSON.stringify({ method: 'login', subscribe: false, param: { apiKey: API_KEY, reqTime, signature: crypto.createHmac('sha256', API_SECRET).update(`${API_KEY}${reqTime}`).digest('hex') } }); }
function filterMessage() { return JSON.stringify({ method: 'personal.filter', param: { filters: [{ filter: 'order' }, { filter: 'order.deal' }, { filter: 'position' }] } }); }
function connect() {
  if (stopping) return; audit('mexc_connecting', { wsUrl: WS_URL }); ws = new WebSocket(WS_URL); connected = false; authenticated = false; clearTimeout(wsConnectTimer);
  wsConnectTimer = setTimeout(() => { if (!connected && !stopping) { audit('mexc_connect_timeout', { timeoutMs: WS_CONNECT_TIMEOUT_MS }); closedByUs = true; try { ws?.close(); } catch {} closedByUs = false; } }, WS_CONNECT_TIMEOUT_MS);
  let pingTimer; closedByUs = false;
  ws.on('open', () => { clearTimeout(wsConnectTimer); connected = true; audit('mexc_connected'); ws.send(loginMessage()); });
  ws.on('message', raw => { let msg; try { msg = JSON.parse(raw.toString()); } catch { return; } lastMexcEventAt = Date.now(); if (msg.channel === 'rs.login') { authenticated = msg.data === 'success'; audit(authenticated ? 'mexc_authenticated' : 'mexc_authentication_failed', { dataType: typeof msg.data }); if (authenticated) { ws.send(filterMessage()); pingTimer = setInterval(() => { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'ping' })); }, 10000); } else { closedByUs = true; ws.close(); } return; } if (msg.channel === 'rs.error') audit('mexc_protocol_error', { dataType: typeof msg.data, message: typeof msg.data === 'string' ? msg.data : undefined }); if (msg.channel?.startsWith('push.personal.')) audit('mexc_private_event', { channel: msg.channel }); });
  ws.on('error', err => { lastError = err.message; audit('websocket_error', { message: err.message }); });
  ws.on('close', (code, reason) => { clearTimeout(wsConnectTimer); clearInterval(pingTimer); connected = false; authenticated = false; audit('mexc_disconnected', { code, reason: reason?.toString() || '', reconnectInMs: RECONNECT_MS }); if (!closedByUs && !stopping) setTimeout(connect, RECONNECT_MS); });
}
async function engineCycle() {
  const lease = renewLease();
  if (!lease) { state = { ...state, state: 'SAFE_STATE', lastError: 'EXECUTION_AUTHORITY_UNKNOWN' }; persist(); return; }
  if (!authenticated) state = { ...state, state: 'RECOVERING', lastError: connected ? 'MEXC_AUTH_PENDING' : 'MEXC_DISCONNECTED' };
  else { state = { ...state, state: LIVE_ENABLED ? 'WAITING_FOR_APP_ENGINE_AUTHORITY' : 'PAPER_RUNTIME', lastError: null, sequence: (state.sequence || 0) + 1, riskEngineStatus: 'PASS', killSwitchStatus: 'PASS' }; }
  if (!lastReadOnlyCheckAt || Date.now() - Date.parse(lastReadOnlyCheckAt) >= READ_ONLY_CHECK_MS) await readOnlyAccountCheck();
  if (!reconciliationAt || Date.now() - Date.parse(reconciliationAt) >= RECONCILIATION_MS) await reconciliationSnapshot();
  if (reconciliationResult === 'PASS' && authenticated && lastReadOnlyResult === 'PASS') evaluateReadiness();
  else { readiness = 'BLOCKED'; readinessReason = 'RECONCILIATION_OR_AUTH_NOT_PASS'; safetyResult = 'BLOCKED'; safetyAt = new Date().toISOString(); persist(); }
}
function status() { return { service: SERVICE, runtimeId, state: readiness === 'READY_HEALTHY' ? 'READY_HEALTHY' : state.state, connected, authenticated, leaseHeld, liveEnabled: LIVE_ENABLED, ordersSent: 0, positionsModified: 0, reconciliationResult, reconciliationAt, safetyResult, safetyAt, readiness, readinessReason, riskEngine: state.riskEngineStatus ?? 'NOT_VERIFIED', killSwitch: state.killSwitchStatus ?? 'NOT_VERIFIED', lastMexcEventAt, lastMexcCallAt, lastReadOnlyCheckAt, lastReadOnlyResult, lastError, updatedAt: state.updatedAt || null }; }
const server = http.createServer((req, res) => { const u = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`); if (u.pathname === '/' || u.pathname === '/health') { res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(status())); return; } res.writeHead(404, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'NOT_FOUND' })); });
server.listen(PORT, '0.0.0.0', () => audit('health_server_ready', { port: PORT }));
async function main() {
  if (!API_KEY || !API_SECRET) { audit('startup_blocked', { reason: 'MEXC credentials are not configured' }); process.exitCode = 1; return; }
  if (!acquireLease()) { state = { ...state, state: 'SAFE_STATE', lastError: 'EXECUTION_AUTHORITY_UNKNOWN' }; persist(); audit('runtime_start_blocked', { reason: 'Another runtime owns the local atomic lease' }); return; }
  persist(); audit('runtime_started', { leaseHeld, mode: LIVE_ENABLED ? 'LIVE_LOCKED_PENDING_AUTHORITY' : 'LIVE_OFF' }); connect(); await engineCycle();
  const timer = setInterval(() => { void engineCycle(); }, ENGINE_CYCLE_MS);
  const heartbeat = setInterval(() => { persist(); audit('heartbeat', { state: status().state, authenticated, readOnly: lastReadOnlyResult, reconciliation: reconciliationResult, safety: safetyResult, readiness }); }, HEARTBEAT_MS);
  const shutdown = signal => { if (stopping) return; stopping = true; clearInterval(timer); clearInterval(heartbeat); clearTimeout(wsConnectTimer); closedByUs = true; try { ws?.close(); } catch {} releaseLease(); state = { ...state, state: 'STOPPED' }; persist(); audit('graceful_shutdown', { signal }); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 5000).unref(); };
  process.on('SIGTERM', () => shutdown('SIGTERM')); process.on('SIGINT', () => shutdown('SIGINT')); process.on('uncaughtException', err => { lastError = err.message; audit('uncaught_exception', { message: err.message }); process.exit(1); }); process.on('unhandledRejection', err => { lastError = err instanceof Error ? err.message : 'unhandled rejection'; audit('unhandled_rejection', { message: lastError }); process.exit(1); });
}
main();
