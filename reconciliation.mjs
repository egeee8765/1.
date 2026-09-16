import crypto from 'node:crypto';
import fs from 'node:fs';

const REST_URL = 'https://api.mexc.com';
const API_KEY = process.env.MEXC_API_KEY;
const API_SECRET = process.env.MEXC_API_SECRET;
const STATE_FILE = process.env.RUNTIME_STATE_FILE || '/tmp/aurevix-runtime-state.json';
const RECON_STATE_FILE = process.env.RECONCILIATION_STATE_FILE || '/tmp/aurevix-reconciliation-state.json';
const INTERVAL_MS = 30000;
const START_DELAY_MS = 5000;
const RECV_WINDOW = '10000';

function signature(apiKey, reqTime, parameterString, secret) {
  return crypto.createHmac('sha256', secret).update(`${apiKey}${reqTime}${parameterString}`).digest('hex');
}
function queryString(params) {
  return Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== '').sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => `${key}=${encodeURIComponent(String(value))}`).join('&');
}
async function privateGet(path, params = {}) {
  if (!API_KEY || !API_SECRET) throw new Error('MEXC credentials are not configured');
  const qs = queryString(params);
  const reqTime = Date.now().toString();
  const response = await fetch(`${REST_URL}${path}${qs ? `?${qs}` : ''}`, { method: 'GET', headers: { ApiKey: API_KEY, 'Request-Time': reqTime, Signature: signature(API_KEY, reqTime, qs, API_SECRET), 'Recv-Window': RECV_WINDOW, Language: 'English', Accept: 'application/json' } });
  const payload = await response.json();
  if (!response.ok || !payload?.success) throw new Error(`${path}: HTTP ${response.status} code=${payload?.code ?? 'UNKNOWN'} message=${typeof payload?.message === 'string' ? payload.message : 'request failed'}`);
  return payload.data;
}
function readJson(path) { try { return JSON.parse(fs.readFileSync(path, 'utf8')); } catch { return null; } }
function writeJson(path, value) { const tmp = `${path}.${process.pid}.tmp`; fs.writeFileSync(tmp, JSON.stringify(value)); fs.renameSync(tmp, path); }
function readInternalState() { return readJson(STATE_FILE); }
function n(value) { return value === null || value === undefined ? null : Number(value); }
function sameNumber(a, b, tolerance = 1e-10) { if (a === null || b === null || Number.isNaN(a) || Number.isNaN(b)) return false; return Math.abs(a - b) <= tolerance * Math.max(1, Math.abs(a), Math.abs(b)); }
function equalValue(a, b) { if (a === 'UNAVAILABLE' || b === 'UNAVAILABLE') return false; if (typeof a === 'number' || typeof b === 'number') return sameNumber(n(a), n(b)); return JSON.stringify(a) === JSON.stringify(b); }
function check(exchange, internal) { return { exchange, internal, result: internal === 'UNAVAILABLE' ? 'UNAVAILABLE' : equalValue(exchange, internal) ? 'EQUAL' : 'MISMATCH' }; }
function normalizePositions(positions) { return (Array.isArray(positions) ? positions : []).map(p => ({ positionId: String(p.positionId ?? ''), symbol: p.symbol ?? null, positionSize: n(p.holdVol), direction: Number(p.positionType) === 1 ? 'LONG' : Number(p.positionType) === 2 ? 'SHORT' : 'UNKNOWN', entryPrice: n(p.holdAvgPrice), leverage: n(p.leverage), unrealizedPnl: n(p.unRealizedPnl) })).sort((a, b) => a.positionId.localeCompare(b.positionId)); }
function normalizeOrders(orders) { return (Array.isArray(orders) ? orders : []).map(o => ({ orderId: String(o.orderId ?? ''), symbol: o.symbol ?? null, positionId: String(o.positionId ?? ''), price: n(o.price), vol: n(o.vol), leverage: n(o.leverage), side: n(o.side), state: n(o.state), reduceOnly: Boolean(o.reduceOnly) })).sort((a, b) => a.orderId.localeCompare(b.orderId)); }
function normalizeStops(stops) { return (Array.isArray(stops) ? stops : []).map(o => ({ id: String(o.id ?? ''), symbol: o.symbol ?? null, positionId: String(o.positionId ?? ''), stopLossPrice: n(o.stopLossPrice), takeProfitPrice: n(o.takeProfitPrice), state: n(o.state), positionType: n(o.positionType), vol: n(o.vol), realityVol: n(o.realityVol) })).sort((a, b) => a.id.localeCompare(b.id)); }

async function reconciliationSnapshot() {
  const snapshotTimestamp = new Date().toISOString();
  const internalExecution = readInternalState();
  if (!internalExecution) throw new Error('INTERNAL_STATE_UNAVAILABLE');

  const [assets, positions, openOrders, stopOrders] = await Promise.all([
    privateGet('/api/v1/private/account/assets'),
    privateGet('/api/v1/private/position/open_positions'),
    privateGet('/api/v1/private/order/list/open_orders', { page_num: 1, page_size: 100 }),
    privateGet('/api/v1/private/stoporder/open_orders'),
  ]);

  const usdt = Array.isArray(assets) ? assets.find(x => String(x.currency).toUpperCase() === 'USDT') : null;
  const current = {
    snapshotTimestamp,
    runtimeId: internalExecution.runtimeId ?? null,
    balance: n(usdt?.cashBalance),
    equity: n(usdt?.equity),
    availableMargin: n(usdt?.availableBalance),
    unrealizedPnl: n(usdt?.unrealized ?? 0),
    positions: normalizePositions(positions),
    openOrders: normalizeOrders(openOrders),
    stopOrders: normalizeStops(stopOrders),
    execution: {
      state: internalExecution.state ?? null,
      liveEnabled: internalExecution.liveEnabled ?? null,
      ordersSent: Number(internalExecution.ordersSent ?? 0),
      positionsModified: Number(internalExecution.positionsModified ?? 0),
    },
  };

  const baseline = readJson(RECON_STATE_FILE);
  if (!baseline) {
    writeJson(RECON_STATE_FILE, current);
    console.log(JSON.stringify({ event: 'reconciliation_snapshot', snapshotTimestamp, result: 'BASELINE_INITIALIZED', runtimeId: current.runtimeId, state: current.execution.state, liveEnabled: false, ordersSent: 0, positionsModified: 0, exchange: { assetCount: Array.isArray(assets) ? assets.length : 0, usdt: { balance: current.balance, equity: current.equity, availableMargin: current.availableMargin, unrealizedPnl: current.unrealizedPnl }, positions: current.positions, openOrders: current.openOrders, stopOrders: current.stopOrders }, internalBaseline: null, checks: { balance: 'UNAVAILABLE', equity: 'UNAVAILABLE', availableMargin: 'UNAVAILABLE', openPositions: 'UNAVAILABLE', positionSize: 'UNAVAILABLE', direction: 'UNAVAILABLE', entryPrice: 'UNAVAILABLE', leverage: 'UNAVAILABLE', unrealizedPnl: 'UNAVAILABLE', openOrders: 'UNAVAILABLE', stopOrders: 'UNAVAILABLE', internalExecutionState: 'UNAVAILABLE' }, reason: 'No prior persisted internal reconciliation baseline exists; PASS is blocked on first snapshot.' }));
    return;
  }

  const positionChecks = check(current.positions, baseline.positions);
  const orderChecks = check(current.openOrders, baseline.openOrders);
  const stopChecks = check(current.stopOrders, baseline.stopOrders);
  const internalExecutionCheck = check(current.execution, baseline.execution);
  const checks = {
    balance: check(current.balance, baseline.balance),
    equity: check(current.equity, baseline.equity),
    availableMargin: check(current.availableMargin, baseline.availableMargin),
    openPositions: check(current.positions.length, baseline.positions.length),
    positionSize: check(current.positions.map(x => x.positionSize), baseline.positions.map(x => x.positionSize)),
    direction: check(current.positions.map(x => x.direction), baseline.positions.map(x => x.direction)),
    entryPrice: check(current.positions.map(x => x.entryPrice), baseline.positions.map(x => x.entryPrice)),
    leverage: check(current.positions.map(x => x.leverage), baseline.positions.map(x => x.leverage)),
    unrealizedPnl: check(current.unrealizedPnl, baseline.unrealizedPnl),
    openOrders: check(current.openOrders.length, baseline.openOrders.length),
    stopOrders: check(current.stopOrders.length, baseline.stopOrders.length),
    internalExecutionState: { exchange: current.execution, internal: baseline.execution, result: internalExecutionCheck.result },
    positionIdentityAndFields: positionChecks,
    openOrderIdentityAndFields: orderChecks,
    stopOrderIdentityAndFields: stopChecks,
  };

  const result = Object.values(checks).every(x => x && x.result === 'EQUAL');
  console.log(JSON.stringify({
    event: 'reconciliation_snapshot', snapshotTimestamp, result: result ? 'PASS' : 'FAIL', runtimeId: current.runtimeId, state: current.execution.state, liveEnabled: false, ordersSent: 0, positionsModified: 0,
    exchange: { assetCount: Array.isArray(assets) ? assets.length : 0, usdt: { balance: current.balance, equity: current.equity, availableMargin: current.availableMargin, unrealizedPnl: current.unrealizedPnl }, positions: current.positions, openOrders: current.openOrders, stopOrders: current.stopOrders },
    internalBaseline: { snapshotTimestamp: baseline.snapshotTimestamp, runtimeId: baseline.runtimeId, balance: baseline.balance, equity: baseline.equity, availableMargin: baseline.availableMargin, unrealizedPnl: baseline.unrealizedPnl, positions: baseline.positions, openOrders: baseline.openOrders, stopOrders: baseline.stopOrders, execution: baseline.execution },
    checks,
    evidence: { endpoints: ['/api/v1/private/account/assets', '/api/v1/private/position/open_positions', '/api/v1/private/order/list/open_orders?page_num=1&page_size=100', '/api/v1/private/stoporder/open_orders'], note: 'Read-only REST responses only; no order/position/SL/TP mutation endpoints are called.' },
  }));

  writeJson(RECON_STATE_FILE, current);
}

async function run() { try { await reconciliationSnapshot(); } catch (error) { console.log(JSON.stringify({ event: 'reconciliation_snapshot', snapshotTimestamp: new Date().toISOString(), result: 'FAIL', reason: error instanceof Error ? error.message : 'unknown', liveEnabled: false, ordersSent: 0, positionsModified: 0 })); } }

const starter = setTimeout(() => { void run(); setInterval(() => { void run(); }, INTERVAL_MS); }, START_DELAY_MS);
starter.unref?.();
