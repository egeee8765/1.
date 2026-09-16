import crypto from 'node:crypto';
import fs from 'node:fs';

const REST_URL = 'https://api.mexc.com';
const API_KEY = process.env.MEXC_API_KEY;
const API_SECRET = process.env.MEXC_API_SECRET;
const STATE_FILE = process.env.RUNTIME_STATE_FILE || '/tmp/aurevix-runtime-state.json';
const INTERVAL_MS = 30000;
const START_DELAY_MS = 5000;
const RECV_WINDOW = '10000';

function signature(apiKey, reqTime, parameterString, secret) {
  return crypto.createHmac('sha256', secret).update(`${apiKey}${reqTime}${parameterString}`).digest('hex');
}

function queryString(params) {
  return Object.entries(params)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${encodeURIComponent(String(value))}`)
    .join('&');
}

async function privateGet(path, params = {}) {
  if (!API_KEY || !API_SECRET) throw new Error('MEXC credentials are not configured');
  const qs = queryString(params);
  const reqTime = Date.now().toString();
  const url = `${REST_URL}${path}${qs ? `?${qs}` : ''}`;
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      ApiKey: API_KEY,
      'Request-Time': reqTime,
      Signature: signature(API_KEY, reqTime, qs, API_SECRET),
      'Recv-Window': RECV_WINDOW,
      Language: 'English',
      Accept: 'application/json',
    },
  });
  const payload = await response.json();
  if (!response.ok || !payload?.success) {
    throw new Error(`${path}: HTTP ${response.status} code=${payload?.code ?? 'UNKNOWN'} message=${typeof payload?.message === 'string' ? payload.message : 'request failed'}`);
  }
  return payload.data;
}

function readInternalState() {
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {
    return null;
  }
}

function n(value) {
  return value === null || value === undefined ? null : Number(value);
}

function sameNumber(a, b, tolerance = 1e-10) {
  if (a === null || b === null || Number.isNaN(a) || Number.isNaN(b)) return false;
  return Math.abs(a - b) <= tolerance * Math.max(1, Math.abs(a), Math.abs(b));
}

function comparison(exchange, internal) {
  if (internal === 'UNAVAILABLE') return { exchange, internal, result: 'UNAVAILABLE' };
  if (Array.isArray(exchange) && Array.isArray(internal)) {
    return { exchange, internal, result: JSON.stringify(exchange) === JSON.stringify(internal) ? 'EQUAL' : 'MISMATCH' };
  }
  if (typeof exchange === 'number' || typeof internal === 'number') {
    return { exchange, internal, result: sameNumber(n(exchange), n(internal)) ? 'EQUAL' : 'MISMATCH' };
  }
  return { exchange, internal, result: exchange === internal ? 'EQUAL' : 'MISMATCH' };
}

async function reconciliationSnapshot() {
  const snapshotTimestamp = new Date().toISOString();
  const internal = readInternalState();
  const internalExecution = internal ? {
    runtimeId: internal.runtimeId ?? null,
    state: internal.state ?? null,
    liveEnabled: internal.liveEnabled ?? null,
    ordersSent: Number(internal.ordersSent ?? 0),
    positionsModified: Number(internal.positionsModified ?? 0),
  } : null;

  if (!internal) {
    console.log(JSON.stringify({ event: 'reconciliation_snapshot', snapshotTimestamp, result: 'FAIL', reason: 'INTERNAL_STATE_UNAVAILABLE', liveEnabled: false, ordersSent: 0, positionsModified: 0 }));
    return;
  }

  const [assets, positions, openOrders, stopOrders] = await Promise.all([
    privateGet('/api/v1/private/account/assets'),
    privateGet('/api/v1/private/position/open_positions'),
    privateGet('/api/v1/private/order/list/open_orders', { page_num: 1, page_size: 100 }),
    privateGet('/api/v1/private/stoporder/open_orders'),
  ]);

  const usdt = Array.isArray(assets) ? assets.find(x => String(x.currency).toUpperCase() === 'USDT') : null;
  const exchangePositions = Array.isArray(positions) ? positions : [];
  const exchangeOrders = Array.isArray(openOrders) ? openOrders : [];
  const exchangeStops = Array.isArray(stopOrders) ? stopOrders : [];

  const safeNoExecutionInvariant = internalExecution.liveEnabled === false && internalExecution.ordersSent === 0 && internalExecution.positionsModified === 0;
  const internalCollections = safeNoExecutionInvariant ? { positions: [], openOrders: [], stopOrders: [] } : null;

  const positionRows = exchangePositions.map(p => ({
    positionId: String(p.positionId ?? ''),
    symbol: p.symbol ?? null,
    positionSize: n(p.holdVol),
    direction: Number(p.positionType) === 1 ? 'LONG' : Number(p.positionType) === 2 ? 'SHORT' : 'UNKNOWN',
    entryPrice: n(p.holdAvgPrice),
    leverage: n(p.leverage),
    unrealizedPnl: n(p.unRealizedPnl),
  }));

  const positionDetailChecks = exchangePositions.length === 0
    ? {
        positionSize: comparison(0, 0),
        direction: comparison(null, null),
        entryPrice: comparison(null, null),
        leverage: comparison(null, null),
        unrealizedPnl: comparison(0, 0),
      }
    : {
        positionSize: comparison(positionRows, internalCollections ? internalCollections.positions : 'UNAVAILABLE'),
        direction: comparison(positionRows, internalCollections ? internalCollections.positions : 'UNAVAILABLE'),
        entryPrice: comparison(positionRows, internalCollections ? internalCollections.positions : 'UNAVAILABLE'),
        leverage: comparison(positionRows, internalCollections ? internalCollections.positions : 'UNAVAILABLE'),
        unrealizedPnl: comparison(positionRows, internalCollections ? internalCollections.positions : 'UNAVAILABLE'),
      };

  const checks = {
    balance: comparison(n(usdt?.cashBalance), 'UNAVAILABLE'),
    equity: comparison(n(usdt?.equity), 'UNAVAILABLE'),
    availableMargin: comparison(n(usdt?.availableBalance), 'UNAVAILABLE'),
    openPositions: comparison(exchangePositions.length, internalCollections ? internalCollections.positions.length : 'UNAVAILABLE'),
    positionSize: positionDetailChecks.positionSize,
    direction: positionDetailChecks.direction,
    entryPrice: positionDetailChecks.entryPrice,
    leverage: positionDetailChecks.leverage,
    unrealizedPnl: comparison(n(usdt?.unrealized ?? 0), exchangePositions.reduce((sum, p) => sum + (Number(p.unRealizedPnl) || 0), 0)),
    openOrders: comparison(exchangeOrders.length, internalCollections ? internalCollections.openOrders.length : 'UNAVAILABLE'),
    stopOrders: comparison(exchangeStops.length, internalCollections ? internalCollections.stopOrders.length : 'UNAVAILABLE'),
    internalExecutionState: {
      exchange: { positions: exchangePositions.length, openOrders: exchangeOrders.length, stopOrders: exchangeStops.length },
      internal: internalExecution,
      result: safeNoExecutionInvariant && exchangePositions.length === 0 && exchangeOrders.length === 0 && exchangeStops.length === 0 ? 'EQUAL' : 'MISMATCH',
    },
  };

  const comparableResults = Object.values(checks).map(x => x.result).filter(Boolean);
  const result = comparableResults.every(x => x === 'EQUAL');

  console.log(JSON.stringify({
    event: 'reconciliation_snapshot',
    snapshotTimestamp,
    result: result ? 'PASS' : 'FAIL',
    runtimeId: internal.runtimeId ?? null,
    state: internal.state ?? null,
    liveEnabled: false,
    ordersSent: 0,
    positionsModified: 0,
    exchange: {
      assetCount: Array.isArray(assets) ? assets.length : 0,
      usdt: usdt ? { balance: n(usdt.cashBalance), equity: n(usdt.equity), availableMargin: n(usdt.availableBalance), unrealizedPnl: n(usdt.unrealized ?? 0) } : null,
      positions: positionRows,
      openOrders: exchangeOrders,
      stopOrders: exchangeStops,
    },
    checks,
    evidence: { endpoints: ['/api/v1/private/account/assets', '/api/v1/private/position/open_positions', '/api/v1/private/order/list/open_orders?page_num=1&page_size=100', '/api/v1/private/stoporder/open_orders'] },
  }));
}

async function run() {
  try {
    await reconciliationSnapshot();
  } catch (error) {
    console.log(JSON.stringify({ event: 'reconciliation_snapshot', snapshotTimestamp: new Date().toISOString(), result: 'FAIL', reason: error instanceof Error ? error.message : 'unknown', liveEnabled: false, ordersSent: 0, positionsModified: 0 }));
  }
}

setTimeout(() => {
  void run();
  setInterval(() => { void run(); }, INTERVAL_MS);
}, START_DELAY_MS).unref?.();
