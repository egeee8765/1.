const TIMEFRAMES = {
  '4H': { interval: 'Hour4', ms: 4 * 60 * 60 * 1000 },
  '1H': { interval: 'Min60', ms: 60 * 60 * 1000 },
  '15M': { interval: 'Min15', ms: 15 * 60 * 1000 },
  '5M': { interval: 'Min5', ms: 5 * 60 * 1000 },
};

export const RISK_CONFIG = Object.freeze({
  normalRiskPct: 0.50,
  dailySoftLossPct: -1.0,
  dailyHardLossPct: -1.5,
  weeklyHardLossPct: -4.0,
  monthlyHardLossPct: -8.0,
  twoLossRiskPct: 0.35,
  threeLossCooldownMs: 60 * 60 * 1000,
  drawdownReductionPct: 5.0,
  drawdownReducedRiskPct: 0.35,
  drawdownRiskPct75: 0.25,
  liveHaltDrawdownPct: 10.0,
  emergencyDrawdownPct: 12.0,
  maxDailyTrades: 6,
  maxOpenPositions: 3,
  maxPortfolioRiskPct: 1.5,
  minLeverage: 7,
  maxLeverage: 18,
  minRR: 1.8,
});

const EPS = 1e-9;
const finite = v => Number.isFinite(Number(v));
const pct = v => Number(v ?? 0);

export function validateCandles(candles, intervalMs, nowMs = Date.now()) {
  const reasons = [];
  if (!Array.isArray(candles) || candles.length < 3) reasons.push('INSUFFICIENT_CANDLES');
  if (!Array.isArray(candles)) return { pass: false, reasons };
  const normalized = candles.map(c => ({
    time: Number(c.time), open: Number(c.open), high: Number(c.high), low: Number(c.low), close: Number(c.close), volume: Number(c.volume),
  }));
  const seen = new Set();
  let previous = null;
  for (const c of normalized) {
    if (!finite(c.time) || !finite(c.open) || !finite(c.high) || !finite(c.low) || !finite(c.close) || !finite(c.volume)) reasons.push('NON_FINITE_CANDLE');
    if (seen.has(c.time)) reasons.push('DUPLICATE_CANDLE');
    seen.add(c.time);
    if (c.volume < 0) reasons.push('NEGATIVE_VOLUME');
    if (c.low > c.high || c.high < Math.max(c.open, c.close) || c.low > Math.min(c.open, c.close) || c.low <= 0 || c.open <= 0 || c.high <= 0 || c.close <= 0) reasons.push('OHLC_INVALID');
    if (c.time % intervalMs !== 0) reasons.push('TIMESTAMP_ALIGNMENT_INVALID');
    if (previous !== null && c.time - previous !== intervalMs) reasons.push('MISSING_OR_GAPPED_CANDLE');
    previous = c.time;
  }
  const sorted = [...normalized].sort((a, b) => a.time - b.time);
  const latestClosed = sorted.at(-1);
  if (latestClosed) {
    const age = nowMs - latestClosed.time;
    if (age < intervalMs - 5000) reasons.push('FUTURE_OR_OPEN_CANDLE');
    if (age > intervalMs * 2.5) reasons.push('STALE_CANDLE');
  }
  const volumes = sorted.map(c => c.volume).filter(v => v >= 0).sort((a, b) => a - b);
  if (volumes.length >= 5) {
    const median = volumes[Math.floor(volumes.length / 2)];
    if (median > EPS && sorted.at(-1).volume > median * 50) reasons.push('VOLUME_ANOMALY');
  }
  return { pass: reasons.length === 0, reasons, latestTimestamp: latestClosed?.time ?? null, candleCount: sorted.length };
}

export function evaluateMarketData({ candlesByTimeframe, nowMs = Date.now(), maxClockSkewMs = 5000 }) {
  const perTimeframe = {};
  const reasons = [];
  for (const [tf, meta] of Object.entries(TIMEFRAMES)) {
    const result = validateCandles(candlesByTimeframe?.[tf], meta.ms, nowMs);
    perTimeframe[tf] = result;
    if (!result.pass) reasons.push(`${tf}:${result.reasons.join(',')}`);
    if (result.latestTimestamp && Math.abs(nowMs - (result.latestTimestamp + meta.ms)) > Math.max(meta.ms * 2.5, maxClockSkewMs) && nowMs > result.latestTimestamp) reasons.push(`${tf}:CLOCK_OR_FRESHNESS`);
  }
  return { pass: reasons.length === 0, reasons, perTimeframe, checkedAt: new Date(nowMs).toISOString() };
}

export function evaluateRisk(input) {
  const c = { ...RISK_CONFIG, ...(input.config || {}) };
  const dailyLoss = pct(input.dailyLossPct);
  const weeklyLoss = pct(input.weeklyLossPct);
  const monthlyLoss = pct(input.monthlyLossPct);
  const dd = Math.max(0, pct(input.drawdownPct));
  const losses = Number(input.consecutiveLosses || 0);
  const openPositions = Number(input.openPositions || 0);
  const dailyTrades = Number(input.dailyTrades || 0);
  const portfolioRisk = pct(input.portfolioRiskPct);
  const leverage = Number(input.leverage ?? c.minLeverage);
  const rr = Number(input.rr ?? 0);
  const cooldownUntil = Number(input.cooldownUntil || 0);
  const nowMs = Number(input.nowMs || Date.now());
  const violations = [];
  let allowedRiskPct = c.normalRiskPct;
  let status = 'PASS';
  if (dailyLoss <= c.dailyHardLossPct) violations.push('DAILY_HARD_LOSS');
  if (weeklyLoss <= c.weeklyHardLossPct) violations.push('WEEKLY_HARD_LOSS');
  if (monthlyLoss <= c.monthlyHardLossPct) violations.push('MONTHLY_HARD_LOSS');
  if (dailyTrades >= c.maxDailyTrades) violations.push('MAX_DAILY_TRADES');
  if (openPositions >= c.maxOpenPositions) violations.push('MAX_OPEN_POSITIONS');
  if (portfolioRisk > c.maxPortfolioRiskPct + EPS) violations.push('MAX_PORTFOLIO_RISK');
  if (leverage < c.minLeverage || leverage > c.maxLeverage) violations.push('LEVERAGE_OUT_OF_RANGE');
  if (rr + EPS < c.minRR) violations.push('MIN_RR');
  if (losses >= 3) { allowedRiskPct = Math.min(allowedRiskPct, c.twoLossRiskPct); if (nowMs < cooldownUntil) violations.push('CONSECUTIVE_LOSS_COOLDOWN'); }
  else if (losses === 2) allowedRiskPct = Math.min(allowedRiskPct, c.twoLossRiskPct);
  if (dailyLoss <= c.dailySoftLossPct && dailyLoss > c.dailyHardLossPct) allowedRiskPct = Math.min(allowedRiskPct, c.normalRiskPct / 2);
  if (dd >= c.emergencyDrawdownPct) violations.push('EMERGENCY_DRAWDOWN');
  else if (dd >= c.liveHaltDrawdownPct) violations.push('LIVE_HALT_DRAWDOWN');
  else if (dd >= 7.5) allowedRiskPct = Math.min(allowedRiskPct, c.drawdownRiskPct75);
  else if (dd >= c.drawdownReductionPct) allowedRiskPct = Math.min(allowedRiskPct, c.drawdownReducedRiskPct);
  if (violations.length) status = 'BLOCKED';
  return { pass: status === 'PASS', status, allowedRiskPct, violations, config: c, winningStreakDoesNotIncreaseRisk: true };
}

export function evaluateKillSwitch(input) {
  const triggers = {
    dailyHardLoss: !!input.dailyHardLoss,
    weeklyLimit: !!input.weeklyLimit,
    monthlyLimit: !!input.monthlyLimit,
    maxDrawdown: !!input.maxDrawdown,
    consecutiveLosses: !!input.consecutiveLosses,
    extremeVolatility: !!input.extremeVolatility,
    apiFailure: !!input.apiFailure,
    dataFailure: !!input.dataFailure,
    reconciliationFailure: !!input.reconciliationFailure,
    emergencyStop: !!input.emergencyStop,
  };
  const active = Object.entries(triggers).filter(([, v]) => v).map(([k]) => k);
  return { pass: active.length === 0, newTrade: active.length === 0 ? 'ALLOWED_BY_KILL_SWITCH' : 'BLOCKED', activeTriggers: active, triggers };
}

export function executionGate({ signalPass, dataIntegrityPass, riskPass, killSwitchPass, executionAuthority, liveOff }) {
  const conditions = { signal: !!signalPass, dataIntegrity: !!dataIntegrityPass, riskEngine: !!riskPass, killSwitch: !!killSwitchPass, executionAuthority: !!executionAuthority, liveOff: !!liveOff };
  const allowed = Object.values(conditions).every(Boolean);
  return { allowed, result: allowed ? 'EXECUTION_ALLOWED' : 'EXECUTION_BLOCKED', conditions };
}

function assert(condition, message) { if (!condition) throw new Error(message); }

export function runDeterministicSafetyTests(nowMs = Date.now()) {
  const pass = [];
  const fail = [];
  const expect = (name, fn) => { try { fn(); pass.push(name); } catch (e) { fail.push(`${name}:${e.message}`); } };
  const baseRisk = { dailyLossPct: 0, weeklyLossPct: 0, monthlyLossPct: 0, drawdownPct: 0, consecutiveLosses: 0, dailyTrades: 0, openPositions: 0, portfolioRiskPct: 0, leverage: 10, rr: 2.0, nowMs };
  expect('risk-normal-0.50', () => assert(evaluateRisk(baseRisk).allowedRiskPct === 0.5 && evaluateRisk(baseRisk).pass, 'normal risk'));
  expect('daily-soft-reduces-risk', () => assert(evaluateRisk({ ...baseRisk, dailyLossPct: -1.1 }).allowedRiskPct === 0.25, 'daily soft'));
  expect('daily-hard-blocks', () => assert(!evaluateRisk({ ...baseRisk, dailyLossPct: -1.5 }).pass, 'daily hard'));
  expect('weekly-hard-blocks', () => assert(!evaluateRisk({ ...baseRisk, weeklyLossPct: -4 }).pass, 'weekly'));
  expect('monthly-hard-blocks', () => assert(!evaluateRisk({ ...baseRisk, monthlyLossPct: -8 }).pass, 'monthly'));
  expect('two-loss-risk-0.35', () => assert(evaluateRisk({ ...baseRisk, consecutiveLosses: 2 }).allowedRiskPct === 0.35, 'two losses'));
  expect('three-loss-cooldown', () => assert(!evaluateRisk({ ...baseRisk, consecutiveLosses: 3, cooldownUntil: nowMs + 3600000 }).pass, 'three losses'));
  expect('dd-5-reduction', () => assert(evaluateRisk({ ...baseRisk, drawdownPct: 5 }).allowedRiskPct === 0.35, 'dd5'));
  expect('dd-7.5-risk-0.25', () => assert(evaluateRisk({ ...baseRisk, drawdownPct: 7.5 }).allowedRiskPct === 0.25, 'dd7.5'));
  expect('dd-10-live-halt', () => assert(!evaluateRisk({ ...baseRisk, drawdownPct: 10 }).pass, 'dd10'));
  expect('dd-12-emergency', () => assert(evaluateRisk({ ...baseRisk, drawdownPct: 12 }).violations.includes('EMERGENCY_DRAWDOWN'), 'dd12'));
  expect('max-6-daily-trades', () => assert(!evaluateRisk({ ...baseRisk, dailyTrades: 6 }).pass, 'daily trades'));
  expect('max-3-open-positions', () => assert(!evaluateRisk({ ...baseRisk, openPositions: 3 }).pass, 'positions'));
  expect('portfolio-risk-1.5', () => assert(!evaluateRisk({ ...baseRisk, portfolioRiskPct: 1.5001 }).pass, 'portfolio'));
  expect('leverage-7-to-18', () => assert(!evaluateRisk({ ...baseRisk, leverage: 6 }).pass && !evaluateRisk({ ...baseRisk, leverage: 19 }).pass, 'leverage'));
  expect('minimum-rr-1.8', () => assert(!evaluateRisk({ ...baseRisk, rr: 1.79 }).pass, 'rr'));
  expect('winning-streak-no-risk-increase', () => assert(evaluateRisk({ ...baseRisk, winningStreak: 20 }).allowedRiskPct === 0.5, 'winning streak'));
  for (const key of ['dailyHardLoss','weeklyLimit','monthlyLimit','maxDrawdown','consecutiveLosses','extremeVolatility','apiFailure','dataFailure','reconciliationFailure','emergencyStop']) expect(`kill-switch-${key}`, () => assert(evaluateKillSwitch({ [key]: true }).newTrade === 'BLOCKED', key));
  expect('execution-bypass-blocked', () => assert(executionGate({ signalPass: true, dataIntegrityPass: false, riskPass: true, killSwitchPass: true, executionAuthority: true, liveOff: true }).allowed === false, 'data bypass'));
  expect('risk-bypass-blocked', () => assert(executionGate({ signalPass: true, dataIntegrityPass: true, riskPass: false, killSwitchPass: true, executionAuthority: true, liveOff: true }).allowed === false, 'risk bypass'));
  expect('kill-switch-bypass-blocked', () => assert(executionGate({ signalPass: true, dataIntegrityPass: true, riskPass: true, killSwitchPass: false, executionAuthority: true, liveOff: true }).allowed === false, 'kill bypass'));
  return { pass: fail.length === 0, passed: pass, failed: fail, count: pass.length + fail.length };
}

export { TIMEFRAMES };
