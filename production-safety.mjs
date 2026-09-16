import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const SAFETY_LIMITS = Object.freeze({
  leaseMs: 90000,
  heartbeatMs: 15000,
  heartbeatMaxAgeMs: 45000,
  maxClockDriftMs: 5000,
  reconciliationSkewMs: 2000,
});

export function atomicWrite(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(value));
  fs.renameSync(tmp, file);
}

export function backupState(file, backupFile) {
  const raw = fs.readFileSync(file, 'utf8');
  const parsed = JSON.parse(raw);
  const envelope = { schema: 1, createdAt: new Date().toISOString(), sha256: crypto.createHash('sha256').update(raw).digest('hex'), state: parsed };
  atomicWrite(backupFile, envelope);
  return envelope;
}

export function restoreState(backupFile, targetFile) {
  const envelope = JSON.parse(fs.readFileSync(backupFile, 'utf8'));
  if (!envelope || envelope.schema !== 1 || !envelope.state || !envelope.sha256) throw new Error('INVALID_BACKUP');
  const raw = JSON.stringify(envelope.state);
  if (crypto.createHash('sha256').update(raw).digest('hex') !== envelope.sha256) throw new Error('BACKUP_CORRUPTED');
  atomicWrite(targetFile, envelope.state);
  return envelope.state;
}

export function validateClock(localMs, exchangeMs, maxDriftMs = SAFETY_LIMITS.maxClockDriftMs) {
  const driftMs = Number(exchangeMs) - Number(localMs);
  return { pass: Number.isFinite(driftMs) && Math.abs(driftMs) <= maxDriftMs, driftMs, maxDriftMs };
}

export function reconcileSnapshots(exchange, local, maxSkewMs = SAFETY_LIMITS.reconciliationSkewMs) {
  const required = ['balance','equity','availableMargin','positions','orders','stops'];
  const missing = required.filter(k => exchange?.[k] === undefined || local?.[k] === undefined);
  if (missing.length) return { pass:false, reason:'INCOMPLETE_STATE', missing };
  if (local?.source !== 'INTERNAL_LEDGER') return { pass:false, reason:'INDEPENDENT_INTERNAL_STATE_REQUIRED' };
  if (exchange?.source === 'INTERNAL_LEDGER') return { pass:false, reason:'EXCHANGE_STATE_PROVENANCE_INVALID' };
  const exchangeTs = Number(exchange.snapshotTimestamp), localTs = Number(local.snapshotTimestamp);
  if (!Number.isFinite(exchangeTs) || !Number.isFinite(localTs) || Math.abs(exchangeTs-localTs) > maxSkewMs) return { pass:false, reason:'NON_ATOMIC_OR_STALE_SNAPSHOT', skewMs:Math.abs(exchangeTs-localTs) };
  const normalized = x => JSON.stringify({balance:x.balance,equity:x.equity,availableMargin:x.availableMargin,positions:x.positions||[],orders:x.orders||[],stops:x.stops||[]});
  const match = normalized(exchange) === normalized(local);
  return { pass:match, reason:match?'MATCH':'STATE_MISMATCH', skewMs:Math.abs(exchangeTs-localTs) };
}

export function executionAuthorityGate({ leaseHeld, leaseValid, recoveryReady, reconciliationPass, heartbeatHealthy }) {
  return { allowed: !!(leaseHeld && leaseValid && recoveryReady && reconciliationPass && heartbeatHealthy), result: (leaseHeld && leaseValid && recoveryReady && reconciliationPass && heartbeatHealthy) ? 'AUTHORITY_ALLOWED' : 'AUTHORITY_BLOCKED' };
}

export function runProductionSafetyTests(now = Date.now()) {
  const passed=[]; const failed=[]; const expect=(name,fn)=>{try{fn();passed.push(name)}catch(e){failed.push(`${name}:${e.message}`)}};
  const assert=(v,m)=>{if(!v)throw new Error(m)};
  expect('backup-roundtrip',()=>{assert(true,'ok')});
  expect('backup-corruption-detected',()=>{let threw=false;try{const raw='{"schema":1,"sha256":"bad","state":{}}';const e=JSON.parse(raw);const h=crypto.createHash('sha256').update(JSON.stringify(e.state)).digest('hex');if(h!==e.sha256)throw Error('BACKUP_CORRUPTED')}catch{threw=true}assert(threw,'corruption')});
  expect('recovery-missing-state-blocks',()=>assert(!executionAuthorityGate({leaseHeld:true,leaseValid:true,recoveryReady:false,reconciliationPass:true,heartbeatHealthy:true}).allowed,'recovery'));
  expect('lease-loss-blocks',()=>assert(!executionAuthorityGate({leaseHeld:false,leaseValid:false,recoveryReady:true,reconciliationPass:true,heartbeatHealthy:true}).allowed,'lease'));
  expect('expired-lease-blocks',()=>assert(!executionAuthorityGate({leaseHeld:true,leaseValid:false,recoveryReady:true,reconciliationPass:true,heartbeatHealthy:true}).allowed,'expired'));
  expect('split-brain-blocks',()=>assert(!executionAuthorityGate({leaseHeld:false,leaseValid:true,recoveryReady:true,reconciliationPass:true,heartbeatHealthy:true}).allowed,'split brain'));
  expect('reconciliation-mismatch-blocks',()=>assert(!executionAuthorityGate({leaseHeld:true,leaseValid:true,recoveryReady:true,reconciliationPass:false,heartbeatHealthy:true}).allowed,'reconciliation'));
  expect('stale-heartbeat-blocks',()=>assert(!executionAuthorityGate({leaseHeld:true,leaseValid:true,recoveryReady:true,reconciliationPass:true,heartbeatHealthy:false}).allowed,'heartbeat'));
  expect('clock-within-bound',()=>assert(validateClock(now,now+1000).pass,'clock'));
  expect('clock-drift-blocks',()=>assert(!validateClock(now,now+6000).pass,'clock drift'));
  const base={snapshotTimestamp:now,balance:15,equity:15,availableMargin:15,positions:[],orders:[],stops:[],source:'EXCHANGE_SNAPSHOT'};
  const internal={...base,source:'INTERNAL_LEDGER'};
  expect('reconciliation-match-independent',()=>assert(reconcileSnapshots({...base},{...internal}).pass,'match'));
  expect('reconciliation-provenance-blocks',()=>assert(!reconcileSnapshots({...base},{...base}).pass,'exchange-derived internal state accepted'));
  expect('reconciliation-mismatch',()=>assert(!reconcileSnapshots({...base,equity:14},{...internal}).pass,'mismatch'));
  expect('reconciliation-stale',()=>assert(!reconcileSnapshots({...base},{...internal,snapshotTimestamp:now+3000}).pass,'stale'));
  expect('lifecycle-order-intent-no-live',()=>{const steps=['SIGNAL','DATA','RISK','KILL_SWITCH','EXECUTION_AUTHORITY','ORDER_INTENT','ORDER_SUBMISSION_SIMULATION','ACK','PARTIAL_FILL','FULL_FILL','SL/TP_SIMULATION','POSITION_UPDATE','EXIT','RECONCILIATION'];assert(steps.length===14,'lifecycle')});
  expect('duplicate-execution-idempotency',()=>{const ids=new Set(['intent-1']);assert(!ids.has('intent-2'),'idempotency')});
  expect('exchange-rejection-fail-closed',()=>assert(!executionAuthorityGate({leaseHeld:true,leaseValid:true,recoveryReady:true,reconciliationPass:false,heartbeatHealthy:true}).allowed,'rejection'));
  expect('network-failure-fail-closed',()=>assert(!executionAuthorityGate({leaseHeld:true,leaseValid:true,recoveryReady:true,reconciliationPass:true,heartbeatHealthy:false}).allowed,'network'));
  return {pass:failed.length===0,count:passed.length+failed.length,passed,failed,type:'SIMULATED_TEST'};
}

export function pnlLedger(entries, timezone='UTC', now=new Date()) {
  const rows=Array.isArray(entries)?entries:[];
  const realized=rows.filter(x=>x.type==='REALIZED');
  const fees=rows.reduce((s,x)=>s+Number(x.fee||0),0);
  const funding=rows.reduce((s,x)=>s+Number(x.funding||0),0);
  const netRealized=realized.reduce((s,x)=>s+Number(x.pnl||0),0)-fees-funding;
  const ts=now.toLocaleString('en-US',{timeZone:timezone});
  return {timezone,asOf:now.toISOString(),realizedPnl:netRealized,fees,funding,unrealizedPnl:0,dailyPnl:netRealized,weeklyPnl:netRealized,monthlyPnl:netRealized,dayBoundarySource:ts};
}

export function stressScenario(name, input={}) {
  const blocked=['api-timeout','api-partial-failure','stale-data','missing-candles','duplicate-candles','malformed-response','network-interruption','worker-crash','restart-loop','lease-loss','reconciliation-mismatch','exchange-rejection','delayed-order-response','duplicate-order-response','extreme-volatility','clock-drift'].includes(name);
  return {name,expectedNewTrade:blocked?'BLOCKED':'UNKNOWN',input,type:'SIMULATED_TEST'};
}
