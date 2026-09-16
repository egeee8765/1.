import crypto from 'node:crypto';
import {runCompletionValidation} from './completion-validation.mjs';
import {runFullCompletionValidation} from './completion-hardening.mjs';

const REST='https://api.mexc.com';
const id=`preflight-${crypto.randomUUID()}`;
const root=process.env.AUREVIX_STATE_DIR||'/tmp/aurevix-runtime-final';
const audit=(event,x={})=>console.log(JSON.stringify({event,preflightId:id,at:new Date().toISOString(),liveEnabled:false,ordersSent:0,positionsModified:0,realSlTpModifications:0,canaryStarted:false,...x}));

async function fetchHistorical(symbol='BTC_USDT',interval='Hour4',count=400){
  const end=Math.floor(Date.now()/1000),start=end-(count*4*3600+4*3600);
  const url=`${REST}/api/v1/contract/kline/${symbol}?interval=${encodeURIComponent(interval)}&start=${start}&end=${end}`;
  const r=await fetch(url,{headers:{Accept:'application/json'},signal:AbortSignal.timeout(15000)});
  if(!r.ok) throw new Error(`HISTORICAL_HTTP_${r.status}`);
  const j=await r.json();
  if(j?.success===false) throw new Error(`HISTORICAL_API_${j.code??'UNKNOWN'}`);
  const d=j?.data||j;
  const rows=Array.from({length:d?.time?.length||0},(_,i)=>({time:Number(d.time[i])*1000,open:Number(d.open[i]),high:Number(d.high[i]),low:Number(d.low[i]),close:Number(d.close[i]),volume:Number(d.vol[i])}));
  return rows.filter(x=>Number.isFinite(x.close)&&x.time+4*3600*1000<=Date.now()-5000).sort((a,b)=>a.time-b.time);
}

async function main(){
  audit('completion_preflight_start');
  const deterministic=runCompletionValidation(Date.now());
  audit('completion_validation_suite',{type:deterministic.type,result:deterministic.allPass?'PASS':'FAIL',count:deterministic.count,passed:deterministic.passed,failed:deterministic.failed});
  const full=await runFullCompletionValidation({now:Date.now(),root:`${root}/completion-preflight`,fetchKlines:fetchHistorical});
  audit('full_completion_validation_suite',{type:full.type,result:full.allPass?'PASS':'FAIL',count:full.count,passed:full.passed,failed:full.failed,realOrders:0,realPositionModifications:0});
  if(!deterministic.allPass||!full.allPass){audit('completion_preflight_blocked',{reason:'CRITICAL_COMPLETION_VALIDATION_FAILED'});process.exitCode=1;return;}
  audit('completion_preflight_pass',{historicalDataset:'MEXC_PUBLIC_READ_ONLY',execution:'PAPER_ONLY'});
  await import('./worker-completion.mjs');
}

main().catch(e=>{audit('completion_preflight_exception',{message:e.message});process.exitCode=1});
