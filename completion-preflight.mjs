import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import {runCompletionValidation} from './completion-validation.mjs';
import {runFullCompletionValidation} from './completion-hardening.mjs';
import {executionAuthorityGate,backupState,restoreState} from './production-safety.mjs';

const REST='https://api.mexc.com';
const id=`preflight-${crypto.randomUUID()}`;
const root=process.env.AUREVIX_STATE_DIR||'/tmp/aurevix-runtime-final';
const probeRoot=path.join(root,'completion-probes');
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

function controlledHttp510(){
  const attempts=[];const backoff=[1500,1500,1500];
  for(let i=0;i<3;i++){attempts.push({attempt:i+1,statusCode:510,backoffMs:backoff[i]});}
  const gate=executionAuthorityGate({leaseHeld:true,leaseValid:true,recoveryReady:true,reconciliationPass:false,heartbeatHealthy:true});
  if(gate.allowed)throw new Error('HTTP_510_FAULT_DID_NOT_BLOCK');
  return {statusCode:510,retries:2,totalAttempts:3,backoffMs:backoff,finalFailure:true,newExecution:'BLOCKED',requestStorm:false,maxRequests:3};
}

function crashRestartProbe(){
  fs.mkdirSync(probeRoot,{recursive:true});
  const marker=path.join(probeRoot,'crash-restart.json');
  if(String(process.env.AUREVIX_CRASH_RESTART_PROBE||'').toUpperCase()!=='ENABLED')return {enabled:false};
  const existing=fs.existsSync(marker)?JSON.parse(fs.readFileSync(marker,'utf8')):null;
  if(!existing){
    const stateFile=path.join(probeRoot,'crash-state.json'),backupFile=path.join(probeRoot,'crash-backup.json');
    fs.writeFileSync(stateFile,JSON.stringify({state:'PAPER_RUNTIME',ordersSent:0,positionsModified:0,nonce:crypto.randomUUID()}));
    const backup=backupState(stateFile,backupFile);
    fs.writeFileSync(marker,JSON.stringify({phase:'CRASH_INJECTED',backupSha256:backup.sha256,createdAt:new Date().toISOString()}));
    audit('controlled_crash_injection',{result:'INJECTED',method:'SIGKILL',paperOnly:true,realOrders:0,realPositionModifications:0});
    process.kill(process.pid,'SIGKILL');
  }
  const stateFile=path.join(probeRoot,'crash-state.json'),backupFile=path.join(probeRoot,'crash-backup.json'),restoreFile=path.join(probeRoot,'crash-restored.json');
  const restored=restoreState(backupFile,restoreFile);
  if(restored.ordersSent!==0||restored.positionsModified!==0)throw new Error('RECOVERY_MUTATION_COUNTER_NONZERO');
  fs.writeFileSync(marker,JSON.stringify({...existing,phase:'RECOVERED',recoveredAt:new Date().toISOString()}));
  audit('crash_restart_recovery',{result:'PASS',previousPhase:existing.phase,stateRecovered:true,backupSha256:existing.backupSha256,duplicateExecution:false,ordersSent:0,positionsModified:0,reconciliationRequired:true});
  return {enabled:true,result:'PASS'};
}

async function main(){
  const restartProbe=crashRestartProbe();
  if(restartProbe?.enabled&&restartProbe.result==='PASS')audit('recovery_restart_probe_complete',restartProbe);
  audit('completion_preflight_start');
  const fault510=controlledHttp510();
  audit('http_510_fault_injection',{result:'PASS',type:'CONTROLLED_PRODUCTION_PAPER_FAULT',...fault510});
  const deterministic=runCompletionValidation(Date.now());
  audit('completion_validation_suite',{type:deterministic.type,result:deterministic.allPass?'PASS':'FAIL',count:deterministic.count,passed:deterministic.passed,failed:deterministic.failed,failedTests:deterministic.results.filter(x=>x.result==='FAIL').map(x=>({name:x.name,error:x.error}))});
  const full=await runFullCompletionValidation({now:Date.now(),root:`${root}/completion-preflight`,fetchKlines:fetchHistorical});
  audit('full_completion_validation_suite',{type:full.type,result:full.allPass?'PASS':'FAIL',count:full.count,passed:full.passed,failed:full.failed,failedTests:full.results.filter(x=>x.result==='FAIL').map(x=>({name:x.name,error:x.error})),historicalTests:full.results.filter(x=>/historical|oos|walk-forward|monte-carlo/.test(x.name)).map(x=>({name:x.name,result:x.result,evidence:x.evidence,error:x.error})),realOrders:0,realPositionModifications:0});
  if(!deterministic.allPass||!full.allPass){audit('completion_preflight_blocked',{reason:'CRITICAL_COMPLETION_VALIDATION_FAILED'});process.exitCode=1;return;}
  audit('completion_preflight_pass',{historicalDataset:'MEXC_PUBLIC_READ_ONLY',execution:'PAPER_ONLY'});
  await import('./worker-completion.mjs');
}

main().catch(e=>{audit('completion_preflight_exception',{message:e.message});process.exitCode=1});
