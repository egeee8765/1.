import http from 'node:http';

const PORT = Number(process.env.PORT || 10000);
const REST = 'https://contract.mexc.com';
const TIMEFRAMES = [
  { key: '4H', interval: 'Hour4', ms: 4 * 60 * 60 * 1000, count: 160 },
  { key: '1H', interval: 'Min60', ms: 60 * 60 * 1000, count: 220 },
  { key: '15M', interval: 'Min15', ms: 15 * 60 * 1000, count: 260 },
  { key: '5M', interval: 'Min5', ms: 5 * 60 * 1000, count: 320 }
];
const DEFAULT_SYMBOLS = ['BTC_USDT', 'ETH_USDT', 'SOL_USDT', 'AVAX_USDT', 'BNB_USDT'];
const cache = new Map();

function num(x) { const n = Number(x); return Number.isFinite(n) ? n : null; }
function mean(a) { return a.length ? a.reduce((x,y)=>x+y,0)/a.length : null; }
function ema(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let e = mean(values.slice(0, period));
  for (let i = period; i < values.length; i++) e = values[i] * k + e * (1-k);
  return e;
}
function rsi(values, period=14) {
  if (values.length <= period) return null;
  let gain=0, loss=0;
  for (let i=1;i<=period;i++) { const d=values[i]-values[i-1]; if(d>=0) gain+=d; else loss-=d; }
  gain/=period; loss/=period;
  for(let i=period+1;i<values.length;i++){ const d=values[i]-values[i-1]; gain=(gain*(period-1)+Math.max(d,0))/period; loss=(loss*(period-1)+Math.max(-d,0))/period; }
  if(loss===0) return 100;
  return 100 - 100/(1+gain/loss);
}
function atr(c, period=14) {
  if(c.length<=period) return null;
  const tr=[];
  for(let i=1;i<c.length;i++) tr.push(Math.max(c[i].high-c[i].low,Math.abs(c[i].high-c[i-1].close),Math.abs(c[i].low-c[i-1].close)));
  return mean(tr.slice(-period));
}
function macd(values) { const a=ema(values,12), b=ema(values,26); return a===null||b===null?null:a-b; }
function vwap(c) { const pv=c.reduce((s,x)=>s+x.close*x.volume,0), v=c.reduce((s,x)=>s+x.volume,0); return v?pv/v:null; }
function structure(c) {
  const n=c.length, w=c.slice(-Math.min(20,n));
  const highs=w.map(x=>x.high), lows=w.map(x=>x.low);
  return { high:Math.max(...highs), low:Math.min(...lows), close:w.at(-1).close };
}
function timeframeScore(c) {
  const closes=c.map(x=>x.close), last=closes.at(-1), e20=ema(closes,20), e50=ema(closes,50), r=rsi(closes), m=macd(closes), v=vwap(c), a=atr(c), s=structure(c);
  let score=0;
  if(last>e20) score+=18; else score-=18;
  if(e20>e50) score+=18; else score-=18;
  if(r>=55) score+=14; else if(r<=45) score-=14;
  if(m>0) score+=12; else score-=12;
  if(v!==null) score += last>v?10:-10;
  const momentum=(last-closes[Math.max(0,closes.length-6)])/closes[Math.max(0,closes.length-6)];
  if(momentum>0.003) score+=10; else if(momentum<-0.003) score-=10;
  const normalized=Math.max(-100,Math.min(100,score));
  return { score:normalized, bias:normalized>=20?'LONG':normalized<=-20?'SHORT':'NEUTRAL', close:last, ema20:e20, ema50:e50, rsi:r, macd:m, vwap:v, atr:a, support:s.low, resistance:s.high, momentumPct:momentum*100 };
}
async function fetchKlines(symbol, tf) {
  const end=Math.floor(Date.now()/1000)-5;
  const start=end-Math.ceil(tf.count*tf.ms/1000);
  const url=`${REST}/api/v1/contract/kline/${symbol}?interval=${tf.interval}&start=${start}&end=${end}`;
  const r=await fetch(url,{headers:{Accept:'application/json'},signal:AbortSignal.timeout(12000)});
  if(!r.ok) throw new Error(`MEXC_KLINE_HTTP_${r.status}`);
  const j=await r.json();
  if(j?.success===false) throw new Error(`MEXC_KLINE_API_${j.code??'UNKNOWN'}`);
  const d=j?.data||j;
  const rows=Array.from({length:d?.time?.length||0},(_,i)=>({time:Number(d.time[i])*1000,open:Number(d.open[i]),high:Number(d.high[i]),low:Number(d.low[i]),close:Number(d.close[i]),volume:Number(d.vol[i])})).filter(x=>Object.values(x).every(Number.isFinite));
  return rows.filter(x=>x.time+tf.ms<=Date.now()-5000).sort((a,b)=>a.time-b.time);
}
async function fetchTicker(symbol){
  const r=await fetch(`${REST}/api/v1/contract/ticker?symbol=${symbol}`,{headers:{Accept:'application/json'},signal:AbortSignal.timeout(10000)});
  if(!r.ok) throw new Error(`MEXC_TICKER_HTTP_${r.status}`);
  const j=await r.json(); return j?.data||j;
}
function classify(scores) {
  const s4=scores['4H'].score, s1=scores['1H'].score, s15=scores['15M'].score, s5=scores['5M'].score;
  const alignment=(Math.abs(s4)>=20?25:0)+(Math.abs(s1)>=20?25:0)+(Math.abs(s15)>=20?25:0)+(Math.abs(s5)>=20?25:0);
  const signs=[s4,s1,s15,s5].map(x=>x>=20?1:x<=-20?-1:0);
  const nonzero=signs.filter(Boolean);
  const direction=nonzero.length===4&&new Set(nonzero).size===1?(nonzero[0]===1?'LONG':'SHORT'):'WAIT';
  const raw=(s4*0.35+s1*0.30+s15*0.20+s5*0.15);
  const confidence=Math.round(Math.max(0,Math.min(100,50+Math.abs(raw)*0.5 + (alignment-50)*0.2)));
  let signal='WAIT';
  if(direction!=='WAIT' && confidence>=70 && alignment===100) signal=direction;
  const atrPct=scores['15M'].atr && scores['15M'].close ? scores['15M'].atr/scores['15M'].close*100 : 0;
  const regime=atrPct>2.5?'HIGH_VOLATILITY':atrPct<0.35?'LOW_VOLATILITY':Math.abs(raw)>45?'TRENDING':'RANGING';
  if(regime==='HIGH_VOLATILITY' || regime==='LOW_VOLATILITY') signal='WAIT';
  return { signal, confidence, alignment, rawScore:Math.round(raw), regime, timeframeConflict:direction==='WAIT' };
}
async function analyze(symbol) {
  const scores={};
  for(const tf of TIMEFRAMES) scores[tf.key]=timeframeScore(await fetchKlines(symbol,tf));
  const ticker=await fetchTicker(symbol);
  const fusion=classify(scores);
  const avgRsi=mean(Object.values(scores).map(x=>x.rsi).filter(Number.isFinite));
  const s15=scores['15M'];
  return { symbol, generatedAt:new Date().toISOString(), signal:fusion.signal, confidence:fusion.confidence, marketRegime:fusion.regime, trendAlignmentPct:fusion.alignment, timeframeConflict:fusion.timeframeConflict, price:num(ticker?.lastPrice), fundingRate:num(ticker?.fundingRate), change24hPct:num(ticker?.riseFallRate)*100, volume24:num(ticker?.volume24), support:s15.support, resistance:s15.resistance, rsi14:avgRsi, timeframes:scores, execution:{enabled:false, ordersSent:0}, methodology:'4H → 1H → 15M → 5M; multi-factor fusion; no automatic execution' };
}
async function getSignal(symbol){
  const key=symbol.toUpperCase();
  const cached=cache.get(key);
  if(cached && Date.now()-cached.ts<15000) return cached.data;
  const data=await analyze(key); cache.set(key,{ts:Date.now(),data}); return data;
}
const server=http.createServer(async(req,res)=>{
  res.setHeader('content-type','application/json; charset=utf-8');
  res.setHeader('cache-control','no-store');
  try {
    if(req.url==='/health'||req.url==='/'){res.writeHead(200);return res.end(JSON.stringify({service:'AUREVIX Signal Bot',status:'READY',automaticTrading:false,ordersSent:0,positionModifications:0,exchange:'MEXC Futures',timeframes:['4H','1H','15M','5M']}));}
    if(req.url==='/signals'){const symbols=(process.env.SYMBOLS||DEFAULT_SYMBOLS.join(',')).split(',').map(x=>x.trim()).filter(Boolean);const results=[];for(const s of symbols){try{results.push(await getSignal(s));}catch(e){results.push({symbol:s,signal:'WAIT',error:e.message});}}res.writeHead(200);return res.end(JSON.stringify({generatedAt:new Date().toISOString(),automaticTrading:false,signals:results},null,2));}
    if(req.url.startsWith('/signal')){const u=new URL(req.url,'http://localhost');const symbol=(u.searchParams.get('symbol')||'BTC_USDT').toUpperCase();const data=await getSignal(symbol);res.writeHead(200);return res.end(JSON.stringify(data,null,2));}
    res.writeHead(404);res.end(JSON.stringify({error:'NOT_FOUND'}));
  } catch(e){res.writeHead(503);res.end(JSON.stringify({signal:'WAIT',error:e.message,automaticTrading:false}));}
});
server.listen(PORT,()=>console.log(JSON.stringify({event:'signal_bot_ready',service:'AUREVIX Signal Bot',port:PORT,automaticTrading:false,ordersSent:0,positionModifications:0}))); 
