import WebSocket from 'ws';
import http from 'node:http';

const BINANCE_WS='wss://stream.binance.com:9443/stream';
const BINANCE_API='https://api.binance.com/api/v3';
const TELEGRAM_TOKEN=process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID=process.env.TELEGRAM_CHAT_ID || '';
const PORT=Number(process.env.PORT||8787);
const COOLDOWN_MS=Number(process.env.ALERT_COOLDOWN_MS||300000);
const MIN_ALERT_SCORE=Number(process.env.MIN_ALERT_SCORE||78);
const MAX_SYMBOLS=Number(process.env.MAX_SYMBOLS||250);

if(!TELEGRAM_TOKEN) console.warn('[V5] TELEGRAM_BOT_TOKEN is not set. Telegram alerts are disabled.');

const state=new Map();
const alerts=new Map();
const subscriptions=new Set();
const sockets=[];
const INTERVALS=['1s','1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M'];
const STREAMS_PER_CONNECTION=1000;
let reconnectTimer=null;
let lastMarketLoad=0;
let connectedAt=null;
let messageCount=0;
let lastEventAt=null;

const now=()=>Date.now();
const n=v=>Number(v||0);
const clamp=(v,a=0,b=100)=>Math.max(a,Math.min(b,v));
const pct=(a,b)=>b?((a-b)/b)*100:0;

async function fetchJson(url){
  const r=await fetch(url,{headers:{'User-Agent':'CryptoRadarAI-V5'}});
  if(!r.ok) throw new Error('HTTP '+r.status);
  return r.json();
}

async function getSymbols(){
  const data=await fetchJson(BINANCE_API+'/exchangeInfo');
  return data.symbols.filter(s=>s.status==='TRADING'&&s.quoteAsset==='USDT'&&s.isSpotTradingAllowed).map(s=>s.symbol.toLowerCase());
}

async function telegram(method,body){
  if(!TELEGRAM_TOKEN) return null;
  const r=await fetch('https://api.telegram.org/bot'+TELEGRAM_TOKEN+'/'+method,{
    method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)
  });
  const j=await r.json();
  if(!j.ok) throw new Error(j.description||'Telegram API error');
  return j.result;
}

async function discoverChat(){
  if(TELEGRAM_CHAT_ID) return TELEGRAM_CHAT_ID;
  if(!TELEGRAM_TOKEN) return '';
  try{
    const u=await telegram('getUpdates',{timeout:0,allowed_updates:['message']});
    const msg=[...u].reverse().find(x=>x.message?.chat?.id);
    return msg?.message?.chat?.id||'';
  }catch(e){console.error('[Telegram]',e.message);return ''}
}

async function sendAlert(a){
  const chat=TELEGRAM_CHAT_ID||await discoverChat();
  if(!chat) return false;
  const icon=a.side==='BUY PRESSURE'?'🟢':a.side==='SELL PRESSURE'?'🔴':'🟡';
  const text=[
    icon+' <b>CRYPTO RADAR AI — '+a.side+'</b>',
    '',
    '<b>'+a.symbol+'</b>  $'+a.price,
    'Anomaly score: <b>'+a.score+'/100</b>',
    'Timeframes: '+a.timeframes.join(', '),
    'Price move: '+a.priceMove.toFixed(2)+'%',
    'Buy/Sell notional: '+a.buyRatio.toFixed(1)+'% / '+(100-a.buyRatio).toFixed(1)+'%',
    'Trade acceleration: '+a.tradeAccel.toFixed(1)+'x',
    'Order-book imbalance: '+a.bookImbalance.toFixed(1)+'%',
    'Liquidity change: '+a.liquidityChange.toFixed(1)+'%',
    '',
    'State: '+a.nextState,
    'Rule-based anomaly signal. Not a guaranteed prediction or personalized financial advice.'
  ].join('\n');
  try{
    await telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML',disable_web_page_preview:true});
    return true;
  }catch(e){console.error('[Telegram]',e.message);return false}
}

function ingestTrade(symbol,p,q,isBuyerMaker){
  const s=state.get(symbol)||{symbol,ticks:[],bookImbalance:0,liquidityChange:0,lastPrice:0,prevDepth:0};
  const notional=q*p;
  s.lastPrice=p;
  s.ticks.push({t:now(),p,q:notional,buy:isBuyerMaker?0:notional,sell:isBuyerMaker?notional:0});
  if(s.ticks.length>5000)s.ticks.splice(0,s.ticks.length-5000);
  state.set(symbol,s);
}

function ingestBook(symbol,bidQty,askQty,bidPrice,askPrice){
  const s=state.get(symbol)||{symbol,ticks:[],bookImbalance:0,liquidityChange:0,lastPrice:0,prevDepth:0};
  const total=bidQty+askQty;
  s.bookImbalance=total?(bidQty-askQty)/total*100:0;
  s.liquidityChange=s.prevDepth?pct(total,s.prevDepth):0;
  s.prevDepth=total;
  s.spread=bidPrice?((askPrice-bidPrice)/bidPrice)*100:0;
  state.set(symbol,s);
}

function analyze(s){
  const windows=[['1m',60000],['3m',180000],['5m',300000],['15m',900000],['1h',3600000]];
  const timeframes=[];
  let directionalScore=0,buy=0,sell=0,trades=0,oldTrades=0,oldVol=0,newVol=0;
  for(const [name,ms] of windows){
    const a=s.ticks.filter(x=>now()-x.t<=ms);
    const b=s.ticks.filter(x=>now()-x.t>ms&&now()-x.t<=ms*2);
    if(a.length<2||b.length<2) continue;
    const av=a.reduce((x,y)=>x+y.q,0),bv=b.reduce((x,y)=>x+y.q,0);
    const ap=pct(a[a.length-1].p,a[0].p);
    const tradeAccel=b.length?a.length/b.length:1;
    const volumeAccel=bv?av/bv:1;
    const buyQ=a.reduce((x,y)=>x+y.buy,0),sellQ=a.reduce((x,y)=>x+y.sell,0);
    const ratio=buyQ+sellQ?buyQ/(buyQ+sellQ)*100:50;
    let local=0;
    if(Math.abs(ap)>=0.5)local+=12;
    if(volumeAccel>=1.8)local+=16;
    if(tradeAccel>=1.7)local+=14;
    if(ratio>=62||ratio<=38)local+=18;
    if(Math.abs(s.bookImbalance)>=12)local+=14;
    if(local>=28)timeframes.push(name);
    directionalScore+=local*(ratio>=50?1:-1);
    buy+=buyQ;sell+=sellQ;trades+=a.length;oldTrades+=b.length;newVol+=av;oldVol+=bv;
  }
  const buyRatio=buy+sell?buy/(buy+sell)*100:50;
  const score=Math.round(clamp(50+directionalScore/2));
  const side=score>=MIN_ALERT_SCORE?'BUY PRESSURE':score<=100-MIN_ALERT_SCORE?'SELL PRESSURE':'WATCH';
  return {score,side,timeframes:timeframes.length?timeframes:['insufficient'],buyRatio,tradeAccel:oldTrades?trades/oldTrades:1,volumeAccel:oldVol?newVol/oldVol:1,nextState:score>=88||score<=12?'CONFIRMED_MOVE':score>=78||score<=22?'EARLY_MOVE':'WATCH'};
}

function shouldAlert(symbol,a){
  if(a.score<MIN_ALERT_SCORE&&a.score>100-MIN_ALERT_SCORE)return false;
  const key=symbol+':'+a.side;
  const last=alerts.get(key)||0;
  if(now()-last<COOLDOWN_MS)return false;
  alerts.set(key,now());
  return true;
}

function closeSockets(){while(sockets.length){const x=sockets.pop();try{x.close()}catch{}}}
function connect(symbols){
  closeSockets();
  const streamsPerSymbol=2+INTERVALS.length;
  const symbolsPerSocket=Math.max(1,Math.floor(STREAMS_PER_CONNECTION/streamsPerSymbol));
  for(let i=0;i<symbols.length;i+=symbolsPerSocket){
    const batch=symbols.slice(i,i+symbolsPerSocket);
    const streams=[];
    for(const s of batch){
      streams.push(s+'@aggTrade',s+'@bookTicker',s+'@depth@100ms');
      for(const tf of INTERVALS) streams.push(s+'@kline_'+tf);
    }
    const socket=new WebSocket(BINANCE_WS+'?streams='+streams.join('/'));
    sockets.push(socket);
    socket.on('open',()=>{connectedAt=new Date().toISOString();console.log('[V5] WebSocket connected:',batch.length,'symbols',streams.length,'streams')});
    socket.on('message',raw=>{
      messageCount++;lastEventAt=new Date().toISOString();
      try{
        const parsed=JSON.parse(raw.toString());
        const m=parsed.data||parsed;
        const e=m.e,s=m.s?.toLowerCase();
        if(!s)return;
        if(e==='aggTrade'){
          const price=n(m.p),qty=n(m.q);
          ingestTrade(s,price,qty,m.m);
          const st=state.get(s);
          if(st.ticks.length%25===0){
            const a=analyze(st);
            if(shouldAlert(s,a)) sendAlert({symbol:s.toUpperCase(),price:price.toLocaleString(undefined,{maximumFractionDigits:10}),priceMove:pct(price,st.ticks[0]?.p||price),bookImbalance:st.bookImbalance,liquidityChange:st.liquidityChange,...a});
          }
        }else if(e==='bookTicker'){
          ingestBook(s,n(m.B),n(m.A),n(m.b),n(m.a));
        }else if(e==='depthUpdate'){
          const bidQty=m.b?.reduce((a,x)=>a+n(x[1]),0)||0;
          const askQty=m.a?.reduce((a,x)=>a+n(x[1]),0)||0;
          const st=state.get(s)||{symbol:s,ticks:[],bookImbalance:0,liquidityChange:0,lastPrice:0,prevDepth:0};
          st.depthEvents=(st.depthEvents||0)+1;
          st.depthBidUpdates=bidQty;st.depthAskUpdates=askQty;state.set(s,st);
        }else if(e==='kline'){
          const k=m.k;
          const st=state.get(s)||{symbol:s,ticks:[],bookImbalance:0,liquidityChange:0,lastPrice:n(k?.c)};
          st.klines=st.klines||{};
          const tf=k.i;
          st.klines[tf]={time:k.t,open:n(k.o),high:n(k.h),low:n(k.l),close:n(k.c),volume:n(k.v),quoteVolume:n(k.q),trades:n(k.n),takerBuyQuote:n(k.Q),closed:k.x};
          st.lastPrice=n(k.c);state.set(s,st);
        }
      }catch{}
    });
    socket.on('close',()=>{console.warn('[V5] WebSocket batch closed; reconnecting');scheduleReconnect(symbols)});
    socket.on('error',e=>console.error('[V5] WebSocket error',e.message));
  }
}

function scheduleReconnect(symbols){
  if(reconnectTimer)return;
  reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect(symbols)},5000);
}

async function refresh(){
  try{
    const symbols=await getSymbols();
    const chosen=symbols.slice(0,MAX_SYMBOLS);
    subscriptions.clear();chosen.forEach(x=>subscriptions.add(x));
    lastMarketLoad=now();
    connect(chosen);
    console.log('[V5] Monitoring',chosen.length,'USDT spot markets');
  }catch(e){console.error('[V5] Market refresh failed',e.message);scheduleReconnect([...subscriptions])}
}

setInterval(()=>{
  for(const [sym,s] of state){
    if(!subscriptions.has(sym)) state.delete(sym);
    else if(s.ticks.length>100){
      const a=analyze(s);
      if(a.side!=='WATCH') console.log('[V5]',sym,a.side,a.score,a.timeframes.join(','));
    }
  }
},15000);

setInterval(refresh,30*60*1000);
refresh();

http.createServer((req,res)=>{
  res.setHeader('content-type','application/json');
  res.setHeader('access-control-allow-origin','*');
  if(req.url==='/health')return res.end(JSON.stringify({ok:true,service:'Crypto Radar AI V5 scanner',connected:sockets.length>0,connectedAt,lastEventAt,markets:subscriptions.size,messages:messageCount,alerts:alerts.size,lastMarketLoad}));
  if(req.url==='/alerts')return res.end(JSON.stringify([...alerts.entries()].map(([key,time])=>({key,time}))));
  res.statusCode=404;res.end(JSON.stringify({error:'not found'}));
}).listen(PORT,()=>console.log('[V5] Health server on '+PORT));