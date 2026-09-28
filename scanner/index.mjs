import WebSocket from 'ws';
import http from 'node:http';

const BINANCE_WS='wss://stream.binance.com:9443/stream';
const BINANCE_API='https://api.binance.com/api/v3';
const TELEGRAM_TOKEN=process.env.TELEGRAM_BOT_TOKEN||'';
const TELEGRAM_CHAT_ID=process.env.TELEGRAM_CHAT_ID||'';
const PORT=Number(process.env.PORT||8787);
const MAX_SYMBOLS=Number(process.env.MAX_SYMBOLS||500);
const MAX_PRICE=Number(process.env.MAX_PRICE||2);
const NEW_COIN_DAYS=Number(process.env.NEW_COIN_DAYS||30);
const MIN_REACTION_SCORE=Number(process.env.MIN_REACTION_SCORE||72);
const NEWS_POLL_MS=Number(process.env.NEWS_POLL_MS||30000);
const ALERT_COOLDOWN_MS=Number(process.env.ALERT_COOLDOWN_MS||900000);
const NEWS_LOOKBACK_HOURS=Number(process.env.NEWS_LOOKBACK_HOURS||2);
const MAX_NEWS_CANDIDATES=Number(process.env.MAX_NEWS_CANDIDATES||40);

const state=new Map();
const symbolMeta=new Map();
const newsSeen=new Map();
const pendingNews=new Map();
const alertSeen=new Map();
const sockets=[];
let reconnectTimer=null;
let connectedAt=null;
let lastEventAt=null;
let messageCount=0;
let lastMarketLoad=0;
let lastNewsPoll=0;
let newsInvestigations=0;
let newsReactionAlerts=0;

const INTERVALS=['1m','5m','15m','1h','4h'];
const STREAMS_PER_CONNECTION=1000;
const now=()=>Date.now();
const n=v=>Number(v||0);
const clamp=(v,a=0,b=100)=>Math.max(a,Math.min(b,v));

async function fetchText(url,headers={}){
  const r=await fetch(url,{headers:{'User-Agent':'CryptoRadarAI-V6/1.0',...headers}});
  if(!r.ok)throw new Error('HTTP '+r.status);
  return r.text();
}
async function fetchJson(url){
  const r=await fetch(url,{headers:{'User-Agent':'CryptoRadarAI-V6/1.0'}});
  if(!r.ok)throw new Error('HTTP '+r.status);
  return r.json();
}
function escapeHtml(s=''){
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

async function getSymbols(){
  const [info,tickers]=await Promise.all([
    fetchJson(BINANCE_API+'/exchangeInfo'),
    fetchJson(BINANCE_API+'/ticker/price')
  ]);
  const prices=new Map(tickers.map(x=>[x.symbol,n(x.price)]));
  const cutoff=now()-NEW_COIN_DAYS*86400000;
  const rows=info.symbols.filter(s=>s.status==='TRADING'&&s.quoteAsset==='USDT'&&s.isSpotTradingAllowed);
  for(const s of rows){
    const price=prices.get(s.symbol)||0;
    const onboard=n(s.onboardDate||s.listingTime);
    if(price>0)symbolMeta.set(s.symbol.toLowerCase(),{
      symbol:s.symbol,baseAsset:s.baseAsset,price,onboardDate:onboard,
      newCoin:!!onboard&&onboard>=cutoff
    });
  }
  return rows
    .filter(s=>{
      const p=prices.get(s.symbol)||0;
      return p>0&&p<=MAX_PRICE;
    })
    .sort((a,b)=>{
      const ma=symbolMeta.get(a.symbol.toLowerCase()),mb=symbolMeta.get(b.symbol.toLowerCase());
      return Number(mb?.newCoin)-Number(ma?.newCoin);
    })
    .slice(0,MAX_SYMBOLS)
    .map(s=>s.symbol.toLowerCase());
}

function getState(symbol){
  let s=state.get(symbol);
  if(!s){
    s={symbol,ticks:[],bookImbalance:0,liquidityChange:0,prevDepth:0,klines:{},lastPrice:0};
    state.set(symbol,s);
  }
  return s;
}
function ingestTrade(symbol,p,q,isBuyerMaker){
  const s=getState(symbol);
  const notional=p*q;
  s.lastPrice=p;
  s.ticks.push({t:now(),p,q:notional,buy:isBuyerMaker?0:notional,sell:isBuyerMaker?notional:0});
  const cutoff=now()-6*3600000;
  while(s.ticks.length&&s.ticks[0].t<cutoff)s.ticks.shift();
}
function ingestBook(symbol,bidQty,askQty,bidPrice,askPrice){
  const s=getState(symbol);
  const total=bidQty+askQty;
  s.bookImbalance=total?(bidQty-askQty)/total*100:0;
  s.liquidityChange=s.prevDepth?pct(total,s.prevDepth):0;
  s.prevDepth=total;
  s.spread=bidPrice?((askPrice-bidPrice)/bidPrice)*100:0;
}
function ingestKline(symbol,k){
  const s=getState(symbol);
  const tf=k.i;
  s.klines[tf]=s.klines[tf]||[];
  const row={time:n(k.t),open:n(k.o),high:n(k.h),low:n(k.l),close:n(k.c),volume:n(k.v),quoteVolume:n(k.q),trades:n(k.n),takerBuyQuote:n(k.Q),closed:!!k.x};
  const arr=s.klines[tf];
  const ix=arr.findIndex(x=>x.time===row.time);
  if(ix>=0)arr[ix]=row;else arr.push(row);
  const cutoff=now()-24*3600000;
  while(arr.length&&arr[0].time<cutoff)arr.shift();
  s.lastPrice=row.close;
}
function pct(a,b){return b?((a-b)/b)*100:0;}

async function telegram(method,body){
  if(!TELEGRAM_TOKEN)return null;
  const r=await fetch('https://api.telegram.org/bot'+TELEGRAM_TOKEN+'/'+method,{
    method:'POST',headers:{'content-type':'application/json'},
    body:JSON.stringify(body)
  });
  const j=await r.json();
  if(!j.ok)throw new Error(j.description||'Telegram API error');
  return j.result;
}
async function discoverChat(){
  if(TELEGRAM_CHAT_ID)return TELEGRAM_CHAT_ID;
  if(!TELEGRAM_TOKEN)return '';
  try{
    const u=await telegram('getUpdates',{timeout:0,allowed_updates:['message']});
    return [...u].reverse().find(x=>x.message?.chat?.id)?.message?.chat?.id||'';
  }catch(e){console.error('[Telegram]',e.message);return ''}
}

function parseXmlItems(xml){
  const items=xml.match(/<item[\s\S]*?<\/item>/gi)||[];
  return items.map(x=>{
    const title=(x.match(/<title>([\s\S]*?)<\/title>/i)||[])[1]||'';
    const link=(x.match(/<link>([\s\S]*?)<\/link>/i)||[])[1]||'';
    const pub=(x.match(/<pubDate>([\s\S]*?)<\/pubDate>/i)||[])[1]||'';
    const source=(x.match(/<source[^>]*>([\s\S]*?)<\/source>/i)||[])[1]||'';
    return {title:decodeXml(title.replace(/<!\[CDATA\[|\]\]>/g,'')),link,source:decodeXml(source),pubDate:pub?Date.parse(pub):0};
  }).filter(x=>x.title&&x.pubDate);
}
function decodeXml(s){
  return s.replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>');
}
function normalizeTitle(t){
  return t.toLowerCase().replace(/[^a-z0-9 ]/g,' ').replace(/\s+/g,' ').trim();
}
function sourceDomain(link){
  try{return new URL(link).hostname.replace(/^www\./,'')}catch{return ''}
}
function eventType(title){
  const t=title.toLowerCase();
  if(/hack|exploit|breach|stolen|drain|attack/.test(t))return 'SECURITY';
  if(/listing|listed|launch|airdrop|mainnet|testnet/.test(t))return 'LISTING/LAUNCH';
  if(/partnership|partner|integrat|collab|deal/.test(t))return 'PARTNERSHIP';
  if(/upgrade|fork|release|network/.test(t))return 'NETWORK';
  if(/etf|approval|sec|regulat|legal|lawsuit/.test(t))return 'REGULATION';
  if(/funding|investment|acquire|acquisition|treasury/.test(t))return 'CAPITAL';
  if(/token burn|burn|unlock|emission|supply/.test(t))return 'TOKENOMICS';
  return 'MARKET/PROJECT';
}

async function searchNews(baseAsset,headline=''){
  const q=headline
    ? encodeURIComponent('"'+headline.replace(/"/g,'')+'" crypto')
    : encodeURIComponent('"'+baseAsset+'" crypto');
  const url='https://news.google.com/rss/search?q='+q+'&hl=en-US&gl=US&ceid=US:en';
  try{
    const xml=await fetchText(url);
    return parseXmlItems(xml).slice(0,12);
  }catch{return []}
}

async function discoverNews(baseAsset){
  const items=await searchNews(baseAsset);
  return items.filter(x=>now()-x.pubDate<=NEWS_LOOKBACK_HOURS*3600000);
}

async function investigateNews(article,baseAsset){
  newsInvestigations++;
  const related=await searchNews(baseAsset,article.title);
  const exact=related.filter(x=>{
    const a=normalizeTitle(article.title),b=normalizeTitle(x.title);
    return a===b||a.includes(b)||b.includes(a);
  });
  const domains=new Set(related.map(x=>sourceDomain(x.link)).filter(Boolean));
  const corroborated=Math.max(0,domains.size-1);
  const ageMinutes=Math.max(0,(now()-article.pubDate)/60000);
  const event=eventType(article.title);
  const confidence=clamp(45+corroborated*12+(exact.length?15:0)+(ageMinutes<=30?8:0));
  return {...article,event,related:related.slice(0,6),corroborated,sourceCount:domains.size,confidence,ageMinutes};
}

async function marketReaction(symbol,s,newsTime){
  let bars=(s.klines['1m']||[]).filter(x=>x.closed);
  const needHistorical=bars.filter(x=>x.time<newsTime).length<5;
  if(needHistorical){
    try{
      const start=Math.max(0,newsTime-10*60000);
      const end=Math.min(now(),newsTime+20*60000);
      const rows=await fetchJson(BINANCE_API+'/klines?symbol='+encodeURIComponent(symbol.toUpperCase())+'&interval=1m&startTime='+start+'&endTime='+end+'&limit=40');
      bars=rows.map(x=>({time:n(x[0]),open:n(x[1]),high:n(x[2]),low:n(x[3]),close:n(x[4]),volume:n(x[5]),quoteVolume:n(x[7]),trades:n(x[8]),takerBuyQuote:n(x[10]),closed:true}));
    }catch(e){console.error('[V6 reaction history]',symbol,e.message)}
  }
  const before=bars.filter(x=>x.time<newsTime).slice(-5);
  const after=bars.filter(x=>x.time>=newsTime&&x.time<=Math.min(now(),newsTime+20*60000));
  if(before.length<3||after.length<2)return {ready:false};
  const base=before[0].close;
  const last=after[after.length-1].close;
  const move=pct(last,base);
  const beforeVol=before.reduce((a,x)=>a+x.quoteVolume,0)/before.length;
  const afterVol=after.reduce((a,x)=>a+x.quoteVolume,0)/after.length;
  const volumeRatio=beforeVol?afterVol/beforeVol:1;
  const beforeTrades=before.reduce((a,x)=>a+x.trades,0)/before.length;
  const afterTrades=after.reduce((a,x)=>a+x.trades,0)/after.length;
  const tradeRatio=beforeTrades?afterTrades/beforeTrades:1;
  const buyQuote=after.reduce((a,x)=>a+x.takerBuyQuote,0);
  const totalQuote=after.reduce((a,x)=>a+x.quoteVolume,0);
  const buyRatio=totalQuote?buyQuote/totalQuote*100:50;
  const direction=move>=0?'UP':'DOWN';
  let score=0;
  if(Math.abs(move)>=0.5)score+=20;
  if(Math.abs(move)>=1)score+=12;
  if(Math.abs(move)>=2)score+=10;
  if(volumeRatio>=1.5)score+=15;
  if(volumeRatio>=2.5)score+=10;
  if(tradeRatio>=1.5)score+=10;
  if(tradeRatio>=2.5)score+=8;
  if((direction==='UP'&&buyRatio>=58)||(direction==='DOWN'&&buyRatio<=42))score+=15;
  if(Math.abs(s.bookImbalance)>=12)score+=5;
  return {ready:true,move,volumeRatio,tradeRatio,buyRatio,direction,score:clamp(score)};
}

async function sendNewsReactionAlert(symbol,meta,news,reaction){
  const key=symbol+':'+normalizeTitle(news.title);
  const last=alertSeen.get(key)||0;
  if(now()-last<ALERT_COOLDOWN_MS)return false;
  if(reaction.score<MIN_REACTION_SCORE)return false;
  alertSeen.set(key,now());
  const chat=TELEGRAM_CHAT_ID||await discoverChat();
  if(!chat)return false;
  const corroboration=news.corroborated>0?'Verified across '+(news.sourceCount)+' news sources':'Single-source lead; verify independently';
  const reactionLine=reaction.direction==='UP'?'🟢 MARKET REACTION: BUYING / PRICE ACCELERATION':'🔴 MARKET REACTION: SELLING / PRICE ACCELERATION';
  const text=[
    '🚨 <b>CRYPTO RADAR AI — NEWS → MARKET REACTION</b>',
    '',
    '<b>'+escapeHtml(meta.symbol)+'</b>  $'+meta.price,
    'Reaction score: <b>'+reaction.score+'/100</b>',
    'Event: <b>'+escapeHtml(news.event)+'</b>',
    '',
    '📰 <b>NEWS</b>',
    escapeHtml(news.title),
    'Source: '+escapeHtml(news.source||sourceDomain(news.link)||'unknown'),
    'News confidence: '+news.confidence+'/100 · '+corroboration,
    news.link?'🔗 '+escapeHtml(news.link):'',
    '',
    reactionLine,
    'Price reaction: '+(reaction.move>=0?'+':'')+reaction.move.toFixed(2)+'%',
    'Volume vs pre-news: '+reaction.volumeRatio.toFixed(2)+'×',
    'Trades vs pre-news: '+reaction.tradeRatio.toFixed(2)+'×',
    'Taker-buy share: '+reaction.buyRatio.toFixed(1)+'%',
    'Order-book imbalance: '+n(getState(symbol).bookImbalance).toFixed(1)+'%',
    '',
    'Interpretation: the scanner found a time-aligned market reaction after the news; this does not prove the news caused the move.',
    '⚠ Rule-based market intelligence, not guaranteed prediction or personalized financial advice.'
  ].filter(Boolean).join('\n');
  try{
    await telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML',disable_web_page_preview:false});
    newsReactionAlerts++;
    return true;
  }catch(e){console.error('[Telegram]',e.message);return false}
}

async function processNews(){
  if(now()-lastNewsPoll<NEWS_POLL_MS)return;
  lastNewsPoll=now();
  const candidates=[...symbolMeta.entries()]
    .filter(([s,m])=>m.price>0&&m.price<=MAX_PRICE)
    .sort((a,b)=>Number(b[1].newCoin)-Number(a[1].newCoin))
    .slice(0,MAX_NEWS_CANDIDATES);
  for(const [symbol,meta] of candidates){
    try{
      const articles=await discoverNews(meta.baseAsset);
      for(const article of articles){
        const key=symbol+':'+normalizeTitle(article.title);
        if(newsSeen.has(key))continue;
        newsSeen.set(key,now());
        const investigated=await investigateNews(article,meta.baseAsset);
        pendingNews.set(key,{symbol,meta,article:investigated,expiresAt:now()+25*60000});
        console.log('[V6 NEWS DETECTED]',symbol,investigated.event,investigated.confidence,investigated.title);
      }
    }catch(e){console.error('[V6 news]',symbol,e.message)}
  }
  for(const [key,item] of pendingNews){
    if(now()>item.expiresAt){pendingNews.delete(key);continue}
    const reaction=await marketReaction(item.meta.symbol,getState(item.symbol),item.article.pubDate);
    if(reaction.ready&&item.article.confidence>=55){
      const sent=await sendNewsReactionAlert(item.symbol,item.meta,item.article,reaction);
      if(sent||reaction.score>=MIN_REACTION_SCORE)pendingNews.delete(key);
    }else{
      console.log('[V6 REACTION WAIT]',item.symbol,'waiting for post-news market evidence');
    }
  }
  for(const [k,t] of newsSeen)if(now()-t>24*3600000)newsSeen.delete(k);
  for(const [k,v] of pendingNews)if(now()>v.expiresAt)pendingNews.delete(k);
}

function analyzeLive(s){
  const bars=(s.klines['1m']||[]).filter(x=>x.closed).slice(-60);
  if(bars.length<10)return {score:50,side:'WATCH',timeframes:[]};
  const recent=bars.slice(-5),prior=bars.slice(-10,-5);
  const pMove=pct(recent.at(-1).close,prior[0].close);
  const rv=(recent.reduce((a,x)=>a+x.quoteVolume,0)/5)/(prior.reduce((a,x)=>a+x.quoteVolume,0)/5||1);
  const tr=(recent.reduce((a,x)=>a+x.trades,0)/5)/(prior.reduce((a,x)=>a+x.trades,0)/5||1);
  const buy=recent.reduce((a,x)=>a+x.takerBuyQuote,0),vol=recent.reduce((a,x)=>a+x.quoteVolume,0);
  const buyRatio=vol?buy/vol*100:50;
  let score=50;
  if(Math.abs(pMove)>=.5)score+=pMove>0?12:-12;
  if(rv>=1.8)score+=pMove>0?15:-15;
  if(tr>=1.7)score+=pMove>0?10:-10;
  if((pMove>0&&buyRatio>=58)||(pMove<0&&buyRatio<=42))score+=pMove>0?12:-12;
  if(Math.abs(s.bookImbalance)>=12)score+=s.bookImbalance>0?7:-7;
  score=clamp(score);
  return {score,side:score>=78?'BUY PRESSURE':score<=22?'SELL PRESSURE':'WATCH',timeframes:['1m','5m'],priceMove:pMove,volumeRatio:rv,tradeRatio:tr,buyRatio};
}

function closeSockets(){while(sockets.length){try{sockets.pop().close()}catch{}}}
function scheduleReconnect(symbols){
  if(reconnectTimer)return;
  reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect(symbols)},5000);
}
function connect(symbols){
  closeSockets();
  const per=Math.max(1,Math.floor(STREAMS_PER_CONNECTION/(INTERVALS.length+3)));
  for(let i=0;i<symbols.length;i+=per){
    const batch=symbols.slice(i,i+per);
    const streams=[];
    for(const s of batch){
      streams.push(s+'@aggTrade',s+'@bookTicker',s+'@depth@100ms');
      for(const tf of INTERVALS)streams.push(s+'@kline_'+tf);
    }
    const ws=new WebSocket(BINANCE_WS+'?streams='+streams.join('/'));
    sockets.push(ws);
    ws.on('open',()=>{connectedAt=new Date().toISOString();console.log('[V6] connected',batch.length,'markets')});
    ws.on('message',raw=>{
      messageCount++;lastEventAt=new Date().toISOString();
      try{
        const x=JSON.parse(raw.toString()),m=x.data||x,e=m.e,s=m.s?.toLowerCase();
        if(!s)return;
        if(e==='aggTrade')ingestTrade(s,n(m.p),n(m.q),!!m.m);
        else if(e==='bookTicker')ingestBook(s,n(m.B),n(m.A),n(m.b),n(m.a));
        else if(e==='kline')ingestKline(s,m.k);
      }catch{}
    });
    ws.on('close',()=>scheduleReconnect(symbols));
    ws.on('error',e=>console.error('[V6 ws]',e.message));
  }
}

async function refreshMarkets(){
  try{
    const symbols=await getSymbols();
    lastMarketLoad=now();
    connect(symbols);
    console.log('[V6] Monitoring',symbols.length,'USDT spot markets <= $'+MAX_PRICE);
  }catch(e){console.error('[V6 markets]',e.message)}
}

setInterval(refreshMarkets,30*60*1000);
setInterval(processNews,NEWS_POLL_MS);
refreshMarkets();
setTimeout(async()=>{
  const chat=TELEGRAM_CHAT_ID||await discoverChat();
  if(chat){
    try{
      await telegram('sendMessage',{chat_id:chat,text:'🟢 <b>Crypto Radar AI V6 ONLINE</b>\\nNews → investigation → market reaction monitoring is active.',parse_mode:'HTML'});
      console.log('[V6] Telegram online test sent');
    }catch(e){console.error('[V6 Telegram startup test]',e.message)}
  }else{
    console.log('[V6] Telegram chat not found. Send /start to the bot or configure TELEGRAM_CHAT_ID.');
  }
},5000);

http.createServer((req,res)=>{
  res.setHeader('content-type','application/json');
  res.setHeader('access-control-allow-origin','*');
  if(req.url==='/health')return res.end(JSON.stringify({
    ok:true,service:'Crypto Radar AI V6 scanner',connected:sockets.some(s=>s.readyState===1),connectedAt,lastEventAt,
    markets:symbolMeta.size,messages:messageCount,maxPrice:MAX_PRICE,newCoinDays:NEW_COIN_DAYS,
    newsInvestigations,newsReactionAlerts,lastMarketLoad,telegramConfigured:!!TELEGRAM_TOKEN,
    telegramChatConfigured:!!TELEGRAM_CHAT_ID
  }));
  if(req.url==='/test-telegram'){
    discoverChat().then(async chat=>{
      if(!chat)return res.end(JSON.stringify({ok:false,error:'No Telegram chat found. Open the bot in Telegram and send /start first.'}));
      try{
        await telegram('sendMessage',{chat_id:chat,text:'✅ <b>Crypto Radar AI V6 Telegram test</b>\nNews → investigation → market-reaction pipeline is connected.',parse_mode:'HTML'});
        res.end(JSON.stringify({ok:true,chatIdFound:true}));
      }catch(e){res.end(JSON.stringify({ok:false,error:e.message}))}
    });
    return;
  }
  if(req.url==='/news-status')return res.end(JSON.stringify({
    seen:newsSeen.size,pending:pendingNews.size,investigations:newsInvestigations,reactionAlerts:newsReactionAlerts,lastNewsPoll,
    flow:['NEWS_DETECTED','NEWS_INVESTIGATED','MARKET_REACTION_MEASURED','ALERT_IF_REACTION_CONFIRMED']
  }));
  res.statusCode=404;res.end(JSON.stringify({error:'not found'}));
}).listen(PORT,()=>console.log('[V6] Health server on '+PORT));
