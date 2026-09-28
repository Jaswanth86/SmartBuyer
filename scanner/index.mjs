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
const RADAR_ALERT_SCORE=Number(process.env.RADAR_ALERT_SCORE||72);
const RADAR_POLL_MS=Number(process.env.RADAR_POLL_MS||15000);

// Runtime bot settings. These can be changed from Telegram without redeploying.
const botSettings={
  radar:true,
  news:true,
  radarScore:RADAR_ALERT_SCORE,
  reactionScore:MIN_REACTION_SCORE,
  cooldownMs:ALERT_COOLDOWN_MS,
  maxPrice:MAX_PRICE,
  newCoinDays:NEW_COIN_DAYS
};

const state=new Map();
const symbolMeta=new Map();
const newsSeen=new Map();
const pendingNews=new Map();
const alertSeen=new Map();
const radarAlertSeen=new Map();
const radarWarmSeen=new Set();
let discoveredChatId=TELEGRAM_CHAT_ID;
let telegramUpdateOffset=0;
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
  if(discoveredChatId)return discoveredChatId;
  if(TELEGRAM_CHAT_ID)return TELEGRAM_CHAT_ID;
  if(!TELEGRAM_TOKEN)return '';
  try{
    const u=await telegram('getUpdates',{offset:telegramUpdateOffset||undefined,timeout:0,allowed_updates:['message']});
    for(const update of u||[]){
      telegramUpdateOffset=Math.max(telegramUpdateOffset,Number(update.update_id||0)+1);
      const chat=update.message?.chat?.id;
      if(chat)discoveredChatId=String(chat);
    }
    return discoveredChatId||'';
  }catch(e){console.error('[Telegram]',e.message);return ''}
}

function formatDuration(ms){
  const total=Math.max(0,Math.round(ms/60000));
  if(total%60===0)return (total/60)+'h';
  return total+'m';
}
function botHelp(){
  return [
    '🤖 <b>CRYPTO RADAR AI V6 — COMMANDS</b>',
    '',
    '<b>Monitoring</b>',
    '/radar on — enable normal market radar',
    '/radar off — disable normal radar alerts',
    '/news on — enable news → reaction alerts',
    '/news off — disable news alerts',
    '',
    '<b>Settings</b>',
    '/score 72 — radar alert score (50–95)',
    '/reaction 65 — news reaction score (0–100)',
    '/cooldown 15m — alert cooldown (1m–24h)',
    '/maxprice 2 — monitor coins up to this price',
    '/newcoins 30 — newer-coin window in days',
    '',
    '<b>Tools</b>',
    '/status — live scanner status',
    '/settings — current bot settings',
    '/refresh — refresh Binance market list',
    '/test — send a Telegram test alert',
    '/help — show this menu'
  ].join('\\n');
}
async function sendBotMessage(chat,text){
  return telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML',disable_web_page_preview:true});
}
async function setTelegramCommands(){
  if(!TELEGRAM_TOKEN)return;
  try{
    await telegram('setMyCommands',{commands:[
      {command:'start',description:'Start Crypto Radar AI'},
      {command:'help',description:'Show all bot commands'},
      {command:'status',description:'Show live scanner status'},
      {command:'settings',description:'Show current settings'},
      {command:'radar',description:'Turn normal radar on/off'},
      {command:'news',description:'Turn news alerts on/off'},
      {command:'score',description:'Set radar alert score'},
      {command:'reaction',description:'Set news reaction score'},
      {command:'cooldown',description:'Set alert cooldown'},
      {command:'maxprice',description:'Set maximum coin price'},
      {command:'newcoins',description:'Set newer-coin window'},
      {command:'refresh',description:'Refresh monitored markets'},
      {command:'test',description:'Send a Telegram test alert'}
    ]});
  }catch(e){console.error('[Telegram commands]',e.message)}
}
async function handleTelegramCommand(chat,raw){
  const parts=String(raw||'').trim().split(/\\s+/);
  const command=(parts.shift()||'').toLowerCase().split('@')[0];
  const arg=parts[0]||'';
  const boolArg=v=>['on','enable','enabled','1','true'].includes(String(v).toLowerCase());
  const offArg=v=>['off','disable','disabled','0','false'].includes(String(v).toLowerCase());
  if(command==='/start'){
    await sendBotMessage(chat,'🟢 <b>Crypto Radar AI V6 is connected.</b>\\n\\nNormal radar and continuous news monitoring run independently.\\n\\nUse /help to see commands.');
    return;
  }
  if(command==='/help'||command==='/commands'){await sendBotMessage(chat,botHelp());return;}
  if(command==='/settings'){
    await sendBotMessage(chat,[
      '⚙️ <b>V6 BOT SETTINGS</b>','',
      'Radar: <b>'+(botSettings.radar?'ON':'OFF')+'</b>',
      'News → reaction: <b>'+(botSettings.news?'ON':'OFF')+'</b>',
      'Radar score: <b>'+botSettings.radarScore+'/100</b>',
      'News reaction score: <b>'+botSettings.reactionScore+'/100</b>',
      'Cooldown: <b>'+formatDuration(botSettings.cooldownMs)+'</b>',
      'Max coin price: <b>
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

async function sendRadarAlert(symbol,meta,signal){
  const key=symbol+':'+signal.side;
  const last=radarAlertSeen.get(key)||0;
  if(now()-last<botSettings.cooldownMs)return false;
  const chat=TELEGRAM_CHAT_ID||discoveredChatId||await discoverChat();
  if(!chat)return false;
  radarAlertSeen.set(key,now());
  const direction=signal.side==='BUY PRESSURE';
  const icon=direction?'🟢':'🔴';
  const text=[
    icon+' <b>CRYPTO RADAR AI — MARKET ALERT</b>',
    '',
    '<b>'+escapeHtml(meta.symbol)+'</b>  $'+n(meta.price).toPrecision(8),
    '<b>'+escapeHtml(signal.side)+'</b> · Radar score <b>'+signal.score+'/100</b>',
    '',
    'Price change: '+(signal.priceMove>=0?'+':'')+signal.priceMove.toFixed(2)+'%',
    'Volume acceleration: '+signal.volumeRatio.toFixed(2)+'×',
    'Trade acceleration: '+signal.tradeRatio.toFixed(2)+'×',
    'Taker-buy share: '+signal.buyRatio.toFixed(1)+'%',
    'Order-book imbalance: '+n(getState(symbol).bookImbalance).toFixed(1)+'%',
    'Liquidity change: '+n(getState(symbol).liquidityChange).toFixed(2)+'%',
    meta.newCoin?'🆕 Newly listed/newer coin window: YES':'',
    '',
    direction?'⚡ Buying pressure and market activity increased together.':'⚠ Selling pressure and market activity increased together.',
    'This is an anomaly/activity alert, not a guaranteed prediction or financial advice.'
  ].filter(Boolean).join('\\n');
  try{
    await telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML'});
    console.log('[V6 RADAR ALERT]',symbol,signal.side,signal.score);
    return true;
  }catch(e){
    console.error('[V6 Radar Telegram]',e.message);
    return false;
  }
}

async function sendNewsReactionAlert(symbol,meta,news,reaction){
  const key=symbol+':'+normalizeTitle(news.title);
  const last=alertSeen.get(key)||0;
  if(now()-last<ALERT_COOLDOWN_MS)return false;
  if(reaction.score<botSettings.reactionScore)return false;
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
  if(!botSettings.news)return;
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

async function warmRadar(symbol){
  if(radarWarmSeen.has(symbol))return;
  const s=getState(symbol);
  if((s.klines['1m']||[]).filter(x=>x.closed).length>=10){radarWarmSeen.add(symbol);return;}
  radarWarmSeen.add(symbol);
  try{
    const rows=await fetchJson(BINANCE_API+'/klines?symbol='+encodeURIComponent(symbol.toUpperCase())+'&interval=1m&limit=12');
    for(const x of rows)ingestKline(symbol,{i:'1m',t:x[0],o:x[1],h:x[2],l:x[3],c:x[4],v:x[5],q:x[7],n:x[8],Q:x[10],x:true});
  }catch(e){console.error('[V6 radar warm]',symbol,e.message);radarWarmSeen.delete(symbol)}
}
async function scanRadar(){
  if(!botSettings.radar)return;
  const candidates=[...symbolMeta.entries()].filter(([s,m])=>m.price>0&&m.price<=botSettings.maxPrice).slice(0,MAX_SYMBOLS);
  let warm=0;
  for(const [symbol,meta] of candidates){
    const s=getState(symbol);
    if((s.klines['1m']||[]).filter(x=>x.closed).length<10&&warm<8){warm++;await warmRadar(symbol)}
    const signal=analyzeLive(getState(symbol));
    const buy=signal.side==='BUY PRESSURE'&&signal.score>=botSettings.radarScore;
    const sell=signal.side==='SELL PRESSURE'&&signal.score<=(100-botSettings.radarScore);
    if(buy||sell){
      const currentMeta=symbolMeta.get(symbol)||meta;
      currentMeta.price=getState(symbol).lastPrice||currentMeta.price;
      await sendRadarAlert(symbol,currentMeta,signal);
    }
  }
}
\nfunction closeSockets(){while(sockets.length){try{sockets.pop().close()}catch{}}}
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
    const cutoff=now()-botSettings.newCoinDays*86400000;
    // getSymbols uses environment defaults, so temporarily expose runtime settings through globals below.
    const [info,tickers]=await Promise.all([fetchJson(BINANCE_API+'/exchangeInfo'),fetchJson(BINANCE_API+'/ticker/price')]);
    const prices=new Map(tickers.map(x=>[x.symbol,n(x.price)]));
    const rows=info.symbols.filter(s=>s.status==='TRADING'&&s.quoteAsset==='USDT'&&s.isSpotTradingAllowed);
    for(const s of rows){
      const price=prices.get(s.symbol)||0;const onboard=n(s.onboardDate||s.listingTime);
      if(price>0)symbolMeta.set(s.symbol.toLowerCase(),{symbol:s.symbol,baseAsset:s.baseAsset,price,onboardDate:onboard,newCoin:!!onboard&&onboard>=cutoff});
    }
    const symbols=rows.filter(s=>(prices.get(s.symbol)||0)>0&&(prices.get(s.symbol)||0)<=botSettings.maxPrice)
      .sort((a,b)=>Number(symbolMeta.get(b.symbol.toLowerCase())?.newCoin)-Number(symbolMeta.get(a.symbol.toLowerCase())?.newCoin))
      .slice(0,MAX_SYMBOLS).map(s=>s.symbol.toLowerCase());
    lastMarketLoad=now();connect(symbols);
    console.log('[V6] Monitoring',symbols.length,'USDT spot markets <= 
setInterval(refreshMarkets,30*60*1000);
setInterval(processNews,NEWS_POLL_MS);
setInterval(scanRadar,RADAR_POLL_MS);
setInterval(pollTelegram,5000);
refreshMarkets();
setTelegramCommands();
setTimeout(async()=>{
  const chat=TELEGRAM_CHAT_ID||await discoverChat();
  if(chat){
    try{
      await setTelegramCommands();
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
    markets:symbolMeta.size,messages:messageCount,maxPrice:botSettings.maxPrice,newCoinDays:botSettings.newCoinDays,
    radar:botSettings.radar,news:botSettings.news,radarScore:botSettings.radarScore,reactionScore:botSettings.reactionScore,
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
+botSettings.maxPrice+'</b>',
      'Newer-coin window: <b>'+botSettings.newCoinDays+' days</b>',
      '',
      'Use /help for commands.'
    ].join('\\n'));return;
  }
  if(command==='/radar'){
    if(!boolArg(arg)&&!offArg(arg)){await sendBotMessage(chat,'Use <code>/radar on</code> or <code>/radar off</code>.');return;}
    botSettings.radar=boolArg(arg);await sendBotMessage(chat,'📡 Normal radar alerts: <b>'+(botSettings.radar?'ON':'OFF')+'</b>');return;
  }
  if(command==='/news'){
    if(!boolArg(arg)&&!offArg(arg)){await sendBotMessage(chat,'Use <code>/news on</code> or <code>/news off</code>.');return;}
    botSettings.news=boolArg(arg);await sendBotMessage(chat,'📰 News → market-reaction alerts: <b>'+(botSettings.news?'ON':'OFF')+'</b>');return;
  }
  if(command==='/score'){
    const v=Number(arg);if(!Number.isFinite(v)||v<50||v>95){await sendBotMessage(chat,'Use a radar score from <b>50–95</b>, e.g. <code>/score 72</code>.');return;}
    botSettings.radarScore=Math.round(v);await sendBotMessage(chat,'✅ Radar alert score set to <b>'+botSettings.radarScore+'/100</b>.');return;
  }
  if(command==='/reaction'){
    const v=Number(arg);if(!Number.isFinite(v)||v<0||v>100){await sendBotMessage(chat,'Use a reaction score from <b>0–100</b>, e.g. <code>/reaction 65</code>.');return;}
    botSettings.reactionScore=Math.round(v);await sendBotMessage(chat,'✅ News reaction score set to <b>'+botSettings.reactionScore+'/100</b>.');return;
  }
  if(command==='/cooldown'){
    const m=String(arg).match(/^(\\d+(?:\\.\\d+)?)(s|m|h)$/i);
    if(!m){await sendBotMessage(chat,'Use <code>/cooldown 15m</code>, <code>/cooldown 30s</code>, or <code>/cooldown 2h</code>.');return;}
    const value=Number(m[1]),unit=m[2].toLowerCase();
    const mult=unit==='s'?1000:unit==='m'?60000:3600000;const ms=value*mult;
    if(ms<60000||ms>86400000){await sendBotMessage(chat,'Cooldown must be between <b>1 minute and 24 hours</b>.');return;}
    botSettings.cooldownMs=ms;await sendBotMessage(chat,'✅ Alert cooldown set to <b>'+formatDuration(ms)+'</b>.');return;
  }
  if(command==='/maxprice'){
    const v=Number(arg);if(!Number.isFinite(v)||v<=0||v>100){await sendBotMessage(chat,'Use a price from <b>$0.01–$100</b>, e.g. <code>/maxprice 2</code>.');return;}
    botSettings.maxPrice=v;await refreshMarkets();await sendBotMessage(chat,'✅ Monitoring price limit set to <b>
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

async function sendRadarAlert(symbol,meta,signal){
  const key=symbol+':'+signal.side;
  const last=radarAlertSeen.get(key)||0;
  if(now()-last<ALERT_COOLDOWN_MS)return false;
  const chat=TELEGRAM_CHAT_ID||discoveredChatId||await discoverChat();
  if(!chat)return false;
  radarAlertSeen.set(key,now());
  const direction=signal.side==='BUY PRESSURE';
  const icon=direction?'🟢':'🔴';
  const text=[
    icon+' <b>CRYPTO RADAR AI — MARKET ALERT</b>',
    '',
    '<b>'+escapeHtml(meta.symbol)+'</b>  $'+n(meta.price).toPrecision(8),
    '<b>'+escapeHtml(signal.side)+'</b> · Radar score <b>'+signal.score+'/100</b>',
    '',
    'Price change: '+(signal.priceMove>=0?'+':'')+signal.priceMove.toFixed(2)+'%',
    'Volume acceleration: '+signal.volumeRatio.toFixed(2)+'×',
    'Trade acceleration: '+signal.tradeRatio.toFixed(2)+'×',
    'Taker-buy share: '+signal.buyRatio.toFixed(1)+'%',
    'Order-book imbalance: '+n(getState(symbol).bookImbalance).toFixed(1)+'%',
    'Liquidity change: '+n(getState(symbol).liquidityChange).toFixed(2)+'%',
    meta.newCoin?'🆕 Newly listed/newer coin window: YES':'',
    '',
    direction?'⚡ Buying pressure and market activity increased together.':'⚠ Selling pressure and market activity increased together.',
    'This is an anomaly/activity alert, not a guaranteed prediction or financial advice.'
  ].filter(Boolean).join('\\n');
  try{
    await telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML'});
    console.log('[V6 RADAR ALERT]',symbol,signal.side,signal.score);
    return true;
  }catch(e){
    console.error('[V6 Radar Telegram]',e.message);
    return false;
  }
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
+v+'</b>. Binance markets refreshed.');return;
  }
  if(command==='/newcoins'){
    const v=Number(arg);if(!Number.isInteger(v)||v<0||v>3650){await sendBotMessage(chat,'Use <b>0–3650 days</b>, e.g. <code>/newcoins 30</code>.');return;}
    botSettings.newCoinDays=v;await refreshMarkets();await sendBotMessage(chat,'✅ Newer-coin window set to <b>'+v+' days</b>. Binance markets refreshed.');return;
  }
  if(command==='/refresh'){
    await refreshMarkets();await sendBotMessage(chat,'🔄 <b>Market list refreshed.</b> Monitoring '+symbolMeta.size+' Binance USDT markets in the current configured range.');return;
  }
  if(command==='/test'){
    await sendBotMessage(chat,'🧪 <b>Telegram command test OK.</b>\\nRadar: '+(botSettings.radar?'ON':'OFF')+' · News: '+(botSettings.news?'ON':'OFF'));
    return;
  }
  if(command.startsWith('/'))await sendBotMessage(chat,'Unknown command. Use /help.');
}
async function pollTelegram(){
  if(!TELEGRAM_TOKEN)return;
  try{
    const updates=await telegram('getUpdates',{
      offset:telegramUpdateOffset||undefined,
      timeout:0,
      allowed_updates:['message']
    });
    for(const update of updates||[]){
      telegramUpdateOffset=Math.max(telegramUpdateOffset,Number(update.update_id||0)+1);
      const msg=update.message;
      if(!msg?.chat?.id)continue;
      discoveredChatId=String(msg.chat.id);
      const raw=String(msg.text||'').trim();
      if(raw.startsWith('/'))await handleTelegramCommand(discoveredChatId,raw);
    }
  }catch(e){console.error('[V6 Telegram poll]',e.message)}
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

async function sendRadarAlert(symbol,meta,signal){
  const key=symbol+':'+signal.side;
  const last=radarAlertSeen.get(key)||0;
  if(now()-last<ALERT_COOLDOWN_MS)return false;
  const chat=TELEGRAM_CHAT_ID||discoveredChatId||await discoverChat();
  if(!chat)return false;
  radarAlertSeen.set(key,now());
  const direction=signal.side==='BUY PRESSURE';
  const icon=direction?'🟢':'🔴';
  const text=[
    icon+' <b>CRYPTO RADAR AI — MARKET ALERT</b>',
    '',
    '<b>'+escapeHtml(meta.symbol)+'</b>  $'+n(meta.price).toPrecision(8),
    '<b>'+escapeHtml(signal.side)+'</b> · Radar score <b>'+signal.score+'/100</b>',
    '',
    'Price change: '+(signal.priceMove>=0?'+':'')+signal.priceMove.toFixed(2)+'%',
    'Volume acceleration: '+signal.volumeRatio.toFixed(2)+'×',
    'Trade acceleration: '+signal.tradeRatio.toFixed(2)+'×',
    'Taker-buy share: '+signal.buyRatio.toFixed(1)+'%',
    'Order-book imbalance: '+n(getState(symbol).bookImbalance).toFixed(1)+'%',
    'Liquidity change: '+n(getState(symbol).liquidityChange).toFixed(2)+'%',
    meta.newCoin?'🆕 Newly listed/newer coin window: YES':'',
    '',
    direction?'⚡ Buying pressure and market activity increased together.':'⚠ Selling pressure and market activity increased together.',
    'This is an anomaly/activity alert, not a guaranteed prediction or financial advice.'
  ].filter(Boolean).join('\\n');
  try{
    await telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML'});
    console.log('[V6 RADAR ALERT]',symbol,signal.side,signal.score);
    return true;
  }catch(e){
    console.error('[V6 Radar Telegram]',e.message);
    return false;
  }
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
+botSettings.maxPrice);
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
+botSettings.maxPrice+'</b>',
      'Newer-coin window: <b>'+botSettings.newCoinDays+' days</b>',
      '',
      'Use /help for commands.'
    ].join('\\n'));return;
  }
  if(command==='/radar'){
    if(!boolArg(arg)&&!offArg(arg)){await sendBotMessage(chat,'Use <code>/radar on</code> or <code>/radar off</code>.');return;}
    botSettings.radar=boolArg(arg);await sendBotMessage(chat,'📡 Normal radar alerts: <b>'+(botSettings.radar?'ON':'OFF')+'</b>');return;
  }
  if(command==='/news'){
    if(!boolArg(arg)&&!offArg(arg)){await sendBotMessage(chat,'Use <code>/news on</code> or <code>/news off</code>.');return;}
    botSettings.news=boolArg(arg);await sendBotMessage(chat,'📰 News → market-reaction alerts: <b>'+(botSettings.news?'ON':'OFF')+'</b>');return;
  }
  if(command==='/score'){
    const v=Number(arg);if(!Number.isFinite(v)||v<50||v>95){await sendBotMessage(chat,'Use a radar score from <b>50–95</b>, e.g. <code>/score 72</code>.');return;}
    botSettings.radarScore=Math.round(v);await sendBotMessage(chat,'✅ Radar alert score set to <b>'+botSettings.radarScore+'/100</b>.');return;
  }
  if(command==='/reaction'){
    const v=Number(arg);if(!Number.isFinite(v)||v<0||v>100){await sendBotMessage(chat,'Use a reaction score from <b>0–100</b>, e.g. <code>/reaction 65</code>.');return;}
    botSettings.reactionScore=Math.round(v);await sendBotMessage(chat,'✅ News reaction score set to <b>'+botSettings.reactionScore+'/100</b>.');return;
  }
  if(command==='/cooldown'){
    const m=String(arg).match(/^(\\d+(?:\\.\\d+)?)(s|m|h)$/i);
    if(!m){await sendBotMessage(chat,'Use <code>/cooldown 15m</code>, <code>/cooldown 30s</code>, or <code>/cooldown 2h</code>.');return;}
    const value=Number(m[1]),unit=m[2].toLowerCase();
    const mult=unit==='s'?1000:unit==='m'?60000:3600000;const ms=value*mult;
    if(ms<60000||ms>86400000){await sendBotMessage(chat,'Cooldown must be between <b>1 minute and 24 hours</b>.');return;}
    botSettings.cooldownMs=ms;await sendBotMessage(chat,'✅ Alert cooldown set to <b>'+formatDuration(ms)+'</b>.');return;
  }
  if(command==='/maxprice'){
    const v=Number(arg);if(!Number.isFinite(v)||v<=0||v>100){await sendBotMessage(chat,'Use a price from <b>$0.01–$100</b>, e.g. <code>/maxprice 2</code>.');return;}
    botSettings.maxPrice=v;await refreshMarkets();await sendBotMessage(chat,'✅ Monitoring price limit set to <b>
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

async function sendRadarAlert(symbol,meta,signal){
  const key=symbol+':'+signal.side;
  const last=radarAlertSeen.get(key)||0;
  if(now()-last<ALERT_COOLDOWN_MS)return false;
  const chat=TELEGRAM_CHAT_ID||discoveredChatId||await discoverChat();
  if(!chat)return false;
  radarAlertSeen.set(key,now());
  const direction=signal.side==='BUY PRESSURE';
  const icon=direction?'🟢':'🔴';
  const text=[
    icon+' <b>CRYPTO RADAR AI — MARKET ALERT</b>',
    '',
    '<b>'+escapeHtml(meta.symbol)+'</b>  $'+n(meta.price).toPrecision(8),
    '<b>'+escapeHtml(signal.side)+'</b> · Radar score <b>'+signal.score+'/100</b>',
    '',
    'Price change: '+(signal.priceMove>=0?'+':'')+signal.priceMove.toFixed(2)+'%',
    'Volume acceleration: '+signal.volumeRatio.toFixed(2)+'×',
    'Trade acceleration: '+signal.tradeRatio.toFixed(2)+'×',
    'Taker-buy share: '+signal.buyRatio.toFixed(1)+'%',
    'Order-book imbalance: '+n(getState(symbol).bookImbalance).toFixed(1)+'%',
    'Liquidity change: '+n(getState(symbol).liquidityChange).toFixed(2)+'%',
    meta.newCoin?'🆕 Newly listed/newer coin window: YES':'',
    '',
    direction?'⚡ Buying pressure and market activity increased together.':'⚠ Selling pressure and market activity increased together.',
    'This is an anomaly/activity alert, not a guaranteed prediction or financial advice.'
  ].filter(Boolean).join('\\n');
  try{
    await telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML'});
    console.log('[V6 RADAR ALERT]',symbol,signal.side,signal.score);
    return true;
  }catch(e){
    console.error('[V6 Radar Telegram]',e.message);
    return false;
  }
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
+v+'</b>. Binance markets refreshed.');return;
  }
  if(command==='/newcoins'){
    const v=Number(arg);if(!Number.isInteger(v)||v<0||v>3650){await sendBotMessage(chat,'Use <b>0–3650 days</b>, e.g. <code>/newcoins 30</code>.');return;}
    botSettings.newCoinDays=v;await refreshMarkets();await sendBotMessage(chat,'✅ Newer-coin window set to <b>'+v+' days</b>. Binance markets refreshed.');return;
  }
  if(command==='/refresh'){
    await refreshMarkets();await sendBotMessage(chat,'🔄 <b>Market list refreshed.</b> Monitoring '+symbolMeta.size+' Binance USDT markets in the current configured range.');return;
  }
  if(command==='/test'){
    await sendBotMessage(chat,'🧪 <b>Telegram command test OK.</b>\\nRadar: '+(botSettings.radar?'ON':'OFF')+' · News: '+(botSettings.news?'ON':'OFF'));
    return;
  }
  if(command.startsWith('/'))await sendBotMessage(chat,'Unknown command. Use /help.');
}
async function pollTelegram(){
  if(!TELEGRAM_TOKEN)return;
  try{
    const updates=await telegram('getUpdates',{
      offset:telegramUpdateOffset||undefined,
      timeout:0,
      allowed_updates:['message']
    });
    for(const update of updates||[]){
      telegramUpdateOffset=Math.max(telegramUpdateOffset,Number(update.update_id||0)+1);
      const msg=update.message;
      if(!msg?.chat?.id)continue;
      discoveredChatId=String(msg.chat.id);
      const raw=String(msg.text||'').trim();
      if(raw.startsWith('/'))await handleTelegramCommand(discoveredChatId,raw);
    }
  }catch(e){console.error('[V6 Telegram poll]',e.message)}
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

async function sendRadarAlert(symbol,meta,signal){
  const key=symbol+':'+signal.side;
  const last=radarAlertSeen.get(key)||0;
  if(now()-last<ALERT_COOLDOWN_MS)return false;
  const chat=TELEGRAM_CHAT_ID||discoveredChatId||await discoverChat();
  if(!chat)return false;
  radarAlertSeen.set(key,now());
  const direction=signal.side==='BUY PRESSURE';
  const icon=direction?'🟢':'🔴';
  const text=[
    icon+' <b>CRYPTO RADAR AI — MARKET ALERT</b>',
    '',
    '<b>'+escapeHtml(meta.symbol)+'</b>  $'+n(meta.price).toPrecision(8),
    '<b>'+escapeHtml(signal.side)+'</b> · Radar score <b>'+signal.score+'/100</b>',
    '',
    'Price change: '+(signal.priceMove>=0?'+':'')+signal.priceMove.toFixed(2)+'%',
    'Volume acceleration: '+signal.volumeRatio.toFixed(2)+'×',
    'Trade acceleration: '+signal.tradeRatio.toFixed(2)+'×',
    'Taker-buy share: '+signal.buyRatio.toFixed(1)+'%',
    'Order-book imbalance: '+n(getState(symbol).bookImbalance).toFixed(1)+'%',
    'Liquidity change: '+n(getState(symbol).liquidityChange).toFixed(2)+'%',
    meta.newCoin?'🆕 Newly listed/newer coin window: YES':'',
    '',
    direction?'⚡ Buying pressure and market activity increased together.':'⚠ Selling pressure and market activity increased together.',
    'This is an anomaly/activity alert, not a guaranteed prediction or financial advice.'
  ].filter(Boolean).join('\\n');
  try{
    await telegram('sendMessage',{chat_id:chat,text,parse_mode:'HTML'});
    console.log('[V6 RADAR ALERT]',symbol,signal.side,signal.score);
    return true;
  }catch(e){
    console.error('[V6 Radar Telegram]',e.message);
    return false;
  }
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
