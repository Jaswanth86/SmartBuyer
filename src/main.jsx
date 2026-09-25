import React,{useEffect,useMemo,useState}from'react';import{createRoot}from'react-dom/client';import'./styles.css';

const API='https://api.binance.com/api/v3';
const num=v=>Number(v||0);
const pct=v=>num(v);
const fmt=v=>num(v).toLocaleString(undefined,{maximumFractionDigits:8});
const clamp=v=>Math.max(0,Math.min(99,Math.round(v)));

function baseScore(c,previous){
  const ch=Math.abs(pct(c.priceChangePercent));
  const vol=num(c.quoteVolume);
  const trades=num(c.count);
  let s=20;
  if(ch>=8)s+=24;else if(ch>=5)s+=18;else if(ch>=3)s+=12;else if(ch>=1.5)s+=7;
  if(trades>=100000)s+=16;else if(trades>=50000)s+=12;else if(trades>=10000)s+=7;
  if(vol>=1e8)s+=18;else if(vol>=5e7)s+=13;else if(vol>=5e6)s+=8;
  if(previous){
    const vdelta=(vol-num(previous.quoteVolume))/Math.max(num(previous.quoteVolume),1)*100;
    const cdelta=(num(c.lastPrice)-num(previous.lastPrice))/Math.max(num(previous.lastPrice),1)*100;
    if(Math.abs(vdelta)>=25)s+=10;
    if(Math.abs(cdelta)>=0.7)s+=6;
  }
  return clamp(s);
}

async function json(url){const r=await fetch(url);if(!r.ok)throw Error('Market data unavailable');return r.json()}

function App(){
 const[coins,setCoins]=useState([]),[news,setNews]=useState([]),[loading,setLoading]=useState(true),[query,setQuery]=useState(''),[filter,setFilter]=useState('radar'),[selected,setSelected]=useState(null),[detail,setDetail]=useState(null),[updated,setUpdated]=useState(null),[error,setError]=useState(''),[history,setHistory]=useState({});
 async function load(){
  try{
   setLoading(true);setError('');
   const all=await json(API+'/ticker/24hr');
   const market=all.filter(x=>x.symbol.endsWith('USDT')&&num(x.quoteVolume)>1e5).map(x=>({...x,radar:baseScore(x,history[x.symbol])}));
   setCoins(market);setUpdated(new Date());
   setHistory(h=>{const next={...h};market.forEach(x=>{next[x.symbol]={lastPrice:x.lastPrice,quoteVolume:x.quoteVolume}});return next});
   try{const n=await fetch('/.netlify/functions/news');const type=n.headers.get('content-type')||'';if(n.ok&&type.includes('application/json'))setNews(await n.json());}catch{}
  }catch(e){setError(e.message)}finally{setLoading(false)}
 }
 useEffect(()=>{load();const t=setInterval(load,30000);return()=>clearInterval(t)},[]);
 async function investigate(c){
  setSelected(c);setDetail(null);
  try{
   const [book,trades,klines]=await Promise.all([
    json(API+'/depth?symbol='+c.symbol+'&limit=100'),
    json(API+'/trades?symbol='+c.symbol+'&limit=500'),
    json(API+'/klines?symbol='+c.symbol+'&interval=5m&limit=36')
   ]);
   const bid=num(book.bids?.reduce((a,x)=>a+num(x[1]),0)),ask=num(book.asks?.reduce((a,x)=>a+num(x[1]),0));
   const imbalance=(bid+ask)?(bid-ask)/(bid+ask)*100:0;
   const buy=trades.filter(t=>!t.isBuyerMaker).length,sell=trades.length-buy;
   const last=klines.slice(-12),first=last[0],end=last[last.length-1];
   const move=first&&end?(num(end[4])-num(first[1]))/Math.max(num(first[1]),1)*100:0;
   const avgVol=last.reduce((a,k)=>a+num(k[5]),0)/Math.max(last.length,1);
   const latestVol=num(end?.[5]);
   setDetail({imbalance,buy,sell,move,latestVol,avgVol,volumeRatio:avgVol?latestVol/avgVol:1});
  }catch(e){setDetail({error:'Deep market scan unavailable for this coin right now.'})}
 }
 const list=useMemo(()=>coins.filter(c=>c.symbol.toLowerCase().includes(query.toLowerCase())).filter(c=>filter==='radar'?c.radar>=35:filter==='pump'?pct(c.priceChangePercent)>=2:filter==='dump'?pct(c.priceChangePercent)<=-2:true).sort((a,b)=>filter==='dump'?pct(a.priceChangePercent)-pct(b.priceChangePercent):b.radar-a.radar).slice(0,80),[coins,query,filter]);
 return <div className="app">
  <header><div className="logo"><span>◈</span> CRYPTO<span>RADAR</span><small> AI / V2</small></div><div className="live"><i/> LIVE MARKET INTELLIGENCE <button onClick={load}>↻</button></div></header>
  <main>
   <section className="hero"><div><div className="eyebrow">AI MARKET SCOUT / V2</div><h1>Detect the move<br/><em>before the crowd.</em></h1><p>Crypto Radar scans Binance spot markets for abnormal price, volume and trading activity, adds short-term acceleration signals, and lets you deep-scan a coin's order book, trades and 5-minute structure.</p><div className="disclaimer">⚠ Anomaly detection is not a guaranteed pump/dump prediction.</div></div><div className="radar"><div className="radar-ring r1"/><div className="radar-ring r2"/><div className="radar-ring r3"/><div className="radar-sweep"/><div className="radar-core">RADAR<br/><b>{coins.length||'—'}</b><small>MARKETS</small></div></div></section>
   <section className="stats"><div><b>{coins.length}</b><span>Markets scanned</span></div><div><b>{coins.filter(c=>c.radar>=70).length}</b><span>High anomalies</span></div><div><b>{coins.filter(c=>pct(c.priceChangePercent)>=5).length}</b><span>Strong up moves</span></div><div><b>{news.length}</b><span>News signals</span></div></section>
   <section className="workspace"><div className="toolbar"><div className="tabs">{[['radar','Radar'],['pump','Up moves'],['dump','Down moves'],['all','All']].map(([k,l])=><button key={k} className={filter===k?'active':''} onClick={()=>setFilter(k)}>{l}</button>)}</div><input value={query} onChange={e=>setQuery(e.target.value)} placeholder="Search any coin…"/></div>
    <div className="grid"><div className="panel markets"><div className="panel-head"><div><div className="eyebrow">01 / MARKET SCAN</div><h2>Unknown coin radar</h2></div><span>{updated?updated.toLocaleTimeString():''}</span></div>
     {loading&&!coins.length?<div className="empty">Connecting to Binance…</div>:error?<div className="empty error">{error}</div>:<div className="coin-list">{list.map(c=><button className="coin" onClick={()=>investigate(c)} key={c.symbol}><span className="coin-name"><strong>{c.symbol.replace('USDT','')}</strong><small>USDT · {num(c.count).toLocaleString()} trades</small></span><span className="bar"><i style={{width:c.radar+'%'}}/></span><span className="coin-price">$ {fmt(c.lastPrice)}</span><span className={pct(c.priceChangePercent)>=0?'up':'down'}>{pct(c.priceChangePercent)>=0?'+':''}{pct(c.priceChangePercent).toFixed(2)}%</span><b className={'score s'+(c.radar>=70?'high':c.radar>=50?'mid':'low')}>{c.radar}</b></button>)}</div>}
    </div>
    <div className="side"><div className="panel"><div className="eyebrow">02 / NEWS INTELLIGENCE</div><h2>What is moving the market?</h2>{news.length?<div className="news">{news.slice(0,10).map((n,i)=><a key={n.link||i} href={n.link} target="_blank" rel="noreferrer"><span>{n.source}</span><strong>{n.title}</strong><small>{n.time}</small></a>)}</div>:<div className="empty">News feed is loading…</div>}</div>
     <div className="panel methodology"><div className="eyebrow">03 / V2 SIGNAL ENGINE</div><h3>Multiple signals. One investigation.</h3><p>Radar combines 24h movement, quote volume, trade activity and refresh-to-refresh acceleration. A coin investigation adds order-book imbalance, recent trade pressure and 5-minute volume/price structure.</p><div className="chips"><span>PRICE</span><span>VOLUME</span><span>TRADES</span><span>ACCELERATION</span><span>ORDER BOOK</span><span>5M STRUCTURE</span></div></div>
     <div className="panel roadmap"><div className="eyebrow">04 / DATA ROADMAP</div><div className="road"><span>✓</span> Binance spot market scan</div><div className="road"><span>✓</span> News feed</div><div className="road"><span>✓</span> Deep order-book scan</div><div className="road"><span>○</span> X / social API signals</div><div className="road"><span>○</span> On-chain whale flows</div><div className="road"><span>○</span> Alerts + backtesting</div></div>
    </div></div>
   </section>
  </main>
  {selected&&<div className="modal" onClick={()=>{setSelected(null);setDetail(null)}}><div className="modal-card" onClick={e=>e.stopPropagation()}><button className="close" onClick={()=>{setSelected(null);setDetail(null)}}>×</button><div className="eyebrow">V2 COIN INVESTIGATION</div><h2>{selected.symbol.replace('USDT','')} / USDT</h2><div className="big-score">{selected.radar}<small>RADAR SCORE</small></div><div className="facts"><div>Price<b>$ {fmt(selected.lastPrice)}</b></div><div>24h change<b className={pct(selected.priceChangePercent)>=0?'up':'down'}>{pct(selected.priceChangePercent).toFixed(2)}%</b></div><div>Quote volume<b>{num(selected.quoteVolume).toLocaleString(undefined,{maximumFractionDigits:0})}</b></div><div>Trades<b>{num(selected.count).toLocaleString()}</b></div></div><div className="deep-head"><div className="eyebrow">LIVE MICROSTRUCTURE</div><span>{detail?'SCANNED':'SCANNING…'}</span></div>{detail?.error?<p className="error">{detail.error}</p>:detail?<div className="deep-grid"><div><span>Order-book bias</span><b className={detail.imbalance>=0?'up':'down'}>{detail.imbalance>=0?'+':''}{detail.imbalance.toFixed(1)}%</b></div><div><span>Buy / sell trades</span><b>{detail.buy} / {detail.sell}</b></div><div><span>5m move</span><b className={detail.move>=0?'up':'down'}>{detail.move>=0?'+':''}{detail.move.toFixed(2)}%</b></div><div><span>Latest / avg volume</span><b>{detail.volumeRatio.toFixed(2)}×</b></div></div>:<div className="scan-line">Fetching order book · trades · 5m candles…</div>}<p>Why flagged: measurable abnormality across market activity. The deep scan provides additional context; it does not predict a future pump or dump.</p><button className="close-btn" onClick={()=>{setSelected(null);setDetail(null)}}>Back to radar</button></div></div>}
 </div>
}
createRoot(document.getElementById('root')).render(<App/>);