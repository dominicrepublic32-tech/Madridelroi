// ═══════════════════════════════════════════════
// GODZILLA DOWNTREND — El Roi Server
// Full Stack Server — All features
// ═══════════════════════════════════════════════
'use strict';

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const fetch     = require('node-fetch');
const path      = require('path');
const fs        = require('fs');

const app     = express();
const server  = http.createServer(app);
const dashWss = new WebSocket.Server({ server, path: '/dashboard' });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT   = process.env.PORT || 3000;
const APP_ID = 1089;

// ── PERSISTENT STORAGE ───────────────────────────
// Save trade history and settings to disk so they survive restarts
const DATA_FILE = path.join(__dirname, 'data.json');

function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const d = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      return d;
    }
  } catch(e) { console.log('Load data error:', e.message); }
  return { tradeLog: [], cfg: {} };
}

function saveData() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ tradeLog, cfg }, null, 2));
  } catch(e) { console.log('Save data error:', e.message); }
}

// Load saved data on startup
const savedData = loadData();

// ── STATE ────────────────────────────────────────
let cfg = {
  apiToken:         process.env.DERIV_TOKEN    || '',
  market:           process.env.MARKET         || '1HZ100V',
  command:          process.env.COMMAND        || 'NOTOUCH',
  stake:            parseFloat(process.env.STAKE         || '1.00'),
  durationMins:     parseInt(process.env.DURATION        || '5'),
  barrierOffset:    process.env.BARRIER        || '+2.1',
  multiplier:       parseInt(process.env.MULTIPLIER      || '10'),
  takeProfit:       parseFloat(process.env.TP            || '4.00'),
  stopLoss:         parseFloat(process.env.SL            || '2.00'),
  scanTF:           process.env.SCAN_TF        || 'M1+M5',
  minTFConfirm:     parseInt(process.env.MIN_TF          || '2'),
  smallTol:         parseInt(process.env.SMALL_TOL       || '10'),
  bigTol:           parseInt(process.env.BIG_TOL         || '15'),
  smallConfirm:     parseInt(process.env.SMALL_CONFIRM   || '1'),
  bigConfirm:       parseInt(process.env.BIG_CONFIRM     || '2'),
  proximityPct:     parseFloat(process.env.PROXIMITY     || '90'),
  maxTrades:        parseInt(process.env.MAX_TRADES       || '0'),
  maxConsecLosses:  parseInt(process.env.MAX_LOSSES       || '2'),
  cooldownSecs:     parseInt(process.env.COOLDOWN         || '1800'),
  teleToken:        process.env.TELE_TOKEN     || '',
  teleChatId:       process.env.TELE_CHAT_ID   || '',
  ...savedData.cfg,
};

let derivWs           = null;
let botActive         = false;
let userStarted       = false;
let derivAccountId    = null; // for new PAT token flow
let currentPrice      = 0;

// Candles — M1 to D1
let candles = { M1:[], M5:[], M15:[], H1:[], H4:[], D1:[] };
let trendStatus = { M1:null, M5:null, M15:null };
let confirmedTrend = false;

// ── MULTIPLE STRUCTURES ──────────────────────────
// Each structure is independent: { peaks, baseDiff, type, tf, tradedLevels, projectedLevels[] }
let activeStructures  = [];  // array of all valid structures
let ignoredLevels     = new Set(); // globally ignored projected levels
let ignoreZones       = [];  // [{a, b}] multiple zones

// Entry state
let inTrade           = false;
let currentContractId = null;
let entryTargets      = []; // [{level, structIdx, pricePassed, passedCount}]
let currentActiveLevel = null;
let currentStructType = null;

// Stats — session only
let tradeCount = 0, wins = 0, losses = 0, sessionPnl = 0;

// Trade log — persistent
let tradeLog = savedData.tradeLog || [];

let consecutiveLosses     = 0;
let lossCountdownPaused   = false;
let lossCountdownTimer    = null;
let lossCountdownRemaining = 0;
let lossCountdownTotal    = 0;

// Take time off
let timeOffPaused    = false;
let timeOffTimer     = null;
let timeOffRemaining = 0;
let timeOffTotal     = 0;

// H1-D1 key support zone detection
let htfSupportZones   = []; // detected from H1/H4/D1
let htfZonePaused     = false; // paused due to uptrend structure in HTF zone
let htfZoneResumeCondition = null; // 'breakout' or 'uptrend_confirmed'

let scanInterval    = null;
let reconnectTimer  = null;
let tickerMsg       = '— GODZILLA READY —';
let statusText      = 'IDLE';

// ── LOGGING ──────────────────────────────────────
function log(msg) {
  const t = new Date().toISOString().replace('T',' ').slice(0,19);
  console.log(`[${t}] ${msg}`);
  broadcastDash({ type:'log', msg:`[${t}] ${msg}` });
}

// ── BROADCAST ─────────────────────────────────────
function broadcastDash(data) {
  const json = JSON.stringify(data);
  dashWss.clients.forEach(c => { if(c.readyState===WebSocket.OPEN) c.send(json); });
}

function broadcastState() {
  broadcastDash({
    type:'state',
    botActive, currentPrice, trendStatus, confirmedTrend,
    activeStructures: activeStructures.map(s=>({
      peaks: s.peaks, baseDiff: s.baseDiff, type: s.type, tf: s.tf,
      projectedLevels: s.projectedLevels, tradedLevels: [...s.tradedLevels]
    })),
    ignoredLevels: [...ignoredLevels],
    ignoreZones,
    htfSupportZones,
    htfZonePaused,
    entryTargets,
    currentActiveLevel, currentStructType,
    tradeCount, wins, losses, sessionPnl,
    consecutiveLosses,
    lossCountdownPaused, lossCountdownRemaining, lossCountdownTotal,
    timeOffPaused, timeOffRemaining, timeOffTotal,
    tickerMsg, statusText, cfg,
    tradeLog: tradeLog.slice(0, 100),
  });
}

function setTicker(msg) { tickerMsg=msg; broadcastDash({type:'ticker',msg}); }
function setStatus(s,t)  { statusText=t; broadcastDash({type:'status',status:s,text:t}); }

// ── TELEGRAM ─────────────────────────────────────
const MKT_NAMES = {
  '1HZ100V':'Volatility 100 (1s)','R_100':'Volatility 100',
  '1HZ75V':'Volatility 75 (1s)','R_75':'Volatility 75',
  '1HZ50V':'Volatility 50 (1s)','R_50':'Volatility 50',
  '1HZ25V':'Volatility 25 (1s)','1HZ10V':'Volatility 10 (1s)',
  'frxEURUSD':'EUR/USD','frxGBPUSD':'GBP/USD','frxXAUUSD':'Gold/USD',
  'cryBTCUSD':'BTC/USD','cryETHUSD':'ETH/USD','stpRNG':'Step Index',
};

async function telegram(msg) {
  if(!cfg.teleToken||!cfg.teleChatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${cfg.teleToken}/sendMessage`,{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({chat_id:cfg.teleChatId,text:`🦎 GODZILLA\n${msg}`,parse_mode:'HTML'})
    });
  } catch(e) { log('Telegram error: '+e.message); }
}

// ── TREND ANALYSIS ────────────────────────────────
function analyzeTrend(tf) {
  const data = candles[tf];
  if(!data||data.length<10) return;
  const recent = data.slice(-20);
  const highs = recent.map(c=>c.high), lows=recent.map(c=>c.low);
  let lh=0,ll=0,hh=0,hl=0;
  for(let i=1;i<highs.length;i++){
    if(highs[i]<highs[i-1])lh++;else hh++;
    if(lows[i]<lows[i-1])ll++;else hl++;
  }
  const total=highs.length-1;
  const ds=(lh+ll)/(total*2), us=(hh+hl)/(total*2);
  trendStatus[tf]=ds>=0.6?'down':us>=0.6?'up':'neutral';
  checkTrendConfirmation();
  broadcastDash({type:'trend',trendStatus,confirmedTrend});
}

function checkTrendConfirmation() {
  const dc=Object.values(trendStatus).filter(t=>t==='down').length;
  const was=confirmedTrend;
  confirmedTrend=dc>=cfg.minTFConfirm;
}

// ── STRUCTURE DETECTION — DO NOT MODIFY ──────────
function findStructuresInData(data) {
  if(data.length<10) return {smallStruct:null,bigStruct:null};
  const LR=2, peaks=[];
  for(let i=LR;i<data.length-LR;i++){
    let top=true;
    for(let j=i-LR;j<=i+LR;j++){
      if(j!==i&&data[j].high>=data[i].high){top=false;break;}
    }
    if(top) peaks.push({price:data[i].high,index:i});
  }
  if(peaks.length<2) return {smallStruct:null,bigStruct:null};

  function findBestGroup(minSpan,maxSpan){
    let best=null;
    for(let s=0;s<peaks.length-1;s++){
      const sp0=peaks[s+1].index-peaks[s].index;
      if(sp0<minSpan||sp0>maxSpan) continue;
      if(peaks[s+1].price>=peaks[s].price) continue;
      const bd=peaks[s].price-peaks[s+1].price;
      if(bd<=0) continue;
      const grp=[peaks[s],peaks[s+1]];
      for(let j=s+2;j<peaks.length;j++){
        const prev=grp[grp.length-1];
        const sp=peaks[j].index-prev.index;
        if(sp<minSpan||sp>maxSpan) continue;
        if(peaks[j].price>=prev.price) continue;
        const diff=prev.price-peaks[j].price;
        if(Math.abs(diff-bd)/bd<=0.10) grp.push(peaks[j]);
      }
      if(grp.length>=2){
        const tol=maxSpan===5?cfg.smallTol:cfg.bigTol;
        const cs=data.length-1-grp[grp.length-1].index;
        if(cs>tol) continue;
        const lp=grp[grp.length-1].price;
        let broken=false;
        for(let k=grp[grp.length-1].index+1;k<data.length;k++){
          if(Math.max(data[k].open,data[k].close)>lp+0.05){broken=true;break;}
        }
        if(broken) continue;
        if(!best||grp.length>best.peaks.length) best={peaks:grp,baseDiff:bd};
      }
    }
    return best;
  }
  return {smallStruct:findBestGroup(2,5),bigStruct:findBestGroup(5,15)};
}

// ── FIND ALL STRUCTURES — EVERY SECOND ───────────
function findLevels() {
  const tfs = cfg.scanTF==='M1'?['M1']:cfg.scanTF==='M5'?['M5']:['M1','M5'];
  const newStructures = [];

  for(const tf of tfs){
    const data=candles[tf];
    if(!data||data.length<10) continue;
    const result=findStructuresInData(data);

    // Add small structure if valid
    if(result.smallStruct){
      const existing = activeStructures.find(s=>
        s.type==='small' && s.tf===tf &&
        Math.abs(s.peaks[0].price - result.smallStruct.peaks[0].price) < 0.05
      );
      const tradedLevels = existing ? existing.tradedLevels : new Set();
      const projectedLevels = computeProjectedLevels(result.smallStruct, tradedLevels);
      newStructures.push({
        ...result.smallStruct,
        type:'small', tf, tradedLevels,
        projectedLevels,
        id: `small_${tf}_${result.smallStruct.peaks[0].price.toFixed(2)}`
      });
    }

    // Add big structure if valid
    if(result.bigStruct){
      const existing = activeStructures.find(s=>
        s.type==='big' && s.tf===tf &&
        Math.abs(s.peaks[0].price - result.bigStruct.peaks[0].price) < 0.05
      );
      const tradedLevels = existing ? existing.tradedLevels : new Set();
      const projectedLevels = computeProjectedLevels(result.bigStruct, tradedLevels);
      newStructures.push({
        ...result.bigStruct,
        type:'big', tf, tradedLevels,
        projectedLevels,
        id: `big_${tf}_${result.bigStruct.peaks[0].price.toFixed(2)}`
      });
    }
  }

  // Replace active structures
  activeStructures = newStructures;

  // Log new structures
  if(activeStructures.length > 0){
    setTicker(`📐 ${activeStructures.length} struct(s) active | ${activeStructures.map(s=>`${s.type.toUpperCase()}(${s.tf})`).join(', ')}`);
  } else {
    setTicker('⏳ Scanning for structures...');
  }

  // Send telegram for new projected levels (when 2+ TFs confirm downtrend)
  const downCount = Object.values(trendStatus).filter(t=>t==='down').length;
  if(downCount >= 2){
    activeStructures.forEach(s=>{
      if(s.projectedLevels && s.projectedLevels.length > 0){
        const np = s.projectedLevels[0];
        const key = np.toFixed(2) + '_' + s.id;
        if(!s._lastTeleLevel || Math.abs(s._lastTeleLevel - np) > 0.01){
          s._lastTeleLevel = np;
          const r1 = s.peaks.length>=2 ? s.peaks[s.peaks.length-2].price : null;
          const r2 = s.peaks[s.peaks.length-1].price;
          const diff = r1 ? Math.abs(r1-r2).toFixed(2) : s.baseDiff.toFixed(2);
          const mkt = MKT_NAMES[cfg.market] || cfg.market;
          telegram(`🎯 <b>NEXT LEVEL ACTIVE</b>\nLevel: <b>${np.toFixed(2)}</b>\n${r1?`R1: ${r1.toFixed(2)} | R2: ${r2.toFixed(2)}\n`:''}Diff: ${diff}\nMarket: ${mkt}\nCommand: ${cfg.command}\nStruct: ${s.type.toUpperCase()} (${s.tf})`);
        }
      }
    });
  }

  broadcastState();
}

function computeProjectedLevels(struct, tradedLevels) {
  if(!struct||!struct.peaks||struct.peaks.length<1) return [];
  const lastLevel = struct.peaks[struct.peaks.length-1].price;
  const projected = [];
  let np = parseFloat((lastLevel - struct.baseDiff).toFixed(2));
  for(let i=0;i<5;i++){
    if(!tradedLevels.has(np.toFixed(2)) && !isLevelInIgnoredZone(np) && !ignoredLevels.has(np.toFixed(2))){
      projected.push(np);
    }
    np = parseFloat((np - struct.baseDiff).toFixed(2));
  }
  return projected;
}

function isLevelInIgnoredZone(level) {
  return ignoreZones.some(z=>{
    const lo=Math.min(z.a,z.b), hi=Math.max(z.a,z.b);
    return level>=lo && level<=hi;
  });
}

// HTF auto-detection removed — zones set manually by user

// ── HTF ZONE — UPTREND STRUCTURE DETECTION ────────
// Uses manually set ignoreZones (A-B) to detect early reversal
// When price is in zone: watches for higher low + break above swing high
function checkHTFZoneUptrend() {
  if(!ignoreZones.length||!currentPrice) return;
  const data = candles['M1'];
  if(!data||data.length<20) return;

  // Find if price is near any manually set ignore zone
  const nearZone = ignoreZones.find(z=>{
    const lo=Math.min(z.a,z.b), hi=Math.max(z.a,z.b);
    return currentPrice >= lo * 0.995 && currentPrice <= hi * 1.01;
  });

  if(!nearZone) {
    // Price left the zone downward = resume if was paused
    if(htfZonePaused){
      const stillInAny = ignoreZones.some(z=>{
        const lo=Math.min(z.a,z.b);
        return currentPrice >= lo * 0.998;
      });
      if(!stillInAny){
        log('✅ Price broke out of HTF zone downward — resuming');
        htfZonePaused = false;
        setStatus('running','RUNNING');
        setTicker('✅ HTF zone cleared — resuming...');
        broadcastState();
      }
    }
    return;
  }

  // M1 downtrend must be present to trade in zone
  if(trendStatus['M1']!=='down') return;

  // Check for upward structure forming on M1 in this zone
  // Find recent swing lows and highs
  const recent = data.slice(-30);
  let swingLows = [], swingHighs = [];
  for(let i=2;i<recent.length-2;i++){
    if(recent[i].low<recent[i-1].low&&recent[i].low<recent[i-2].low&&
       recent[i].low<recent[i+1].low&&recent[i].low<recent[i+2].low){
      swingLows.push({price:recent[i].low,idx:i});
    }
    if(recent[i].high>recent[i-1].high&&recent[i].high>recent[i-2].high&&
       recent[i].high>recent[i+1].high&&recent[i].high>recent[i+2].high){
      swingHighs.push({price:recent[i].high,idx:i});
    }
  }

  // Detect: higher low formed AND price broke above previous swing high
  // This is the upward structure signal
  if(swingLows.length>=2 && swingHighs.length>=1){
    const lastLow = swingLows[swingLows.length-1];
    const prevLow = swingLows[swingLows.length-2];
    const lastHigh = swingHighs[swingHighs.length-1];

    // Higher low = potential uptrend start
    if(lastLow.price > prevLow.price && lastLow.idx > prevLow.idx){
      // Check if current price broke above the last swing high
      if(currentPrice > lastHigh.price && lastHigh.idx > prevLow.idx){
        if(!htfZonePaused){
          log(`⚠ Uptrend structure in HTF zone — pausing bot`);
          htfZonePaused = true;
          setStatus('scanning','PAUSED — HTF ZONE');
          const lo=Math.min(nearZone.a,nearZone.b), hi=Math.max(nearZone.a,nearZone.b);
          setTicker(`⚠ Uptrend forming in zone ${lo.toFixed(2)}–${hi.toFixed(2)} — paused`);
          telegram(`⚠ <b>Bot paused</b>\nUptrend structure forming in zone\nWill resume on breakout or full uptrend`);
          broadcastState();
        }
      }
    }
  }

  // Check if all M1/M5/M15 turned uptrend — safe to resume for next downtrend
  const upCount = Object.values(trendStatus).filter(t=>t==='up').length;
  if(htfZonePaused && upCount >= 3){
    log('✅ All TFs turned uptrend — HTF zone resolved, bot waiting for next downtrend');
    htfZonePaused = false;
    setStatus('running','RUNNING');
    setTicker('✅ All TFs uptrend — waiting for next downtrend to start...');
    broadcastState();
  }
}

// ── ENTRY CHECK ───────────────────────────────────
function checkEntry() {
  if(!botActive||inTrade||!confirmedTrend) return;
  if(lossCountdownPaused||timeOffPaused||htfZonePaused) return;
  if(!activeStructures.length) return;
  if(cfg.maxTrades>0&&tradeCount>=cfg.maxTrades){stopBot();return;}

  const data = candles['M1'];
  if(!data||data.length<3) return;

  // Check all active structures for valid entry
  for(const struct of activeStructures){
    if(!struct.projectedLevels||!struct.projectedLevels.length) continue;

    const target = struct.projectedLevels[0];
    if(struct.tradedLevels.has(target.toFixed(2))) continue;
    if(isLevelInIgnoredZone(target)) continue;
    if(ignoredLevels.has(target.toFixed(2))) continue;

    const pct    = cfg.proximityPct/100;
    const bd     = struct.baseDiff || 5;
    const maxGap = bd*(1-pct);
    const confirmCount = struct.type==='small'?cfg.smallConfirm:cfg.bigConfirm;

    // Find or create entry target state for this structure
    let et = entryTargets.find(e=>e.structId===struct.id&&Math.abs(e.level-target)<0.01);
    if(!et){
      et = {structId:struct.id, level:target, pricePassed:false, passedCount:0};
      entryTargets.push(et);
    }

    if(!et.pricePassed){
      let count=0;
      for(let i=data.length-1;i>=Math.max(0,data.length-40);i--){
        if(Math.max(data[i].open,data[i].close)<target) count++;
        else break;
      }
      if(count>=confirmCount){
        et.pricePassed=true; et.passedCount=count;
        setTicker(`✅ ${count} candles below ${target.toFixed(2)} [${struct.type}/${struct.tf}] — waiting pullback...`);
      } else {
        continue;
      }
    }

    if(currentPrice>=target){ et.pricePassed=false; et.passedCount=0; continue; }
    if(currentPrice<target-maxGap) continue;

    const last=data[data.length-1], prev=data[data.length-2];
    if(last.close<=prev.close) continue;

    // ✅ ENTRY
    setTicker(`⚡ ENTRY! ${currentPrice.toFixed(2)} at ${target.toFixed(2)} [${struct.type}/${struct.tf}]`);
    struct.tradedLevels.add(target.toFixed(2));
    // Recompute projected levels
    struct.projectedLevels = computeProjectedLevels(struct, struct.tradedLevels);
    currentActiveLevel = target;
    currentStructType  = struct.type;
    // Clear this entry target
    entryTargets = entryTargets.filter(e=>!(e.structId===struct.id&&Math.abs(e.level-target)<0.01));
    placeTrade();
    return; // only one trade at a time
  }
}

// ── PLACE TRADE ───────────────────────────────────
function placeTrade() {
  if(!derivWs||derivWs.readyState!==WebSocket.OPEN){inTrade=false;return;}
  inTrade=true;
  const duration=cfg.durationMins*60;
  const type={NOTOUCH:'NOTOUCH',TOUCH:'ONETOUCH',HIGHER:'CALL',LOWER:'PUT',RISE:'CALL',FALL:'PUT',CALL_MULT:'MULTUP',PUT_MULT:'MULTDOWN'}[cfg.command]||'NOTOUCH';

  if(cfg.command==='CALL_MULT'||cfg.command==='PUT_MULT'){
    derivWs.send(JSON.stringify({buy:1,price:cfg.stake,parameters:{contract_type:type,symbol:cfg.market,basis:'stake',amount:cfg.stake,currency:'USD',multiplier:cfg.multiplier}}));
    setTimeout(()=>{
      if(currentContractId&&derivWs?.readyState===WebSocket.OPEN)
        derivWs.send(JSON.stringify({proposal_open_contract:1,contract_id:currentContractId,subscribe:1}));
    },2000);
  } else {
    const params={contract_type:type,symbol:cfg.market,duration,duration_unit:'s',basis:'stake',amount:cfg.stake,currency:'USD'};
    if(['NOTOUCH','TOUCH','HIGHER','LOWER'].includes(cfg.command)) params.barrier=cfg.barrierOffset;
    derivWs.send(JSON.stringify({buy:1,price:cfg.stake,parameters:params}));
    setTimeout(()=>{
      if(currentContractId&&derivWs?.readyState===WebSocket.OPEN)
        derivWs.send(JSON.stringify({proposal_open_contract:1,contract_id:currentContractId}));
    },(duration+5)*1000);
  }
  broadcastState();
}

// ── LOSS CONTROL ──────────────────────────────────
function startLossCountdown(totalSecs) {
  stopLossCountdown();
  lossCountdownPaused=true; lossCountdownRemaining=totalSecs; lossCountdownTotal=totalSecs;
  const label=totalSecs===1800?'30 MIN':totalSecs===3600?'1 HR':'4 HR';
  log(`⏸ Cooldown: ${label}`);
  setStatus('scanning','PAUSED — COOLDOWN');
  lossCountdownTimer=setInterval(()=>{
    lossCountdownRemaining--;
    broadcastDash({type:'countdown',remaining:lossCountdownRemaining,total:lossCountdownTotal});
    if(lossCountdownRemaining<=0) resumeAfterCooldown();
  },1000);
}

function stopLossCountdown() {
  if(lossCountdownTimer){clearInterval(lossCountdownTimer);lossCountdownTimer=null;}
}

function resumeAfterCooldown() {
  lossCountdownPaused=false; stopLossCountdown();
  log('✅ Cooldown done — resuming');
  setStatus('running','RUNNING');
  setTicker('✅ Cooldown done — scanning...');
  broadcastState();
  if(botActive) findLevels();
}

// ── TAKE TIME OFF ─────────────────────────────────
function startTimeOff(totalSecs) {
  stopTimeOff();
  timeOffPaused=true; timeOffRemaining=totalSecs; timeOffTotal=totalSecs;
  const label=totalSecs===1200?'20 MIN':totalSecs===1800?'30 MIN':'1 HR';
  log(`⏰ Time off: ${label}`);
  setStatus('scanning','TIME OFF');
  timeOffTimer=setInterval(()=>{
    timeOffRemaining--;
    broadcastDash({type:'time_off',remaining:timeOffRemaining,total:timeOffTotal});
    if(timeOffRemaining<=0) resumeAfterTimeOff();
  },1000);
}

function stopTimeOff() {
  if(timeOffTimer){clearInterval(timeOffTimer);timeOffTimer=null;}
}

function resumeAfterTimeOff() {
  timeOffPaused=false; stopTimeOff();
  log('✅ Time off done — resuming');
  setStatus('running','RUNNING');
  setTicker('✅ Time off done — scanning...');
  broadcastState();
  if(botActive) findLevels();
}

// ── RESULT ────────────────────────────────────────
function finalizeResult(profit) {
  if(!inTrade) return;
  inTrade=false; tradeCount++; sessionPnl+=profit;
  const won=profit>0;
  if(won) wins++; else losses++;
  const wr=Math.round((wins/tradeCount)*100);

  const card={
    id: Date.now(), // unique ID for filtering
    tradeNum: tradeCount,
    time: new Date().toLocaleTimeString(),
    date: new Date().toLocaleDateString(),
    timestamp: Date.now(),
    won, profit,
    level: currentActiveLevel?.toFixed(2),
    struct: currentStructType,
    command: cfg.command,
    market: cfg.market,
    stake: cfg.stake,
    wr,
  };

  tradeLog.unshift(card);
  if(tradeLog.length>500) tradeLog.pop();
  saveData(); // persist to disk

  log(`${won?'✅ WIN':'❌ LOSS'} #${tradeCount} | ${profit>=0?'+':''}$${profit.toFixed(2)} | P&L: ${sessionPnl>=0?'+':''}$${sessionPnl.toFixed(2)} | WR: ${wr}%`);

  // Telegram: trade result only
  const mkt = MKT_NAMES[cfg.market]||cfg.market;
  telegram(`${won?'✅ WIN':'❌ LOSS'}\nLevel: <b>${currentActiveLevel?.toFixed(2)}</b>\nProfit: <b>${profit>=0?'+':''}$${profit.toFixed(2)}</b>\nMarket: ${mkt}\nCommand: ${cfg.command}\nWR: ${wr}%`);

  currentContractId=null;
  broadcastDash({type:'trade',card});
  broadcastState();

  if(won){
    consecutiveLosses=0;
    setTicker(`✅ WIN +$${profit.toFixed(2)} — scanning...`);
    setTimeout(()=>{if(botActive)findLevels();},1000);
  } else {
    consecutiveLosses++;
    if(consecutiveLosses>=cfg.maxConsecLosses){
      botActive=false; lossCountdownPaused=false; stopLossCountdown(); stopScanner();
      setStatus('stopped',`STOPPED — ${cfg.maxConsecLosses} LOSSES`);
      setTicker(`🛑 ${cfg.maxConsecLosses} consecutive losses — restart manually`);
      log(`🛑 Stopped after ${cfg.maxConsecLosses} consecutive losses`);
      broadcastState();
    } else {
      setTicker(`❌ LOSS — starting cooldown...`);
      startLossCountdown(cfg.cooldownSecs);
    }
  }
}

// ── WEBSOCKET TO DERIV ────────────────────────────
function connectDeriv() {
  if(derivWs){try{derivWs.terminate();}catch(e){}}
  log('🔌 Connecting to Deriv...');
  setStatus('connecting','CONNECTING');

  // Detect token type: new PAT (pat_...) or old short token
  const isPAT = cfg.apiToken && cfg.apiToken.startsWith('pat_');

  if(isPAT){
    connectWithPAT();
  } else {
    connectWithLegacyToken();
  }
}

async function connectWithPAT() {
  try {
    // Step 1: Get account list to find account ID
    if(!derivAccountId){
      log('🔑 PAT token — fetching account info...');
      const accRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', {
        headers: {
          'Authorization': `Bearer ${cfg.apiToken}`,
          'Deriv-App-ID': String(APP_ID)
        }
      });
      const accData = await accRes.json();
      if(!accData.data || !accData.data.length){
        log('❌ Could not get account list: ' + JSON.stringify(accData));
        setStatus('stopped','AUTH FAILED');
        userStarted=false;
        broadcastState();
        return;
      }
      // Pick real account if available, else demo
      const realAcc = accData.data.find(a=>!a.is_virtual) || accData.data[0];
      derivAccountId = realAcc.account_id;
      log(`✅ Account: ${derivAccountId}`);
    }

    // Step 2: Get OTP WebSocket URL
    const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${derivAccountId}/otp`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${cfg.apiToken}`,
        'Deriv-App-ID': String(APP_ID)
      }
    });
    const otpData = await otpRes.json();
    if(!otpData.data || !otpData.data.url){
      log('❌ Could not get OTP URL: ' + JSON.stringify(otpData));
      setStatus('stopped','AUTH FAILED');
      userStarted=false;
      broadcastState();
      return;
    }

    log('🔌 Connecting with OTP URL...');
    doConnect(otpData.data.url);

  } catch(e) {
    log('❌ PAT connection error: ' + e.message);
    setStatus('stopped','CONNECTION ERROR');
    userStarted=false;
    broadcastState();
  }
}

function connectWithLegacyToken() {
  doConnect(`wss://ws.binaryws.com/websockets/v3?app_id=${APP_ID}`);
}

function doConnect(wsUrl) {
  derivWs=new WebSocket(wsUrl);

  derivWs.on('open',()=>{
    const isPAT = cfg.apiToken && cfg.apiToken.startsWith('pat_');
    if(isPAT){
      // PAT + OTP URL = already authenticated, no authorize message needed
      log('🔗 Connected via OTP — fetching balance...');
      derivWs.send(JSON.stringify({balance:1}));
      derivWs.send(JSON.stringify({ticks:cfg.market,subscribe:1}));
      ['M1','M5','M15','H1','H4','D1'].forEach(tf=>fetchCandles(tf));
      botActive=true;
      setStatus('running','RUNNING');
      startScanner();
      broadcastState();
      log(`✅ Bot started | Market: ${cfg.market} | ${cfg.command} | $${cfg.stake}`);
    } else {
      log('🔗 Connected — authorizing...');
      derivWs.send(JSON.stringify({authorize:cfg.apiToken}));
    }
  });

  derivWs.on('message',(raw)=>{
    let d; try{d=JSON.parse(raw);}catch(e){return;}

    if(d.msg_type==='balance'){
      log(`💰 Balance: $${d.balance?.balance || 0}`);
      broadcastDash({type:'balance', balance: d.balance?.balance || 0});
    }

    if(d.msg_type==='authorize'){
      if(d.error){log('❌ Auth: '+d.error.message);setStatus('stopped','AUTH FAILED');userStarted=false;broadcastState();derivWs.close();return;}
      log(`✅ Auth: ${d.authorize.loginid} | $${d.authorize.balance}`);
      botActive=true;
      setStatus('running','RUNNING');
      derivWs.send(JSON.stringify({ticks:cfg.market,subscribe:1}));
      // Subscribe all timeframes
      ['M1','M5','M15','H1','H4','D1'].forEach(tf=>fetchCandles(tf));
      startScanner();
      broadcastState();
    }

    if(d.msg_type==='tick'){
      currentPrice=parseFloat(d.tick.quote);
      broadcastDash({type:'price',price:currentPrice});
      if(botActive&&!inTrade){
        checkEntry();
        checkHTFZoneUptrend();
      }
    }

    if(d.msg_type==='candles'){
      const gran=d.echo_req.granularity;
      const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===3600?'H1':gran===14400?'H4':'D1';
      candles[tf]=d.candles.map(c=>({time:c.epoch,open:parseFloat(c.open),high:parseFloat(c.high),low:parseFloat(c.low),close:parseFloat(c.close)}));
      log(`📊 ${tf}: ${candles[tf].length} candles`);
      if(['M1','M5','M15'].includes(tf)) analyzeTrend(tf);
      broadcastDash({type:'candles',tf,candles:candles[tf].slice(-100)});
    }

    if(d.msg_type==='ohlc'){
      const gran=d.ohlc.granularity;
      const tf=gran===60?'M1':gran===300?'M5':gran===900?'M15':gran===3600?'H1':gran===14400?'H4':'D1';
      const c={time:d.ohlc.open_time,open:parseFloat(d.ohlc.open),high:parseFloat(d.ohlc.high),low:parseFloat(d.ohlc.low),close:parseFloat(d.ohlc.close)};
      if(!candles[tf]) candles[tf]=[];
      if(candles[tf].length&&candles[tf][candles[tf].length-1].time===c.time) candles[tf][candles[tf].length-1]=c;
      else{candles[tf].push(c);if(candles[tf].length>300)candles[tf].shift();}
      if(['M1','M5','M15'].includes(tf)) analyzeTrend(tf);
      broadcastDash({type:'candle_update',tf,candle:c});
    }

    if(d.msg_type==='buy'){
      if(d.error){log('❌ '+d.error.message);inTrade=false;broadcastState();return;}
      currentContractId=d.buy.contract_id;
      log(`📝 Contract: ${currentContractId}`);
    }

    if(d.msg_type==='proposal_open_contract'){
      const con=d.proposal_open_contract; if(!con) return;
      const profit=parseFloat(con.profit)||0;
      if(cfg.command==='CALL_MULT'||cfg.command==='PUT_MULT'){
        if(profit>=cfg.takeProfit||profit<=-cfg.stopLoss)
          derivWs.send(JSON.stringify({sell:currentContractId,price:0}));
      }
      if(con.status==='sold'||con.is_expired||con.is_settleable) finalizeResult(profit);
    }

    if(d.msg_type==='sell'){
      if(d.sell) finalizeResult(parseFloat(d.sell.sold_for)-cfg.stake);
    }
  });

  derivWs.on('close',()=>{
    log('Disconnected');
    botActive=false; stopScanner();
    setStatus('stopped','DISCONNECTED');
    broadcastState();
    if(userStarted){
      log('Reconnecting in 5s...');
      if(reconnectTimer) clearTimeout(reconnectTimer);
      // Reset account ID so fresh OTP is fetched on reconnect
      derivAccountId=null;
      reconnectTimer=setTimeout(connectDeriv,5000);
    }
  });

  derivWs.on('error',(e)=>log('WS error: '+e.message));
}

function fetchCandles(tf) {
  if(!derivWs||derivWs.readyState!==WebSocket.OPEN) return;
  const gran=tf==='M1'?60:tf==='M5'?300:tf==='M15'?900:tf==='H1'?3600:tf==='H4'?14400:86400;
  derivWs.send(JSON.stringify({ticks_history:cfg.market,adjust_start_time:1,count:200,end:'latest',granularity:gran,start:1,style:'candles',subscribe:1}));
}

function startScanner() {
  if(scanInterval) clearInterval(scanInterval);
  findLevels();
  scanInterval=setInterval(()=>{
    if(!botActive||inTrade||lossCountdownPaused||timeOffPaused) return;
    findLevels();
  },1000); // every second
}

function stopScanner() {
  if(scanInterval){clearInterval(scanInterval);scanInterval=null;}
}

function stopBot() {
  userStarted=false;
  botActive=false; stopScanner(); stopLossCountdown(); stopTimeOff();
  if(derivWs){try{derivWs.close();}catch(e){}}
  setStatus('stopped','STOPPED');
  setTicker('— GODZILLA STOPPED —');
  broadcastState();
}

// ── DASHBOARD WEBSOCKET ───────────────────────────
dashWss.on('connection',(ws)=>{
  log('📱 Dashboard connected');
  ws.send(JSON.stringify({type:'state',...getFullState()}));
  Object.keys(candles).forEach(tf=>{
    if(candles[tf]&&candles[tf].length)
      ws.send(JSON.stringify({type:'candles',tf,candles:candles[tf].slice(-100)}));
  });

  ws.on('message',(raw)=>{
    let msg; try{msg=JSON.parse(raw);}catch(e){return;}

    if(msg.type==='start'){
      if(msg.cfg) cfg={...cfg,...msg.cfg};
      if(!cfg.apiToken){ws.send(JSON.stringify({type:'error',msg:'No API token'}));return;}
      tradeCount=0;wins=0;losses=0;sessionPnl=0;
      consecutiveLosses=0;lossCountdownPaused=false;
      activeStructures=[]; entryTargets=[];
      userStarted=true;
      saveData();
      connectDeriv();
    }

    if(msg.type==='stop') stopBot();
    if(msg.type==='skip_cooldown'&&lossCountdownPaused) resumeAfterCooldown();
    if(msg.type==='time_off') startTimeOff(msg.secs);
    if(msg.type==='cancel_time_off') resumeAfterTimeOff();

    if(msg.type==='ignore_level'){
      const lv = parseFloat(msg.level).toFixed(2);
      if(ignoredLevels.has(lv)){ ignoredLevels.delete(lv); log(`✅ Level ${lv} un-ignored`); }
      else { ignoredLevels.add(lv); log(`🚫 Level ${lv} ignored`); }
      // Recompute projected levels
      activeStructures.forEach(s=>{ s.projectedLevels=computeProjectedLevels(s,s.tradedLevels); });
      broadcastState();
    }

    if(msg.type==='add_zone'){
      ignoreZones.push({a:parseFloat(msg.a),b:parseFloat(msg.b)});
      log(`🚫 Zone added: ${msg.a}–${msg.b}`);
      activeStructures.forEach(s=>{ s.projectedLevels=computeProjectedLevels(s,s.tradedLevels); });
      broadcastState();
    }

    if(msg.type==='remove_zone'){
      ignoreZones.splice(msg.idx,1);
      log('✅ Zone removed');
      activeStructures.forEach(s=>{ s.projectedLevels=computeProjectedLevels(s,s.tradedLevels); });
      broadcastState();
    }

    if(msg.type==='update_cfg'){
      cfg={...cfg,...msg.cfg};
      saveData();
      log('⚙ Settings updated');
    }

    if(msg.type==='get_state') ws.send(JSON.stringify({type:'state',...getFullState()}));

    if(msg.type==='get_history'){
      ws.send(JSON.stringify({type:'full_history',tradeLog}));
    }
  });

  ws.on('close',()=>log('📱 Dashboard disconnected'));
});

function getFullState() {
  return {
    botActive, currentPrice, trendStatus, confirmedTrend,
    activeStructures: activeStructures.map(s=>({
      peaks:s.peaks, baseDiff:s.baseDiff, type:s.type, tf:s.tf,
      projectedLevels:s.projectedLevels, tradedLevels:[...s.tradedLevels], id:s.id
    })),
    ignoredLevels:[...ignoredLevels],
    ignoreZones, htfSupportZones, htfZonePaused,
    entryTargets, currentActiveLevel, currentStructType,
    tradeCount,wins,losses,sessionPnl,consecutiveLosses,
    lossCountdownPaused,lossCountdownRemaining,lossCountdownTotal,
    timeOffPaused,timeOffRemaining,timeOffTotal,
    tickerMsg,statusText,cfg,
    tradeLog:tradeLog.slice(0,100),
  };
}

// ── REST ──────────────────────────────────────────
app.get('/ping',(req,res)=>res.send('OK'));
app.get('/api/state',(req,res)=>res.json(getFullState()));
app.get('/api/history',(req,res)=>res.json(tradeLog));

// Status every 5 min
setInterval(()=>{
  if(!botActive) return;
  const wr=tradeCount>0?Math.round((wins/tradeCount)*100):0;
  log(`📊 Price:${currentPrice} Trades:${tradeCount} WR:${wr}% P&L:${sessionPnl>=0?'+':''}$${sessionPnl.toFixed(2)} Structs:${activeStructures.length} HTFZone:${htfZonePaused?'PAUSED':'OK'}`);
},5*60*1000);

server.listen(PORT,()=>{
  log(`🦎 GODZILLA EL ROI running on port ${PORT}`);
});

process.on('SIGINT',()=>{stopBot();saveData();setTimeout(()=>process.exit(0),1000);});
process.on('SIGTERM',()=>{stopBot();saveData();setTimeout(()=>process.exit(0),1000);});
