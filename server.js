const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const APP_ID = process.env.DERIV_APP_ID || '1089';
const WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`;

// ─── SSE ─────────────────────────────────────────────────────────────────────
const sseClients = [];
function pushSSE(data) {
  const p = `data: ${JSON.stringify(data)}\n\n`;
  sseClients.forEach(r => r.write(p));
}

// ─── Deriv account ────────────────────────────────────────────────────────────
class DerivAccount {
  constructor() {
    this.token = ''; this.ws = null; this.ready = false;
    this.loginid = null; this.balance = null; this.currency = null;
    this.reqs = new Map(); this.watchers = new Map(); this.rid = 1; this.ping = null;
  }
  setToken(t) {
    if (!t || t === this.token) return;
    this.token = t; this.ready = false;
    if (this.ws) { try { this.ws.close(); } catch {} }
  }
  connect() {
    return new Promise((resolve, reject) => {
      if (!this.token) return reject(new Error('No API token — enter it in the dashboard'));
      this.ws = new WebSocket(WS_URL);
      this.ws.on('open', () => {
        this.send({ authorize: this.token }).then(r => {
          if (r.error) return reject(new Error('Auth failed: ' + r.error.message));
          this.ready = true;
          this.loginid = r.authorize?.loginid;
          this.balance = r.authorize?.balance;
          this.currency = r.authorize?.currency;
          slog('Account connected', { loginid: this.loginid });
          this.ping = setInterval(() => {
            if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ ping: 1 }));
          }, 25000);
          this.send({ balance: 1, subscribe: 1 }).catch(() => {});
          pushSSE({ type: 'account', data: this.info() });
          resolve();
        }).catch(reject);
      });
      this.ws.on('message', raw => {
        let msg; try { msg = JSON.parse(raw); } catch { return; }
        if (msg.req_id && this.reqs.has(msg.req_id)) {
          const { res } = this.reqs.get(msg.req_id);
          this.reqs.delete(msg.req_id); res(msg);
        }
        if (msg.msg_type === 'proposal_open_contract') {
          const c = msg.proposal_open_contract;
          if (c && this.watchers.has(c.contract_id)) this.watchers.get(c.contract_id)(msg);
        }
        if (msg.msg_type === 'balance') {
          this.balance = msg.balance?.balance;
          pushSSE({ type: 'account', data: this.info() });
        }
      });
      this.ws.on('error', e => slog('WS error', { error: e.message }));
      this.ws.on('close', () => {
        this.ready = false;
        if (this.ping) clearInterval(this.ping);
        pushSSE({ type: 'account', data: this.info() });
      });
    });
  }
  send(obj) {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN)
        return reject(new Error('Socket not open'));
      const req_id = this.rid++;
      this.reqs.set(req_id, { res: resolve });
      this.ws.send(JSON.stringify({ ...obj, req_id }));
      setTimeout(() => {
        if (this.reqs.has(req_id)) { this.reqs.delete(req_id); reject(new Error('Request timed out')); }
      }, 15000);
    });
  }
  buy(params) { return this.send({ buy: 1, price: params.amount, parameters: params }); }
  watchContract(contractId) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { this.watchers.delete(contractId); reject(new Error('Contract watch timed out')); }, 5 * 60 * 1000);
      this.watchers.set(contractId, msg => {
        const c = msg.proposal_open_contract;
        if (!c) return;
        if (c.is_sold || c.is_expired) {
          clearTimeout(t); this.watchers.delete(contractId);
          this.send({ forget: msg.subscription?.id }).catch(() => {});
          resolve(c);
        }
      });
      this.send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1 })
        .catch(e => { clearTimeout(t); this.watchers.delete(contractId); reject(e); });
    });
  }
  subscribeCandles(symbol, onCandle) {
    const ws = new WebSocket(WS_URL);
    ws.on('open', () => ws.send(JSON.stringify({ ticks_history: symbol, adjust_start_time: 1, count: 30, end: 'latest', granularity: 60, style: 'candles', subscribe: 1 })));
    ws.on('message', raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      if (msg.msg_type === 'candles' && msg.candles) msg.candles.forEach(c => onCandle(c, false));
      if (msg.msg_type === 'ohlc' && msg.ohlc) onCandle(msg.ohlc, true);
    });
    ws.on('error', () => {});
    return ws;
  }
  subscribeTicks(symbol, onTick) {
    const ws = new WebSocket(WS_URL);
    ws.on('open', () => ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 })));
    ws.on('message', raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      if (msg.msg_type === 'tick' && msg.tick) onTick(parseFloat(msg.tick.quote));
    });
    ws.on('error', () => {});
    return ws;
  }
  info() { return { ready: this.ready, loginid: this.loginid, balance: this.balance, currency: this.currency }; }
}

const account = new DerivAccount();

// ─── fire a single trade ──────────────────────────────────────────────────────
async function fireTrade(slot, contractType, barrier) {
  if (!account.ready) throw new Error('Account not connected');
  const s = slot.settings;
  const params = {
    amount: Number(s.stake), basis: 'stake',
    contract_type: contractType, currency: 'USD',
    duration: Number(s.duration_value), duration_unit: s.duration_unit, symbol: s.symbol,
  };
  if (barrier != null) params.barrier = String(barrier);
  slot.log(`Opening ${contractType}${barrier ? ' barrier:' + barrier : ''} | ${s.duration_value}${s.duration_unit} | $${s.stake}`);
  const result = await account.buy(params);
  if (result.error) throw new Error(result.error.message);
  const cid = result.buy.contract_id;
  slot.log(`Contract #${cid} open`);
  const settled = await account.watchContract(cid);
  const profit = parseFloat(settled.profit || 0);
  const won = profit > 0;
  slot.stats.trades++;
  slot.stats[won ? 'wins' : 'losses']++;
  slot.stats.profit = parseFloat((slot.stats.profit + profit).toFixed(2));
  slot.log(`${won ? '✓ WIN' : '✗ LOSS'} $${Math.abs(profit).toFixed(2)}`, won ? 'win' : 'loss');
  const rec = { time: new Date().toISOString(), contract_id: cid, symbol: s.symbol, type: contractType, barrier, stake: s.stake, profit, won };
  slot.history.unshift(rec);
  if (slot.history.length > 100) slot.history.pop();
  slot.pushState();
  pushSSE({ type: 'trade', slotId: slot.id, data: rec });
  return { won, profit };
}

// ─── momentum check (Logic 1 & 2) ────────────────────────────────────────────
// Called at the exact moment of trigger — instant, no waiting
function checkMomentum(slot) {
  const s = slot.settings;
  if (!s.momentum_check) return true;
  const buf = slot.buffer;
  const n = Math.min(s.momentum_candles, buf.length);
  if (n < 2) return true;

  if (s.observe_mode === 'ticks') {
    const recent = buf.slice(-n);
    const allUp   = recent.every((p, i) => i === 0 || p >= recent[i-1]);
    const allDown  = recent.every((p, i) => i === 0 || p <= recent[i-1]);
    if (!allUp && !allDown) {
      slot.log(`Momentum check failed — last ${n} ticks not directional, skipping`, 'err');
      return false;
    }
    // Price not dead-center of zone
    if (slot.lockedHigh && slot.lockedLow) {
      const range = slot.lockedHigh - slot.lockedLow;
      const pos = (recent[recent.length-1] - slot.lockedLow) / range;
      if (pos > 0.35 && pos < 0.65) {
        slot.log(`Momentum check failed — price in dead center of zone, skipping`, 'err');
        return false;
      }
    }
  } else {
    const recent = buf.slice(-n);
    const bullish = recent.every(c => parseFloat(c.close) >= parseFloat(c.open));
    const bearish = recent.every(c => parseFloat(c.close) <= parseFloat(c.open));
    if (!bullish && !bearish) {
      slot.log(`Momentum check failed — last ${n} candles not directional, skipping`, 'err');
      return false;
    }
    // Body size: last candle should be at least momentum_body_mult * average
    if (s.momentum_body_mult > 0 && buf.length >= 3) {
      const avgBody = buf.slice(-10).reduce((sum, c) => sum + Math.abs(parseFloat(c.close) - parseFloat(c.open)), 0) / Math.min(10, buf.length);
      const last = recent[recent.length-1];
      const lastBody = Math.abs(parseFloat(last.close) - parseFloat(last.open));
      if (lastBody < avgBody * s.momentum_body_mult) {
        slot.log(`Momentum check failed — breakout candle body too small (${lastBody.toFixed(4)} < ${(avgBody * s.momentum_body_mult).toFixed(4)}), skipping`, 'err');
        return false;
      }
    }
  }
  return true;
}

// ─── trend filter (Logic 3) ───────────────────────────────────────────────────
// Checks trendBuffer (always M1 candles) — runs before any entry for Logic 3
function checkTrending(slot) {
  const s = slot.settings;
  if (!s.trend_check) return true;
  const buf = slot.trendBuffer;
  const n = Math.min(s.trend_candles, buf.length);
  if (n < 3) return true; // not enough data yet — allow

  const candles = buf.slice(-n);

  // 1. Average body size
  const avgBody = candles.reduce((sum, c) => sum + Math.abs(parseFloat(c.close) - parseFloat(c.open)), 0) / n;
  if (avgBody < s.min_body_size) {
    slot.log(`Trend filter: avg body ${avgBody.toFixed(4)} < min ${s.min_body_size} — choppy market, skipping`, 'err');
    return false;
  }

  // 2. Candle overlap ratio — high overlap = ranging
  let totalOverlap = 0;
  for (let i = 1; i < candles.length; i++) {
    const prev = candles[i-1], curr = candles[i];
    const oH = Math.min(parseFloat(prev.high), parseFloat(curr.high));
    const oL = Math.max(parseFloat(prev.low),  parseFloat(curr.low));
    const overlap = Math.max(0, oH - oL);
    const range = Math.max(parseFloat(prev.high) - parseFloat(prev.low), parseFloat(curr.high) - parseFloat(curr.low));
    totalOverlap += range > 0 ? overlap / range : 1;
  }
  const avgOverlap = totalOverlap / (candles.length - 1);
  if (avgOverlap > s.max_overlap) {
    slot.log(`Trend filter: overlap ${avgOverlap.toFixed(2)} > max ${s.max_overlap} — ranging market, skipping`, 'err');
    return false;
  }

  // 3. Consecutive directional candles
  let maxConsec = 1, currConsec = 1;
  for (let i = 1; i < candles.length; i++) {
    const pb = parseFloat(candles[i-1].close) >= parseFloat(candles[i-1].open);
    const cb = parseFloat(candles[i].close)   >= parseFloat(candles[i].open);
    if (pb === cb) { currConsec++; maxConsec = Math.max(maxConsec, currConsec); } else currConsec = 1;
  }
  if (maxConsec < s.min_dir_candles) {
    slot.log(`Trend filter: max consecutive ${maxConsec} < min ${s.min_dir_candles} — no trend, skipping`, 'err');
    return false;
  }

  return true;
}

// ─── market slot ──────────────────────────────────────────────────────────────
class MarketSlot {
  constructor(index) {
    this.index = index;
    this.id = `slot${index}`;
    this.settings = {
      symbol: ['1HZ10V','1HZ25V','1HZ50V','1HZ75V','1HZ100V','1HZ10V','1HZ25V','1HZ50V','1HZ75V','1HZ100V'][index],
      logic: 1,
      observe_mode: 'ticks',
      observe_count: 5,
      reaction_range: 0.3,
      confirm_count: 1,
      stake: 1,
      duration_value: 5,
      duration_unit: 't',
      barrier1: '+0.25',
      barrier2: '-0.25',
      contract_type: 'ONETOUCH',
      rest_seconds: 30,
      // Momentum filter — Logic 1 & 2
      momentum_check: true,
      momentum_candles: 3,
      momentum_body_mult: 1.0,
      // Trend filter — Logic 3
      trend_check: true,
      trend_candles: 5,
      min_body_size: 0.05,
      max_overlap: 0.5,
      min_dir_candles: 3,
    };
    this.running = false;
    this.busy = false;
    this.warming = false;
    this.logs = [];
    this.stats = { trades: 0, wins: 0, losses: 0, profit: 0 };
    this.history = [];
    this.dataWs = null;
    this.trendWs = null;
    this.buffer = [];
    this.trendBuffer = [];
    this.phase = 'watching';
    this.lockedHigh = null;
    this.lockedLow = null;
    this.confirmSide = null;
    this.confirmBuf = [];
  }

  log(msg, level = 'info') {
    const entry = { time: new Date().toISOString(), msg, level };
    this.logs.push(entry);
    if (this.logs.length > 300) this.logs.shift();
    pushSSE({ type: 'log', slotId: this.id, entry });
  }

  pushState() {
    pushSSE({ type: 'state', slotId: this.id, data: { running: this.running, busy: this.busy, stats: this.stats, phase: this.phase } });
  }

  reset() {
    this.phase = 'watching';
    this.lockedHigh = null;
    this.lockedLow = null;
    this.confirmSide = null;
    this.confirmBuf = [];
    this.buffer = [];
    this.warming = false;
  }

  snapshot() {
    return { id: this.id, index: this.index, running: this.running, busy: this.busy, stats: this.stats, settings: this.settings, phase: this.phase };
  }
}

const slots = Array.from({ length: 10 }, (_, i) => new MarketSlot(i));
const slotMap = new Map(slots.map(s => [s.id, s]));
function slog(msg, data) { console.log(`[${new Date().toISOString()}] ${msg}`, data || ''); }

// ─── helpers ──────────────────────────────────────────────────────────────────
function getPrices(buffer, mode) {
  if (mode === 'candles') return buffer.map(c => [parseFloat(c.high), parseFloat(c.low)]).flat();
  return buffer.map(p => parseFloat(p));
}

// ─── Logic 1: Zone Touch ──────────────────────────────────────────────────────
function onDataLogic1(slot, value) {
  if (!slot.running || slot.busy || slot.warming) return;
  slot.buffer.push(value);
  if (slot.buffer.length > slot.settings.observe_count) slot.buffer.shift();
  if (slot.buffer.length < slot.settings.observe_count) return;

  const prices = getPrices(slot.buffer, slot.settings.observe_mode);
  const high = Math.max(...prices), low = Math.min(...prices);
  const spread = parseFloat((high - low).toFixed(5));

  if (spread <= slot.settings.reaction_range) {
    // Lock zone for dead-center check in momentum
    slot.lockedHigh = high; slot.lockedLow = low;
    if (!checkMomentum(slot)) { slot.lockedHigh = null; slot.lockedLow = null; return; }

    slot.log(`Consolidation + momentum confirmed — spread: ${spread} | firing both barriers`);
    slot.busy = true; slot.buffer = []; slot.pushState();

    const p1 = fireTrade(slot, 'ONETOUCH', slot.settings.barrier1);
    const p2 = fireTrade(slot, 'ONETOUCH', slot.settings.barrier2);
    Promise.all([p1, p2]).catch(e => slot.log('Trade error: ' + e.message, 'err')).finally(() => afterTrade(slot));
  }
}

// ─── Logic 2 & 3: Reversal — watching phase ───────────────────────────────────
function onDataLogic23_watching(slot, value) {
  if (!slot.running || slot.busy || slot.phase !== 'watching' || slot.warming) return;
  slot.buffer.push(value);
  if (slot.buffer.length > slot.settings.observe_count) slot.buffer.shift();
  if (slot.buffer.length < slot.settings.observe_count) return;

  // Logic 3: trend filter before even detecting consolidation
  if (slot.settings.logic === 3 && !checkTrending(slot)) return;

  const prices = getPrices(slot.buffer, slot.settings.observe_mode);
  const high = Math.max(...prices), low = Math.min(...prices);
  const spread = parseFloat((high - low).toFixed(5));

  if (spread <= slot.settings.reaction_range) {
    slot.lockedHigh = high; slot.lockedLow = low;
    slot.phase = 'confirming';
    slot.confirmBuf = []; slot.confirmSide = null;
    slot.log(`Consolidation locked — HIGH: ${high} | LOW: ${low} | spread: ${spread}`);
    slot.log('Waiting for price to touch HIGH or LOW…');
    slot.pushState();
  }
}

// ─── Logic 2 & 3: Reversal — confirming phase ─────────────────────────────────
function onDataLogic23_confirming(slot, price) {
  if (!slot.running || slot.busy || slot.phase !== 'confirming') return;

  // First touch — determine side (first wins, other ignored)
  if (!slot.confirmSide) {
    if (price >= slot.lockedHigh) {
      slot.confirmSide = 'high'; slot.confirmBuf = [price];
      slot.log(`Price touched HIGH (${slot.lockedHigh}) — waiting ${slot.settings.confirm_count} confirmation tick(s)/candle(s) back`);
    } else if (price <= slot.lockedLow) {
      slot.confirmSide = 'low'; slot.confirmBuf = [price];
      slot.log(`Price touched LOW (${slot.lockedLow}) — waiting ${slot.settings.confirm_count} confirmation tick(s)/candle(s) back`);
    }
    return;
  }

  slot.confirmBuf.push(price);

  if (slot.confirmSide === 'high') {
    const pullbacks = slot.confirmBuf.filter(p => p < slot.lockedHigh);
    if (pullbacks.length >= slot.settings.confirm_count) {
      // Momentum check at exact moment of entry
      if (!checkMomentum(slot)) {
        slot.confirmSide = null; slot.confirmBuf = [];
        slot.log('Momentum check failed at confirmation — resetting, watching HIGH/LOW again');
        return;
      }
      executeReversal(slot, 'high');
    }
  } else if (slot.confirmSide === 'low') {
    const bounces = slot.confirmBuf.filter(p => p > slot.lockedLow);
    if (bounces.length >= slot.settings.confirm_count) {
      if (!checkMomentum(slot)) {
        slot.confirmSide = null; slot.confirmBuf = [];
        slot.log('Momentum check failed at confirmation — resetting, watching HIGH/LOW again');
        return;
      }
      executeReversal(slot, 'low');
    }
  }
}

function executeReversal(slot, side) {
  slot.phase = 'trading'; slot.busy = true; slot.pushState();
  const barrier = side === 'high' ? slot.settings.barrier1 : slot.settings.barrier2;
  const ct = slot.settings.logic === 2 ? 'ONETOUCH' : slot.settings.contract_type;
  slot.log(`Confirmation met — firing ${ct} | barrier: ${barrier} | ${side.toUpperCase()} reversal`);
  fireTrade(slot, ct, barrier)
    .catch(e => slot.log('Trade error: ' + e.message, 'err'))
    .finally(() => afterTrade(slot));
}

// ─── after trade ─────────────────────────────────────────────────────────────
function afterTrade(slot) {
  if (!slot.running) return;
  const rest = Math.max(0, Number(slot.settings.rest_seconds)) * 1000;
  if (rest > 0) slot.log(`Resting ${slot.settings.rest_seconds}s…`);
  setTimeout(() => {
    if (!slot.running) return;
    slot.busy = false; slot.reset(); slot.pushState();
    slot.log('Ready — watching for consolidation…');
  }, rest);
}

// ─── start watchers ───────────────────────────────────────────────────────────
function startSlotWatcher(slot) {
  stopSlotWatcher(slot);
  slot.reset();
  slot.warming = true; // ignore historical data — no instant fire on start
  const s = slot.settings;
  const logic = s.logic;

  // Logic 3 always gets a dedicated M1 candle stream for trend detection
  if (logic === 3) {
    slot.trendWs = account.subscribeCandles(s.symbol, (candle) => {
      if (!slot.running) return;
      slot.trendBuffer.push(candle);
      if (slot.trendBuffer.length > 30) slot.trendBuffer.shift();
    });
  }

  if (s.observe_mode === 'candles') {
    slot.dataWs = account.subscribeCandles(s.symbol, (candle, isLive) => {
      if (!slot.running) return;
      // Historical candles go into buffer for context but NEVER trigger
      if (!isLive) {
        slot.buffer.push(candle);
        if (slot.buffer.length > s.observe_count) slot.buffer.shift();
        return;
      }
      // First live candle — warmup done, evaluation begins
      slot.warming = false;
      if (logic === 1) onDataLogic1(slot, candle);
      else {
        if (slot.phase === 'watching') onDataLogic23_watching(slot, candle);
        else onDataLogic23_confirming(slot, parseFloat(candle.close));
      }
    });
  } else {
    // Ticks — buffer starts empty after reset(), so no instant fire possible
    slot.dataWs = account.subscribeTicks(s.symbol, price => {
      if (!slot.running) return;
      slot.warming = false;
      if (logic === 1) onDataLogic1(slot, price);
      else {
        if (slot.phase === 'watching') onDataLogic23_watching(slot, price);
        else onDataLogic23_confirming(slot, price);
      }
    });
  }

  slot.dataWs.on('error', e => slot.log('Watcher error: ' + e.message, 'err'));
  slot.dataWs.on('close', () => { if (slot.running) slot.log('Watcher closed unexpectedly', 'err'); });
  slot.log(`Started — Logic ${logic} | ${s.observe_mode === 'candles' ? 'M1 Candles' : 'Ticks'}`);
}

function stopSlotWatcher(slot) {
  ['dataWs','trendWs'].forEach(k => { if (slot[k]) { try { slot[k].close(); } catch {} slot[k] = null; } });
  slot.trendBuffer = [];
}

async function startSlot(slot) {
  if (slot.running) return { error: 'Already running' };
  if (!account.ready) { try { await account.connect(); } catch (e) { return { error: e.message }; } }
  slot.running = true; slot.busy = false;
  slot.log(`Bot started — monitoring market (warming up…)`);
  slot.pushState();
  startSlotWatcher(slot);
  return { success: true };
}

function stopSlot(slot) {
  slot.running = false; slot.busy = false;
  stopSlotWatcher(slot); slot.reset();
  slot.log('Bot stopped'); slot.pushState();
  return { success: true };
}

// ─── symbols ──────────────────────────────────────────────────────────────────
let symsCache = null;
function fetchSymbols() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    const t = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('Timed out')); }, 10000);
    ws.on('open', () => ws.send(JSON.stringify({ active_symbols: 'full', product_type: 'basic' })));
    ws.on('message', raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      if (msg.msg_type === 'active_symbols') {
        clearTimeout(t); try { ws.close(); } catch {}
        if (msg.error) return reject(new Error(msg.error.message));
        resolve(msg.active_symbols);
      }
    });
    ws.on('error', e => { clearTimeout(t); reject(e); });
  });
}

// ─── routes ───────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders(); sseClients.push(res);
  res.write(`data: ${JSON.stringify({ type: 'init', account: account.info(), slots: slots.map(s => s.snapshot()) })}\n\n`);
  req.on('close', () => { const i = sseClients.indexOf(res); if (i !== -1) sseClients.splice(i, 1); });
});
app.get('/api/account', (req, res) => res.json({ tokenSet: !!account.token, ...account.info() }));
app.post('/api/token', async (req, res) => {
  const { token } = req.body; if (!token) return res.status(400).json({ error: 'No token' });
  account.setToken(token);
  try { await account.connect(); res.json({ success: true, ...account.info() }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/symbols', async (req, res) => {
  try {
    if (!symsCache) {
      const raw = await fetchSymbols();
      symsCache = raw.filter(s => s.market === 'synthetic_index' && (/^1HZ/.test(s.symbol) || /\(1s\)/i.test(s.display_name)))
        .map(s => ({ symbol: s.symbol, display_name: s.display_name })).sort((a,b) => a.display_name.localeCompare(b.display_name));
    }
    res.json(symsCache);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/slots', (req, res) => res.json(slots.map(s => s.snapshot())));
app.get('/api/slot/:id', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(s.snapshot()); });
app.post('/api/slot/:id/start', async (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(await startSlot(s)); });
app.post('/api/slot/:id/stop', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(stopSlot(s)); });
app.post('/api/slot/:id/settings', (req, res) => {
  const slot = slotMap.get(req.params.id); if (!slot) return res.status(404).json({ error: 'Not found' });
  slot.settings = { ...slot.settings, ...req.body }; res.json(slot.settings);
});
app.get('/api/slot/:id/logs', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(s.logs); });
app.get('/api/slot/:id/history', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(s.history); });
app.post('/api/stopall', (req, res) => { slots.forEach(s => { if (s.running) stopSlot(s); }); res.json({ success: true }); });

app.listen(PORT, () => slog(`Zone Touch 10-in-1 v3 running on port ${PORT}`));
