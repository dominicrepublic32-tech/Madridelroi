const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const APP_ID = process.env.DERIV_APP_ID || '1089';
const WS = `wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`;

// ─── SSE ─────────────────────────────────────────────────────────────────────
const clients = [];
function push(data) {
  const s = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(r => r.write(s));
}

// ─── Deriv account ────────────────────────────────────────────────────────────
class Account {
  constructor() {
    this.token = ''; this.ws = null; this.ready = false;
    this.loginid = null; this.balance = null; this.currency = null;
    this.reqs = new Map(); this.watchers = new Map();
    this.rid = 1; this.pinger = null;
  }

  setToken(t) {
    if (!t || t === this.token) return;
    this.token = t; this.ready = false;
    if (this.ws) try { this.ws.close(); } catch {}
  }

  connect() {
    return new Promise((resolve, reject) => {
      if (!this.token) return reject(new Error('No token — enter it in the dashboard'));
      const ws = new WebSocket(WS);
      this.ws = ws;

      ws.on('open', () => {
        this.send({ authorize: this.token }).then(r => {
          if (r.error) return reject(new Error('Auth failed: ' + r.error.message));
          this.ready = true;
          this.loginid  = r.authorize?.loginid;
          this.balance  = r.authorize?.balance;
          this.currency = r.authorize?.currency;
          log('Account connected', { loginid: this.loginid });
          this.pinger = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 }));
          }, 25000);
          this.send({ balance: 1, subscribe: 1 }).catch(() => {});
          push({ type: 'account', data: this.info() });
          resolve();
        }).catch(reject);
      });

      ws.on('message', raw => {
        let m; try { m = JSON.parse(raw); } catch { return; }
        if (m.req_id && this.reqs.has(m.req_id)) {
          this.reqs.get(m.req_id)(m);
          this.reqs.delete(m.req_id);
        }
        if (m.msg_type === 'proposal_open_contract') {
          const c = m.proposal_open_contract;
          if (c && this.watchers.has(c.contract_id)) this.watchers.get(c.contract_id)(m);
        }
        if (m.msg_type === 'balance') {
          this.balance = m.balance?.balance;
          push({ type: 'account', data: this.info() });
        }
      });

      ws.on('error', e => log('Account error', { err: e.message }));
      ws.on('close', () => {
        this.ready = false;
        if (this.pinger) clearInterval(this.pinger);
        push({ type: 'account', data: this.info() });
      });
    });
  }

  send(obj) {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN)
        return reject(new Error('Socket not open'));
      const req_id = this.rid++;
      this.reqs.set(req_id, resolve);
      this.ws.send(JSON.stringify({ ...obj, req_id }));
      setTimeout(() => {
        if (this.reqs.has(req_id)) { this.reqs.delete(req_id); reject(new Error('Timed out')); }
      }, 15000);
    });
  }

  buy(params) { return this.send({ buy: 1, price: params.amount, parameters: params }); }

  watchContract(id) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { this.watchers.delete(id); reject(new Error('Watch timed out')); }, 5 * 60 * 1000);
      this.watchers.set(id, msg => {
        const c = msg.proposal_open_contract;
        if (!c) return;
        if (c.is_sold || c.is_expired) {
          clearTimeout(t); this.watchers.delete(id);
          this.send({ forget: msg.subscription?.id }).catch(() => {});
          resolve(c);
        }
      });
      this.send({ proposal_open_contract: 1, contract_id: id, subscribe: 1 })
        .catch(e => { clearTimeout(t); this.watchers.delete(id); reject(e); });
    });
  }

  // Open M1 candle stream — calls cb(candle, isLive)
  openCandleStream(symbol, cb) {
    const ws = new WebSocket(WS);
    ws.on('open', () => ws.send(JSON.stringify({
      ticks_history: symbol, adjust_start_time: 1, count: 50,
      end: 'latest', granularity: 60, style: 'candles', subscribe: 1
    })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'candles' && m.candles) m.candles.forEach(c => cb(c, false));
      if (m.msg_type === 'ohlc' && m.ohlc) cb(m.ohlc, true);
    });
    ws.on('error', () => {});
    return ws;
  }

  // Open tick stream — calls cb(price)
  openTickStream(symbol, cb) {
    const ws = new WebSocket(WS);
    ws.on('open', () => ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'tick' && m.tick) cb(parseFloat(m.tick.quote));
    });
    ws.on('error', () => {});
    return ws;
  }

  info() { return { ready: this.ready, loginid: this.loginid, balance: this.balance, currency: this.currency }; }
}

const acct = new Account();

function log(msg, data) { console.log(`[${new Date().toISOString()}] ${msg}`, data || ''); }

// ─── execute one trade and settle ─────────────────────────────────────────────
async function placeTrade(slot, contractType, barrier) {
  if (!acct.ready) throw new Error('Account not connected');
  const s = slot.cfg;
  const params = {
    amount: Number(s.stake), basis: 'stake',
    contract_type: contractType, currency: 'USD',
    duration: Number(s.duration_value), duration_unit: s.duration_unit,
    symbol: s.symbol,
  };
  if (barrier != null) params.barrier = String(barrier);

  slot.emit(`Opening ${contractType}${barrier ? ' | barrier ' + barrier : ''} | ${s.duration_value}${s.duration_unit} | $${s.stake}`);

  const res = await acct.buy(params);
  if (res.error) throw new Error(res.error.message);

  const cid = res.buy.contract_id;
  slot.emit(`Contract #${cid} opened`);

  const settled = await acct.watchContract(cid);
  const profit = parseFloat(settled.profit || 0);
  const won = profit > 0;

  slot.stats.trades++;
  slot.stats[won ? 'wins' : 'losses']++;
  slot.stats.profit = parseFloat((slot.stats.profit + profit).toFixed(2));
  slot.emit(`${won ? '✓ WIN' : '✗ LOSS'} $${Math.abs(profit).toFixed(2)}`, won ? 'win' : 'loss');

  const rec = {
    time: new Date().toISOString(), contract_id: cid,
    symbol: s.symbol, type: contractType, barrier: barrier ?? null,
    stake: s.stake, profit, won
  };
  slot.history.unshift(rec);
  if (slot.history.length > 100) slot.history.pop();
  slot.pushState();
  push({ type: 'trade', slotId: slot.id, data: rec });
  return rec;
}

// ─── FILTERS ─────────────────────────────────────────────────────────────────

// Momentum: check at exact trigger moment — no delay
function passMomentum(slot) {
  const { cfg, buf } = slot;
  const n = Math.min(cfg.momentum_candles, buf.length);
  if (n < 2) return true;

  if (cfg.observe_mode === 'ticks') {
    const recent = buf.slice(-n);
    const up   = recent.every((p, i) => i === 0 || p >= recent[i - 1]);
    const down = recent.every((p, i) => i === 0 || p <= recent[i - 1]);
    if (!up && !down) {
      slot.emit(`Momentum filter: last ${n} ticks not directional — skip`, 'err');
      return false;
    }
    // must not be dead-center of zone
    if (slot.zoneHigh && slot.zoneLow) {
      const range = slot.zoneHigh - slot.zoneLow || 1;
      const last  = recent[recent.length - 1];
      const pos   = (last - slot.zoneLow) / range;
      if (pos > 0.35 && pos < 0.65) {
        slot.emit('Momentum filter: price dead-center of zone — skip', 'err');
        return false;
      }
    }
  } else {
    const recent = buf.slice(-n);
    const bull = recent.every(c => parseFloat(c.close) >= parseFloat(c.open));
    const bear = recent.every(c => parseFloat(c.close) <= parseFloat(c.open));
    if (!bull && !bear) {
      slot.emit(`Momentum filter: last ${n} candles mixed — skip`, 'err');
      return false;
    }
    if (cfg.momentum_body_mult > 0 && buf.length >= 3) {
      const slice  = buf.slice(-10);
      const avgBody = slice.reduce((s, c) => s + Math.abs(parseFloat(c.close) - parseFloat(c.open)), 0) / slice.length;
      const last   = recent[recent.length - 1];
      const body   = Math.abs(parseFloat(last.close) - parseFloat(last.open));
      if (body < avgBody * cfg.momentum_body_mult) {
        slot.emit('Momentum filter: breakout candle too weak — skip', 'err');
        return false;
      }
    }
  }
  return true;
}

// Trend: scan M1 candles before entry
function passTrend(slot) {
  const { cfg, trendBuf } = slot;
  const n = Math.min(cfg.trend_candles, trendBuf.length);
  if (n < 3) return true;
  const candles = trendBuf.slice(-n);

  // 1. avg body size
  const avgBody = candles.reduce((s, c) => s + Math.abs(parseFloat(c.close) - parseFloat(c.open)), 0) / n;
  if (avgBody < cfg.min_body_size) {
    slot.emit(`Trend filter: avg body ${avgBody.toFixed(4)} < ${cfg.min_body_size} — choppy — skip`, 'err');
    return false;
  }

  // 2. candle overlap
  let ov = 0;
  for (let i = 1; i < candles.length; i++) {
    const a = candles[i - 1], b = candles[i];
    const oH = Math.min(parseFloat(a.high), parseFloat(b.high));
    const oL = Math.max(parseFloat(a.low),  parseFloat(b.low));
    const overlap = Math.max(0, oH - oL);
    const range   = Math.max(parseFloat(a.high) - parseFloat(a.low), parseFloat(b.high) - parseFloat(b.low)) || 1;
    ov += overlap / range;
  }
  const avgOv = ov / (candles.length - 1);
  if (avgOv > cfg.max_overlap) {
    slot.emit(`Trend filter: overlap ${avgOv.toFixed(2)} > ${cfg.max_overlap} — ranging — skip`, 'err');
    return false;
  }

  // 3. consecutive directional candles
  let maxRun = 1, run = 1;
  for (let i = 1; i < candles.length; i++) {
    const ab = parseFloat(candles[i - 1].close) >= parseFloat(candles[i - 1].open);
    const bb = parseFloat(candles[i].close)     >= parseFloat(candles[i].open);
    if (ab === bb) { run++; maxRun = Math.max(maxRun, run); } else run = 1;
  }
  if (maxRun < cfg.min_dir_candles) {
    slot.emit(`Trend filter: only ${maxRun} consecutive candles — no trend — skip`, 'err');
    return false;
  }
  return true;
}

function passFilter(slot) {
  const f = slot.cfg.filter;
  if (f === 'momentum') return passMomentum(slot);
  if (f === 'trend')    return passTrend(slot);
  return true;
}

// ─── slot ─────────────────────────────────────────────────────────────────────
const DEFAULT_SYMS = ['1HZ10V','1HZ25V','1HZ50V','1HZ75V','1HZ100V','1HZ10V','1HZ25V','1HZ50V','1HZ75V','1HZ100V'];

function defaultCfg(idx) {
  return {
    symbol:            DEFAULT_SYMS[idx],
    logic:             1,           // 1=ZoneTouch 2=ReversalTouch 3=ReversalAny
    observe_mode:      'ticks',     // 'ticks' | 'candles'
    observe_count:     5,
    reaction_range:    0.3,
    confirm_count:     1,           // Logic 2/3: ticks/candles back after HIGH/LOW touch
    stake:             1,
    duration_value:    5,
    duration_unit:     't',         // 't'=ticks 's'=seconds 'm'=minutes
    barrier1:          '+0.25',     // L1: upper barrier. L2/3: barrier on HIGH touch
    barrier2:          '-0.25',     // L1: lower barrier. L2/3: barrier on LOW touch
    contract_type:     'ONETOUCH',  // Logic 3 only
    rest_seconds:      30,
    filter:            'none',      // 'none' | 'momentum' | 'trend'
    momentum_candles:  3,
    momentum_body_mult: 1.0,
    trend_candles:     5,
    min_body_size:     0.05,
    max_overlap:       0.5,
    min_dir_candles:   3,
  };
}

class Slot {
  constructor(idx) {
    this.idx     = idx;
    this.id      = `slot${idx}`;
    this.cfg     = defaultCfg(idx);
    this.running = false;
    this.busy    = false;
    this.logs    = [];
    this.stats   = { trades: 0, wins: 0, losses: 0, profit: 0 };
    this.history = [];
    // watcher sockets
    this.dataWs  = null;
    this.trendWs = null;
    // observation buffer (prices or candles)
    this.buf      = [];
    // trend filter buffer (always M1 candles)
    this.trendBuf = [];
    // warmup: how many LIVE data points to collect before evaluating
    this.warmup   = 0;
    // phase: 'watching' | 'confirming' | 'trading'
    this.phase    = 'watching';
    // Logic 2/3 zone
    this.zoneHigh = null;
    this.zoneLow  = null;
    this.confSide = null;  // 'high' | 'low'
    this.confBuf  = [];
  }

  emit(msg, level = 'info') {
    const e = { time: new Date().toISOString(), msg, level };
    this.logs.push(e);
    if (this.logs.length > 300) this.logs.shift();
    push({ type: 'log', slotId: this.id, entry: e });
  }

  pushState() {
    push({ type: 'state', slotId: this.id, data: {
      running: this.running, busy: this.busy,
      stats: this.stats, phase: this.phase, warmup: this.warmup
    }});
  }

  softReset() {
    this.phase    = 'watching';
    this.zoneHigh = null; this.zoneLow  = null;
    this.confSide = null; this.confBuf  = [];
    this.buf      = [];
    this.warmup   = this.cfg.observe_count; // require fresh data again
  }

  snap() {
    return {
      id: this.id, idx: this.idx,
      running: this.running, busy: this.busy,
      stats: this.stats, cfg: this.cfg,
      phase: this.phase, warmup: this.warmup
    };
  }
}

const slots  = Array.from({ length: 10 }, (_, i) => new Slot(i));
const slotOf = new Map(slots.map(s => [s.id, s]));

// ─── price helpers ────────────────────────────────────────────────────────────
function prices(buf, mode) {
  if (mode === 'candles') return buf.flatMap(c => [parseFloat(c.high), parseFloat(c.low)]);
  return buf.map(Number);
}

function spread(buf, mode) {
  const ps = prices(buf, mode);
  return parseFloat((Math.max(...ps) - Math.min(...ps)).toFixed(5));
}

// ─── Logic 1: Zone Touch ──────────────────────────────────────────────────────
function L1_onData(slot, val) {
  if (!slot.running || slot.busy) return;

  // Warmup: collect live data but don't evaluate yet
  if (slot.warmup > 0) {
    slot.buf.push(val);
    if (slot.buf.length > slot.cfg.observe_count) slot.buf.shift();
    slot.warmup--;
    if (slot.warmup === 0) slot.emit('Warmup complete — now monitoring market');
    return;
  }

  slot.buf.push(val);
  if (slot.buf.length > slot.cfg.observe_count) slot.buf.shift();
  if (slot.buf.length < slot.cfg.observe_count) return;

  const sp = spread(slot.buf, slot.cfg.observe_mode);
  if (sp > slot.cfg.reaction_range) return;

  const ps = prices(slot.buf, slot.cfg.observe_mode);
  slot.zoneHigh = Math.max(...ps);
  slot.zoneLow  = Math.min(...ps);

  if (!passFilter(slot)) { slot.zoneHigh = null; slot.zoneLow = null; return; }

  slot.emit(`Zone detected — spread: ${sp} ≤ ${slot.cfg.reaction_range} | firing both barriers`);
  slot.busy = true;
  slot.buf  = [];
  slot.pushState();

  Promise.all([
    placeTrade(slot, 'ONETOUCH', slot.cfg.barrier1),
    placeTrade(slot, 'ONETOUCH', slot.cfg.barrier2),
  ])
    .catch(e => slot.emit('Trade error: ' + e.message, 'err'))
    .finally(() => afterTrade(slot));
}

// ─── Logic 2 & 3: Reversal ────────────────────────────────────────────────────
function L23_onData(slot, val) {
  if (!slot.running || slot.busy) return;

  if (slot.phase === 'watching') {
    L23_watching(slot, val);
  } else if (slot.phase === 'confirming') {
    const price = slot.cfg.observe_mode === 'candles' ? parseFloat(val.close) : val;
    L23_confirming(slot, price);
  }
}

function L23_watching(slot, val) {
  // Warmup
  if (slot.warmup > 0) {
    slot.buf.push(val);
    if (slot.buf.length > slot.cfg.observe_count) slot.buf.shift();
    slot.warmup--;
    if (slot.warmup === 0) slot.emit('Warmup complete — now monitoring market');
    return;
  }

  slot.buf.push(val);
  if (slot.buf.length > slot.cfg.observe_count) slot.buf.shift();
  if (slot.buf.length < slot.cfg.observe_count) return;

  if (!passFilter(slot)) return;

  const sp = spread(slot.buf, slot.cfg.observe_mode);
  if (sp > slot.cfg.reaction_range) return;

  const ps = prices(slot.buf, slot.cfg.observe_mode);
  slot.zoneHigh = Math.max(...ps);
  slot.zoneLow  = Math.min(...ps);
  slot.phase    = 'confirming';
  slot.confSide = null;
  slot.confBuf  = [];
  slot.emit(`Zone locked — HIGH: ${slot.zoneHigh} | LOW: ${slot.zoneLow} | spread: ${sp}`);
  slot.emit('Waiting for price to reach HIGH or LOW…');
  slot.pushState();
}

function L23_confirming(slot, price) {
  if (!slot.confSide) {
    if (price >= slot.zoneHigh) {
      slot.confSide = 'high';
      slot.confBuf  = [price];
      slot.emit(`HIGH touched (${slot.zoneHigh}) — need ${slot.cfg.confirm_count} tick(s)/candle(s) back`);
    } else if (price <= slot.zoneLow) {
      slot.confSide = 'low';
      slot.confBuf  = [price];
      slot.emit(`LOW touched (${slot.zoneLow}) — need ${slot.cfg.confirm_count} tick(s)/candle(s) back`);
    }
    return;
  }

  slot.confBuf.push(price);

  const needed = slot.cfg.confirm_count;

  if (slot.confSide === 'high') {
    // Need price to pull back below HIGH
    if (slot.confBuf.filter(p => p < slot.zoneHigh).length >= needed || needed === 0) {
      fireReversal(slot, 'high');
    }
  } else {
    // Need price to bounce above LOW
    if (slot.confBuf.filter(p => p > slot.zoneLow).length >= needed || needed === 0) {
      fireReversal(slot, 'low');
    }
  }
}

function fireReversal(slot, side) {
  slot.phase = 'trading';
  slot.busy  = true;
  slot.pushState();
  const barrier = side === 'high' ? slot.cfg.barrier1 : slot.cfg.barrier2;
  const ct = slot.cfg.logic === 2 ? 'ONETOUCH' : slot.cfg.contract_type;
  slot.emit(`Confirmed — firing ${ct} | barrier: ${barrier} | ${side.toUpperCase()} reversal`);
  placeTrade(slot, ct, barrier)
    .catch(e => slot.emit('Trade error: ' + e.message, 'err'))
    .finally(() => afterTrade(slot));
}

// ─── after trade ─────────────────────────────────────────────────────────────
function afterTrade(slot) {
  if (!slot.running) return;
  const rest = Math.max(0, Number(slot.cfg.rest_seconds)) * 1000;
  if (rest > 0) slot.emit(`Resting ${slot.cfg.rest_seconds}s…`);
  setTimeout(() => {
    if (!slot.running) return;
    slot.busy = false;
    slot.softReset();
    slot.pushState();
    slot.emit('Monitoring — waiting for consolidation…');
  }, rest);
}

// ─── start/stop watchers ──────────────────────────────────────────────────────
function closeWs(ws) { if (ws) try { ws.close(); } catch {} }

function startWatcher(slot) {
  // Close any existing streams
  closeWs(slot.dataWs); slot.dataWs = null;
  closeWs(slot.trendWs); slot.trendWs = null;

  // Full reset
  slot.buf      = [];
  slot.trendBuf = [];
  slot.phase    = 'watching';
  slot.zoneHigh = null; slot.zoneLow  = null;
  slot.confSide = null; slot.confBuf  = [];
  slot.busy     = false;
  // Set warmup counter — bot must collect this many LIVE points before evaluating
  slot.warmup   = slot.cfg.observe_count;

  const { cfg } = slot;

  // Trend filter needs its own M1 candle stream regardless of observe_mode
  if (cfg.filter === 'trend') {
    slot.trendWs = acct.openCandleStream(cfg.symbol, (candle) => {
      if (!slot.running) return;
      slot.trendBuf.push(candle);
      if (slot.trendBuf.length > 50) slot.trendBuf.shift();
    });
  }

  const dispatch = (val) => {
    if (!slot.running) return;
    if (cfg.logic === 1) L1_onData(slot, val);
    else L23_onData(slot, val);
  };

  if (cfg.observe_mode === 'candles') {
    slot.dataWs = acct.openCandleStream(cfg.symbol, (candle, isLive) => {
      // CRITICAL: ignore all historical candles — only process live ones
      // This prevents any instant-fire on start
      if (!isLive) return;
      dispatch(candle);
    });
  } else {
    // Ticks: buffer starts empty, warmup counter prevents early evaluation
    slot.dataWs = acct.openTickStream(cfg.symbol, price => dispatch(price));
  }

  slot.dataWs.on('error', e => slot.emit('Stream error: ' + e.message, 'err'));
  slot.dataWs.on('close', () => { if (slot.running) slot.emit('Stream closed unexpectedly', 'err'); });

  slot.emit(`Monitoring ${cfg.symbol} | Logic ${cfg.logic} | ${cfg.observe_mode === 'candles' ? 'M1 Candles' : 'Ticks'} | Filter: ${cfg.filter} | Warmup: ${slot.warmup} data points`);
}

async function startSlot(slot) {
  if (slot.running) return { error: 'Already running' };
  if (!acct.ready) {
    try { await acct.connect(); }
    catch (e) { return { error: e.message }; }
  }
  slot.running = true;
  slot.pushState();
  startWatcher(slot);
  return { success: true };
}

function stopSlot(slot) {
  slot.running = false;
  slot.busy    = false;
  slot.warmup  = 0;
  slot.phase   = 'watching';
  closeWs(slot.dataWs);  slot.dataWs  = null;
  closeWs(slot.trendWs); slot.trendWs = null;
  slot.buf      = [];
  slot.trendBuf = [];
  slot.emit('Stopped');
  slot.pushState();
  return { success: true };
}

// ─── symbols ──────────────────────────────────────────────────────────────────
let symsCache = null;
function fetchSymbols() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS);
    const t  = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('Timed out')); }, 10000);
    ws.on('open', () => ws.send(JSON.stringify({ active_symbols: 'full', product_type: 'basic' })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'active_symbols') {
        clearTimeout(t); try { ws.close(); } catch {}
        if (m.error) return reject(new Error(m.error.message));
        resolve(m.active_symbols);
      }
    });
    ws.on('error', e => { clearTimeout(t); reject(e); });
  });
}

// ─── routes ───────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();
  clients.push(res);
  res.write(`data: ${JSON.stringify({ type: 'init', account: acct.info(), slots: slots.map(s => s.snap()) })}\n\n`);
  req.on('close', () => { const i = clients.indexOf(res); if (i !== -1) clients.splice(i, 1); });
});

app.get('/api/account', (req, res) => res.json(acct.info()));

app.post('/api/token', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'No token' });
  acct.setToken(token);
  try { await acct.connect(); res.json({ success: true, ...acct.info() }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/symbols', async (req, res) => {
  try {
    if (!symsCache) {
      const raw = await fetchSymbols();
      symsCache = raw
        .filter(s => s.market === 'synthetic_index' && (/^1HZ/.test(s.symbol) || /\(1s\)/i.test(s.display_name)))
        .map(s => ({ symbol: s.symbol, display_name: s.display_name }))
        .sort((a, b) => a.display_name.localeCompare(b.display_name));
    }
    res.json(symsCache);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/slots',           (req, res) => res.json(slots.map(s => s.snap())));
app.get('/api/slot/:id',        (req, res) => { const s = slotOf.get(req.params.id); s ? res.json(s.snap()) : res.status(404).json({ error: 'Not found' }); });
app.get('/api/slot/:id/logs',   (req, res) => { const s = slotOf.get(req.params.id); s ? res.json(s.logs)   : res.status(404).json({ error: 'Not found' }); });
app.get('/api/slot/:id/history',(req, res) => { const s = slotOf.get(req.params.id); s ? res.json(s.history): res.status(404).json({ error: 'Not found' }); });

app.post('/api/slot/:id/start', async (req, res) => {
  const s = slotOf.get(req.params.id);
  if (!s) return res.status(404).json({ error: 'Not found' });
  res.json(await startSlot(s));
});

app.post('/api/slot/:id/stop', (req, res) => {
  const s = slotOf.get(req.params.id);
  if (!s) return res.status(404).json({ error: 'Not found' });
  res.json(stopSlot(s));
});

app.post('/api/slot/:id/cfg', (req, res) => {
  const s = slotOf.get(req.params.id);
  if (!s) return res.status(404).json({ error: 'Not found' });
  s.cfg = { ...s.cfg, ...req.body };
  res.json(s.cfg);
});

app.post('/api/stopall', (req, res) => {
  slots.forEach(s => { if (s.running) stopSlot(s); });
  res.json({ success: true });
});

app.listen(PORT, () => log(`Zone Touch 10-in-1 running on port ${PORT}`));
