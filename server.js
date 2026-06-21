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
      const t = setTimeout(() => {
        this.watchers.delete(contractId);
        reject(new Error('Contract watch timed out'));
      }, 5 * 60 * 1000);
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

  // Subscribe to candles (M1 OHLC)
  subscribeCandles(symbol, onCandle) {
    const ws = new WebSocket(WS_URL);
    ws.on('open', () => {
      ws.send(JSON.stringify({ ticks_history: symbol, adjust_start_time: 1, count: 50, end: 'latest', granularity: 60, style: 'candles', subscribe: 1 }));
    });
    ws.on('message', raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      // initial history
      if (msg.msg_type === 'candles' && msg.candles) msg.candles.forEach(c => onCandle(c, false));
      // live updates
      if (msg.msg_type === 'ohlc' && msg.ohlc) onCandle(msg.ohlc, true);
    });
    ws.on('error', () => {});
    return ws;
  }

  // Subscribe to raw ticks
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
    amount: Number(s.stake),
    basis: 'stake',
    contract_type: contractType,
    currency: 'USD',
    duration: Number(s.duration_value),
    duration_unit: s.duration_unit,
    symbol: s.symbol,
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

// ─── market slot ──────────────────────────────────────────────────────────────
class MarketSlot {
  constructor(index) {
    this.index = index;
    this.id = `slot${index}`;
    this.settings = {
      symbol: ['1HZ10V','1HZ25V','1HZ50V','1HZ75V','1HZ100V','1HZ10V','1HZ25V','1HZ50V','1HZ75V','1HZ100V'][index],
      logic: 1,                  // 1 = Zone Touch, 2 = Reversal Touch, 3 = Reversal Any
      observe_mode: 'ticks',     // 'ticks' or 'candles'
      observe_count: 5,          // N ticks or N candles to observe
      reaction_range: 0.3,       // max spread for consolidation
      confirm_count: 1,          // N ticks/candles confirmation before firing
      stake: 1,
      duration_value: 5,
      duration_unit: 't',
      barrier1: '+0.25',         // Logic 1: upper barrier. Logic 2/3: barrier when hitting HIGH
      barrier2: '-0.25',         // Logic 1: lower barrier. Logic 2/3: barrier when hitting LOW
      contract_type: 'ONETOUCH', // Logic 3 only
      rest_seconds: 30,
    };
    this.running = false;
    this.busy = false;
    this.logs = [];
    this.stats = { trades: 0, wins: 0, losses: 0, profit: 0 };
    this.history = [];
    this.dataWs = null;   // tick or candle WebSocket
    this.liveWs = null;   // live tick watch after zone locked (logic 2/3)
    this.buffer = [];     // rolling tick prices or candle objects
    this.phase = 'watching'; // 'watching' | 'confirming' | 'trading'
    this.lockedHigh = null;
    this.lockedLow = null;
    this.confirmSide = null;  // 'high' | 'low'
    this.confirmBuf = [];     // confirmation ticks/candles buffer
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
  }

  snapshot() {
    return { id: this.id, index: this.index, running: this.running, busy: this.busy, stats: this.stats, settings: this.settings, phase: this.phase };
  }
}

const slots = Array.from({ length: 10 }, (_, i) => new MarketSlot(i));
const slotMap = new Map(slots.map(s => [s.id, s]));

function slog(msg, data) { console.log(`[${new Date().toISOString()}] ${msg}`, data || ''); }

// ─── Logic 1: Zone Touch ──────────────────────────────────────────────────────
// Fires when N ticks/candles consolidate inside reaction_range → fire both barriers simultaneously
function onDataLogic1(slot, value) {
  if (!slot.running || slot.busy) return;

  slot.buffer.push(value);
  if (slot.buffer.length > slot.settings.observe_count) slot.buffer.shift();
  if (slot.buffer.length < slot.settings.observe_count) return;

  const prices = getPrices(slot.buffer, slot.settings.observe_mode);
  const high = Math.max(...prices);
  const low  = Math.min(...prices);
  const spread = parseFloat((high - low).toFixed(5));

  if (spread <= slot.settings.reaction_range) {
    slot.log(`Consolidation detected — spread: ${spread} | zone: ${slot.settings.reaction_range}`);
    slot.busy = true;
    slot.buffer = [];
    slot.pushState();

    const { barrier1, barrier2 } = slot.settings;
    slot.log(`Firing both ONETOUCH trades — ${barrier1} and ${barrier2}`);

    const p1 = fireTrade(slot, 'ONETOUCH', barrier1);
    const p2 = fireTrade(slot, 'ONETOUCH', barrier2);

    Promise.all([p1, p2])
      .catch(e => slot.log('Trade error: ' + e.message, 'err'))
      .finally(() => afterTrade(slot));
  }
}

// ─── Logic 2 & 3: Reversal ───────────────────────────────────────────────────
// Phase 1 WATCHING: collect N ticks/candles, detect consolidation → lock HIGH/LOW
// Phase 2 CONFIRMING: watch live price for touch of HIGH or LOW, then confirm N ticks back
// Phase 3 TRADING: fire trade

function onDataLogic23_watching(slot, value) {
  if (!slot.running || slot.busy || slot.phase !== 'watching') return;

  slot.buffer.push(value);
  if (slot.buffer.length > slot.settings.observe_count) slot.buffer.shift();
  if (slot.buffer.length < slot.settings.observe_count) return;

  const prices = getPrices(slot.buffer, slot.settings.observe_mode);
  const high = Math.max(...prices);
  const low  = Math.min(...prices);
  const spread = parseFloat((high - low).toFixed(5));

  if (spread <= slot.settings.reaction_range) {
    slot.lockedHigh = high;
    slot.lockedLow  = low;
    slot.phase = 'confirming';
    slot.confirmBuf = [];
    slot.confirmSide = null;
    slot.log(`Consolidation locked — HIGH: ${high} | LOW: ${low} | spread: ${spread}`);
    slot.log('Waiting for price to touch HIGH or LOW…');
    slot.pushState();
  }
}

function onDataLogic23_confirming(slot, price) {
  if (!slot.running || slot.busy || slot.phase !== 'confirming') return;

  const livePrice = typeof price === 'object' ? parseFloat(price.close) : price;

  // First touch — determine side (first one wins)
  if (!slot.confirmSide) {
    if (livePrice >= slot.lockedHigh) {
      slot.confirmSide = 'high';
      slot.confirmBuf = [livePrice];
      slot.log(`Price touched HIGH (${slot.lockedHigh}) — waiting for ${slot.settings.confirm_count} confirmation tick(s)/candle(s) back`);
    } else if (livePrice <= slot.lockedLow) {
      slot.confirmSide = 'low';
      slot.confirmBuf = [livePrice];
      slot.log(`Price touched LOW (${slot.lockedLow}) — waiting for ${slot.settings.confirm_count} confirmation tick(s)/candle(s) back`);
    }
    return;
  }

  // Collect confirmation
  slot.confirmBuf.push(livePrice);

  if (slot.confirmSide === 'high') {
    // Need price to pull back (go lower)
    const pullbacks = slot.confirmBuf.filter(p => p < slot.lockedHigh);
    if (pullbacks.length >= slot.settings.confirm_count) {
      slot.phase = 'trading';
      slot.busy  = true;
      slot.pushState();
      const barrier = slot.settings.barrier1; // barrier when hitting HIGH (negative, price going down)
      const ct = slot.settings.logic === 2 ? 'ONETOUCH' : slot.settings.contract_type;
      slot.log(`Confirmation met — firing ${ct} with barrier ${barrier} (HIGH reversal)`);
      fireTrade(slot, ct, barrier)
        .catch(e => slot.log('Trade error: ' + e.message, 'err'))
        .finally(() => afterTrade(slot));
    }
  } else if (slot.confirmSide === 'low') {
    // Need price to bounce up (go higher)
    const bounces = slot.confirmBuf.filter(p => p > slot.lockedLow);
    if (bounces.length >= slot.settings.confirm_count) {
      slot.phase = 'trading';
      slot.busy  = true;
      slot.pushState();
      const barrier = slot.settings.barrier2; // barrier when hitting LOW (positive, price going up)
      const ct = slot.settings.logic === 2 ? 'ONETOUCH' : slot.settings.contract_type;
      slot.log(`Confirmation met — firing ${ct} with barrier ${barrier} (LOW reversal)`);
      fireTrade(slot, ct, barrier)
        .catch(e => slot.log('Trade error: ' + e.message, 'err'))
        .finally(() => afterTrade(slot));
    }
  }
}

// ─── after trade: rest then reset ────────────────────────────────────────────
function afterTrade(slot) {
  if (!slot.running) return;
  const rest = Math.max(0, Number(slot.settings.rest_seconds)) * 1000;
  if (rest > 0) slot.log(`Resting ${slot.settings.rest_seconds}s…`);
  setTimeout(() => {
    if (!slot.running) return;
    slot.busy = false;
    slot.reset();
    slot.pushState();
    slot.log('Ready — watching for consolidation…');
  }, rest);
}

// ─── extract prices from buffer ───────────────────────────────────────────────
function getPrices(buffer, mode) {
  if (mode === 'candles') {
    return buffer.map(c => [parseFloat(c.high), parseFloat(c.low)]).flat();
  }
  return buffer.map(p => parseFloat(p));
}

// ─── start / stop watchers ────────────────────────────────────────────────────
function startSlotWatcher(slot) {
  stopSlotWatcher(slot);
  slot.reset();
  const s = slot.settings;
  const logic = s.logic;

  if (s.observe_mode === 'candles') {
    slot.dataWs = account.subscribeCandles(s.symbol, (candle, isLive) => {
      if (!slot.running) return;
      if (logic === 1) onDataLogic1(slot, candle);
      else if (isLive || slot.phase === 'confirming') {
        if (slot.phase === 'watching') onDataLogic23_watching(slot, candle);
        else onDataLogic23_confirming(slot, parseFloat(candle.close));
      } else {
        onDataLogic23_watching(slot, candle);
      }
    });
    slot.log(`Watching ${s.symbol} — M1 candles | observe: ${s.observe_count} | range: ${s.reaction_range}`);
  } else {
    slot.dataWs = account.subscribeTicks(s.symbol, price => {
      if (!slot.running) return;
      if (logic === 1) onDataLogic1(slot, price);
      else {
        if (slot.phase === 'watching') onDataLogic23_watching(slot, price);
        else onDataLogic23_confirming(slot, price);
      }
    });
    slot.log(`Watching ${s.symbol} — ticks | observe: ${s.observe_count} | range: ${s.reaction_range}`);
  }

  slot.dataWs.on('error', e => slot.log('Watcher error: ' + e.message, 'err'));
  slot.dataWs.on('close', () => { if (slot.running) slot.log('Watcher closed unexpectedly', 'err'); });
}

function stopSlotWatcher(slot) {
  if (slot.dataWs) { try { slot.dataWs.close(); } catch {} slot.dataWs = null; }
  if (slot.liveWs) { try { slot.liveWs.close(); } catch {} slot.liveWs = null; }
}

async function startSlot(slot) {
  if (slot.running) return { error: 'Already running' };
  if (!account.ready) {
    try { await account.connect(); } catch (e) { return { error: e.message }; }
  }
  slot.running = true;
  slot.busy = false;
  slot.log(`Started — Logic ${slot.settings.logic}`);
  slot.pushState();
  startSlotWatcher(slot);
  return { success: true };
}

function stopSlot(slot) {
  slot.running = false;
  slot.busy = false;
  stopSlotWatcher(slot);
  slot.reset();
  slot.log('Stopped');
  slot.pushState();
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
  res.flushHeaders();
  sseClients.push(res);
  res.write(`data: ${JSON.stringify({ type: 'init', account: account.info(), slots: slots.map(s => s.snapshot()) })}\n\n`);
  req.on('close', () => { const i = sseClients.indexOf(res); if (i !== -1) sseClients.splice(i, 1); });
});

app.get('/api/account', (req, res) => res.json({ tokenSet: !!account.token, ...account.info() }));

app.post('/api/token', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'No token' });
  account.setToken(token);
  try { await account.connect(); res.json({ success: true, ...account.info() }); }
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

app.get('/api/slots', (req, res) => res.json(slots.map(s => s.snapshot())));
app.get('/api/slot/:id', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(s.snapshot()); });
app.post('/api/slot/:id/start', async (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(await startSlot(s)); });
app.post('/api/slot/:id/stop', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(stopSlot(s)); });
app.post('/api/slot/:id/settings', (req, res) => {
  const slot = slotMap.get(req.params.id);
  if (!slot) return res.status(404).json({ error: 'Not found' });
  slot.settings = { ...slot.settings, ...req.body };
  res.json(slot.settings);
});
app.get('/api/slot/:id/logs', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(s.logs); });
app.get('/api/slot/:id/history', (req, res) => { const s = slotMap.get(req.params.id); if (!s) return res.status(404).json({ error: 'Not found' }); res.json(s.history); });
app.post('/api/stopall', (req, res) => { slots.forEach(s => { if (s.running) stopSlot(s); }); res.json({ success: true }); });

app.listen(PORT, () => slog(`Zone Touch 10-in-1 v2 running on port ${PORT}`));
