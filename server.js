const express = require('express');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const APP_ID = process.env.DERIV_APP_ID || '1089';
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const BRIDGE_KEY = process.env.BRIDGE_KEY || 'secretary-bridge-key';
const DERIV_WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`;
const MEMORY_FILE = path.join(__dirname, 'memory.json');

// ─── memory ────────────────────────────────────────────────────────────────
let memory = {
  preferences: {},
  strategies: [],
  notes: [],
  conversation_history: [],
  trade_history: []
};

function loadMemory() {
  try {
    if (fs.existsSync(MEMORY_FILE)) {
      memory = JSON.parse(fs.readFileSync(MEMORY_FILE, 'utf8'));
      log('Memory loaded', { messages: memory.conversation_history.length, trades: memory.trade_history.length });
    }
  } catch (e) { log('Memory load error', { error: e.message }); }
}

function saveMemory() {
  try { fs.writeFileSync(MEMORY_FILE, JSON.stringify(memory, null, 2)); }
  catch (e) { log('Memory save error', { error: e.message }); }
}

loadMemory();

// ─── sse / logs ────────────────────────────────────────────────────────────
const sseClients = [];
let logs = [];

function log(msg, data) {
  const entry = { time: new Date().toISOString(), msg, data: data || null };
  logs.push(entry);
  if (logs.length > 500) logs.shift();
  sseClients.forEach(r => r.write(`data: ${JSON.stringify({ _log: entry })}\n\n`));
  console.log(`[${entry.time}] ${msg}`, data || '');
}

function pushSSE(type, payload) {
  const raw = `data: ${JSON.stringify({ type, payload })}\n\n`;
  sseClients.forEach(r => r.write(raw));
}

// ─── pending actions ────────────────────────────────────────────────────────
const pendingActions = new Map();
let actionCounter = 1;

function createAction(type, details) {
  const id = `action_${actionCounter++}`;
  pendingActions.set(id, { id, type, details, status: 'pending', created: Date.now() });
  return id;
}

// ─── MT5 bridge ─────────────────────────────────────────────────────────────
const mt5Queue = [];
const mt5Results = new Map();

// ─── Deriv account ──────────────────────────────────────────────────────────
class DerivAccount {
  constructor(name) {
    this.name = name;
    this.token = '';
    this.ws = null;
    this.ready = false;
    this.loginid = null;
    this.balance = null;
    this.currency = null;
    this.pendingReqs = new Map();
    this.reqIdCounter = 1;
    this.pingInterval = null;
    this._contractWatcher = null;
  }

  setToken(token) {
    if (!token || token === this.token) return;
    this.token = token;
    this.ready = false;
    if (this.ws) { try { this.ws.close(); } catch {} }
  }

  connect() {
    return new Promise((resolve, reject) => {
      if (!this.token) return reject(new Error(`${this.name}: no API token set`));
      this.ws = new WebSocket(DERIV_WS_URL);

      this.ws.on('open', () => {
        this.send({ authorize: this.token }).then(res => {
          if (res.error) return reject(new Error(`${this.name} auth failed: ${res.error.message}`));
          this.ready = true;
          this.loginid  = res.authorize?.loginid;
          this.balance  = res.authorize?.balance;
          this.currency = res.authorize?.currency;
          log(`${this.name} connected`, { loginid: this.loginid, balance: this.balance });
          this.pingInterval = setInterval(() => {
            if (this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ ping: 1 }));
          }, 30000);
          resolve();
        }).catch(reject);
      });

      this.ws.on('message', raw => {
        let msg; try { msg = JSON.parse(raw); } catch { return; }
        if (msg.req_id && this.pendingReqs.has(msg.req_id)) {
          const { resolve: res } = this.pendingReqs.get(msg.req_id);
          this.pendingReqs.delete(msg.req_id);
          res(msg);
        }
        if (msg.msg_type === 'proposal_open_contract' && this._contractWatcher) this._contractWatcher(msg);
        if (msg.msg_type === 'balance') {
          this.balance = msg.balance?.balance;
          pushSSE('balance', { account: this.name, balance: this.balance, currency: this.currency });
        }
      });

      this.ws.on('error', err => log(`${this.name} error`, { error: err.message }));
      this.ws.on('close', () => {
        this.ready = false;
        if (this.pingInterval) clearInterval(this.pingInterval);
        log(`${this.name} disconnected`);
      });
    });
  }

  send(obj) {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN)
        return reject(new Error(`${this.name}: socket not open`));
      const req_id = this.reqIdCounter++;
      this.pendingReqs.set(req_id, { resolve, reject });
      this.ws.send(JSON.stringify({ ...obj, req_id }));
      setTimeout(() => {
        if (this.pendingReqs.has(req_id)) {
          this.pendingReqs.delete(req_id);
          reject(new Error(`${this.name}: request timed out`));
        }
      }, 15000);
    });
  }

  subscribeBalance() {
    return this.send({ balance: 1, subscribe: 1 });
  }

  buy(params) {
    return this.send({ buy: 1, price: params.stake, parameters: params });
  }

  watchContract(contractId) {
    return new Promise(resolve => {
      this._contractWatcher = msg => {
        const c = msg.proposal_open_contract;
        if (!c || c.contract_id !== contractId) return;
        if (c.is_sold || c.is_expired) {
          this.send({ forget: msg.subscription?.id }).catch(() => {});
          this._contractWatcher = null;
          resolve(c);
        }
      };
      this.send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1 }).catch(() => {});
    });
  }

  async sellContract(contractId, price) {
    return this.send({ sell: contractId, price: price || 0 });
  }

  async getOpenContracts() {
    return this.send({ portfolio: 1 });
  }

  info() {
    return { name: this.name, ready: this.ready, loginid: this.loginid, balance: this.balance, currency: this.currency };
  }
}

const accA = new DerivAccount('Account 1');
const accB = new DerivAccount('Account 2');

// ─── tick watcher ───────────────────────────────────────────────────────────
let tickWs = null;
let tickBuffer = [];
let watchedSymbol = null;
const MAX_TICKS = 100;

function startTickWatch(symbol) {
  if (tickWs) { try { tickWs.close(); } catch {} }
  tickBuffer = [];
  watchedSymbol = symbol;
  tickWs = new WebSocket(DERIV_WS_URL);
  tickWs.on('open', () => tickWs.send(JSON.stringify({ ticks: symbol, subscribe: 1 })));
  tickWs.on('message', raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.msg_type === 'tick' && msg.tick) {
      tickBuffer.push({ price: parseFloat(msg.tick.quote), time: msg.tick.epoch });
      if (tickBuffer.length > MAX_TICKS) tickBuffer.shift();
      pushSSE('tick', { symbol, price: tickBuffer[tickBuffer.length-1].price });
    }
  });
  tickWs.on('error', err => log('Tick watcher error', { error: err.message }));
  tickWs.on('close', () => { watchedSymbol = null; });
  log(`Tick watcher started on ${symbol}`);
}

function stopTickWatch() {
  if (tickWs) { try { tickWs.close(); } catch {} tickWs = null; }
  tickBuffer = [];
  watchedSymbol = null;
}

function getTickStats() {
  if (!tickBuffer.length) return null;
  const prices = tickBuffer.map(t => t.price);
  const high = Math.max(...prices), low = Math.min(...prices);
  return {
    symbol: watchedSymbol,
    last: prices[prices.length-1],
    high, low,
    range: parseFloat((high-low).toFixed(5)),
    count: prices.length,
    range5:  prices.length >= 5  ? parseFloat((Math.max(...prices.slice(-5))  - Math.min(...prices.slice(-5))).toFixed(5))  : null,
    range10: prices.length >= 10 ? parseFloat((Math.max(...prices.slice(-10)) - Math.min(...prices.slice(-10))).toFixed(5)) : null,
    range20: prices.length >= 20 ? parseFloat((Math.max(...prices.slice(-20)) - Math.min(...prices.slice(-20))).toFixed(5)) : null,
    recent: prices.slice(-20),
  };
}

// ─── AI brain ───────────────────────────────────────────────────────────────
const SYSTEM_PROMPT = `You are a personal AI trading secretary. You are direct, sharp, and precise. You help your trader execute strategies on Deriv and MT5.

CAPABILITIES:
- Deriv: all contract types — Rise/Fall, Touch/No Touch, Digits, Vanillas, Turbos, Higher/Lower, and more
- MT5: market orders, pending orders, close, modify via EA bridge
- Market analysis using live tick data
- Remembering strategies, preferences, and past trades

WHEN PROPOSING AN ACTION:
You MUST output the action in this exact format at the END of your message:
::ACTION::
{"type":"deriv_trade","account":"Account 1","details":{"contract_type":"ONETOUCH","symbol":"1HZ100V","duration":5,"duration_unit":"t","stake":1,"barrier":"+0.25","currency":"USD"}}
::END::

For MT5 trades:
::ACTION::
{"type":"mt5_trade","details":{"action":"open","symbol":"EURUSD","type":"buy","volume":0.01,"sl":0,"tp":0}}
::END::

For closing a Deriv contract early:
::ACTION::
{"type":"deriv_close","account":"Account 1","details":{"contract_id":12345678}}
::END::

For watching a market:
::ACTION::
{"type":"watch_market","details":{"symbol":"1HZ100V"}}
::END::

RULES:
- Never execute anything without the trader's explicit confirmation
- Always explain what you're proposing and why before outputting the action block
- If the trader asks you to remember something, acknowledge it — it will be stored
- Be concise but thorough when analyzing markets
- If you don't have live data, say so and ask the trader to connect to a market
- You can propose multiple actions in sequence but only one action block per message`;

async function callClaude(userMessage, contextData) {
  if (!ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not set in environment variables');

  // build context string
  let context = '';
  if (contextData.ticks) {
    const t = contextData.ticks;
    context += `\nLIVE MARKET DATA (${t.symbol}):
Current price: ${t.last}
5-tick range: ${t.range5 ?? 'N/A'}
10-tick range: ${t.range10 ?? 'N/A'}
20-tick range: ${t.range20 ?? 'N/A'}
Full range (${t.count} ticks): ${t.range}
Recent prices: ${t.recent.slice(-10).join(', ')}`;
  }
  if (contextData.accounts) {
    context += `\nACCOUNT STATUS:`;
    contextData.accounts.forEach(a => {
      context += `\n${a.name}: ${a.ready ? `Connected (${a.loginid}) Balance: ${a.currency} ${a.balance}` : 'Not connected'}`;
    });
  }
  if (contextData.mt5Connected) context += `\nMT5: Connected via EA bridge`;
  if (memory.preferences && Object.keys(memory.preferences).length)
    context += `\nTRADER PREFERENCES: ${JSON.stringify(memory.preferences)}`;
  if (memory.strategies?.length)
    context += `\nKNOWN STRATEGIES: ${memory.strategies.map(s=>s.name).join(', ')}`;
  if (memory.notes?.length)
    context += `\nNOTES: ${memory.notes.slice(-5).join('; ')}`;

  const historyMessages = memory.conversation_history.slice(-16).map(m => ({
    role: m.role,
    content: m.content
  }));

  const finalUserContent = context ? `${context}\n\n${userMessage}` : userMessage;

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-6',
      max_tokens: 1024,
      system: SYSTEM_PROMPT,
      messages: [...historyMessages, { role: 'user', content: finalUserContent }]
    })
  });

  const data = await res.json();
  if (data.error) throw new Error(data.error.message);
  return data.content?.[0]?.text || '';
}

function parseAIResponse(raw) {
  const actionMatch = raw.match(/::ACTION::\s*([\s\S]*?)\s*::END::/);
  let action = null;
  let text = raw;

  if (actionMatch) {
    try {
      action = JSON.parse(actionMatch[1].trim());
      text = raw.replace(/::ACTION::[\s\S]*?::END::/, '').trim();
    } catch (e) { log('Action parse error', { error: e.message, raw: actionMatch[1] }); }
  }

  // check if AI wants to remember something
  const rememberMatch = text.match(/\[REMEMBER:\s*(.*?)\]/i);
  if (rememberMatch) {
    memory.notes.push(rememberMatch[1]);
    saveMemory();
    text = text.replace(/\[REMEMBER:.*?\]/gi, '').trim();
  }

  return { text, action };
}

async function executeDerivTrade(account, details) {
  const acc = account === 'Account 2' ? accB : accA;
  if (!acc.ready) throw new Error(`${acc.name} is not connected`);

  const params = {
    contract_type: details.contract_type,
    symbol: details.symbol,
    duration: details.duration,
    duration_unit: details.duration_unit,
    stake: details.stake,
    amount: details.stake,
    basis: 'stake',
    currency: details.currency || 'USD'
  };
  if (details.barrier)  params.barrier  = String(details.barrier);
  if (details.barrier2) params.barrier2 = String(details.barrier2);

  const result = await acc.buy(params);
  if (result.error) throw new Error(result.error.message);

  const contract_id = result.buy.contract_id;
  log('Trade executed', { account, contract_id, payout: result.buy.payout });

  memory.trade_history.push({
    time: new Date().toISOString(),
    platform: 'Deriv',
    account,
    details: params,
    contract_id,
    payout: result.buy.payout,
    status: 'open'
  });
  saveMemory();

  // watch and settle
  acc.watchContract(contract_id).then(c => {
    const profit = c.profit;
    const won = profit > 0;
    log(`Contract settled`, { contract_id, profit, status: c.status });
    const trade = memory.trade_history.find(t => t.contract_id === contract_id);
    if (trade) { trade.profit = profit; trade.status = won ? 'won' : 'lost'; saveMemory(); }
    pushSSE('trade_settled', { contract_id, profit, won, account });
  }).catch(() => {});

  return { contract_id, payout: result.buy.payout };
}

async function executeDerivClose(account, details) {
  const acc = account === 'Account 2' ? accB : accA;
  if (!acc.ready) throw new Error(`${acc.name} is not connected`);
  const result = await acc.sellContract(details.contract_id, details.price || 0);
  if (result.error) throw new Error(result.error.message);
  log('Contract sold early', { contract_id: details.contract_id });
  return result.sell;
}

function enqueueMT5Order(details) {
  const id = `mt5_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
  mt5Queue.push({ id, ...details, queued: Date.now() });
  log('MT5 order queued', { id, ...details });
  return id;
}

// ─── symbols ────────────────────────────────────────────────────────────────
let symbolsCache = null;

async function fetchSymbols() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(DERIV_WS_URL);
    const t = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('Timed out')); }, 10000);
    ws.on('open', () => ws.send(JSON.stringify({ active_symbols: 'full', product_type: 'basic' })));
    ws.on('message', raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      if (msg.msg_type === 'active_symbols') {
        clearTimeout(t);
        try { ws.close(); } catch {}
        if (msg.error) return reject(new Error(msg.error.message));
        resolve(msg.active_symbols);
      }
    });
    ws.on('error', err => { clearTimeout(t); reject(err); });
  });
}

// ─── routes ─────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();
  sseClients.push(res);
  req.on('close', () => { const i = sseClients.indexOf(res); if (i !== -1) sseClients.splice(i, 1); });
});

app.get('/api/logs', (req, res) => res.json(logs));

app.get('/api/status', (req, res) => {
  res.json({
    accounts: [accA.info(), accB.info()],
    mt5Connected: mt5Queue !== null,
    mt5Pending: mt5Queue.length,
    watchedSymbol,
    tickCount: tickBuffer.length,
    ticks: getTickStats()
  });
});

app.get('/api/tokens', (req, res) => res.json({ token1Set: !!accA.token, token2Set: !!accB.token }));

app.post('/api/tokens', async (req, res) => {
  const { token1, token2 } = req.body;
  if (token1) { accA.setToken(token1); try { await accA.connect(); await accA.subscribeBalance(); } catch (e) { log('Acc1 connect error', { error: e.message }); } }
  if (token2) { accB.setToken(token2); try { await accB.connect(); await accB.subscribeBalance(); } catch (e) { log('Acc2 connect error', { error: e.message }); } }
  res.json({ token1Set: !!accA.token, token2Set: !!accB.token, acc1: accA.info(), acc2: accB.info() });
});

app.get('/api/symbols', async (req, res) => {
  try {
    if (!symbolsCache) {
      const raw = await fetchSymbols();
      symbolsCache = raw
        .filter(s => s.market === 'synthetic_index' && (/^1HZ/.test(s.symbol) || /\(1s\)/i.test(s.display_name)))
        .map(s => ({ symbol: s.symbol, display_name: s.display_name, group: s.submarket_display_name || 'Synthetic Indices' }))
        .sort((a,b) => a.display_name.localeCompare(b.display_name));
    }
    res.json(symbolsCache);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/watch', (req, res) => {
  const { symbol, stop } = req.body;
  if (stop) { stopTickWatch(); return res.json({ watching: false }); }
  if (symbol) startTickWatch(symbol);
  res.json({ watching: true, symbol });
});

// main chat
app.post('/api/chat', async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: 'No message' });

  const contextData = {
    ticks: getTickStats(),
    accounts: [accA.info(), accB.info()],
    mt5Connected: mt5Queue.length >= 0
  };

  try {
    const raw = await callClaude(message, contextData);
    const { text, action } = parseAIResponse(raw);

    // save to history
    memory.conversation_history.push({ role: 'user', content: message, time: new Date().toISOString() });
    memory.conversation_history.push({ role: 'assistant', content: raw, time: new Date().toISOString() });
    if (memory.conversation_history.length > 200) memory.conversation_history = memory.conversation_history.slice(-200);
    saveMemory();

    let actionId = null;
    if (action) {
      actionId = createAction(action.type, action.details || action);
      const a = pendingActions.get(actionId);
      if (a) a.account = action.account;
    }

    res.json({ text, action: action ? { id: actionId, ...action } : null });
  } catch (e) {
    log('Chat error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// confirm action
app.post('/api/action/confirm', async (req, res) => {
  const { id, edits } = req.body;
  const action = pendingActions.get(id);
  if (!action) return res.status(404).json({ error: 'Action not found' });
  if (action.status !== 'pending') return res.status(400).json({ error: `Action already ${action.status}` });

  if (edits) action.details = { ...action.details, ...edits };
  action.status = 'confirmed';

  try {
    let result;
    if (action.type === 'deriv_trade')
      result = await executeDerivTrade(action.account || 'Account 1', action.details);
    else if (action.type === 'deriv_close')
      result = await executeDerivClose(action.account || 'Account 1', action.details);
    else if (action.type === 'mt5_trade')
      result = { queued: enqueueMT5Order(action.details) };
    else if (action.type === 'watch_market') {
      startTickWatch(action.details.symbol);
      result = { watching: action.details.symbol };
    }
    else result = { done: true };

    pushSSE('action_executed', { id, type: action.type, result });
    res.json({ success: true, result });
  } catch (e) {
    action.status = 'pending';
    log('Action execution error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// reject action
app.post('/api/action/reject', (req, res) => {
  const { id } = req.body;
  const action = pendingActions.get(id);
  if (!action) return res.status(404).json({ error: 'Action not found' });
  action.status = 'rejected';
  res.json({ success: true });
});

// edit action (update details, stays pending for re-confirmation)
app.post('/api/action/edit', (req, res) => {
  const { id, edits } = req.body;
  const action = pendingActions.get(id);
  if (!action) return res.status(404).json({ error: 'Action not found' });
  action.details = { ...action.details, ...edits };
  res.json({ success: true, action });
});

// memory
app.get('/api/memory', (req, res) => res.json(memory));
app.post('/api/memory/clear', (req, res) => {
  memory.conversation_history = [];
  saveMemory();
  res.json({ success: true });
});
app.get('/api/memory/export', (req, res) => {
  res.setHeader('Content-Disposition', 'attachment; filename="secretary-memory.json"');
  res.json(memory);
});
app.post('/api/memory/import', (req, res) => {
  try {
    memory = { ...memory, ...req.body };
    saveMemory();
    res.json({ success: true });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// trade history
app.get('/api/trades', (req, res) => res.json(memory.trade_history.slice(-100).reverse()));

// ─── MT5 bridge (EA polls these) ────────────────────────────────────────────
function checkBridgeKey(req, res) {
  const key = req.headers['x-bridge-key'];
  if (key !== BRIDGE_KEY) { res.status(401).json({ error: 'Unauthorized' }); return false; }
  return true;
}

app.get('/mt5/pending', (req, res) => {
  if (!checkBridgeKey(req, res)) return;
  const orders = mt5Queue.splice(0, mt5Queue.length);
  res.json(orders);
});

app.post('/mt5/result', (req, res) => {
  if (!checkBridgeKey(req, res)) return;
  const { id, success, message } = req.body;
  mt5Results.set(id, { success, message, time: Date.now() });
  log(`MT5 result: ${success ? 'OK' : 'FAIL'}`, { id, message });
  pushSSE('mt5_result', { id, success, message });
  memory.trade_history.push({ time: new Date().toISOString(), platform: 'MT5', id, success, message });
  saveMemory();
  res.json({ received: true });
});

app.listen(PORT, () => log(`Trading Secretary listening on port ${PORT}`));
