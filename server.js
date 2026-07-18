process.on('uncaughtException', (e) => { console.error('UNCAUGHT:', e); });
process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); });

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "signalbot2024";
const DATA_FILE = path.join(__dirname, "bot_state.json");

// ── State management (file-based, simple persistence) ────
function loadState() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      return JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    }
  } catch (e) { console.log("Load state error:", e.message); }
  return {
    capital: 1000,
    trades: [],
    dailyPnl: 0,
    dailyTrades: 0,
    openTrade: null,
    autoMode: false,
    autoPairs: ["BTCUSDT"],
    autoTFs: ["15m", "1h"],
    minConfidence: 70,
    requireMTF: true,
    maxDailyGainPct: 5,
    maxDailyLossPct: 3,
    consecutiveLosses: 0,
    lastResetDate: new Date().toDateString()
  };
}

function saveState(state) {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));
  } catch (e) { console.log("Save state error:", e.message); }
}

let state = loadState();

// ── HMAC ──────────────────────────────────────────────────
function hmac(secret, message) {
  return crypto.createHmac("sha256", secret).update(message).digest("hex");
}

// ── Telegram ──────────────────────────────────────────────
async function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) { console.log('Telegram no configurado'); return; }
  try {
    const r = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: "HTML" })
    });
    const d = await r.json();
    console.log('Telegram:', d.ok ? 'OK' : d.description);
  } catch (e) { console.log("Telegram error:", e.message); }
}

// ── Technical Analysis (ported from frontend) ─────────────
function calcEMA(d, p) {
  if (d.length < p) return null;
  const k = 2 / (p + 1);
  let e = d.slice(0, p).reduce((a, b) => a + b, 0) / p;
  for (let i = p; i < d.length; i++) e = d[i] * k + e * (1 - k);
  return e;
}
function calcSMA(d, p) {
  if (d.length < p) return null;
  return d.slice(-p).reduce((a, b) => a + b, 0) / p;
}
function calcRSI(c, p = 14) {
  if (c.length < p + 1) return null;
  let g = 0, l = 0;
  for (let i = c.length - p; i < c.length; i++) {
    const d = c[i] - c[i - 1];
    if (d >= 0) g += d; else l -= d;
  }
  const ag = g / p, al = l / p;
  if (al === 0) return 100;
  return 100 - 100 / (1 + ag / al);
}
function calcBB(c, p = 20) {
  if (c.length < p) return null;
  const s = c.slice(-p);
  const m = s.reduce((a, b) => a + b, 0) / p;
  const std = Math.sqrt(s.reduce((a, b) => a + Math.pow(b - m, 2), 0) / p);
  return { upper: m + 2 * std, middle: m, lower: m - 2 * std };
}
function calcMACD(c) {
  if (c.length < 26) return null;
  const e12 = calcEMA(c, 12), e26 = calcEMA(c, 26);
  if (!e12 || !e26) return null;
  return { macdLine: e12 - e26 };
}
function calcATR(h, l, c, p = 14) {
  if (h.length < p + 1) return null;
  let atr = 0;
  for (let i = h.length - p; i < h.length; i++) {
    const tr = Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1]));
    atr += tr;
  }
  return atr / p;
}

function analyze(closes, highs, lows) {
  if (!closes || closes.length < 30) return null;
  const price = closes[closes.length - 1];
  const rsi = calcRSI(closes);
  const macd = calcMACD(closes);
  const bb = calcBB(closes);
  const sma20 = calcSMA(closes, 20);
  const ema50 = calcEMA(closes, Math.min(50, closes.length));
  const sma200 = closes.length >= 200 ? calcSMA(closes, 200) : null;
  const atr = calcATR(highs, lows, closes) || price * 0.02;
  let bull = 0, bear = 0;
  if (rsi !== null) { if (rsi < 35) bull += 2; else if (rsi > 65) bear += 2; }
  if (macd) { if (macd.macdLine > 0) bull += 1; else bear += 1; }
  if (bb) { if (price < bb.lower) bull += 2; else if (price > bb.upper) bear += 2; }
  if (sma20) { if (price > sma20) bull += 1; else bear += 1; }
  if (ema50) { if (price > ema50) bull += 1; else bear += 1; }
  if (sma200) { if (price > sma200) bull += 1; else bear += 1; }
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal, direction;
  if (diff >= 2) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -2) { signal = 'VENDER'; direction = 'SHORT'; }
  else { signal = 'NEUTRO'; direction = 'ESPERAR'; }
  const rH = Math.max(...highs.slice(-14));
  const rL = Math.min(...lows.slice(-14));
  const range = rH - rL;
  let entry = price, tp, sl;
  if (signal === 'COMPRAR') { tp = price + range * 0.618; sl = price - range * 0.382; }
  else if (signal === 'VENDER') { tp = price - range * 0.618; sl = price + range * 0.382; }
  else { tp = price + range * 0.3; sl = price - range * 0.3; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, bullScore: bull, bearScore: bear, rsi };
}

async function fetchKlines(pair, tf, limit = 100) {
  const res = await fetch(`https://api.binance.com/api/v3/klines?symbol=${pair}&interval=${tf}&limit=${limit}`);
  if (!res.ok) throw new Error('Par no encontrado');
  const data = await res.json();
  return {
    closes: data.map(k => parseFloat(k[4])),
    highs: data.map(k => parseFloat(k[2])),
    lows: data.map(k => parseFloat(k[3]))
  };
}

// ── Auto trading loop (runs server-side, 24/7) ────────────
function openTrade(pair, tf, analysis) {
  const size = state.capital * 0.20;
  const qty = analysis.entry > 0 ? size / analysis.entry : 0;
  state.openTrade = {
    id: Date.now(), pair, signal: analysis.signal, direction: analysis.direction,
    entry: analysis.entry, tp: analysis.tp, sl: analysis.sl, qty, size, tf,
    openTime: new Date().toLocaleString('es-AR', {timeZone:'America/Argentina/Buenos_Aires'}),
    confidence: analysis.confidence, auto: true
  };
  saveState(state);
  const emoji = analysis.signal === 'COMPRAR' ? '🟢' : '🔴';
  sendTelegram(`${emoji} ${analysis.signal} AUTO (Servidor)\n📊 ${pair.replace('USDT','/USDT')} · ${tf.toUpperCase()}\n💵 Entrada: $${analysis.entry.toFixed(2)}\n🎯 TP: $${analysis.tp.toFixed(2)}\n🛑 SL: $${analysis.sl.toFixed(2)}\n📊 R/R: 1:${analysis.rr.toFixed(2)}\n🎯 Confianza: ${analysis.confidence}%`);
}

function closeTrade(exitPrice, reason) {
  if (!state.openTrade) return;
  const t = state.openTrade;
  const pricePct = t.signal === 'COMPRAR' ? (exitPrice - t.entry) / t.entry : (t.entry - exitPrice) / t.entry;
  const rawPnl = t.size * pricePct;
  const pnl = Math.max(-t.size, Math.min(rawPnl, t.size * 5));
  const pnlPct = pricePct * 100;
  const closed = { ...t, exitPrice, pnl, pnlPct, closeTime: new Date().toLocaleString('es-AR', {timeZone:'America/Argentina/Buenos_Aires'}), reason };
  state.trades.unshift(closed);
  if (state.trades.length > 500) state.trades = state.trades.slice(0, 500);
  state.capital += pnl;
  if (state.capital < 0) state.capital = 0;
  state.dailyPnl += pnl;
  state.dailyTrades += 1;
  if (pnl < 0) state.consecutiveLosses += 1; else state.consecutiveLosses = 0;
  state.openTrade = null;
  saveState(state);
  const emoji = pnl >= 0 ? '✅' : '❌';
  sendTelegram(`${emoji} OPERACIÓN CERRADA (Servidor)\n📊 ${t.pair.replace('USDT','/USDT')} · ${t.tf}\n${t.signal} · ${t.direction}\n💵 $${t.entry.toFixed(2)} → $${exitPrice.toFixed(2)}\n${pnl>=0?'💰':'📉'} PnL: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnlPct.toFixed(2)}%)\n🏷 ${reason}\n💰 Capital: $${state.capital.toFixed(2)}`);
  if (state.consecutiveLosses >= 3) {
    sendTelegram(`⚠️ BOT PAUSADO (Servidor)\n3 pérdidas seguidas\n🛡 Capital protegido: $${state.capital.toFixed(2)}`);
    state.autoMode = false;
    saveState(state);
  }
}

async function runAutoCheck() {
  if (!state.autoMode) return;
  const today = new Date().toDateString();
  if (state.lastResetDate !== today) {
    state.dailyPnl = 0; state.dailyTrades = 0; state.lastResetDate = today;
    saveState(state);
  }
  const maxGain = state.capital * state.maxDailyGainPct / 100;
  const maxLoss = state.capital * state.maxDailyLossPct / 100;
  if (state.dailyPnl >= maxGain) {
    sendTelegram(`✅ Límite de ganancia diaria alcanzado ($${state.dailyPnl.toFixed(2)})`);
    state.autoMode = false; saveState(state); return;
  }
  if (state.dailyPnl <= -maxLoss) {
    sendTelegram(`🛑 Límite de pérdida diaria alcanzado ($${state.dailyPnl.toFixed(2)})`);
    state.autoMode = false; saveState(state); return;
  }

  // Check if open trade should close
  if (state.openTrade) {
    try {
      const { closes } = await fetchKlines(state.openTrade.pair, "1m", 2);
      const currentPrice = closes[closes.length - 1];
      const t = state.openTrade;
      if (t.signal === 'COMPRAR' && currentPrice >= t.tp) closeTrade(currentPrice, 'TP Auto');
      else if (t.signal === 'COMPRAR' && currentPrice <= t.sl) closeTrade(currentPrice, 'SL Auto');
      else if (t.signal === 'VENDER' && currentPrice <= t.tp) closeTrade(currentPrice, 'TP Auto');
      else if (t.signal === 'VENDER' && currentPrice >= t.sl) closeTrade(currentPrice, 'SL Auto');
    } catch (e) { console.log('Check open trade error:', e.message); }
    return;
  }

  // Look for new signal across pairs and timeframes
  let allSignals = [];
  for (const pair of state.autoPairs) {
    let signals = [];
    for (const tf of state.autoTFs) {
      try {
        const { closes, highs, lows } = await fetchKlines(pair, tf, 100);
        const a = analyze(closes, highs, lows);
        if (a) signals.push({ tf, pair, signal: a.signal, confidence: a.confidence, analysis: a });
      } catch (e) { console.log(`Analyze error ${pair} ${tf}:`, e.message); }
    }
    const buys = signals.filter(s => s.signal === 'COMPRAR' && s.confidence >= state.minConfidence);
    const sells = signals.filter(s => s.signal === 'VENDER' && s.confidence >= state.minConfidence);
    const threshold = state.requireMTF ? 2 : 1;
    if (buys.length >= threshold) {
      const best = buys.sort((a, b) => b.confidence - a.confidence)[0];
      allSignals.push({ ...best, direction: 'COMPRAR', score: buys.length * best.confidence });
    } else if (sells.length >= threshold) {
      const best = sells.sort((a, b) => b.confidence - a.confidence)[0];
      allSignals.push({ ...best, direction: 'VENDER', score: sells.length * best.confidence });
    }
  }

  if (allSignals.length > 0) {
    const best = allSignals.sort((a, b) => b.score - a.score)[0];
    openTrade(best.pair, best.tf, best.analysis);
  }
}

// ── Routes ────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({ status: "Signal Bot Backend OK", time: new Date().toISOString(), autoMode: state.autoMode });
});

app.get("/state", (req, res) => {
  res.json(state);
});

app.post("/state/config", (req, res) => {
  const { autoPairs, autoTFs, minConfidence, requireMTF, maxDailyGainPct, maxDailyLossPct } = req.body;
  if (autoPairs) state.autoPairs = autoPairs;
  if (autoTFs) state.autoTFs = autoTFs;
  if (minConfidence !== undefined) state.minConfidence = minConfidence;
  if (requireMTF !== undefined) state.requireMTF = requireMTF;
  if (maxDailyGainPct !== undefined) state.maxDailyGainPct = maxDailyGainPct;
  if (maxDailyLossPct !== undefined) state.maxDailyLossPct = maxDailyLossPct;
  saveState(state);
  res.json({ success: true, state });
});

app.post("/state/toggle-auto", (req, res) => {
  state.autoMode = !state.autoMode;
  saveState(state);
  sendTelegram(state.autoMode ? '▶ Bot automático activado (Servidor 24/7)' : '■ Bot automático detenido (Servidor)');
  if (state.autoMode) runAutoCheck();
  res.json({ success: true, autoMode: state.autoMode });
});

app.post("/state/reset", (req, res) => {
  state = {
    capital: 1000, trades: [], dailyPnl: 0, dailyTrades: 0, openTrade: null,
    autoMode: false, autoPairs: ["BTCUSDT"], autoTFs: ["15m", "1h"],
    minConfidence: 70, requireMTF: true, maxDailyGainPct: 5, maxDailyLossPct: 3,
    consecutiveLosses: 0, lastResetDate: new Date().toDateString()
  };
  saveState(state);
  res.json({ success: true, state });
});

// ── Binance & Telegram routes (existing) ──────────────────
app.post("/balance", async (req, res) => {
  const { apiKey, apiSecret } = req.body;
  if (!apiKey || !apiSecret) return res.status(400).json({ error: "Faltan claves" });
  try {
    const timestamp = Date.now();
    const query = `timestamp=${timestamp}`;
    const signature = hmac(apiSecret, query);
    const response = await fetch(`https://api.binance.com/api/v3/account?${query}&signature=${signature}`, {
      headers: { "X-MBX-APIKEY": apiKey }
    });
    const data = await response.json();
    if (data.code) return res.status(400).json({ error: data.msg });
    const usdt = data.balances?.find(b => b.asset === "USDT");
    res.json({ usdt: usdt ? parseFloat(usdt.free) : 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/order", async (req, res) => {
  const { apiKey, apiSecret, symbol, side, quantity, type = "MARKET" } = req.body;
  if (!apiKey || !apiSecret || !symbol || !side || !quantity) {
    return res.status(400).json({ error: "Faltan parámetros" });
  }
  try {
    const timestamp = Date.now();
    const params = `symbol=${symbol}&side=${side}&type=${type}&quantity=${quantity}&timestamp=${timestamp}`;
    const signature = hmac(apiSecret, params);
    const response = await fetch(`https://api.binance.com/api/v3/order`, {
      method: "POST",
      headers: { "X-MBX-APIKEY": apiKey, "Content-Type": "application/x-www-form-urlencoded" },
      body: `${params}&signature=${signature}`
    });
    const data = await response.json();
    if (data.code) return res.status(400).json({ error: data.msg });
    await sendTelegram(`🔔 ORDEN REAL EJECUTADA\n${symbol} ${side}\nCantidad: ${data.executedQty}`);
    res.json({ success: true, orderId: data.orderId, executedQty: data.executedQty, price: data.fills?.[0]?.price });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/alert", async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: "Falta mensaje" });
  await sendTelegram(message);
  res.json({ success: true });
});

app.post("/webhook", async (req, res) => {
  const { secret, action, symbol, quantity, apiKey, apiSecret } = req.body;
  if (secret !== WEBHOOK_SECRET) return res.status(401).json({ error: "Secret inválido" });
  try {
    const side = action === "buy" ? "BUY" : "SELL";
    const timestamp = Date.now();
    const params = `symbol=${symbol}&side=${side}&type=MARKET&quantity=${quantity}&timestamp=${timestamp}`;
    const signature = hmac(apiSecret, params);
    const response = await fetch(`https://api.binance.com/api/v3/order`, {
      method: "POST",
      headers: { "X-MBX-APIKEY": apiKey, "Content-Type": "application/x-www-form-urlencoded" },
      body: `${params}&signature=${signature}`
    });
    const data = await response.json();
    await sendTelegram(`🎯 WEBHOOK: ${side} ${symbol}`);
    res.json({ success: true, order: data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Daily summary (22hs Argentina) ────────────────────────
function scheduleDailySummary() {
  const now = new Date();
  const next = new Date();
  next.setUTCHours(1, 0, 0, 0);
  if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
  const ms = next - now;
  setTimeout(() => {
    sendDailySummaryMsg();
    setInterval(sendDailySummaryMsg, 24 * 60 * 60 * 1000);
  }, ms);
}

function sendDailySummaryMsg() {
  const wins = state.trades.filter(t => t.pnl > 0).length;
  const losses = state.trades.filter(t => t.pnl < 0).length;
  const winRate = state.trades.length > 0 ? Math.round(wins / state.trades.length * 100) : 0;
  let motivacion = '';
  if (state.dailyPnl > 0 && winRate >= 60) motivacion = '🚀 Excelente día! Seguí así, campeón!';
  else if (state.dailyPnl > 0) motivacion = '🟢 Buen día! De a poco se llega lejos.';
  else if (state.dailyPnl < 0 && losses >= 3) motivacion = '💪 Dale vos podés! Mañana es otro día.';
  else if (state.dailyPnl < 0) motivacion = '🔴 Día difícil. Revisá las señales y descansá.';
  else motivacion = '⚪ Día tranquilo. El mercado espera su momento.';
  const now = new Date().toLocaleString('es-AR', {timeZone:'America/Argentina/Buenos_Aires'});
  sendTelegram(`📊 RESUMEN DIARIO (Servidor 24/7)\n📅 ${now}\n\n💰 Capital: $${state.capital.toFixed(2)}\n📈 P&L hoy: ${state.dailyPnl>=0?'+':''}$${state.dailyPnl.toFixed(2)}\n🎯 Operaciones hoy: ${state.dailyTrades}\n✅ Ganadas: ${wins}\n❌ Perdidas: ${losses}\n📊 Win Rate: ${winRate}%\n\n${motivacion}`);
  state.dailyPnl = 0; state.dailyTrades = 0;
  saveState(state);
}

app.listen(PORT, () => {
  console.log(`Backend v2 corriendo en puerto ${PORT} - AUTO 24/7 habilitado`);
  scheduleDailySummary();
  sendTelegram(`🟢 Signal Bot Backend v2 iniciado\n⏰ ${new Date().toLocaleString('es-AR', {timeZone:'America/Argentina/Buenos_Aires'})}\n🤖 Modo AUTO ahora corre en el servidor 24/7`);
  // Start the auto-check loop (runs every 60 seconds regardless of browser)
  setInterval(runAutoCheck, 60000);
});
