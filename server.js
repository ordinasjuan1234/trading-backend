process.on('uncaughtException', (e) => { console.error('UNCAUGHT:', e); });
process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); });

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const { MongoClient } = require("mongodb");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "signalbot2024";
const MONGODB_URI = process.env.MONGODB_URI;

const DEFAULT_STATE = {
  capital: 1000,
  trades: [],
  dailyPnl: 0,
  dailyTrades: 0,
  openTrades: [], // Multi-posición: una operación abierta por par como máximo
  autoMode: false,
  autoPairs: ["BTCUSDT", "ETHUSDT"], // Validated by backtest: positive results in 180 days
  autoTFs: ["4h"], // Validated by backtest: 4h gives best results vs 15m/1h
  minConfidence: 70,
  requireMTF: false, // Only one TF now (4h), so multi-TF confirmation not needed
  maxDailyGainPct: 5,
  maxDailyLossPct: 3,
  positionSizePct: 20, // % del capital por operación individual (probando 10/15/20)
  consecutiveLosses: 0,
  lastResetDate: new Date().toDateString()
};

// ── State management (MongoDB - truly persistent) ─────────
let mongoClient = null;
let stateCollection = null;
let state = { ...DEFAULT_STATE };

async function initMongo() {
  if (!MONGODB_URI) {
    console.log("MONGODB_URI no configurado - usando estado solo en memoria (se pierde al reiniciar)");
    return;
  }
  try {
    mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 8000, connectTimeoutMS: 8000 });
    await mongoClient.connect();
    const db = mongoClient.db("signalbot");
    stateCollection = db.collection("bot_state");
    console.log("MongoDB conectado correctamente");
    const saved = await stateCollection.findOne({ _id: "main" });
    if (saved) {
      delete saved._id;
      state = { ...DEFAULT_STATE, ...saved };
      console.log("Estado cargado desde MongoDB - Capital:", state.capital);
    } else {
      await stateCollection.insertOne({ _id: "main", ...DEFAULT_STATE });
      console.log("Estado inicial creado en MongoDB");
    }
  } catch (e) {
    console.log("Error conectando MongoDB:", e.message);
  }
}

async function saveState(newState) {
  state = newState;
  if (!stateCollection) return;
  try {
    await stateCollection.updateOne(
      { _id: "main" },
      { $set: { ...state } },
      { upsert: true }
    );
  } catch (e) { console.log("Save state error:", e.message); }
}

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

function calcRSISeries(c, p = 14) {
  const rsiValues = [];
  for (let i = p; i < c.length; i++) {
    let g = 0, l = 0;
    for (let j = i - p + 1; j <= i; j++) {
      const d = c[j] - c[j-1];
      if (d >= 0) g += d; else l -= d;
    }
    const ag = g / p, al = l / p;
    rsiValues.push(al === 0 ? 100 : 100 - 100 / (1 + ag / al));
  }
  return rsiValues;
}
function calcMACDSeries(c) {
  const macdLine = [];
  for (let i = 26; i <= c.length; i++) {
    const slice = c.slice(0, i);
    const e12 = calcEMA(slice, 12), e26 = calcEMA(slice, 26);
    if (e12 && e26) macdLine.push(e12 - e26);
  }
  return macdLine;
}
function calcVolatilityRank(c, lookback = 50) {
  if (c.length < lookback + 14) return 0.5;
  const atrs = [];
  for (let i = c.length - lookback; i < c.length; i++) {
    const window = c.slice(Math.max(0, i - 14), i);
    if (window.length < 14) continue;
    const range = Math.max(...window) - Math.min(...window);
    atrs.push(range);
  }
  const current = atrs[atrs.length - 1];
  const sorted = [...atrs].sort((a, b) => a - b);
  const rank = sorted.indexOf(current) / sorted.length;
  return rank;
}

function analyzeImproved(closes, highs, lows) {
  if (!closes || closes.length < 210) return null;
  const price = closes[closes.length - 1];
  const sma200Now = calcSMA(closes, 200);
  const sma200Before = calcSMA(closes.slice(0, -10), 200);
  const trendUp = sma200Now && sma200Before && sma200Now > sma200Before;
  const trendDown = sma200Now && sma200Before && sma200Now < sma200Before;
  const rsiSeries = calcRSISeries(closes, 14);
  const rsi = rsiSeries[rsiSeries.length - 1];
  const rsiPrev = rsiSeries[rsiSeries.length - 2];
  const rsiTurningUp = rsi > rsiPrev;
  const rsiTurningDown = rsi < rsiPrev;
  const macdSeries = calcMACDSeries(closes);
  const macdNow = macdSeries[macdSeries.length - 1];
  const macdPrev = macdSeries[macdSeries.length - 2];
  const macdCrossUp = macdPrev < 0 && macdNow > macdPrev;
  const macdCrossDown = macdPrev > 0 && macdNow < macdPrev;
  const ema20 = calcEMA(closes, 20);
  const ema50 = calcEMA(closes, 50);
  const volRank = calcVolatilityRank(closes);
  const isVolatileEnough = volRank > 0.3;
  const atr = calcATR(highs, lows, closes) || price * 0.02;
  let bull = 0, bear = 0;
  if (rsi < 40 && rsiTurningUp) bull += 3; else if (rsi < 40) bull += 1;
  if (rsi > 60 && rsiTurningDown) bear += 3; else if (rsi > 60) bear += 1;
  if (macdCrossUp) bull += 2;
  if (macdCrossDown) bear += 2;
  if (trendUp) bull += 2;
  if (trendDown) bear += 2;
  if (ema20) { if (price > ema20) bull += 1; else bear += 1; }
  if (ema50) { if (price > ema50) bull += 1; else bear += 1; }
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal = 'NEUTRO', direction = 'ESPERAR';
  if (diff >= 4 && isVolatileEnough && !trendDown) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && !trendUp) { signal = 'VENDER'; direction = 'SHORT'; }
  let entry = price, tp, sl;
  const slMultiplier = 1.5, tpMultiplier = 3.0;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Reversión', atr };
}

// Segunda vía de señal: sigue tendencias suaves y sostenidas que la estrategia
// de reversión (arriba) ignora porque exige RSI en zona de sobrecompra/sobreventa.
// Esta detecta subas/bajas parejas donde precio y EMAs están alineadas y en movimiento.
function analyzeTrendFollow(closes, highs, lows) {
  if (!closes || closes.length < 60) return null;
  const price = closes[closes.length - 1];
  const ema20Now = calcEMA(closes, 20);
  const ema20Before = calcEMA(closes.slice(0, -5), 20);
  const ema50Now = calcEMA(closes, 50);
  const ema50Before = calcEMA(closes.slice(0, -5), 50);
  if (!ema20Now || !ema50Now || !ema20Before || !ema50Before) return null;
  const volRank = calcVolatilityRank(closes);
  const isVolatileEnough = volRank > 0.3;
  const atr = calcATR(highs, lows, closes) || price * 0.02;
  let bull = 0, bear = 0;
  if (price > ema20Now) bull += 1; else bear += 1;
  if (price > ema50Now) bull += 1; else bear += 1;
  if (ema20Now > ema50Now) bull += 1; else bear += 1; // alineación alcista/bajista de EMAs
  if (ema20Now > ema20Before) bull += 2; else if (ema20Now < ema20Before) bear += 2; // EMA20 en movimiento
  if (ema50Now > ema50Before) bull += 1; else if (ema50Now < ema50Before) bear += 1; // EMA50 en movimiento
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal = 'NEUTRO', direction = 'ESPERAR';
  if (diff >= 4 && isVolatileEnough) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough) { signal = 'VENDER'; direction = 'SHORT'; }
  let entry = price, tp, sl;
  const slMultiplier = 1.5, tpMultiplier = 3.0;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Tendencia', atr };
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
  const res = await fetch(`https://data-api.binance.vision/api/v3/klines?symbol=${pair}&interval=${tf}&limit=${limit}`);
  if (!res.ok) throw new Error('Par no encontrado');
  const data = await res.json();
  return {
    closes: data.map(k => parseFloat(k[4])),
    highs: data.map(k => parseFloat(k[2])),
    lows: data.map(k => parseFloat(k[3]))
  };
}

// ── Auto trading loop (runs server-side, 24/7) ────────────
async function openTrade(pair, tf, analysis) {
  const pct = state.positionSizePct || 20;
  const size = state.capital * (pct / 100);
  const qty = analysis.entry > 0 ? size / analysis.entry : 0;
  const trade = {
    id: Date.now() + '-' + pair, pair, signal: analysis.signal, direction: analysis.direction,
    entry: analysis.entry, tp: analysis.tp, sl: analysis.sl, qty, size, tf,
    strategy: analysis.strategy || 'Reversión',
    atr: analysis.atr || Math.abs(analysis.entry - analysis.sl) / 1.5,
    peakPrice: analysis.entry,
    trailingActive: false,
    openTime: new Date().toLocaleString('es-AR', {timeZone:'America/Argentina/Buenos_Aires'}),
    openTimestamp: Date.now(),
    confidence: analysis.confidence, auto: true
  };
  state.openTrades.push(trade);
  await saveState(state);
  const emoji = analysis.signal === 'COMPRAR' ? '🟢' : '🔴';
  sendTelegram(`${emoji} ${analysis.signal} AUTO (Servidor)\n📊 ${pair.replace('USDT','/USDT')} · ${tf.toUpperCase()}\n🧠 Estrategia: ${trade.strategy}\n💵 Entrada: $${analysis.entry.toFixed(2)}\n🎯 TP: $${analysis.tp.toFixed(2)}\n🛑 SL: $${analysis.sl.toFixed(2)}\n📊 R/R: 1:${analysis.rr.toFixed(2)}\n🎯 Confianza: ${analysis.confidence}%\n💰 Tamaño: ${pct}% del capital`);
}

async function closeTradeById(tradeId, exitPrice, reason) {
  const idx = state.openTrades.findIndex(t => t.id === tradeId);
  if (idx === -1) return;
  const t = state.openTrades[idx];
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
  state.openTrades.splice(idx, 1);
  await saveState(state);
  const emoji = pnl >= 0 ? '✅' : '❌';
  sendTelegram(`${emoji} OPERACIÓN CERRADA (Servidor)\n📊 ${t.pair.replace('USDT','/USDT')} · ${t.tf}\n${t.signal} · ${t.direction}\n💵 $${t.entry.toFixed(2)} → $${exitPrice.toFixed(2)}\n${pnl>=0?'💰':'📉'} PnL: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnlPct.toFixed(2)}%)\n🏷 ${reason}\n💰 Capital: $${state.capital.toFixed(2)}`);
  if (state.consecutiveLosses >= 3) {
    sendTelegram(`⚠️ BOT PAUSADO (Servidor)\n3 pérdidas seguidas\n🛡 Capital protegido: $${state.capital.toFixed(2)}`);
    state.autoMode = false;
    await saveState(state);
  }
}

async function runAutoCheck() {
  if (!state.autoMode) return;
  const today = new Date().toDateString();
  if (state.lastResetDate !== today) {
    state.dailyPnl = 0; state.dailyTrades = 0; state.lastResetDate = today;
    await saveState(state);
  }
  const maxGain = state.capital * state.maxDailyGainPct / 100;
  const maxLoss = state.capital * state.maxDailyLossPct / 100;
  if (state.dailyPnl >= maxGain) {
    sendTelegram(`✅ Límite de ganancia diaria alcanzado ($${state.dailyPnl.toFixed(2)})`);
    state.autoMode = false; await saveState(state); return;
  }
  if (state.dailyPnl <= -maxLoss) {
    sendTelegram(`🛑 Límite de pérdida diaria alcanzado ($${state.dailyPnl.toFixed(2)})`);
    state.autoMode = false; await saveState(state); return;
  }

  // Check each open trade individually (multi-posición: una por par)
  for (const t of [...state.openTrades]) {
    try {
      const { closes } = await fetchKlines(t.pair, "1m", 2);
      const currentPrice = closes[closes.length - 1];

      // ── Trailing stop: asegura ganancia moviendo el SL a favor cuando la
      // operación viene ganando, sin retroceder nunca a un SL peor que el anterior.
      const atr = t.atr || Math.abs(t.entry - t.sl) / 1.5;
      const ACTIVATION_ATR = 1.0;  // se activa cuando la ganancia flotante llega a 1x ATR
      const TRAIL_DISTANCE_ATR = 1.0; // el SL persigue el precio a 1x ATR de distancia del mejor precio alcanzado
      if (t.signal === 'COMPRAR') {
        if (currentPrice > t.peakPrice) t.peakPrice = currentPrice;
        const favorableMove = t.peakPrice - t.entry;
        if (favorableMove >= atr * ACTIVATION_ATR) {
          const candidateSl = Math.max(t.entry, t.peakPrice - atr * TRAIL_DISTANCE_ATR);
          if (candidateSl > t.sl) {
            const wasActive = t.trailingActive;
            t.sl = candidateSl; t.trailingActive = true;
            await saveState(state);
            if (!wasActive) sendTelegram(`🔒 Trailing stop activado\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nSL asegurado en $${candidateSl.toFixed(2)} (protege ganancia mínima)`);
          }
        }
      } else if (t.signal === 'VENDER') {
        if (currentPrice < t.peakPrice) t.peakPrice = currentPrice;
        const favorableMove = t.entry - t.peakPrice;
        if (favorableMove >= atr * ACTIVATION_ATR) {
          const candidateSl = Math.min(t.entry, t.peakPrice + atr * TRAIL_DISTANCE_ATR);
          if (candidateSl < t.sl) {
            const wasActive = t.trailingActive;
            t.sl = candidateSl; t.trailingActive = true;
            await saveState(state);
            if (!wasActive) sendTelegram(`🔒 Trailing stop activado\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nSL asegurado en $${candidateSl.toFixed(2)} (protege ganancia mínima)`);
          }
        }
      }

      // Time-based safety close: if a trade has been open too long without hitting TP/SL,
      // close it at market price to avoid capital being stuck indefinitely
      const MAX_HOURS_OPEN = 48;
      const openTimestamp = t.openTimestamp || Date.now();
      const hoursOpen = (Date.now() - openTimestamp) / (1000 * 60 * 60);

      if (t.signal === 'COMPRAR' && currentPrice >= t.tp) await closeTradeById(t.id, currentPrice, 'TP Auto');
      else if (t.signal === 'COMPRAR' && currentPrice <= t.sl) await closeTradeById(t.id, currentPrice, 'SL Auto');
      else if (t.signal === 'VENDER' && currentPrice <= t.tp) await closeTradeById(t.id, currentPrice, 'TP Auto');
      else if (t.signal === 'VENDER' && currentPrice >= t.sl) await closeTradeById(t.id, currentPrice, 'SL Auto');
      else if (hoursOpen >= MAX_HOURS_OPEN) {
        await closeTradeById(t.id, currentPrice, `Cierre por tiempo (${MAX_HOURS_OPEN}hs)`);
        sendTelegram(`⏰ OPERACIÓN CERRADA POR TIEMPO\n${t.pair.replace('USDT','/USDT')} llevaba más de ${MAX_HOURS_OPEN}hs abierta sin tocar TP/SL\nSe cerró al precio de mercado para liberar el capital.`);
      }
    } catch (e) { console.log('Check open trade error:', e.message); }
  }

  // Look for new signal only on pairs that don't already have an open trade
  const openPairs = new Set(state.openTrades.map(t => t.pair));
  const freePairs = state.autoPairs.filter(p => !openPairs.has(p));
  if (freePairs.length === 0) return;

  let allSignals = [];
  for (const pair of freePairs) {
    let signals = [];
    for (const tf of state.autoTFs) {
      try {
        // Improved strategy needs more history (210 candles) for SMA200 trend filter
        const { closes, highs, lows } = await fetchKlines(pair, tf, 220);
        const a = analyzeImproved(closes, highs, lows);
        if (a) signals.push({ tf, pair, signal: a.signal, confidence: a.confidence, analysis: a });
        const b = analyzeTrendFollow(closes, highs, lows);
        if (b) signals.push({ tf, pair, signal: b.signal, confidence: b.confidence, analysis: b });
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

  // Open a trade for EVERY free pair with a valid signal (not just the single best) —
  // this is what actually increases daily trade frequency vs. the old one-at-a-time logic
  for (const best of allSignals) {
    await openTrade(best.pair, best.tf, best.analysis);
  }
}

// ── Routes ────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({ status: "Signal Bot Backend OK", time: new Date().toISOString(), autoMode: state.autoMode });
});

app.get("/state", (req, res) => {
  res.json(state);
});

app.post("/state/config", async (req, res) => {
  const { autoPairs, autoTFs, minConfidence, requireMTF, maxDailyGainPct, maxDailyLossPct, positionSizePct } = req.body;
  if (autoPairs) state.autoPairs = autoPairs;
  if (autoTFs) state.autoTFs = autoTFs;
  if (minConfidence !== undefined) state.minConfidence = minConfidence;
  if (requireMTF !== undefined) state.requireMTF = requireMTF;
  if (maxDailyGainPct !== undefined) state.maxDailyGainPct = maxDailyGainPct;
  if (maxDailyLossPct !== undefined) state.maxDailyLossPct = maxDailyLossPct;
  if (positionSizePct !== undefined) state.positionSizePct = positionSizePct;
  await saveState(state);
  res.json({ success: true, state });
});

app.post("/state/toggle-auto", async (req, res) => {
  state.autoMode = !state.autoMode;
  await saveState(state);
  sendTelegram(state.autoMode ? '▶ Bot automático activado (Servidor 24/7)' : '■ Bot automático detenido (Servidor)');
  if (state.autoMode) runAutoCheck();
  res.json({ success: true, autoMode: state.autoMode });
});

app.post("/state/close-trade", async (req, res) => {
  if (!state.openTrades || state.openTrades.length === 0) return res.status(400).json({ error: "No hay operaciones abiertas para cerrar" });
  const { pair, id } = req.body || {};
  let target;
  if (id) target = state.openTrades.find(t => t.id === id);
  else if (pair) target = state.openTrades.find(t => t.pair === pair);
  else if (state.openTrades.length === 1) target = state.openTrades[0];
  else return res.status(400).json({ error: "Hay varias operaciones abiertas, especificá 'pair' o 'id' para elegir cuál cerrar", openTrades: state.openTrades });
  if (!target) return res.status(404).json({ error: "No se encontró esa operación abierta" });
  try {
    const { closes } = await fetchKlines(target.pair, "1m", 2);
    const currentPrice = closes[closes.length - 1];
    await closeTradeById(target.id, currentPrice, "Cierre Manual");
    res.json({ success: true, state });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/state/reset", async (req, res) => {
  state = { ...DEFAULT_STATE, trades: [], openTrades: [], dailyPnl: 0, dailyTrades: 0, consecutiveLosses: 0, lastResetDate: new Date().toDateString() };
  await saveState(state);
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

async function sendDailySummaryMsg() {
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
  await saveState(state);
}

// ── Backtest Engine ───────────────────────────────────────
async function fetchHistoricalCandles(pair, tf, days) {
  const limit = 1000;
  const tfMs = { '5m': 5*60000, '15m': 15*60000, '1h': 3600000, '4h': 4*3600000, '1d': 86400000 }[tf];
  const totalCandles = Math.min(Math.ceil((days * 86400000) / tfMs), 5000); // cap for safety
  let allCandles = [];
  let endTime = Date.now();
  
  while (allCandles.length < totalCandles) {
    const res = await fetch(`https://data-api.binance.vision/api/v3/klines?symbol=${pair}&interval=${tf}&limit=${limit}&endTime=${endTime}`);
    if (!res.ok) throw new Error('Binance fetch failed: ' + res.status);
    const data = await res.json();
    if (data.length === 0) break;
    allCandles = data.concat(allCandles);
    endTime = data[0][0] - 1;
    if (data.length < limit) break;
    if (allCandles.length >= totalCandles) break;
  }
  
  return allCandles.map(k => ({
    time: k[0], open: parseFloat(k[1]), high: parseFloat(k[2]),
    low: parseFloat(k[3]), close: parseFloat(k[4])
  }));
}

function runBacktestEngine(candles, config) {
  const { minConfidence, riskPct, initialCapital, strategy = 'original' } = config;
  const analyzeFn = strategy === 'improved' ? analyzeImproved : analyze;
  const minHistory = strategy === 'improved' ? 210 : 200;
  let capital = initialCapital;
  let trades = [];
  let openTrade = null;
  let peakCapital = initialCapital;
  let maxDrawdown = 0;
  
  for (let i = minHistory; i < candles.length; i++) {
    const window = candles.slice(Math.max(0, i - minHistory), i + 1);
    const closes = window.map(c => c.close);
    const highs = window.map(c => c.high);
    const lows = window.map(c => c.low);
    const current = candles[i];
    
    if (openTrade) {
      let closed = false, exitPrice = null, reason = null;
      if (openTrade.signal === 'COMPRAR') {
        if (current.high >= openTrade.tp) { exitPrice = openTrade.tp; reason = 'TP'; closed = true; }
        else if (current.low <= openTrade.sl) { exitPrice = openTrade.sl; reason = 'SL'; closed = true; }
      } else {
        if (current.low <= openTrade.tp) { exitPrice = openTrade.tp; reason = 'TP'; closed = true; }
        else if (current.high >= openTrade.sl) { exitPrice = openTrade.sl; reason = 'SL'; closed = true; }
      }
      if (closed) {
        const pricePct = openTrade.signal === 'COMPRAR' 
          ? (exitPrice - openTrade.entry) / openTrade.entry 
          : (openTrade.entry - exitPrice) / openTrade.entry;
        const pnl = openTrade.size * pricePct;
        capital += pnl;
        trades.push({ ...openTrade, exitPrice, pnl, reason, closeTime: current.time });
        openTrade = null;
        if (capital > peakCapital) peakCapital = capital;
        const dd = (peakCapital - capital) / peakCapital;
        if (dd > maxDrawdown) maxDrawdown = dd;
      }
      continue;
    }
    
    const a = analyzeFn(closes, highs, lows);
    if (a && a.signal !== 'NEUTRO' && a.confidence >= minConfidence) {
      const size = capital * riskPct;
      openTrade = { signal: a.signal, entry: a.entry, tp: a.tp, sl: a.sl, size, openTime: current.time, confidence: a.confidence };
    }
  }
  
  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const winRate = trades.length > 0 ? (wins / trades.length * 100) : 0;
  const totalPnl = capital - initialCapital;
  const totalReturn = (totalPnl / initialCapital) * 100;
  
  return {
    trades: trades.length, wins, losses,
    winRate: winRate.toFixed(1),
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalReturn: totalReturn.toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2)
  };
}

app.post("/backtest", async (req, res) => {
  const { pair = 'BTCUSDT', tf = '15m', days = 30, minConfidence = 70, riskPct = 0.20, initialCapital = 1000, strategy = 'original' } = req.body;
  try {
    const candles = await fetchHistoricalCandles(pair, tf, days);
    if (candles.length < 250) {
      return res.status(400).json({ error: 'No hay suficientes datos históricos para este período' });
    }
    const result = runBacktestEngine(candles, { minConfidence, riskPct, initialCapital, strategy });
    res.json({
      success: true,
      config: { pair, tf, days, minConfidence, riskPct, initialCapital, strategy },
      dataRange: { from: new Date(candles[0].time).toISOString(), to: new Date(candles[candles.length-1].time).toISOString(), totalCandles: candles.length },
      result
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, async () => {
  console.log(`Backend v2 corriendo en puerto ${PORT} - AUTO 24/7 habilitado`);
  await initMongo();
  scheduleDailySummary();
  sendTelegram(`🟢 Signal Bot Backend v2 iniciado\n⏰ ${new Date().toLocaleString('es-AR', {timeZone:'America/Argentina/Buenos_Aires'})}\n💾 Persistencia: ${stateCollection ? 'MongoDB conectado ✅' : 'Solo memoria ⚠️'}\n🤖 Modo AUTO: ${state.autoMode ? 'Activo' : 'Inactivo'}`);
  // Start the auto-check loop (runs every 60 seconds regardless of browser)
  setInterval(runAutoCheck, 60000);
});
