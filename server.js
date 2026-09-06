process.on('uncaughtException', (e) => { console.error('UNCAUGHT:', e); });
process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); });

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const { MongoClient } = require("mongodb");
const { calcPositionSize } = require("./risk"); // Fase 1 refactor: tamaño por riesgo (solo backtest por ahora)
// ── Fase 2: interruptor de tamaño para el VIVO (openTrade y manual) ──
// 'fixed' = comportamiento de siempre (positionSizePct del capital, hoy 30%).
// 'risk'  = módulo de riesgo: arriesga RISK_PER_TRADE_PCT del capital según la distancia al SL.
// Se deploya en 'fixed' y se cambia a 'risk' a propósito, en un commit aparte, igual que AUTO_TRADING_LIVE_ENABLED.
// Validado en backtest 5/9/2026: 0,75% tiene menor drawdown que el 30% fijo en las 4 ventanas de validación.
const SIZING_MODE = 'risk';
const RISK_PER_TRADE_PCT = 0.75;
// ── Fase 3a: vigilancia mínima ──
// AUTO_TRADING_LIVE_ENABLED vive ahora acá arriba (antes estaba dentro de runAutoCheckInner) para que
// /state, el resumen diario y el aviso de bloqueo lean el mismo valor que usa el loop.
// Sigue en false: el auto-trading solo opera en modo demo. Cambiar a true es una decisión aparte y explícita.
const AUTO_TRADING_LIVE_ENABLED = false;
// Aviso por Telegram cuando el auto-trading está bloqueado por el modo (autoMode=true, modo≠demo, live deshabilitado).
// Ya pasó dos veces (5/9 y 6/9) sin que nadie se enterara: el bot quedaba en silencio y parecía "falta de señal".
let modeBlockWarnedAt = 0;
const MODE_BLOCK_WARN_EVERY_MS = 6 * 60 * 60 * 1000; // repite el aviso cada 6 h mientras siga bloqueado
function autoTradingBlockedByMode() {
  return !!state.autoMode && state.tradingMode !== 'demo' && !AUTO_TRADING_LIVE_ENABLED;
}
function checkModeBlockAndWarn() {
  const now = Date.now();
  if (autoTradingBlockedByMode()) {
    if (now - modeBlockWarnedAt > MODE_BLOCK_WARN_EVERY_MS) {
      modeBlockWarnedAt = now;
      sendTelegram(`⚠️ AUTO-TRADING BLOQUEADO POR MODO\nEl bot está en modo ${String(state.tradingMode).toUpperCase()} con autoMode activo, pero el auto-trading real está deshabilitado (AUTO_TRADING_LIVE_ENABLED=false).\nNo va a abrir ninguna operación automática hasta volver a DEMO.\nPara corregir: POST /mode/set {"mode":"demo"} o desde el panel.`);
    }
  } else if (modeBlockWarnedAt) {
    modeBlockWarnedAt = 0;
    sendTelegram(`✅ Auto-trading desbloqueado: modo ${String(state.tradingMode).toUpperCase()}, vuelve a poder operar.`);
  }
}

// Formatea fecha/hora de Argentina a mano, sin depender de toLocaleString.
// Esto evita bugs de compatibilidad si el servidor corre con soporte de
// idiomas reducido (ICU chico), donde el formato en español a veces no
// respeta bien las opciones de 24hs.
function formatArgTime(date) {
  const argTime = new Date(date.getTime() - 3 * 60 * 60 * 1000); // Argentina = UTC-3 fijo
  const day = argTime.getUTCDate();
  const month = argTime.getUTCMonth() + 1;
  const year = argTime.getUTCFullYear();
  const hh = String(argTime.getUTCHours()).padStart(2, '0');
  const mm = String(argTime.getUTCMinutes()).padStart(2, '0');
  const ss = String(argTime.getUTCSeconds()).padStart(2, '0');
  return `${day}/${month}/${year}, ${hh}:${mm}:${ss}`;
}

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
  autoTFs: ["30m"], // Actualizado 1/8 — Tendencia corre en 30m (término medio entre 15-30min de Scalping/Rebote y las 48hs de antes)
  minConfidence: 90,
  requireMTF: false, // Only one TF now (4h), so multi-TF confirmation not needed
  maxDailyGainPct: 5,
  maxDailyLossPct: 3,
  positionSizePct: 30, // Actualizado 1/8 — bajado de 60% tras evidencia de pérdidas más grandes sin mejor filo
  subSlThresholdMin: 15, // Actualizado 1/8
  tpAtrMultiplier: 3.0, // qué tan lejos pide el TP en múltiplos de ATR (probando 2/3/4 — el SL siempre es la mitad, R:R 2:1 fijo)
  cooldownMinutes: 30, // minutos de enfriamiento por par+dirección después de un cierre por Sub-SL
  pairCooldowns: {}, // { "BTCUSDT-VENDER": timestampHastaElQueEstaBloqueado }
  subSlStreak: {}, // { "BTCUSDT-COMPRAR": 2 } — cuántas veces seguidas se repitió el mismo Sub-SL en esa dirección
  ghostTrades: [], // posiciones "fantasma": mismas condiciones que un trade cortado por Sub-SL,
                    // pero sin plata real — para medir si el Sub-SL realmente ayuda o perjudica
  ghostResults: [], // historial de resultados fantasma ya resueltos
  tradingMode: 'demo', // 'demo' | 'testnet' | 'real' — demo es 100% simulado, testnet/real ejecutan órdenes de verdad
  realModeConfirmed: false, // requiere una confirmación explícita antes de poder activar testnet/real por primera vez
  killSwitchActive: false, // interruptor de emergencia: fuerza todo a demo y detiene el AUTO
  consecutiveLosses: 0,
  lastResetDate: new Date().toDateString()
};

// ── State management (MongoDB - truly persistent) ─────────
let mongoClient = null;
let stateCollection = null;
let state = { ...DEFAULT_STATE };

// Campos que se COMPARTEN entre demo/testnet/real (la configuración del bot es
// la misma sin importar el modo — no tendría sentido operar distinto en cada uno).
const CONFIG_FIELDS = ['autoMode', 'autoPairs', 'autoTFs', 'minConfidence', 'requireMTF',
  'maxDailyGainPct', 'maxDailyLossPct', 'positionSizePct', 'subSlThresholdMin',
  'tpAtrMultiplier', 'cooldownMinutes', 'tradingMode', 'realModeConfirmed', 'killSwitchActive'];

// Campos FINANCIEROS: estos sí quedan completamente separados por modo — el
// capital, historial y operaciones de demo nunca se mezclan con los de testnet/real.
const FINANCIAL_FIELDS = ['capital', 'trades', 'openTrades', 'dailyPnl', 'dailyTrades',
  'consecutiveLosses', 'pairCooldowns', 'subSlStreak', 'ghostTrades', 'ghostResults', 'lastResetDate'];

function freshFinancialState() {
  const fresh = {};
  for (const f of FINANCIAL_FIELDS) fresh[f] = DEFAULT_STATE[f];
  return fresh;
}

async function loadFinancialDoc(mode) {
  const doc = await stateCollection.findOne({ _id: 'financial_' + mode });
  if (doc) { delete doc._id; return { ...freshFinancialState(), ...doc }; }
  return freshFinancialState();
}

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

    // Si existe el documento viejo "main" (de antes de separar por modo), lo
    // usamos como fuente para migrar TANTO la configuración como lo financiero
    // — la vez pasada me olvidé de migrar la config y reseteó tus umbrales sin avisar.
    const oldMain = await stateCollection.findOne({ _id: "main" });

    // 1) Config compartida
    let config = {};
    const configDoc = await stateCollection.findOne({ _id: "config" });
    if (configDoc) {
      delete configDoc._id;
      for (const f of CONFIG_FIELDS) config[f] = configDoc[f] !== undefined ? configDoc[f] : DEFAULT_STATE[f];
    } else if (oldMain) {
      // Primera vez con el esquema nuevo: migramos la config real desde "main", no defaults en blanco.
      for (const f of CONFIG_FIELDS) config[f] = oldMain[f] !== undefined ? oldMain[f] : DEFAULT_STATE[f];
      await stateCollection.insertOne({ _id: "config", ...config });
      console.log("Migración: configuración real copiada desde 'main' (no se resetearon los umbrales)");
    } else {
      for (const f of CONFIG_FIELDS) config[f] = DEFAULT_STATE[f];
      await stateCollection.insertOne({ _id: "config", ...config });
    }

    // 2) Datos financieros del modo que estaba activo (demo/testnet/real, cada uno en su propio documento)
    const financial = await loadFinancialDoc(config.tradingMode || 'demo');
    state = { ...DEFAULT_STATE, ...config, ...financial };
    console.log(`🔧 Config cargada al arrancar — minConfidence: ${state.minConfidence}, autoTFs: ${JSON.stringify(state.autoTFs)}, positionSizePct: ${state.positionSizePct}`);

    // Migración de compatibilidad: si todavía no hay nada guardado en
    // "financial_demo", migramos el capital/trades de "main" una sola vez.
    const demoFinancialExists = await stateCollection.findOne({ _id: "financial_demo" });
    if (oldMain && !demoFinancialExists) {
      delete oldMain._id;
      const migratedFinancial = {};
      for (const f of FINANCIAL_FIELDS) migratedFinancial[f] = oldMain[f] !== undefined ? oldMain[f] : DEFAULT_STATE[f];
      await stateCollection.updateOne({ _id: "financial_demo" }, { $set: migratedFinancial }, { upsert: true });
      if ((config.tradingMode || 'demo') === 'demo') state = { ...state, ...migratedFinancial };
      console.log("Migración: historial del documento viejo 'main' copiado a 'financial_demo'");
    }

    console.log("Estado cargado desde MongoDB - Modo:", state.tradingMode, "- Capital:", state.capital);
  } catch (e) {
    console.log("Error conectando MongoDB:", e.message);
  }
}

async function saveState(newState) {
  state = newState;
  if (!stateCollection) return;
  try {
    const configPart = {};
    for (const f of CONFIG_FIELDS) configPart[f] = state[f];
    const financialPart = {};
    for (const f of FINANCIAL_FIELDS) financialPart[f] = state[f];
    await stateCollection.updateOne({ _id: "config" }, { $set: configPart }, { upsert: true });
    await stateCollection.updateOne({ _id: "financial_" + state.tradingMode }, { $set: financialPart }, { upsert: true });
  } catch (e) { console.log("Save state error:", e.message); }
}

// ── HMAC ──────────────────────────────────────────────────
// ── Modo real/testnet: claves SOLO desde variables de entorno del servidor,
// nunca desde el navegador — mucho más seguro que guardarlas en cookies.
function getBinanceCredentials(mode) {
  if (mode === 'testnet') {
    return {
      apiKey: process.env.BINANCE_TESTNET_API_KEY,
      apiSecret: process.env.BINANCE_TESTNET_API_SECRET,
      baseUrl: 'https://testnet.binance.vision'
    };
  }
  if (mode === 'real') {
    return {
      apiKey: process.env.BINANCE_REAL_API_KEY,
      apiSecret: process.env.BINANCE_REAL_API_SECRET,
      baseUrl: 'https://api.binance.com'
    };
  }
  return null;
}

async function getRealBalance(mode) {
  const creds = getBinanceCredentials(mode);
  if (!creds || !creds.apiKey || !creds.apiSecret) throw new Error(`Faltan las claves de Binance (${mode}) configuradas en el servidor (variables de entorno)`);
  const timestamp = Date.now();
  const query = `timestamp=${timestamp}`;
  const signature = hmac(creds.apiSecret, query);
  const response = await fetch(`${creds.baseUrl}/api/v3/account?${query}&signature=${signature}`, {
    headers: { "X-MBX-APIKEY": creds.apiKey }
  });
  const data = await response.json();
  if (data.code) throw new Error(data.msg || 'Error de Binance');
  const usdt = data.balances?.find(b => b.asset === "USDT");
  return usdt ? parseFloat(usdt.free) : 0;
}

async function placeRealOrder(mode, symbol, side, quantity) {
  const creds = getBinanceCredentials(mode);
  if (!creds || !creds.apiKey || !creds.apiSecret) throw new Error(`Faltan las claves de Binance (${mode}) configuradas en el servidor (variables de entorno)`);
  const timestamp = Date.now();
  const params = `symbol=${symbol}&side=${side}&type=MARKET&quantity=${quantity}&timestamp=${timestamp}`;
  const signature = hmac(creds.apiSecret, params);
  const response = await fetch(`${creds.baseUrl}/api/v3/order`, {
    method: "POST",
    headers: { "X-MBX-APIKEY": creds.apiKey, "Content-Type": "application/x-www-form-urlencoded" },
    body: `${params}&signature=${signature}`
  });
  const data = await response.json();
  if (data.code) throw new Error(data.msg || 'Error de Binance');
  return data;
}

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
// Compara la volatilidad reciente (últimas 5 velas) contra la volatilidad
// "normal" del par (últimas 50 velas) para calibrar qué tan lejos pedir el
// TP/SL. Un día con fuerza (rompiendo noticias, momentum) agranda el objetivo;
// un día plano lo achica, en vez de pedir siempre el mismo % fijo.
function calcVolatilityRegime(highs, lows, closes) {
  const shortATR = calcATR(highs, lows, closes, 5);
  const baseATR = calcATR(highs, lows, closes, 50);
  if (!shortATR || !baseATR || baseATR === 0) return { regime: 'normal', ratio: 1, multiplierScale: 1 };
  const ratio = shortATR / baseATR;
  let regime, multiplierScale;
  if (ratio >= 1.3) { regime = 'Alta (día con fuerza)'; multiplierScale = 1.4; }
  else if (ratio <= 0.7) { regime = 'Baja (día plano)'; multiplierScale = 0.7; }
  else { regime = 'Normal'; multiplierScale = 1.0; }
  return { regime, ratio, multiplierScale };
}

// ADX: mide qué tan FUERTE es una tendencia, sin importar la dirección.
// ADX bajo (<20) = mercado lateral/sin dirección clara — acá es donde las
// estrategias de tendencia (Reversión/Tendencia) generan señales falsas por
// leer ruido de cierre como si fuera tendencia real.
function calcADX(highs, lows, closes, period = 14) {
  const n = highs.length;
  if (n < period * 2 + 1) return null;
  let plusDM = [], minusDM = [], tr = [];
  for (let i = 1; i < n; i++) {
    const upMove = highs[i] - highs[i - 1];
    const downMove = lows[i - 1] - lows[i];
    plusDM.push(upMove > downMove && upMove > 0 ? upMove : 0);
    minusDM.push(downMove > upMove && downMove > 0 ? downMove : 0);
    tr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
  }
  const wilderSmooth = (arr) => {
    let smoothed = [arr.slice(0, period).reduce((a, b) => a + b, 0)];
    for (let i = period; i < arr.length; i++) smoothed.push(smoothed[smoothed.length - 1] - (smoothed[smoothed.length - 1] / period) + arr[i]);
    return smoothed;
  };
  const smoothTR = wilderSmooth(tr), smoothPlusDM = wilderSmooth(plusDM), smoothMinusDM = wilderSmooth(minusDM);
  const plusDI = smoothPlusDM.map((v, i) => smoothTR[i] > 0 ? (v / smoothTR[i]) * 100 : 0);
  const minusDI = smoothMinusDM.map((v, i) => smoothTR[i] > 0 ? (v / smoothTR[i]) * 100 : 0);
  const dx = plusDI.map((v, i) => {
    const sum = v + minusDI[i];
    return sum === 0 ? 0 : (Math.abs(v - minusDI[i]) / sum) * 100;
  });
  if (dx.length < period) return null;
  let adx = dx.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < dx.length; i++) adx = (adx * (period - 1) + dx[i]) / period;
  return adx;
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

// Nube de Ichimoku: calcula dónde está el techo/piso de la nube que corresponde
// al precio ACTUAL (los Senkou Spans se proyectan 26 velas hacia adelante cuando
// se dibujan, así que la nube "de hoy" se calculó con datos de hace 26 velas).
function calcIchimokuCloud(highs, lows, closes) {
  const p1 = 9, p2 = 26, p3 = 52, disp = 26;
  const n = closes.length;
  if (n < p3 + disp + 1) return null;
  const donchian = (period, idx) => {
    const h = highs.slice(idx - period + 1, idx + 1);
    const l = lows.slice(idx - period + 1, idx + 1);
    return (Math.max(...h) + Math.min(...l)) / 2;
  };
  const lastIdx = n - 1;
  const cloudIdx = lastIdx - disp; // vela desde la que se proyectó la nube que cae sobre el precio de hoy
  if (cloudIdx - p3 + 1 < 0) return null;
  const senkouA = (donchian(p1, cloudIdx) + donchian(p2, cloudIdx)) / 2;
  const senkouB = donchian(p3, cloudIdx);
  const cloudTop = Math.max(senkouA, senkouB);
  const cloudBottom = Math.min(senkouA, senkouB);
  const price = closes[lastIdx];
  return {
    cloudTop, cloudBottom, price,
    aboveCloud: price > cloudTop,
    belowCloud: price < cloudBottom,
    insideCloud: price >= cloudBottom && price <= cloudTop
  };
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
// Estrategia REBOTE: detecta divergencia entre el precio y el RSI en 15m —
// si el precio hace un mínimo más bajo pero el RSI hace un mínimo MÁS ALTO
// (el impulso vendedor se está agotando aunque el precio siga cayendo), es
// una señal de rebote de corto plazo. Funciona DENTRO de cualquier tendencia
// mayor (no necesita que el mercado esté lateral, a diferencia de Rango) —
// pensada para resolverse en 15-30 minutos, no en horas.
function analyzeRebote(closes, highs, lows) {
  const lookback = 20;
  if (!closes || closes.length < lookback + 20) return null;
  const rsiSeries = calcRSISeries(closes, 14);
  const rsiOffset = closes.length - rsiSeries.length; // rsiSeries[i] corresponde a closes[i+rsiOffset]
  const n = closes.length;
  const half = Math.floor(lookback / 2);
  const window = Array.from({ length: lookback }, (_, i) => n - lookback + i);
  const firstHalf = window.slice(0, half).filter(i => i - rsiOffset >= 0);
  const secondHalf = window.slice(half).filter(i => i - rsiOffset >= 0);
  if (firstHalf.length === 0 || secondHalf.length === 0) return null;

  const idxOlderLow = firstHalf.reduce((best, i) => lows[i] < lows[best] ? i : best, firstHalf[0]);
  const idxRecentLow = secondHalf.reduce((best, i) => lows[i] < lows[best] ? i : best, secondHalf[0]);
  const idxOlderHigh = firstHalf.reduce((best, i) => highs[i] > highs[best] ? i : best, firstHalf[0]);
  const idxRecentHigh = secondHalf.reduce((best, i) => highs[i] > highs[best] ? i : best, secondHalf[0]);

  const rsiOlderLow = rsiSeries[idxOlderLow - rsiOffset], rsiRecentLow = rsiSeries[idxRecentLow - rsiOffset];
  const rsiOlderHigh = rsiSeries[idxOlderHigh - rsiOffset], rsiRecentHigh = rsiSeries[idxRecentHigh - rsiOffset];

  let signal = 'NEUTRO', direction = 'ESPERAR', confidence = 0;
  if (lows[idxRecentLow] < lows[idxOlderLow] && rsiRecentLow > rsiOlderLow) {
    signal = 'COMPRAR'; direction = 'LARGO';
    confidence = Math.round(Math.min(90, 65 + (rsiRecentLow - rsiOlderLow) * 1.5));
  } else if (highs[idxRecentHigh] > highs[idxOlderHigh] && rsiRecentHigh < rsiOlderHigh) {
    signal = 'VENDER'; direction = 'SHORT';
    confidence = Math.round(Math.min(90, 65 + (rsiOlderHigh - rsiRecentHigh) * 1.5));
  }

  const price = closes[n - 1];
  const atr = calcATR(highs, lows, closes) || price * 0.01;
  let entry = price, tp, sl;
  // Objetivos agrandados (30/7, a pedido de Juan) — los anteriores eran
  // demasiado chicos en dólares frente a la comisión pagada.
  if (signal === 'COMPRAR') { tp = price + atr * 2.2; sl = price - atr * 1.4; }
  else if (signal === 'VENDER') { tp = price - atr * 2.2; sl = price + atr * 1.4; }
  else { tp = price + atr; sl = price - atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence, price, entry, tp, sl, rr, strategy: 'Rebote', atr };
}

// Estrategia SCALPING: pensada para operar como lo hace Juan a mano — varias
// veces por hora, 15-30 minutos por operación, no horas. Dispara con un cruce
// rápido de medias (9/21) en 5m, con RSI confirmando que no está en un extremo
// agotado. Usa DOS niveles de TP explícitos (no trailing): TP1 cierra la mitad
// rápido, TP2 se queda con el resto buscando un poco más antes de cerrar del todo.
// Lee la "estructura" real del precio en 1m para decidir si el momento actual
// tiene tendencia clara (aunque venga en serrucho) o es genuinamente lateral —
// usando 3 factores: pendiente general, fuerza del cuerpo de las velas
// (cuerpo grande = convicción, muchos dojis = indecisión), y si el volumen
// reciente confirma que hay "combustible" real detrás del movimiento.
function detectMicroRegime(opens, highs, lows, closes, volumes) {
  const n = closes.length;
  const lookback = Math.min(20, n - 1);
  if (lookback < 10) return { isTrending: false, direction: 'lateral' };
  const adx = calcADX(highs, lows, closes, 14);

  let bodySum = 0, rangeSum = 0;
  for (let i = n - lookback; i < n; i++) {
    bodySum += Math.abs(closes[i] - opens[i]);
    rangeSum += (highs[i] - lows[i]) || 0.00001;
  }
  const bodyRatio = bodySum / rangeSum; // 0 a 1 — más alto = velas con cuerpo fuerte, no dojis

  const recentVol = volumes.slice(-5).reduce((a, b) => a + b, 0) / 5;
  const baseVol = volumes.slice(-lookback, -5).reduce((a, b) => a + b, 0) / Math.max(1, lookback - 5);
  const volConfirms = recentVol > baseVol * 1.05;

  // Detección de desaceleración: comparamos el tamaño de las últimas 4 velas
  // contra las 4 anteriores a esas — si vienen achicándose, el impulso se
  // está agotando, aunque la pendiente general todavía diga "tendencia". Es
  // justo el momento más incierto, mejor esperar a que se defina de nuevo.
  const recentBodies = [];
  for (let i = n - 4; i < n; i++) recentBodies.push(Math.abs(closes[i] - opens[i]));
  const priorBodies = [];
  for (let i = n - 8; i < n - 4; i++) priorBodies.push(Math.abs(closes[i] - opens[i]));
  const avgRecentBody = recentBodies.reduce((a, b) => a + b, 0) / recentBodies.length;
  const avgPriorBody = priorBodies.reduce((a, b) => a + b, 0) / priorBodies.length;
  const isDecelerating = avgPriorBody > 0 && avgRecentBody < avgPriorBody * 0.6;

  const slopePct = (closes[n - 1] - closes[n - lookback]) / closes[n - lookback];
  const isTrending = adx !== null && adx >= 20 && bodyRatio >= 0.45 && volConfirms && !isDecelerating;
  return { isTrending, adx, bodyRatio, volConfirms, isDecelerating, slopePct, direction: slopePct >= 0 ? 'up' : 'down' };
}

function analyzeScalping(closes, highs, lows, opens1m, highs1m, lows1m, closes1m, volumes1m, highs15, lows15, closes15) {
  if (!closes || closes.length < 31) return null;
  const price = closes[closes.length - 1];
  // El ATR de 5m es chico por naturaleza — usarlo para el objetivo hacía que
  // la operación se resolviera en minutos, no en los 15-30 min que buscamos.
  // 5m/1m siguen usándose para LEER hacia dónde va el mercado (la entrada),
  // pero el TAMAÑO del objetivo ahora se calcula con la volatilidad real de
  // 15m — mucho más representativa de un movimiento de media hora.
  const atr15 = (highs15 && lows15 && closes15) ? calcATR(highs15, lows15, closes15) : null;
  const atr = atr15 || calcATR(highs, lows, closes) || price * 0.005;
  const regime = (opens1m && closes1m) ? detectMicroRegime(opens1m, highs1m, lows1m, closes1m, volumes1m) : { isTrending: false, direction: 'lateral' };

  let signal = 'NEUTRO', direction = 'ESPERAR', confidence = 0;
  let lateralTp1 = null, lateralTp2 = null, lateralSl = null;

  if (regime.isTrending) {
    // Hay tendencia clara en el corto plazo (aunque sea en serrucho) — entra
    // mientras la tendencia SIGA confirmada por el detector (pendiente,
    // volumen, sin desacelerar), no solo en el instante exacto del cruce.
    // Antes solo entraba en el segundo que el cruce se confirmaba — si la
    // tendencia ya venía corriendo hace 10-15 velas, el bot se la perdía.
    const ema9 = calcEMA(closes, 9), ema21 = calcEMA(closes, 21);
    if (ema9 && ema21) {
      const rsiSeries = calcRSISeries(closes, 14);
      const rsi = rsiSeries[rsiSeries.length - 1];
      const alignedUp = ema9 > ema21; // el promedio corto sigue por encima del largo
      const alignedDown = ema9 < ema21;
      if (alignedUp && rsi < 70 && regime.direction === 'up') {
        signal = 'COMPRAR'; direction = 'LARGO';
        confidence = Math.round(Math.min(88, 65 + (70 - rsi) / 2));
      } else if (alignedDown && rsi > 30 && regime.direction === 'down') {
        signal = 'VENDER'; direction = 'SHORT';
        confidence = Math.round(Math.min(88, 65 + (rsi - 30) / 2));
      }
    }
  } else {
    // Scalping-Lateral REACTIVADA con arreglo (1/9/2026) — estuvo pausada
    // desde el 31/7 (0% de aciertos en 16 operaciones, luego 42/50 perdedoras
    // tras 3 intentos de arreglo). Causa real diagnosticada en su momento:
    // el objetivo se calculaba como fracción del rango de 30 min sin piso —
    // en mercado tranquilo (rango angosto), el objetivo era tan chico que ni
    // tocándolo cubría la comisión. Se agrega ahora el piso que quedó
    // pendiente: MIN_RANGE_ATR exige que el rango completo sea al menos
    // este múltiplo del ATR antes de operar — si el rango es angosto, no
    // hay señal, sin importar la posición del precio adentro. Backtesteado
    // (ver runScalpingRealisticBacktest con onlySubStrategy) antes de
    // confiar en esto en vivo — no reactivar sin ese backtest.
    const MIN_RANGE_ATR = 1.5;
    const rangeLookback = Math.min(6, highs.length);
    const recentHighs = highs.slice(-rangeLookback);
    const recentLows = lows.slice(-rangeLookback);
    const rangeTop = Math.max(...recentHighs);
    const rangeBottom = Math.min(...recentLows);
    const rangeSize = rangeTop - rangeBottom || price * 0.001;
    if (rangeSize >= atr * MIN_RANGE_ATR) {
      const posInRange = (price - rangeBottom) / rangeSize;
      if (posInRange <= 0.15) {
        signal = 'COMPRAR'; direction = 'LARGO';
        confidence = Math.round(75 + (0.15 - posInRange) * 60);
      } else if (posInRange >= 0.85) {
        signal = 'VENDER'; direction = 'SHORT';
        confidence = Math.round(75 + (posInRange - 0.85) * 60);
      }
      if (signal === 'COMPRAR') {
        lateralTp1 = rangeBottom + rangeSize * 0.6; lateralTp2 = rangeTop; lateralSl = rangeBottom - atr * 0.5;
      } else if (signal === 'VENDER') {
        lateralTp1 = rangeTop - rangeSize * 0.6; lateralTp2 = rangeBottom; lateralSl = rangeTop + atr * 0.5;
      }
    }
  }

  let entry = price, tp1, tp2, sl;
  // Objetivos agrandados (30/7, a pedido de Juan) — los anteriores eran
  // demasiado chicos en dólares, la comisión se comía casi toda la ganancia
  // aunque tocaran TP. Ahora apuntan a movimientos más parecidos a los que
  // busca en manual, no solo a resolverse rápido.
  if (lateralTp1 !== null) {
    // Lateral: usamos el objetivo del rango real, no el ATR genérico.
    tp1 = lateralTp1; tp2 = lateralTp2; sl = lateralSl;
  } else if (signal === 'COMPRAR') {
    tp1 = price + atr * 1.5; tp2 = price + atr * 3.0; sl = price - atr * 1.8;
  } else if (signal === 'VENDER') {
    tp1 = price - atr * 1.5; tp2 = price - atr * 3.0; sl = price + atr * 1.8;
  } else {
    tp1 = price + atr; tp2 = price + atr * 2; sl = price - atr;
  }
  const rr = Math.abs(tp2 - entry) / Math.abs(sl - entry);
  const regimeLabel = regime.isTrending ? `Tendencia ${regime.direction}` : (regime.isDecelerating ? 'Desacelerando (se esperó)' : 'Lateral');
  return { signal, direction, confidence, price, entry, tp: tp1, tp2, sl, rr, strategy: 'Scalping', atr, regime: regimeLabel };
}

// PRIMERA VERSIÓN (básica) de entrada multi-timeframe para Tendencia: antes
// de dejarla entrar, chequea en 15m si el precio ya se alejó mucho de su
// propio promedio de corto plazo — si está muy estirado, es señal de que el
// movimiento de 4h ya viene "cansado" en el corto plazo, y probablemente
// estemos llegando tarde. Se puede afinar más adelante (esperar un
// retroceso + vela de rebote a favor), esto es el primer paso.
async function checkGoodEntry15m(pair, direction) {
  try {
    const { closes: closes15 } = await fetchKlines(pair, '15m', 30);
    const ema9_15 = calcEMA(closes15, 9);
    const price15 = closes15[closes15.length - 1];
    if (!ema9_15) return true; // sin datos suficientes, no bloqueamos por las dudas
    const distPct = (price15 - ema9_15) / ema9_15;
    // Si es COMPRAR, no queremos que el precio ya esté muy por ENCIMA de su
    // promedio corto (comprando caro); si es VENDER, no muy por DEBAJO.
    if (direction === 'LARGO') return distPct < 0.004;
    if (direction === 'SHORT') return distPct > -0.004;
    return true;
  } catch (e) { return true; } // si falla la consulta, no bloqueamos la señal por eso
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
  const adxVal = calcADX(highs, lows, closes);
  const trendConfirmed = adxVal === null || adxVal >= 20; // sin ADX suficiente, no bloqueamos (fallback conservador)
  if (diff >= 4 && isVolatileEnough && !trendDown && trendConfirmed) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && !trendUp && trendConfirmed) { signal = 'VENDER'; direction = 'SHORT'; }
  const cloud = calcIchimokuCloud(highs, lows, closes);
  if (cloud) {
    if (signal === 'COMPRAR' && !cloud.aboveCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; } // resistencia de la nube sin romper
    if (signal === 'VENDER' && !cloud.belowCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; } // soporte de la nube sin romper
  }
  let entry = price, tp, sl;
  const vol = calcVolatilityRegime(highs, lows, closes);
  const baseTp = state.tpAtrMultiplier || 3.0;
  const slMultiplier = (baseTp / 2) * vol.multiplierScale, tpMultiplier = baseTp * vol.multiplierScale;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Reversión', atr, cloud, volRegime: vol.regime, adx: adxVal };
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
  const adxVal = calcADX(highs, lows, closes);
  const trendConfirmed = adxVal === null || adxVal >= 20;
  if (diff >= 4 && isVolatileEnough && trendConfirmed) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && trendConfirmed) { signal = 'VENDER'; direction = 'SHORT'; }
  const cloud = calcIchimokuCloud(highs, lows, closes);
  if (cloud) {
    if (signal === 'COMPRAR' && !cloud.aboveCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; } // resistencia de la nube sin romper
    if (signal === 'VENDER' && !cloud.belowCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; } // soporte de la nube sin romper
  }
  let entry = price, tp, sl;
  const vol = calcVolatilityRegime(highs, lows, closes);
  const baseTp = state.tpAtrMultiplier || 3.0;
  const slMultiplier = (baseTp / 2) * vol.multiplierScale, tpMultiplier = baseTp * vol.multiplierScale;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Tendencia', atr, cloud, volRegime: vol.regime, adx: adxVal };
}

// Variante experimental para backtest (5/8/2026): igual que analyzeTrendFollow,
// pero además de escalar por volatilidad de precio, escala la distancia del
// TP/SL por la FUERZA de la tendencia (ADX) — hipótesis del usuario: si el ADX
// recién cruzó el mínimo (20-25), la tendencia es débil/reciente y conviene un
// objetivo más cerca; si el ADX es alto (35+), hay más convicción real y
// conviene dejarlo correr más lejos. Mantiene el mismo R:R 2:1 — solo cambia
// la distancia absoluta, no la proporción entre SL y TP.
function analyzeTrendFollowAdx(closes, highs, lows) {
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
  if (ema20Now > ema50Now) bull += 1; else bear += 1;
  if (ema20Now > ema20Before) bull += 2; else if (ema20Now < ema20Before) bear += 2;
  if (ema50Now > ema50Before) bull += 1; else if (ema50Now < ema50Before) bear += 1;
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal = 'NEUTRO', direction = 'ESPERAR';
  const adxVal = calcADX(highs, lows, closes);
  const trendConfirmed = adxVal === null || adxVal >= 20;
  if (diff >= 4 && isVolatileEnough && trendConfirmed) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && trendConfirmed) { signal = 'VENDER'; direction = 'SHORT'; }
  const cloud = calcIchimokuCloud(highs, lows, closes);
  if (cloud) {
    if (signal === 'COMPRAR' && !cloud.aboveCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
    if (signal === 'VENDER' && !cloud.belowCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
  }
  let entry = price, tp, sl;
  const vol = calcVolatilityRegime(highs, lows, closes);
  const baseTp = state.tpAtrMultiplier || 3.0;
  let adxScale = 1.0;
  if (adxVal !== null) {
    if (adxVal >= 35) adxScale = 1.3;      // tendencia muy fuerte -> objetivo más lejos
    else if (adxVal < 25) adxScale = 0.7;  // tendencia recién confirmada/débil -> objetivo más cerca
  }
  const slMultiplier = (baseTp / 2) * vol.multiplierScale * adxScale, tpMultiplier = baseTp * vol.multiplierScale * adxScale;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Tendencia', atr, cloud, volRegime: vol.regime, adx: adxVal, adxScale };
}

// Variante experimental (10/8/2026) — misma entrada que analyzeTrendFollowAdx,
// pero SIN el estirón de 1.3x cuando el ADX es fuerte (solo mantiene el
// achique a 0.7x cuando es débil). Hipótesis: el backtest realista mostró que
// ni dándole 5x más tiempo el TP se llega a tocar — puede que el objetivo
// esté pedido demasiado lejos, no que le falte paciencia. Esto prueba un
// objetivo más conservador (nunca más lejos que el base, a veces más cerca).
function analyzeTrendFollowAdxCapped(closes, highs, lows) {
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
  if (ema20Now > ema50Now) bull += 1; else bear += 1;
  if (ema20Now > ema20Before) bull += 2; else if (ema20Now < ema20Before) bear += 2;
  if (ema50Now > ema50Before) bull += 1; else if (ema50Now < ema50Before) bear += 1;
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal = 'NEUTRO', direction = 'ESPERAR';
  const adxVal = calcADX(highs, lows, closes);
  const trendConfirmed = adxVal === null || adxVal >= 20;
  if (diff >= 4 && isVolatileEnough && trendConfirmed) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && trendConfirmed) { signal = 'VENDER'; direction = 'SHORT'; }
  const cloud = calcIchimokuCloud(highs, lows, closes);
  if (cloud) {
    if (signal === 'COMPRAR' && !cloud.aboveCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
    if (signal === 'VENDER' && !cloud.belowCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
  }
  let entry = price, tp, sl;
  const vol = calcVolatilityRegime(highs, lows, closes);
  const baseTp = state.tpAtrMultiplier || 3.0;
  let adxScale = 1.0;
  if (adxVal !== null && adxVal < 25) adxScale = 0.7; // solo achica, nunca estira
  const slMultiplier = (baseTp / 2) * vol.multiplierScale * adxScale, tpMultiplier = baseTp * vol.multiplierScale * adxScale;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Tendencia', atr, cloud, volRegime: vol.regime, adx: adxVal, adxScale };
}

// Variante experimental (10/8/2026) — misma entrada, pero con la base del TP
// reducida a la mitad (1.5x ATR en vez de 3x) y SIN escalado por ADX (para
// aislar el efecto de una sola variable: ¿el problema es que el objetivo de
// base ya es demasiado ambicioso, más allá de cualquier ajuste por ADX?).
// R:R se mantiene fijo en 2:1.
function analyzeTrendFollowSmallTp(closes, highs, lows) {
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
  if (ema20Now > ema50Now) bull += 1; else bear += 1;
  if (ema20Now > ema20Before) bull += 2; else if (ema20Now < ema20Before) bear += 2;
  if (ema50Now > ema50Before) bull += 1; else if (ema50Now < ema50Before) bear += 1;
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal = 'NEUTRO', direction = 'ESPERAR';
  const adxVal = calcADX(highs, lows, closes);
  const trendConfirmed = adxVal === null || adxVal >= 20;
  if (diff >= 4 && isVolatileEnough && trendConfirmed) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && trendConfirmed) { signal = 'VENDER'; direction = 'SHORT'; }
  const cloud = calcIchimokuCloud(highs, lows, closes);
  if (cloud) {
    if (signal === 'COMPRAR' && !cloud.aboveCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
    if (signal === 'VENDER' && !cloud.belowCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
  }
  let entry = price, tp, sl;
  const vol = calcVolatilityRegime(highs, lows, closes);
  const baseTp = 1.5; // fijo, mitad del original (3.0), sin escalado por ADX
  const slMultiplier = (baseTp / 2) * vol.multiplierScale, tpMultiplier = baseTp * vol.multiplierScale;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr2 = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr: rr2, strategy: 'Tendencia', atr, cloud, volRegime: vol.regime, adx: adxVal, adxScale: 1.0 };
}
// 2:1), acá se arriesga más para ganar menos (R:R 1:2 — SL el doble de
// lejos que el TP). Necesita mucho más win rate para ser rentable; se prueba
// para comparar contra la convención "SL corto / TP largo" que ya funciona.
// Variante experimental (11/8/2026) — misma lógica que analyzeTrendFollowAdx,
// pero exige ADX≥25 para confirmar tendencia (en vez de ≥20). Menos señales,
// pero solo las que ya vienen con más fuerza real — probando si eso ayuda a
// que la comisión pese menos al operar con menos frecuencia.
// ── Estructura de mercado (12/8/2026) — detección de swings SIN lookahead:
// un candle solo se confirma como swing high/low después de que pasaron
// `confirmBars` velas adicionales (igual que lo haría un trader mirando el
// gráfico en tiempo real, nunca sabe que es un swing hasta que ya pasó).
// closes/highs/lows deben venir SOLO hasta el índice actual del backtest —
// no se les pasa nada del futuro.
function detectConfirmedSwings(highs, lows, lookback = 3, confirmBars = 3) {
  const n = highs.length;
  const swingHighs = [], swingLows = [];
  // Recorre hasta n - confirmBars - 1: el último candle evaluable es el que
  // ya tiene `confirmBars` velas después para confirmar que fue un pico/valle real.
  for (let i = lookback; i < n - confirmBars; i++) {
    let isHigh = true, isLow = true;
    for (let j = i - lookback; j <= i + confirmBars; j++) {
      if (j === i) continue;
      if (highs[j] >= highs[i]) isHigh = false;
      if (lows[j] <= lows[i]) isLow = false;
    }
    if (isHigh) swingHighs.push({ idx: i, price: highs[i] });
    if (isLow) swingLows.push({ idx: i, price: lows[i] });
  }
  return { swingHighs, swingLows };
}

// Clasifica estructura con los últimos 2 swings confirmados de cada tipo:
// HH+HL = alcista, LH+LL = bajista, cualquier otra combinación = sin estructura clara (rango).
function classifyStructure(swingHighs, swingLows) {
  if (swingHighs.length < 2 || swingLows.length < 2) return 'sin_datos';
  const lastHigh = swingHighs[swingHighs.length - 1], prevHigh = swingHighs[swingHighs.length - 2];
  const lastLow = swingLows[swingLows.length - 1], prevLow = swingLows[swingLows.length - 2];
  const higherHigh = lastHigh.price > prevHigh.price;
  const higherLow = lastLow.price > prevLow.price;
  const lowerHigh = lastHigh.price < prevHigh.price;
  const lowerLow = lastLow.price < prevLow.price;
  if (higherHigh && higherLow) return 'alcista';
  if (lowerHigh && lowerLow) return 'bajista';
  return 'rango';
}

// Filtro de espacio: ¿hay lugar real hasta el próximo obstáculo estructural
// antes de llegar al TP? Exige que la distancia libre sea al menos 2x la
// distancia al SL (mismo criterio que el ejemplo que diste).
function checkStructuralSpace(entry, slDistance, direction, swingHighs, swingLows) {
  if (direction === 'LARGO') {
    const obstaclesAbove = swingHighs.filter(s => s.price > entry).map(s => s.price);
    if (obstaclesAbove.length === 0) return { hasSpace: true, distanceToObstacle: null };
    const nearestResistance = Math.min(...obstaclesAbove);
    const freeSpace = nearestResistance - entry;
    return { hasSpace: freeSpace >= slDistance * 2, distanceToObstacle: freeSpace };
  } else {
    const obstaclesBelow = swingLows.filter(s => s.price < entry).map(s => s.price);
    if (obstaclesBelow.length === 0) return { hasSpace: true, distanceToObstacle: null };
    const nearestSupport = Math.max(...obstaclesBelow);
    const freeSpace = entry - nearestSupport;
    return { hasSpace: freeSpace >= slDistance * 2, distanceToObstacle: freeSpace };
  }
}

// Entrada nueva por estructura (1/9/2026) — reemplaza EMA20/50+ADX+Ichimoku
// por completo, no es un filtro más encima. Idea, según el marco de auditoría
// del 12/8 (tareas 2-5): el 4h es CONTEXTO (¿hacia dónde va el régimen
// grande?), nunca el gatillo directo. El gatillo real es la ruptura de un
// swing confirmado en el timeframe operativo (30m), a favor de ese régimen —
// no un cruce de EMA ni un nivel de ADX. El SL es estructural (el swing que
// originó la ruptura), no un múltiplo de ATR. El TP usa el filtro de espacio
// ya construido (checkStructuralSpace): apunta al obstáculo estructural más
// cercano, nunca más lejos de lo que hay lugar libre real.
function analyzeStructuralEntry(closes, highs, lows, closes4h, highs4h, lows4h, minAdx4h = 20) {
  if (closes.length < 40 || !closes4h || closes4h.length < 20) return null;

  // 0. Filtro de régimen macro (1/9/2026) — el hallazgo del split out-of-sample
  // mostró que esta entrada funciona bien en mercado con tendencia y pierde en
  // mercado lateral (mar-jun 2026 vs jun-sep 2026, confirmado igual en BTC y
  // ETH, y corroborado con noticias reales de esos meses: "consolidación
  // lateral" en marzo-abril). El ADX en 4h mide justo eso — fuerza de
  // tendencia, no dirección — así que se usa como gate previo: si el mercado
  // amplio no tiene tendencia fuerte, no se opera, sin importar que la
  // estructura local se vea bien (una ruptura en mercado lateral es
  // justamente el tipo de señal falsa que este filtro busca evitar).
  const adx4h = calcADX(highs4h, lows4h, closes4h, 14);
  if (adx4h === null || adx4h < minAdx4h) {
    return { signal: 'NEUTRO', strategy: 'Estructura', reason: `ADX 4h insuficiente (${adx4h !== null ? adx4h.toFixed(1) : 'null'} < ${minAdx4h}) — mercado sin tendencia clara`, adx4h };
  }

  // 1. Régimen en 4h — contexto, nunca gatillo directo.
  const { swingHighs: sh4h, swingLows: sl4h } = detectConfirmedSwings(highs4h, lows4h, 2, 2);
  const regime4h = classifyStructure(sh4h, sl4h);
  if (regime4h !== 'alcista' && regime4h !== 'bajista') {
    return { signal: 'NEUTRO', strategy: 'Estructura', reason: 'Sin régimen 4h claro (rango o datos insuficientes)', adx4h };
  }
  const wantLong = regime4h === 'alcista';

  // 2. Estructura en el timeframe operativo — debe confirmar el mismo sesgo del 4h.
  const { swingHighs, swingLows } = detectConfirmedSwings(highs, lows, 3, 3);
  const structureLocal = classifyStructure(swingHighs, swingLows);
  if ((wantLong && structureLocal !== 'alcista') || (!wantLong && structureLocal !== 'bajista')) {
    return { signal: 'NEUTRO', strategy: 'Estructura', reason: 'Estructura local no confirma el régimen 4h', adx4h };
  }
  if (swingHighs.length < 2 || swingLows.length < 2) return { signal: 'NEUTRO', strategy: 'Estructura', reason: 'Swings insuficientes', adx4h };

  // 3. Gatillo: ruptura del último swing confirmado en la dirección operada.
  const lastClose = closes[closes.length - 1];
  const lastSwingHigh = swingHighs[swingHighs.length - 1];
  const lastSwingLow = swingLows[swingLows.length - 1];

  let signal = 'NEUTRO', entry, sl, direction;
  if (wantLong && lastClose > lastSwingHigh.price) {
    signal = 'COMPRAR'; direction = 'LARGO'; entry = lastClose; sl = lastSwingLow.price;
  } else if (!wantLong && lastClose < lastSwingLow.price) {
    signal = 'VENDER'; direction = 'SHORT'; entry = lastClose; sl = lastSwingHigh.price;
  } else {
    return { signal: 'NEUTRO', strategy: 'Estructura', reason: 'Esperando ruptura del swing' };
  }

  const slDist = Math.abs(entry - sl);
  if (slDist <= 0) return { signal: 'NEUTRO', strategy: 'Estructura', reason: 'SL inválido (swing pegado al precio)', adx4h };

  // 4. Espacio hasta el próximo obstáculo estructural — reusa checkStructuralSpace tal cual.
  const space = checkStructuralSpace(entry, slDist, direction, swingHighs, swingLows);
  if (!space.hasSpace) return { signal: 'NEUTRO', strategy: 'Estructura', reason: 'Sin espacio suficiente hasta el próximo obstáculo', adx4h };

  // TP: 90% de la distancia al obstáculo más cercano (deja margen, no apunta
  // justo al nivel donde el precio históricamente reaccionó). Si no hay
  // obstáculo (camino libre), 2x la distancia al SL como piso razonable.
  const tp = space.distanceToObstacle
    ? entry + (direction === 'LARGO' ? 1 : -1) * space.distanceToObstacle * 0.9
    : entry + (direction === 'LARGO' ? 1 : -1) * slDist * 2;

  const rr = Math.abs(tp - entry) / slDist;
  // El ADX ahora se usa como gate previo (paso 0), no para graduar
  // confianza — mantenemos confianza fija para no reintroducir el
  // "cuanto más ADX, más convicción" que ya mostró ser poco confiable.
  const confidence = 75;

  return { signal, direction, entry, tp, sl, rr, confidence, strategy: 'Estructura', atr: slDist / 1.5, regime4h, adx4h };
}

function analyzeTrendFollowAdx25(closes, highs, lows) {
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
  if (ema20Now > ema50Now) bull += 1; else bear += 1;
  if (ema20Now > ema20Before) bull += 2; else if (ema20Now < ema20Before) bear += 2;
  if (ema50Now > ema50Before) bull += 1; else if (ema50Now < ema50Before) bear += 1;
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal = 'NEUTRO', direction = 'ESPERAR';
  const adxVal = calcADX(highs, lows, closes);
  const trendConfirmed = adxVal !== null && adxVal >= 25; // más exigente que el original (20)
  if (diff >= 4 && isVolatileEnough && trendConfirmed) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && trendConfirmed) { signal = 'VENDER'; direction = 'SHORT'; }
  const cloud = calcIchimokuCloud(highs, lows, closes);
  if (cloud) {
    if (signal === 'COMPRAR' && !cloud.aboveCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
    if (signal === 'VENDER' && !cloud.belowCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
  }
  let entry = price, tp, sl;
  const vol = calcVolatilityRegime(highs, lows, closes);
  const baseTp = state.tpAtrMultiplier || 3.0;
  let adxScale = 1.0;
  if (adxVal !== null) {
    if (adxVal >= 35) adxScale = 1.3;
    else if (adxVal < 25) adxScale = 0.7; // no debería darse nunca acá, por las dudas
  }
  const slMultiplier = (baseTp / 2) * vol.multiplierScale * adxScale, tpMultiplier = baseTp * vol.multiplierScale * adxScale;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Tendencia', atr, cloud, volRegime: vol.regime, adx: adxVal, adxScale };
}

function analyzeTrendFollowInvertedRR(closes, highs, lows) {
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
  if (ema20Now > ema50Now) bull += 1; else bear += 1;
  if (ema20Now > ema20Before) bull += 2; else if (ema20Now < ema20Before) bear += 2;
  if (ema50Now > ema50Before) bull += 1; else if (ema50Now < ema50Before) bear += 1;
  const total = bull + bear;
  const conf = total > 0 ? Math.round((Math.max(bull, bear) / total) * 100) : 50;
  const diff = bull - bear;
  let signal = 'NEUTRO', direction = 'ESPERAR';
  const adxVal = calcADX(highs, lows, closes);
  const trendConfirmed = adxVal === null || adxVal >= 20;
  if (diff >= 4 && isVolatileEnough && trendConfirmed) { signal = 'COMPRAR'; direction = 'LARGO'; }
  else if (diff <= -4 && isVolatileEnough && trendConfirmed) { signal = 'VENDER'; direction = 'SHORT'; }
  const cloud = calcIchimokuCloud(highs, lows, closes);
  if (cloud) {
    if (signal === 'COMPRAR' && !cloud.aboveCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
    if (signal === 'VENDER' && !cloud.belowCloud) { signal = 'NEUTRO'; direction = 'ESPERAR'; }
  }
  let entry = price, tp, sl;
  const vol = calcVolatilityRegime(highs, lows, closes);
  const baseTp = state.tpAtrMultiplier || 3.0;
  let adxScale = 1.0;
  if (adxVal !== null) {
    if (adxVal >= 35) adxScale = 1.3;
    else if (adxVal < 25) adxScale = 0.7;
  }
  // Invertido: el SL usa el multiplicador grande, el TP el chico (la mitad).
  const slMultiplier = baseTp * vol.multiplierScale * adxScale, tpMultiplier = (baseTp / 2) * vol.multiplierScale * adxScale;
  if (signal === 'COMPRAR') { sl = price - atr * slMultiplier; tp = price + atr * tpMultiplier; }
  else if (signal === 'VENDER') { sl = price + atr * slMultiplier; tp = price - atr * tpMultiplier; }
  else { sl = price - atr; tp = price + atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence: conf, price, entry, tp, sl, rr, strategy: 'Tendencia', atr, cloud, volRegime: vol.regime, adx: adxVal, adxScale };
}
// Variante experimental (5/8/2026) — misma lógica que analyzeTrendFollowAdx,
// pero exige además que el timeframe corto (15m) no esté en un movimiento
// inmediato contrario justo al momento de entrar. Hipótesis del usuario:
// el bot ve "largo" en la tendencia mayor, pero si el precio en ese instante
// puntual viene bajando en el corto plazo, igual entra — y eso explicó las
// pérdidas del 5/8. Regla simple: el último cierre corto tiene que estar del
// lado correcto de su propia EMA9 (no en medio de una sacudida en contra).
function analyzeTrendFollowAdxSubfilter(closes, highs, lows, closesShort) {
  const base = analyzeTrendFollowAdx(closes, highs, lows);
  if (!base || base.signal === 'NEUTRO') return base;
  if (!closesShort || closesShort.length < 12) return base; // sin data corta suficiente, no filtramos
  const emaShort = calcEMA(closesShort, 9);
  if (!emaShort) return base;
  const lastShortClose = closesShort[closesShort.length - 1];
  const shortAgrees = base.signal === 'COMPRAR' ? lastShortClose >= emaShort : lastShortClose <= emaShort;
  if (!shortAgrees) return { ...base, signal: 'NEUTRO', direction: 'ESPERAR', filteredBySubTF: true };
  return base;
}

// Estrategia de RANGO: solo opera cuando el ADX confirma mercado LATERAL
// (sin tendencia real). En vez de forzar una apuesta direccional que no
// existe, opera el rebote entre el piso y el techo del rango reciente —
// compra cerca del piso apuntando al techo, vende cerca del techo apuntando
// al piso. Objetivos chicos y cierre rápido, no pensada para durar horas.
function analyzeRango(closes, highs, lows) {
  const lookback = 15;
  if (!closes || closes.length < lookback + 20) return null;
  const adxVal = calcADX(highs, lows, closes);
  const isLateral = adxVal !== null && adxVal < 20;
  const recentHighs = highs.slice(-lookback);
  const recentLows = lows.slice(-lookback);
  const rangeTop = Math.max(...recentHighs);
  const rangeBottom = Math.min(...recentLows);
  const rangeSize = rangeTop - rangeBottom;
  const price = closes[closes.length - 1];
  if (rangeSize <= 0) return null;
  const posInRange = (price - rangeBottom) / rangeSize; // 0 = pegado al piso, 1 = pegado al techo
  const atr = calcATR(highs, lows, closes) || rangeSize * 0.3;

  let signal = 'NEUTRO', direction = 'ESPERAR', confidence = 0;
  if (isLateral && posInRange <= 0.15) {
    signal = 'COMPRAR'; direction = 'LARGO';
    confidence = Math.round(70 + ((0.15 - posInRange) / 0.15) * 25);
  } else if (isLateral && posInRange >= 0.85) {
    signal = 'VENDER'; direction = 'SHORT';
    confidence = Math.round(70 + ((posInRange - 0.85) / 0.15) * 25);
  }

  let entry = price, tp, sl;
  if (signal === 'COMPRAR') { tp = rangeTop - rangeSize * 0.1; sl = rangeBottom - atr * 0.5; }
  else if (signal === 'VENDER') { tp = rangeBottom + rangeSize * 0.1; sl = rangeTop + atr * 0.5; }
  else { tp = price + atr; sl = price - atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence, price, entry, tp, sl, rr, strategy: 'Rango', atr, adx: adxVal, rangeTop, rangeBottom };
}

// Correlación de Pearson entre los retornos (% de cambio vela a vela) de dos
// activos — mide qué tan pegados se mueven. 1 = siempre juntos, 0 = sin
// relación, -1 = siempre opuestos. Las criptos grandes suelen estar muy
// correlacionadas (BTC/ETH típicamente > 0.7), así que abrir la MISMA
// dirección en las dos es, en la práctica, una sola apuesta duplicada.
function calcCorrelation(closesA, closesB) {
  const n = Math.min(closesA.length, closesB.length);
  if (n < 20) return null;
  const a = closesA.slice(-n), b = closesB.slice(-n);
  const retA = [], retB = [];
  for (let i = 1; i < n; i++) {
    retA.push((a[i] - a[i - 1]) / a[i - 1]);
    retB.push((b[i] - b[i - 1]) / b[i - 1]);
  }
  const avg = (arr) => arr.reduce((s, v) => s + v, 0) / arr.length;
  const meanA = avg(retA), meanB = avg(retB);
  let num = 0, denA = 0, denB = 0;
  for (let i = 0; i < retA.length; i++) {
    const da = retA[i] - meanA, db = retB[i] - meanB;
    num += da * db; denA += da * da; denB += db * db;
  }
  const den = Math.sqrt(denA * denB);
  return den === 0 ? 0 : num / den;
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
    opens: data.map(k => parseFloat(k[1])),
    closes: data.map(k => parseFloat(k[4])),
    highs: data.map(k => parseFloat(k[2])),
    lows: data.map(k => parseFloat(k[3])),
    volumes: data.map(k => parseFloat(k[5]))
  };
}

// ── Auto trading loop (runs server-side, 24/7) ────────────
// Redondea la cantidad al número de decimales que Binance suele aceptar para
// cada par (regla LOT_SIZE) — sin esto, una orden real puede ser rechazada
// por tener más precisión de la permitida.
function roundQtyForBinance(pair, qty) {
  const decimals = { BTCUSDT: 5, ETHUSDT: 4 }[pair] ?? 3;
  const factor = Math.pow(10, decimals);
  return Math.floor(qty * factor) / factor;
}

// Extrae el precio promedio REAL de ejecución de una orden de Binance (usa
// cummulativeQuoteQty/executedQty, que es más preciso que el precio "price"
// del pedido, sobre todo en órdenes MARKET que se llenan a varios precios).
function extractFillPrice(orderResult, fallbackPrice) {
  const qty = parseFloat(orderResult.executedQty);
  const quote = parseFloat(orderResult.cummulativeQuoteQty);
  if (qty > 0 && quote > 0) return quote / qty;
  return fallbackPrice;
}

async function openTrade(pair, tf, analysis) {
  // --- Filtro de edge mínimo sobre comisión real ---
  // Comisión ida y vuelta confirmada: 0.20% (Spot, market, sin BNB)
  // Margen de seguridad 3x -> movimiento proyectado debe ser >= 0.60%
  const MIN_EDGE_STRATEGIES = ['Tendencia', 'Scalping-Tendencia']; // Rebote ya pausada (sin filo); Scalping-Tendencia sumada el 4/8 tras backtest: bruto +$7.60 pero comisión -$20.24 — filo real, muy chico frente al costo
  const MIN_EDGE_PCT = 0.006;

  const stratKey = analysis.regime
    ? `Scalping-${analysis.regime.startsWith('Tendencia') ? 'Tendencia' : 'Lateral'}`
    : (analysis.strategy || 'Reversión');

  if (MIN_EDGE_STRATEGIES.includes(stratKey)) {
    const projectedMovePct = Math.abs(analysis.tp - analysis.entry) / analysis.entry;
    if (projectedMovePct < MIN_EDGE_PCT) {
      console.log(`Filtro edge mínimo: se descarta ${stratKey} en ${pair} (${(projectedMovePct*100).toFixed(3)}% < 0.60%)`);
      return;
    }
  }

  const pct = state.positionSizePct || 20;
  // En Testnet/Real, el tamaño se calcula sobre el saldo REAL de la cuenta de
  // Binance — no sobre el capital simulado interno, que no tiene relación
  // con la plata de verdad una vez que empezamos a operar ahí.
  let capitalBase = state.capital;
  if (state.tradingMode !== 'demo' && !state.killSwitchActive) {
    try {
      capitalBase = await getRealBalance(state.tradingMode);
    } catch (e) {
      sendTelegram(`⚠️ No se pudo leer el saldo real de ${state.tradingMode.toUpperCase()} para calcular el tamaño — se saltea esta señal.\nMotivo: ${e.message}`);
      return;
    }
  }
  let size, riskUsd = null;
  if (SIZING_MODE === 'risk') {
    const r = calcPositionSize({ capital: capitalBase, entry: analysis.entry, sl: analysis.sl, overrides: { riskPerTradePct: RISK_PER_TRADE_PCT } });
    if (!r.ok) {
      console.log(`Módulo de riesgo: se saltea ${pair} — ${r.reason}`);
      sendTelegram(`⚠️ Señal descartada por el módulo de riesgo en ${pair.replace('USDT','/USDT')}: ${r.reason}`);
      return;
    }
    size = r.size; riskUsd = r.riskUsd;
    console.log(`Módulo de riesgo: ${pair} compra $${r.size} (${r.exposurePct}% del capital), arriesga $${r.riskUsd}, SL a ${r.slDistPct}%${r.capped ? ' [topado por techo de exposición]' : ''}`);
  } else {
    size = capitalBase * (pct / 100);
  }
  let qty = analysis.entry > 0 ? size / analysis.entry : 0;
  let realEntry = analysis.entry;

  // ── Ejecución real (Testnet/Real): abre la posición de verdad en Binance ──
  if (state.tradingMode !== 'demo' && !state.killSwitchActive) {
    qty = roundQtyForBinance(pair, qty);
    if (qty <= 0) {
      console.log(`Cantidad calculada demasiado chica para ${pair}, se saltea la apertura real.`);
      return;
    }
    const side = analysis.signal === 'COMPRAR' ? 'BUY' : 'SELL';
    try {
      const order = await placeRealOrder(state.tradingMode, pair, side, qty);
      realEntry = extractFillPrice(order, analysis.entry);
      qty = parseFloat(order.executedQty) || qty;
      sendTelegram(`✅ ORDEN REAL EJECUTADA (${state.tradingMode.toUpperCase()})\n${pair.replace('USDT','/USDT')} · ${side}\nCantidad: ${qty} · Precio real: $${realEntry.toFixed(2)}`);
    } catch (e) {
      sendTelegram(`❌ FALLÓ LA ORDEN REAL (${state.tradingMode.toUpperCase()})\n${pair.replace('USDT','/USDT')} · No se abrió ninguna posición.\nMotivo: ${e.message}`);
      console.log('Error al abrir orden real:', e.message);
      return; // no se crea el trade si la orden real falló
    }
  }

  const trade = {
    id: Date.now() + '-' + pair, pair, signal: analysis.signal, direction: analysis.direction,
    entry: realEntry, tp: analysis.tp, sl: analysis.sl, qty, size, tf,
    sizingMode: SIZING_MODE, riskUsd, // Fase 2: cuánto se arriesgó (null en modo fixed) — base del journal en R
    strategy: analysis.strategy || 'Reversión',
    // Mi aporte: si es Scalping, guardamos también CUÁL de las dos ramas
    // internas la abrió (siguiendo tendencia o apostando al rebote lateral) —
    // sin esto, "Scalping" sería una sola bolsa y nunca sabríamos cuál de
    // las dos mitades funciona de verdad, solo el promedio de las dos.
    subStrategy: analysis.regime ? `Scalping-${analysis.regime.startsWith('Tendencia') ? 'Tendencia' : 'Lateral'}` : null,
    tp2: analysis.tp2 || null, // solo Scalping usa un segundo nivel de TP
    adxScale: analysis.adxScale || 1.0, // para escalar el límite de tiempo junto con el TP (Tendencia)
    // Guardamos el ATR EFECTIVO (la distancia real usada para el SL, ya con el
    // ajuste de volatilidad del día aplicado) — no el ATR crudo — para que el
    // trailing stop se active de forma consistente con el TP/SL real de esta operación.
    atr: Math.abs(analysis.entry - analysis.sl) / 1.5,
    peakPrice: realEntry,
    trailingActive: false,
    partialTaken: false,
    trendDisagreeCount: 0,
    openTime: formatArgTime(new Date()),
    openTimestamp: Date.now(),
    confidence: analysis.confidence, auto: true
  };
  state.openTrades.push(trade);
  await saveState(state);
  const emoji = analysis.signal === 'COMPRAR' ? '🟢' : '🔴';
  const cloudInfo = analysis.cloud ? `\n☁️ ${analysis.signal==='COMPRAR' ? 'Por encima de la nube (ruptura confirmada)' : 'Por debajo de la nube (ruptura confirmada)'}` : '';
  const volInfo = analysis.volRegime ? `\n📊 Volatilidad del momento: ${analysis.volRegime}` : '';
  const adxInfo = (analysis.adx !== undefined && analysis.adx !== null) ? `\n📐 ADX: ${analysis.adx.toFixed(1)} (${analysis.adx >= 20 ? 'tendencia confirmada' : 'mercado lateral'})` : '';
  const regimeInfo = analysis.regime ? `\n🔎 Régimen (1m): ${analysis.regime}` : '';
  // Estructura (1/9/2026) usa sus propios campos (adx4h, regime4h) en vez de
  // adx/regime genéricos — sin esto, la notificación de su primera operación
  // no mostraría nada de contexto, justo la que más interesa ver en detalle
  // por ser la primera vez que opera con plata (demo) de verdad.
  const structInfo = (analysis.adx4h !== undefined && analysis.adx4h !== null)
    ? `\n📐 ADX 4h: ${analysis.adx4h.toFixed(1)} (régimen: ${analysis.regime4h})`
    : '';
  sendTelegram(`${emoji} ${analysis.signal} AUTO (Servidor)\n📊 ${pair.replace('USDT','/USDT')} · ${tf.toUpperCase()}\n🧠 Estrategia: ${trade.strategy}${cloudInfo}${volInfo}${adxInfo}${regimeInfo}${structInfo}\n💵 Entrada: $${realEntry.toFixed(2)}\n🎯 TP: $${analysis.tp.toFixed(2)}\n🛑 SL: $${analysis.sl.toFixed(2)}\n📊 R/R: 1:${analysis.rr.toFixed(2)}\n🎯 Confianza: ${analysis.confidence}%\n💰 Tamaño: ${pct}% del capital`);
}

// Toma de ganancia parcial: cierra el 50% de la posición asegurando esa ganancia,
// y deja el resto corriendo (con el trailing stop protegiéndolo). Se dispara una
// sola vez por operación, en el mismo momento en que se activa el trailing.
async function partialCloseTrade(t, exitPrice) {
  const halfSize = t.size / 2;
  const halfQty = t.qty / 2;
  let realExitPrice = exitPrice;

  // ── Ejecución real (Testnet/Real): cierra la mitad de la posición de verdad ──
  if (state.tradingMode !== 'demo' && !state.killSwitchActive) {
    const closeSide = t.signal === 'COMPRAR' ? 'SELL' : 'BUY'; // opuesto a como se abrió
    const roundedHalfQty = roundQtyForBinance(t.pair, halfQty);
    if (roundedHalfQty > 0) {
      try {
        const order = await placeRealOrder(state.tradingMode, t.pair, closeSide, roundedHalfQty);
        realExitPrice = extractFillPrice(order, exitPrice);
      } catch (e) {
        sendTelegram(`❌ FALLÓ EL CIERRE PARCIAL REAL (${state.tradingMode.toUpperCase()})\n${t.pair.replace('USDT','/USDT')}\nMotivo: ${e.message}\nLa mitad de la posición sigue abierta de verdad en Binance, aunque acá se registre como cerrada — revisar manualmente.`);
        console.log('Error al cerrar parcial real:', e.message);
      }
    }
  }

  const pricePct = t.signal === 'COMPRAR' ? (realExitPrice - t.entry) / t.entry : (t.entry - realExitPrice) / t.entry;
  const rawPnl = halfSize * pricePct;
  const pnlBeforeFees = Math.max(-halfSize, Math.min(rawPnl, halfSize * 5));
  const COMMISSION_PCT = 0.001;
  const commission = halfSize * COMMISSION_PCT * 2;
  const pnl = pnlBeforeFees - commission;
  const pnlPct = (pnl / halfSize) * 100;
  const closedPortion = {
    ...t, size: halfSize, qty: halfQty, exitPrice: realExitPrice, pnl, pnlPct, pnlBeforeFees, commission,
    closeTime: formatArgTime(new Date()),
    reason: 'TP Parcial (50%)'
  };
  state.trades.unshift(closedPortion);
  if (state.trades.length > 500) state.trades = state.trades.slice(0, 500);
  state.capital += pnl;
  state.dailyPnl += pnl;
  state.dailyTrades += 1;
  // Reducimos la posición que queda abierta a la mitad restante
  t.size = halfSize;
  t.qty = halfQty;
  t.partialTaken = true;
  await saveState(state);
  const emoji = pnl >= 0 ? '💰' : '📉';
  sendTelegram(`${emoji} GANANCIA PARCIAL ASEGURADA (50%)\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nPnL parcial: ${pnl>=0?'+':''}$${pnl.toFixed(2)}\nQueda abierto el otro 50%, protegido con trailing stop.`);
}

// Una operación con toma parcial deja DOS filas en state.trades con el mismo
// id (el pedazo parcial + el cierre final) — contarlas por separado como
// "ganada"/"perdida" es engañoso: una operación puede tener un parcial en
// verde y cerrar en rojo neto, y las dos filas sueltas la mostrarían mal.
// Esto agrupa por id y clasifica ganada/perdida sobre la SUMA neta real.
function summarizeTradesByOutcome(trades) {
  const byId = {};
  for (const t of trades) {
    const key = t.id || Math.random(); // por si alguna fila vieja no tiene id
    if (!byId[key]) byId[key] = { pnl: 0, subStrategy: t.subStrategy, strategy: t.strategy };
    byId[key].pnl += t.pnl;
  }
  const netTrades = Object.values(byId);
  const total = netTrades.length;
  const wins = netTrades.filter(nt => nt.pnl >= 0).length;
  const losses = total - wins;
  const pnlTotal = netTrades.reduce((s, nt) => s + nt.pnl, 0);
  return { total, wins, losses, winRate: total > 0 ? Math.round((wins / total) * 1000) / 10 : 0, pnlTotal, netTrades };
}

async function closeTradeById(tradeId, exitPrice, reason) {
  const idx = state.openTrades.findIndex(t => t.id === tradeId);
  if (idx === -1) return;
  const t = state.openTrades[idx];

  // ── Ejecución real (Testnet/Real): cierra la posición de verdad en Binance ──
  if (state.tradingMode !== 'demo' && !state.killSwitchActive) {
    const closeSide = t.signal === 'COMPRAR' ? 'SELL' : 'BUY'; // opuesto a como se abrió
    const roundedQty = roundQtyForBinance(t.pair, t.qty);
    if (roundedQty > 0) {
      try {
        const order = await placeRealOrder(state.tradingMode, t.pair, closeSide, roundedQty);
        exitPrice = extractFillPrice(order, exitPrice);
      } catch (e) {
        sendTelegram(`❌ FALLÓ EL CIERRE REAL (${state.tradingMode.toUpperCase()})\n${t.pair.replace('USDT','/USDT')}\nMotivo: ${e.message}\nLa posición sigue abierta de verdad en Binance, aunque acá se registre como cerrada — revisar manualmente cuanto antes.`);
        console.log('Error al cerrar orden real:', e.message);
      }
    }
  }

  const pricePct = t.signal === 'COMPRAR' ? (exitPrice - t.entry) / t.entry : (t.entry - exitPrice) / t.entry;
  const rawPnl = t.size * pricePct;
  const pnlBeforeFees = Math.max(-t.size, Math.min(rawPnl, t.size * 5));
  // Comisión simulada de Binance: 0.1% por lado (entrada + salida) sobre el tamaño de la posición
  const COMMISSION_PCT = 0.001;
  const commission = t.size * COMMISSION_PCT * 2;
  const pnl = pnlBeforeFees - commission;
  const pnlPct = (pnl / t.size) * 100;
  const rMultiple = t.riskUsd ? Math.round(pnl / t.riskUsd * 100) / 100 : null; // Fase 2: resultado en unidades de riesgo
  const closed = { ...t, exitPrice, pnl, pnlPct, pnlBeforeFees, commission, rMultiple, closeTime: formatArgTime(new Date()), reason };
  state.trades.unshift(closed);
  if (state.trades.length > 500) state.trades = state.trades.slice(0, 500);
  state.capital += pnl;
  // Si esta vez NO se cortó por Sub-SL, la racha de "el 1h insiste en la misma
  // apuesta y el corto plazo la desmiente" quedó rota — reseteamos el enfriamiento
  // escalonado para esa dirección.
  if (!reason.includes('Sub-SL')) {
    const streakKey = t.pair + '-' + t.signal;
    if (state.subSlStreak && state.subSlStreak[streakKey]) state.subSlStreak[streakKey] = 0;
  }
  if (state.capital < 0) state.capital = 0;
  state.dailyPnl += pnl;
  state.dailyTrades += 1;
  if (pnl < 0) state.consecutiveLosses += 1; else state.consecutiveLosses = 0;
  // Enfriamiento por cualquier pérdida (no solo Sub-SL) — evita reabrir la
  // misma apuesta perdedora casi al instante si el capital se libera enseguida.
  // OJO: si ya viene de un Sub-SL, ESE bloque ya calculó y guardó el cooldown
  // escalonado (30→60→120→240 min según la racha) — no lo pisamos acá con
  // el valor fijo de 30, o se pierde el escalado.
  if (pnl < 0 && !reason.includes('Sub-SL')) {
    if (!state.pairCooldowns) state.pairCooldowns = {};
    state.pairCooldowns[t.pair + '-' + t.signal] = Date.now() + (state.cooldownMinutes || 30) * 60 * 1000;
  }
  state.openTrades.splice(idx, 1);
  await saveState(state);
  const emoji = pnl >= 0 ? '✅' : '❌';
  sendTelegram(`${emoji} OPERACIÓN CERRADA (Servidor)\n📊 ${t.pair.replace('USDT','/USDT')} · ${t.tf}\n${t.signal} · ${t.direction}\n💵 $${t.entry.toFixed(2)} → $${exitPrice.toFixed(2)}\n${pnl>=0?'💰':'📉'} PnL neto: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnlPct.toFixed(2)}%)\n💸 Comisión simulada: -$${commission.toFixed(2)}\n🏷 ${reason}\n💰 Capital: $${state.capital.toFixed(2)}`);
  if (state.consecutiveLosses >= 3) {
    sendTelegram(`⚠️ BOT PAUSADO (Servidor)\n3 pérdidas seguidas\n🛡 Capital protegido: $${state.capital.toFixed(2)}`);
    state.autoMode = false;
    await saveState(state);
  }
}

// Confirma si el desacuerdo de tendencia viene con volumen REAL detrás, o si
// es solo ruido de precio sin fuerza de mercado. Compara el volumen de las
// últimas velas contra el promedio más largo — si no hay volumen elevado,
// probablemente sea fluctuación normal, no un giro real.
function calcVolumeConfirm(volumes, recentN = 5) {
  if (!volumes || volumes.length < recentN + 10) return true; // sin datos suficientes, no bloqueamos por las dudas
  const recent = volumes.slice(-recentN);
  const recentAvg = recent.reduce((a, b) => a + b, 0) / recent.length;
  const baseline = volumes.slice(0, -recentN);
  const baselineAvg = baseline.reduce((a, b) => a + b, 0) / baseline.length;
  if (baselineAvg === 0) return true;
  return recentAvg >= baselineAvg * 0.9; // el movimiento reciente tiene al menos volumen normal/alto, no está "vacío"
}

// Mide la tendencia de las últimas 15 velas de 15m — se usa como "vigía" de corto
// plazo para operaciones en timeframes más largos (1h/4h). Si contradice la
// dirección de una operación abierta de forma sostenida, dispara un cierre
// anticipado (sub-SL) sin esperar a que el precio recorra todo el SL original.
function calcShortTermTrend(closes) {
  if (!closes || closes.length < 16) return 'neutral';
  const now = closes[closes.length - 1];
  const then = closes[closes.length - 16]; // 15 velas atrás
  const pctChange = (now - then) / then;
  if (pctChange > 0.001) return 'alcista';
  if (pctChange < -0.001) return 'bajista';
  return 'neutral';
}

// Revisa cada posición fantasma (creada cuando el Sub-SL cortó una operación
// de Tendencia/Reversión) contra su TP/SL ORIGINAL, sin tocar plata real.
// Sirve para responder con datos: "¿el Sub-SL nos salvó de una pérdida mayor,
// o nos sacó de una operación que igual hubiera ganado?"
async function checkGhostTrades() {
  if (!state.ghostTrades || state.ghostTrades.length === 0) return;
  for (const g of [...state.ghostTrades]) {
    try {
      const { highs, lows } = await fetchKlines(g.pair, "1m", 3);
      const recentHigh = Math.max(...highs);
      const recentLow = Math.min(...lows);
      const hoursOpen = (Date.now() - g.ghostStartTimestamp) / (1000 * 60 * 60);
      let resolved = null, hypotheticalExit = null;
      if (g.signal === 'COMPRAR' && recentHigh >= g.tp) { resolved = 'TP'; hypotheticalExit = g.tp; }
      else if (g.signal === 'COMPRAR' && recentLow <= g.sl) { resolved = 'SL'; hypotheticalExit = g.sl; }
      else if (g.signal === 'VENDER' && recentLow <= g.tp) { resolved = 'TP'; hypotheticalExit = g.tp; }
      else if (g.signal === 'VENDER' && recentHigh >= g.sl) { resolved = 'SL'; hypotheticalExit = g.sl; }
      else if (hoursOpen >= 48) { resolved = 'TIEMPO'; hypotheticalExit = g.signal === 'COMPRAR' ? recentHigh : recentLow; }

      if (resolved) {
        const pricePct = g.signal === 'COMPRAR' ? (hypotheticalExit - g.entry) / g.entry : (g.entry - hypotheticalExit) / g.entry;
        const commission = g.size * 0.001 * 2;
        const hypotheticalPnl = (g.size * pricePct) - commission;
        const idx = state.ghostTrades.findIndex(x => x.id === g.id);
        if (idx > -1) state.ghostTrades.splice(idx, 1);
        if (!state.ghostResults) state.ghostResults = [];
        state.ghostResults.unshift({ ...g, resolved, hypotheticalExit, hypotheticalPnl, resolveTime: formatArgTime(new Date()) });
        if (state.ghostResults.length > 200) state.ghostResults = state.ghostResults.slice(0, 200);
        await saveState(state);
        const habriaGanado = hypotheticalPnl >= 0;
        sendTelegram(`👻 RESULTADO FANTASMA (Sub-SL)\n${g.pair.replace('USDT','/USDT')} · ${g.tf} · ${g.strategy}\nSi NO hubiéramos cortado esta operación, habría cerrado en ${resolved} con ${habriaGanado ? 'GANANCIA' : 'PÉRDIDA'} hipotética de ${hypotheticalPnl>=0?'+':''}$${hypotheticalPnl.toFixed(2)}\n(Esto es solo comparación — no afectó tu capital real)`);
      }
    } catch (e) { console.log('Ghost check error:', e.message); }
  }
}

// Candado anti-solapamiento: si una revisión todavía está corriendo (por ej.
// porque tardó más de 60s en llamadas a Binance), la siguiente NO arranca
// encima — se salta ese ciclo y espera al próximo. Esto es lo que evitaba que
// dos revisiones simultáneas vieran "el par está libre" a la vez y abrieran
// la misma operación duplicada (el bug de las 4 operaciones idénticas).
let isAutoCheckRunning = false;
async function runAutoCheck() {
  if (isAutoCheckRunning) {
    console.log('runAutoCheck: la revisión anterior todavía está en curso, se saltea este ciclo.');
    return;
  }
  isAutoCheckRunning = true;
  try {
    checkModeBlockAndWarn(); // Fase 3a: avisa si el modo bloquea el auto-trading, aunque no haya señales
    await runAutoCheckInner();
  } catch (e) {
    console.log('Error en runAutoCheck:', e.message);
  } finally {
    isAutoCheckRunning = false;
  }
}

async function runAutoCheckInner() {
  // OJO: el chequeo de autoMode/límites diarios se movió más abajo, justo
  // antes de buscar señales NUEVAS — así, aunque el bot esté pausado (por
  // 3 pérdidas seguidas, o por un límite diario), las operaciones que ya
  // están abiertas SIGUEN vigiladas (TP/SL/Sub-SL) y protegidas, en vez de
  // quedar sueltas sin control hasta que alguien reactive el AUTO a mano.
  const today = new Date().toDateString();
  if (state.lastResetDate !== today) {
    state.dailyPnl = 0; state.dailyTrades = 0; state.lastResetDate = today;
    await saveState(state);
  }

  // Check each open trade individually (multi-posición: una por par)
  for (const t of [...state.openTrades]) {
    try {
      // Traemos varias velas de 1m para cubrir el intervalo desde el último chequeo
      // (60s) y usamos máximo/mínimo (mecha), no solo el cierre — así se detecta
      // igual que lo haría una orden real de TP/SL puesta en el exchange.
      const { closes, highs, lows } = await fetchKlines(t.pair, "1m", 3);
      const currentPrice = closes[closes.length - 1];
      const recentHigh = Math.max(...highs);
      const recentLow = Math.min(...lows);

      // ── Trailing stop: asegura ganancia moviendo el SL a favor cuando la
      // operación viene ganando, sin retroceder nunca a un SL peor que el anterior.
      const atr = t.atr || Math.abs(t.entry - t.sl) / 1.5;
      const ACTIVATION_ATR = 1.0;  // se activa cuando la ganancia flotante llega a 1x ATR...
      // ...pero nunca con menos de este piso en %, para asegurar que la ganancia
      // bruta al activarse sea varias veces mayor que la comisión (0.2% ida y
      // vuelta) — si no, el trailing/parcial se activa con margen menor al
      // costo de operar, y cualquier retroceso chico ya deja pérdida neta.
      const MIN_ACTIVATION_PCT = 0.006; // 0.6% — 3x la comisión de 0.2%
      const activationDistance = Math.max(atr * ACTIVATION_ATR, t.entry * MIN_ACTIVATION_PCT);
      // Tendencia (11/8/2026): validado por backtest de 60 días — trailing más
      // ajustado (0.6x en vez de 1x) protege más de la ganancia una vez activo.
      // El resto de las estrategias sigue con 1x, no se probó ahí todavía.
      const TRAIL_DISTANCE_ATR = t.strategy === 'Tendencia' ? 0.6 : 1.0;

      // Breakeven, SOLO Tendencia (actualizado 12/8/2026) — subido de 0.8x a
      // 1.5x ATR: el backtest de 60 días mostró que activarlo tan temprano
      // (0.8x) cortaba la ganancia antes de que creciera — pérdidas grandes,
      // ganancias de centavos. Con 1.5x, el resultado se dio vuelta de
      // -$2.76 neto a +$16.82 neto (78.5% aciertos, sobre 79 operaciones).
      if (t.strategy === 'Tendencia') {
        const BREAKEVEN_TRIGGER_ATR = 1.5;
        if (t.signal === 'COMPRAR') {
          const favorableMoveBE = recentHigh - t.entry;
          if (favorableMoveBE >= atr * BREAKEVEN_TRIGGER_ATR && t.sl < t.entry) {
            t.sl = t.entry;
            await saveState(state);
          }
        } else if (t.signal === 'VENDER') {
          const favorableMoveBE = t.entry - recentLow;
          if (favorableMoveBE >= atr * BREAKEVEN_TRIGGER_ATR && t.sl > t.entry) {
            t.sl = t.entry;
            await saveState(state);
          }
        }
      }

      // Manual (31/8) y Estructura (1/9): sin trailing/breakeven automático,
      // a propósito — Manual porque el usuario define su TP/SL, Estructura
      // porque el backtest que la validó usó SL/TP fijos sin gestión activa;
      // agregarle trailing acá correría distinto de lo que se probó.
      if (t.signal === 'COMPRAR' && t.strategy !== 'Manual' && t.strategy !== 'Estructura') {
        if (recentHigh > t.peakPrice) t.peakPrice = recentHigh;
        const favorableMove = t.peakPrice - t.entry;
        if (favorableMove >= activationDistance) {
          // 27/8/2026: antes el piso era solo t.entry (breakeven) — el trailing
          // podía activarse y luego, al retroceder TRAIL_DISTANCE_ATR desde el
          // pico, terminar cerrando con una ganancia real por debajo del 0.6%
          // (MIN_ACTIVATION_PCT) que se supone garantiza cubrir la comisión.
          // Ahora el piso es el mismo 0.6% usado para decidir la activación,
          // no solo "no perder" — así una ganancia "cerrada por trailing" es
          // siempre una ganancia real, no un empate técnico que la comisión
          // se come. No toca TRAIL_DISTANCE_ATR (0.6x, validado 11/8) ni
          // BREAKEVEN_TRIGGER_ATR (1.5x, validado 12/8).
          const candidateSl = Math.max(t.entry * (1 + MIN_ACTIVATION_PCT), t.peakPrice - atr * TRAIL_DISTANCE_ATR);
          if (candidateSl > t.sl) {
            const wasActive = t.trailingActive;
            t.sl = candidateSl; t.trailingActive = true;
            await saveState(state);
            if (!wasActive) {
              sendTelegram(`🔒 Trailing stop activado\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nSL asegurado en $${candidateSl.toFixed(2)} (protege ganancia mínima)`);
              // La activación se evaluó contra el máximo (mecha), pero acá ejecutamos
              // al precio de cierre — que puede ser bastante menor. Si para este
              // momento el precio actual ya no cubre el mismo piso (0.6%), no
              // tomamos la parcial todavía: el trailing ya quedó protegiendo, y
              // esperamos a que el precio esté realmente donde corresponde.
              const currentMovePct = (currentPrice - t.entry) / t.entry;
              // Tendencia excluida (5/8/2026): el backtest validado (+11.96%)
              // simula solo TP/SL completo, sin parciales — dejarla tomar
              // parciales en vivo corre distinto a lo que probamos.
              if (!t.partialTaken && currentMovePct >= MIN_ACTIVATION_PCT && t.strategy !== 'Tendencia') await partialCloseTrade(t, currentPrice);
            }
          }
        }
      } else if (t.signal === 'VENDER' && t.strategy !== 'Manual' && t.strategy !== 'Estructura') {
        if (recentLow < t.peakPrice) t.peakPrice = recentLow;
        const favorableMove = t.entry - t.peakPrice;
        if (favorableMove >= activationDistance) {
          const candidateSl = Math.min(t.entry * (1 - MIN_ACTIVATION_PCT), t.peakPrice + atr * TRAIL_DISTANCE_ATR);
          if (candidateSl < t.sl) {
            const wasActive = t.trailingActive;
            t.sl = candidateSl; t.trailingActive = true;
            await saveState(state);
            if (!wasActive) {
              sendTelegram(`🔒 Trailing stop activado\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nSL asegurado en $${candidateSl.toFixed(2)} (protege ganancia mínima)`);
              const currentMovePct = (t.entry - currentPrice) / t.entry;
              if (!t.partialTaken && currentMovePct >= MIN_ACTIVATION_PCT && t.strategy !== 'Tendencia') await partialCloseTrade(t, currentPrice);
            }
          }
        }
      }

      // ── Sub-SL por tendencia de corto plazo (15m): solo aplica a operaciones
      // en timeframes más largos (1h/4h). Si el corto plazo viene claramente en
      // contra de forma SOSTENIDA (no un ruido de un momento) y la operación
      // todavía no está en ganancia, se corta antes de llegar al SL completo.
      // El Sub-SL no aplica a Rango: esa estrategia apuesta A FAVOR de que la
      // tendencia de corto plazo se agote y revierta — aplicarle "cortá si el
      // corto plazo va en contra" es contradecir la lógica misma de la entrada.
      if ((t.tf === '1h' || t.tf === '4h') && !t.trailingActive && t.strategy !== 'Rango') {
        try {
          const { closes: closes15m, volumes: volumes15m } = await fetchKlines(t.pair, '15m', 30);
          const shortTrend = calcShortTermTrend(closes15m);
          const contradicts = (t.signal === 'COMPRAR' && shortTrend === 'bajista') || (t.signal === 'VENDER' && shortTrend === 'alcista');
          const notYetProfitable = t.signal === 'COMPRAR' ? currentPrice <= t.entry : currentPrice >= t.entry;
          // El desacuerdo solo cuenta si además viene con volumen real detrás —
          // un movimiento de precio sin volumen suele ser ruido, no un giro genuino.
          const volumeConfirms = calcVolumeConfirm(volumes15m);
          if (contradicts && notYetProfitable && volumeConfirms) {
            t.trendDisagreeCount = (t.trendDisagreeCount || 0) + 1;
          } else {
            // Antes: reseteaba a 0 con un solo desacuerdo puntual, obligando a
            // 15 chequeos CONSECUTIVOS perfectos — casi nunca pasaba rápido,
            // y mientras tanto el precio seguía en contra sin protección real.
            // Ahora: descuenta de a 1, tolera ruido de un minuto sin perder
            // toda la racha, pero sigue exigiendo desacuerdo sostenido de fondo.
            t.trendDisagreeCount = Math.max(0, (t.trendDisagreeCount || 0) - 1);
          }
          const DISAGREE_THRESHOLD = state.subSlThresholdMin || 5; // minutos de desacuerdo sostenido (chequeo cada 60s ≈ 1 por minuto)
          if (t.trendDisagreeCount >= DISAGREE_THRESHOLD) {
            // Enfriamiento escalonado: si el MISMO par+dirección viene de cortarse
            // por Sub-SL varias veces seguidas (ej: el 1h "no se entera" todavía
            // de que la tendencia giró y sigue reabriendo la misma apuesta), cada
            // repetición duplica el enfriamiento — hasta un tope de 4 horas.
            if (!state.subSlStreak) state.subSlStreak = {};
            const streakKey = t.pair + '-' + t.signal;
            state.subSlStreak[streakKey] = (state.subSlStreak[streakKey] || 0) + 1;
            const streak = state.subSlStreak[streakKey];
            const baseCooldown = state.cooldownMinutes || 30;
            const escalatedCooldown = Math.min(baseCooldown * Math.pow(2, streak - 1), 240);
            sendTelegram(`⚠️ CIERRE ANTICIPADO (Sub-SL por tendencia 15m)\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nLas últimas 15 velas de 15m vienen sostenidamente ${shortTrend === 'bajista' ? 'a la baja' : 'al alza'}, en contra de esta operación (${t.signal}).\nSe cerró antes de llegar al SL completo, para no seguir esperando si el corto plazo ya lo está desmintiendo.\n🧊 Este par+dirección queda en enfriamiento ${escalatedCooldown} min (racha: ${streak}${streak > 1 ? ' seguidas — se duplicó el enfriamiento' : ''}).\n👻 Arrancamos un seguimiento fantasma (sin plata real) para ver qué hubiera pasado si no cortábamos acá.`);
            if (!state.pairCooldowns) state.pairCooldowns = {};
            state.pairCooldowns[streakKey] = Date.now() + escalatedCooldown * 60 * 1000;
            // Como el trailing todavía no estaba activo (condición de arriba),
            // t.tp/t.sl siguen siendo los originales de la entrada — perfecto
            // para el seguimiento fantasma.
            if (!state.ghostTrades) state.ghostTrades = [];
            state.ghostTrades.push({
              id: t.id + '-ghost-' + Date.now(),
              pair: t.pair, signal: t.signal, direction: t.direction, tf: t.tf, strategy: t.strategy,
              entry: t.entry, tp: t.tp, sl: t.sl, size: t.size,
              realExitPrice: currentPrice,
              ghostStartTimestamp: Date.now()
            });
            await closeTradeById(t.id, currentPrice, 'Sub-SL: tendencia 15m en contra');
            continue;
          }
          await saveState(state);
        } catch (e) { console.log('Short-term trend check error:', e.message); }
      }

      // Time-based safety close: if a trade has been open too long without hitting TP/SL,
      // close it at market price to avoid capital being stuck indefinitely
      // Tendencia: si el TP se estiró por ADX fuerte (adxScale > 1), el límite
      // de tiempo también se estira en la misma proporción — no tiene sentido
      // pedirle un objetivo más lejos sin darle más tiempo para llegar. Si el
      // TP se acortó (adxScale < 1), el límite también baja, coherente.
      const MAX_HOURS_OPEN = (t.strategy === 'Scalping' || t.strategy === 'Rebote') ? 0.75
        : (t.strategy === 'Rango') ? 2
        : (t.strategy === 'Tendencia') ? 2 * (t.adxScale || 1.0)
        : 48;
      const openTimestamp = t.openTimestamp || Date.now();
      const hoursOpen = (Date.now() - openTimestamp) / (1000 * 60 * 60);

      // Salida por reversión (Scalping): si aparece un cruce de medias en CONTRA
      // de la posición, el movimiento que la abrió ya se dio vuelta — cerramos
      // y aseguramos lo que haya, en vez de esperar ciegamente al TP fijo o al
      // límite de tiempo. Es "tomar la mejor acción, no la peor".
      // OJO: esto SOLO aplica a la rama de tendencia — en la rama lateral, el
      // cruce de medias va y viene todo el tiempo (es ruido normal del vaivén),
      // aplicarle esto mismo cerraba operaciones laterales en segundos.
      if (t.strategy === 'Scalping' && t.subStrategy === 'Scalping-Tendencia') {
        try {
          const { closes: closesRev } = await fetchKlines(t.pair, '5m', 40);
          const ema9r = calcEMA(closesRev, 9), ema21r = calcEMA(closesRev, 21);
          const reversedAgainstLong = t.signal === 'COMPRAR' && ema9r < ema21r;
          const reversedAgainstShort = t.signal === 'VENDER' && ema9r > ema21r;
          if (reversedAgainstLong || reversedAgainstShort) {
            await closeTradeById(t.id, currentPrice, 'Salida por reversión');
            continue;
          }
        } catch (e) { console.log('Reversal check error:', e.message); }
      }

      // TP/SL ahora se detectan por mecha (high/low), pero el cierre se registra
      // al precio EXACTO del TP/SL (así como llenaría una orden real), no al
      // precio de cierre de la vela, que puede ser distinto.
      if (t.strategy === 'Scalping' && t.tp2 && !t.partialTaken) {
        // Scalping usa 2 TPs explícitos: al tocar el primero, asegura la mitad
        // y mueve el objetivo del resto al segundo TP (más lejos).
        const hitTP1 = (t.signal === 'COMPRAR' && recentHigh >= t.tp) || (t.signal === 'VENDER' && recentLow <= t.tp);
        // Lateral tiene más paciencia en el SL (31/7, a pedido de Juan): si
        // entró mal, esperamos a que el precio se ASIENTE más allá del límite
        // (usando el cierre de la vela), no que cierre apenas la MECHA lo toca
        // por un instante — así le damos margen real a que se revierta, en
        // vez de saltar a la primera sacudida.
        const hitSL = t.subStrategy === 'Scalping-Lateral'
          ? ((t.signal === 'COMPRAR' && currentPrice <= t.sl) || (t.signal === 'VENDER' && currentPrice >= t.sl))
          : ((t.signal === 'COMPRAR' && recentLow <= t.sl) || (t.signal === 'VENDER' && recentHigh >= t.sl));
        if (hitTP1) {
          await partialCloseTrade(t, t.tp);
          t.tp = t.tp2;
          t.tp2 = null;
          await saveState(state);
        } else if (hitSL) {
          // Antes de rendirse, damos UNA sola extensión de paciencia (31/7, a
          // pedido de Juan): si el detector de 1m todavía muestra que la
          // dirección original sigue en pie (no se dio vuelta de verdad), le
          // damos más margen en vez de cortar apenas toca el límite. Si ya se
          // usó la extensión, o la tendencia genuinamente se invirtió, cortamos.
          if (!t.slExtended) {
            try {
              const { opens: opensCheck, highs: highsCheck, lows: lowsCheck, closes: closesCheck, volumes: volumesCheck } = await fetchKlines(t.pair, '1m', 30);
              const checkRegime = detectMicroRegime(opensCheck, highsCheck, lowsCheck, closesCheck, volumesCheck);
              const stillFavorsLong = t.signal === 'COMPRAR' && (checkRegime.direction === 'up' && !checkRegime.isDecelerating);
              const stillFavorsShort = t.signal === 'VENDER' && (checkRegime.direction === 'down' && !checkRegime.isDecelerating);
              if (stillFavorsLong || stillFavorsShort) {
                t.slExtended = true;
                t.sl = t.signal === 'COMPRAR' ? t.sl - t.atr * 0.7 : t.sl + t.atr * 0.7;
                await saveState(state);
                console.log(`${t.pair} SL extendido una vez — la dirección original todavía sostiene`);
              } else {
                // Ejecutar en t.sl (el nivel calculado), no en currentPrice — que
                // puede haber sobrepasado el stop y agrandar la pérdida real más
                // allá de lo que el R:R de la estrategia contemplaba. Mismo
                // criterio que ya usa la rama de Tendencia/Reversión más abajo.
                await closeTradeById(t.id, t.sl, 'SL Auto');
              }
            } catch (e) {
              await closeTradeById(t.id, t.sl, 'SL Auto'); // si falla el chequeo, cortamos por las dudas
            }
          } else {
            await closeTradeById(t.id, t.sl, 'SL Auto (tras extensión)');
          }
        } else if (hoursOpen >= MAX_HOURS_OPEN) {
          // Prioridad: no cerrar en pérdida solo por haberse cumplido el tiempo.
          // OJO: "en pérdida" se mide por PnL NETO (después de comisión), no solo
          // si el precio está a favor o en contra — un movimiento a favor pero
          // muy chico igual da neto negativo una vez descontada la comisión.
          const pricePct = t.signal === 'COMPRAR' ? (currentPrice - t.entry) / t.entry : (t.entry - currentPrice) / t.entry;
          const COMMISSION_ROUNDTRIP_PCT = 0.002; // 0.2% comisión ida y vuelta real
          const inLoss = pricePct < COMMISSION_ROUNDTRIP_PCT;
          const HARD_MAX_HOURS_OPEN = MAX_HOURS_OPEN * 3;
          const hitHardCap = hoursOpen >= HARD_MAX_HOURS_OPEN;
          if (!inLoss || hitHardCap) {
            // El motivo distingue si cerró al límite normal (ya en ganancia/neutro)
            // o si esperó hasta el límite duro sin lograr salir de la pérdida.
            const reason = hitHardCap && inLoss ? `Cierre por tiempo (límite duro ${HARD_MAX_HOURS_OPEN}hs, seguía en pérdida)` : `Cierre por tiempo (${MAX_HOURS_OPEN}hs)`;
            await closeTradeById(t.id, currentPrice, reason);
          }
        }
      } else if (t.signal === 'COMPRAR' && recentHigh >= t.tp) await closeTradeById(t.id, t.tp, 'TP Auto');
      else if (t.signal === 'COMPRAR' && recentLow <= t.sl) await closeTradeById(t.id, t.sl, 'SL Auto');
      else if (t.signal === 'VENDER' && recentLow <= t.tp) await closeTradeById(t.id, t.tp, 'TP Auto');
      else if (t.signal === 'VENDER' && recentHigh >= t.sl) await closeTradeById(t.id, t.sl, 'SL Auto');
      else if (hoursOpen >= MAX_HOURS_OPEN) {
        // Misma prioridad que arriba: no cortar en pérdida NETA (no solo precio en
        // contra) salvo que se llegue al límite duro (3x) para no atar capital.
        const pricePct = t.signal === 'COMPRAR' ? (currentPrice - t.entry) / t.entry : (t.entry - currentPrice) / t.entry;
        const COMMISSION_ROUNDTRIP_PCT = 0.002; // 0.2% comisión ida y vuelta real
        const inLoss = pricePct < COMMISSION_ROUNDTRIP_PCT;
        const HARD_MAX_HOURS_OPEN = MAX_HOURS_OPEN * 3;
        const hitHardCap = hoursOpen >= HARD_MAX_HOURS_OPEN;
        if (!inLoss || hitHardCap) {
          const reason = hitHardCap && inLoss ? `Cierre por tiempo (límite duro ${HARD_MAX_HOURS_OPEN}hs, seguía en pérdida)` : `Cierre por tiempo (${MAX_HOURS_OPEN}hs)`;
          await closeTradeById(t.id, currentPrice, reason);
          sendTelegram(`⏰ OPERACIÓN CERRADA POR TIEMPO\n${t.pair.replace('USDT','/USDT')} llevaba ${hoursOpen.toFixed(2)}hs abierta sin tocar TP/SL${hitHardCap && inLoss ? ' (esperó hasta el límite duro, seguía en pérdida)' : ''}\nSe cerró al precio de mercado para liberar el capital.`);
        }
      }
    } catch (e) { console.log('Check open trade error:', e.message); }
  }

  // La vigilancia de operaciones abiertas y el seguimiento fantasma YA
  // corrieron arriba, sin importar el estado del AUTO — lo único que se
  // frena acá es la búsqueda de operaciones NUEVAS.
  await checkGhostTrades();
  if (!state.autoMode) return;
  const maxGain = state.capital * state.maxDailyGainPct / 100;
  const maxLoss = state.capital * state.maxDailyLossPct / 100;
  if (state.dailyPnl >= maxGain) {
    sendTelegram(`✅ Límite de ganancia diaria alcanzado ($${state.dailyPnl.toFixed(2)}) — no se abren operaciones nuevas, pero las que ya están abiertas siguen vigiladas.`);
    state.autoMode = false; await saveState(state); return;
  }
  if (state.dailyPnl <= -maxLoss) {
    sendTelegram(`🛑 Límite de pérdida diaria alcanzado ($${state.dailyPnl.toFixed(2)}) — no se abren operaciones nuevas, pero las que ya están abiertas siguen vigiladas.`);
    state.autoMode = false; await saveState(state); return;
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
        // Reversión PAUSADA desde el 30/7 — Tendencia se reactiva el 31/7,
        // corriendo específicamente en 30m (no 1h/4h como antes) — término
        // medio real entre los 15-30 min de Scalping/Rebote y las 8+ horas
        // que llegaba a tardar Tendencia en 1h/4h.
        // const a = analyzeImproved(closes, highs, lows);
        // if (a) signals.push({ tf, pair, signal: a.signal, confidence: a.confidence, analysis: a });
        // Tendencia PAUSADA (31/8/2026) — todos los backtests anteriores
        // (los que validaron ADX≥25, breakeven 1.5x, trailing 0.6x) corrían
        // sin querer en 4h, no en 30m/15m que es lo que este loop realmente
        // opera — faltaba "30m" en el mapeo de timeframes del backtest y
        // devolvía 0 velas en silencio. Corregido el bug y repetido el
        // backtest de 60 días YA en 30m (el timeframe real): 97 operaciones,
        // bruto de apenas +$0.49 — sin filo direccional real, más allá de
        // cualquier costo. Neto -$55.70 (-5.57%). Coincide con lo que se
        // venía viendo en vivo (42-54% de aciertos, neto negativo). No se
        // sigue operando con una entrada sin filo confirmado — la sigue
        // vigilando el sistema por si hace falta cerrar algo abierto, pero
        // no abre operaciones nuevas hasta rediseñar la entrada (estructura
        // de mercado, no solo EMA+ADX+nube).
        // const b = analyzeTrendFollowAdx25(closes, highs, lows);
        // if (b && b.signal !== 'NEUTRO') {
        //   const goodEntry = await checkGoodEntry15m(pair, b.direction);
        //   if (goodEntry) {
        //     signals.push({ tf, pair, signal: b.signal, confidence: b.confidence, analysis: b });
        //   } else {
        //     console.log(`${pair} ${tf} Tendencia ${b.signal} bloqueada — precio muy estirado en 15m, probable entrada tardía`);
        //   }
        // }
        // Rango PAUSADA desde el 30/7 — 13.3% de aciertos en 15 operaciones reales,
        // evidencia clara de que el diagnóstico "ADX bajo = mercado lateral" no
        // alcanza para detectar un rango operable de verdad. Queda el código
        // intacto para cuando se rediseñe (agregar confirmación de que el precio
        // realmente viene rebotando entre piso y techo, no solo ADX bajo).
        // const c = analyzeRango(closes, highs, lows);
        // if (c) signals.push({ tf, pair, signal: c.signal, confidence: c.confidence, analysis: c });
      } catch (e) { console.log(`Analyze error ${pair} ${tf}:`, e.message); }
    }

    // Entrada por Estructura (1/9/2026, ajustada 4/9/2026) — CONECTADA al
    // auto-trading en vivo, validada cross-asset out-of-sample (BTC y ETH,
    // dos ventanas de 90 días cada uno). Se probó la curva completa de
    // umbral (20/25/30): ADX≥25 ganó en las 4 ventanas sin excepción —
    // primera vez que las 4 dan positivo a la vez, y con más operaciones
    // en los períodos de mercado más flojo (donde antes casi no operaba).
    // minAdx4h=25 (bajado de 30 el 4/9, con backtest que lo respalda).
    try {
      const { closes: closes30e, highs: highs30e, lows: lows30e } = await fetchKlines(pair, '30m', 100);
      const { closes: closesH4e, highs: highsH4e, lows: lowsH4e } = await fetchKlines(pair, '4h', 60);
      const f = analyzeStructuralEntry(closes30e, highs30e, lows30e, closesH4e, highsH4e, lowsH4e, 25);
      if (f && f.signal !== 'NEUTRO') signals.push({ tf: '30m', pair, signal: f.signal, confidence: f.confidence, analysis: f });
    } catch (e) { console.log(`Analyze Estructura error ${pair}:`, e.message); }
    // Rebote PAUSADA (4/8/2026) — backtest de 90 días / 264 operaciones dio
    // PnL BRUTO negativo (-$28.47, antes de comisión): la señal de entrada no
    // tiene filo real, no es un problema de calibración de salida. Winrate
    // 33.3% está por debajo del ~38.9% que necesitaría para empatar con su
    // R:R de 1:1.57. No reactivar sin un cambio real en analyzeRebote() que
    // se vuelva a validar con backtest.
    // try {
    //   const { closes: closes15, highs: highs15, lows: lows15 } = await fetchKlines(pair, '15m', 60);
    //   const d = analyzeRebote(closes15, highs15, lows15);
    //   if (d) signals.push({ tf: '15m', pair, signal: d.signal, confidence: d.confidence, analysis: d });
    // } catch (e) { console.log(`Analyze Rebote error ${pair}:`, e.message); }

    // Scalping opera SIEMPRE en 5m — pensada para operar varias veces por
    // hora, 15-30 minutos por operación, con 2 niveles de TP. Además mira 1m
    // para diagnosticar si el momento actual tiene tendencia real o es lateral.
    try {
      const { closes: closes5, highs: highs5, lows: lows5 } = await fetchKlines(pair, '5m', 40);
      const { opens: opens1, highs: highs1, lows: lows1, closes: closes1, volumes: volumes1 } = await fetchKlines(pair, '1m', 30);
      const { highs: highs15sc, lows: lows15sc, closes: closes15sc } = await fetchKlines(pair, '15m', 30);
      const e = analyzeScalping(closes5, highs5, lows5, opens1, highs1, lows1, closes1, volumes1, highs15sc, lows15sc, closes15sc);
      // Scalping-Tendencia PAUSADA (27/8/2026) — 3 backtests distintos sobre
      // los mismos 60 días (filtro de comisión mínima solo, minConfidence 75,
      // confirmación de 2 velas en la salida por reversión) dieron todos
      // bruto ~$0 o negativo y neto negativo (-$16 a -$19). El desglose por
      // motivo de cierre mostró que las 17 ganadoras SIEMPRE fueron por TP
      // completo y las ~31 salidas por reversión SIEMPRE perdedoras — no es
      // un problema de timing de salida, es que la entrada revierte antes de
      // llegar al TP con demasiada frecuencia. Mismo veredicto que Rebote:
      // sin filo real confirmado.
      // Scalping-Lateral: corrección importante (1/9/2026) — durante meses
      // este comentario decía "sigue activa, no se tocó" pero eso era
      // INCORRECTO: desde el 31/7 el código que genera la señal de esta rama
      // estaba comentado (analyzeScalping) — nunca disparó una sola señal en
      // vivo, más allá de cualquier umbral de confianza. Hoy se le agregó el
      // piso de rango mínimo (MIN_RANGE_ATR) que quedó pendiente en su
      // momento, y la señal ya vuelve a generarse — pero todavía NO se
      // habilita acá el push a producción (LATERAL_LIVE_ENABLED) hasta
      // backtestearla con el piso nuevo. Cambiar a true recién después de
      // validar con runScalpingRealisticBacktest(onlySubStrategy:'Scalping-Lateral').
      const LATERAL_LIVE_ENABLED = false;
      const isLateral = e && e.regime && !e.regime.startsWith('Tendencia');
      if (e && e.regime && e.regime.startsWith('Tendencia')) {
        // Tendencia: generada pero pausada — no se empuja (comportamiento sin cambios).
      } else if (isLateral && LATERAL_LIVE_ENABLED) {
        signals.push({ tf: '5m', pair, signal: e.signal, confidence: e.confidence, analysis: e });
      }
    } catch (e) { console.log(`Analyze Scalping error ${pair}:`, e.message); }
    // Rebote y Scalping usan su PROPIO umbral de confianza (más bajo a propósito)
    // en vez del global — son estrategias distintas, pensadas para operar seguido
    // con objetivos chicos, no tiene sentido exigirles la misma convicción que a Tendencia.
    const passesConfidence = (s) => {
      // Lateral necesita más convicción — era la rama con peor resultado (0%
      // de aciertos en 16 operaciones), muchas entradas de baja convicción.
      if (s.analysis.strategy === 'Scalping' && s.analysis.regime === 'Lateral') return s.confidence >= 78;
      if (s.analysis.strategy === 'Rebote' || s.analysis.strategy === 'Scalping') return s.confidence >= 60;
      // Estructura ya se autofiltra adentro (ADX 4h≥30 + ruptura de swing
      // confirmada) — no tiene un número de confianza graduado como las
      // demás (siempre 75), así que el umbral genérico de 90 no aplica acá.
      if (s.analysis.strategy === 'Estructura') return true;
      return s.confidence >= state.minConfidence;
    };
    const buys = signals.filter(s => s.signal === 'COMPRAR' && passesConfidence(s));
    const sells = signals.filter(s => s.signal === 'VENDER' && passesConfidence(s));
    const threshold = state.requireMTF ? 2 : 1;
    // Antes esto era if/else if, lo que hacía que COMPRAR siempre le ganara a VENDER
    // por defecto cuando había señales de ambos lados al mismo tiempo (con 2 estrategias
    // x 2 timeframes, esto pasaba seguido). Ahora se evalúan las dos y gana la que
    // tenga más fuerza real (más señales coincidiendo × confianza), sin sesgo direccional.
    let chosen = null;
    if (buys.length >= threshold) {
      const best = buys.sort((a, b) => b.confidence - a.confidence)[0];
      chosen = { ...best, direction: 'COMPRAR', score: buys.length * best.confidence };
    }
    if (sells.length >= threshold) {
      const best = sells.sort((a, b) => b.confidence - a.confidence)[0];
      const sellCandidate = { ...best, direction: 'VENDER', score: sells.length * best.confidence };
      if (!chosen || sellCandidate.score > chosen.score) chosen = sellCandidate;
    }
    if (chosen) {
      const cooldownKey = pair + '-' + chosen.direction;
      const cooldownUntil = (state.pairCooldowns || {})[cooldownKey];
      const stillCoolingDown = cooldownUntil && Date.now() < cooldownUntil;
      // Filtro de correlación: si ya hay una operación abierta en OTRO par
      // monitoreado, en la MISMA dirección, y los dos activos vienen muy
      // correlacionados (>0.7), abrir esta segunda es duplicar la misma
      // apuesta con otro nombre — no diversifica de verdad el riesgo.
      let blockedByCorrelation = false;
      const sameDirectionOther = state.openTrades.find(t => t.pair !== pair && t.signal === chosen.direction);
      if (sameDirectionOther && !stillCoolingDown) {
        try {
          const dataA = await fetchKlines(pair, '1h', 50);
          const dataB = await fetchKlines(sameDirectionOther.pair, '1h', 50);
          const corr = calcCorrelation(dataA.closes, dataB.closes);
          if (corr !== null && corr > 0.7) {
            blockedByCorrelation = true;
            console.log(`${pair} bloqueado por correlación con ${sameDirectionOther.pair} (${corr.toFixed(2)}) — misma dirección, no diversifica`);
          }
        } catch (e) { console.log('Correlation check error:', e.message); }
      }
      if (!stillCoolingDown && !blockedByCorrelation) {
        allSignals.push(chosen);
      } else if (stillCoolingDown) {
        console.log(`${cooldownKey} en enfriamiento, se salta esta señal (${Math.round((cooldownUntil - Date.now()) / 60000)} min restantes)`);
      }
    }
  }

  // Open a trade for EVERY free pair with a valid signal (not just the single best) —
  // this is what actually increases daily trade frequency vs. the old one-at-a-time logic
  //
  // SEGURO (5/9/2026): el auto-trading (Estructura, o cualquier estrategia
  // automática futura) NUNCA debe ejecutar una orden real en Testnet/Real sin
  // una habilitación aparte y explícita — independiente de que el modo global
  // esté en Testnet/Real para poder operar MANUAL ahí. Antes, cambiar el modo
  // global ya alcanzaba para que el auto-trading empezara a mandar órdenes
  // reales en su próxima señal, sin que nadie lo pidiera específicamente.
  // AUTO_TRADING_LIVE_ENABLED (apagado por defecto) es el interruptor
  // separado — cambiar a true solo después de validar manual a fondo en
  // Testnet, y con Juan confirmando explícitamente que quiere ese paso.
  // (AUTO_TRADING_LIVE_ENABLED se declara arriba del archivo — Fase 3a)
  if (state.tradingMode !== 'demo' && !AUTO_TRADING_LIVE_ENABLED) {
    if (allSignals.length > 0) {
      console.log(`Auto-trading real DESHABILITADO (AUTO_TRADING_LIVE_ENABLED=false) — se ignoran ${allSignals.length} señal(es) automática(s) mientras el modo es ${state.tradingMode}. El trading manual no se ve afectado por esto.`);
    }
    return;
  }
  for (const best of allSignals) {
    await openTrade(best.pair, best.tf, best.analysis);
  }
}

// ── Routes ────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({ status: "Signal Bot Backend OK", time: new Date().toISOString(), autoMode: state.autoMode });
});

app.get("/debug/signals", async (req, res) => {
  const pair = req.query.pair || 'ETHUSDT';
  // Agrega entrada/TP/SL y % de distancia a cada resultado — así se puede
  // ver qué tan largo o corto es el TP sin esperar a que abra una operación real.
  function withDistances(analysis) {
    if (!analysis || !analysis.tp || !analysis.sl || !analysis.entry) return {};
    const tpPct = Math.abs(analysis.tp - analysis.entry) / analysis.entry * 100;
    const slPct = Math.abs(analysis.sl - analysis.entry) / analysis.entry * 100;
    return {
      entry: analysis.entry,
      tp: analysis.tp, tpPct: tpPct.toFixed(2) + '%',
      sl: analysis.sl, slPct: slPct.toFixed(2) + '%',
      rr: analysis.rr ? analysis.rr.toFixed(2) : null
    };
  }
  try {
    const results = [];
    for (const tf of ['1h', '4h']) {
      const { closes, highs, lows } = await fetchKlines(pair, tf, 220);
      const a = analyzeImproved(closes, highs, lows);
      if (a) results.push({ tf, strategy: 'Reversión', signal: a.signal, confidence: a.confidence, ...withDistances(a) });
      const b = analyzeTrendFollowAdx25(closes, highs, lows);
      if (b) results.push({ tf, strategy: 'Tendencia', signal: b.signal, confidence: b.confidence, ...withDistances(b) });
    }
    const { closes: closes15, highs: highs15, lows: lows15 } = await fetchKlines(pair, '15m', 60);
    const d = analyzeRebote(closes15, highs15, lows15);
    if (d) results.push({ tf: '15m', strategy: 'Rebote', signal: d.signal, confidence: d.confidence, ...withDistances(d) });
    const { closes: closes5, highs: highs5, lows: lows5 } = await fetchKlines(pair, '5m', 40);
    const { opens: opens1, highs: highs1, lows: lows1, closes: closes1, volumes: volumes1 } = await fetchKlines(pair, '1m', 30);
    const e = analyzeScalping(closes5, highs5, lows5, opens1, highs1, lows1, closes1, volumes1, highs15, lows15, closes15);
    if (e) results.push({ tf: '5m', strategy: 'Scalping', signal: e.signal, confidence: e.confidence, regimen: e.regime, ...withDistances(e) });

    // Entrada por estructura (1/9/2026, ajustada 4/9/2026) — ya está
    // conectada al auto-trading real (el label "solo observación" quedó
    // desactualizado, se mantiene para no romper nada que lea este campo).
    // Umbral ADX≥25 (bajado de 30 el 4/9: backtest out-of-sample mostró que
    // 25 gana en las 4 ventanas probadas, sin excepción).
    try {
      const { closes: closes30, highs: highs30, lows: lows30 } = await fetchKlines(pair, '30m', 100);
      const { closes: closesH4, highs: highsH4, lows: lowsH4 } = await fetchKlines(pair, '4h', 60);
      const f = analyzeStructuralEntry(closes30, highs30, lows30, closesH4, highsH4, lowsH4, 25);
      if (f) results.push({ tf: '30m', strategy: 'Estructura (solo observación)', signal: f.signal, confidence: f.confidence, reason: f.reason, adx4h: f.adx4h !== undefined ? f.adx4h.toFixed(1) : undefined, regime4h: f.regime4h, ...withDistances(f) });
    } catch (structErr) {
      results.push({ tf: '30m', strategy: 'Estructura (solo observación)', signal: 'ERROR', error: structErr.message });
    }

    res.json({ success: true, pair, minConfidence: state.minConfidence, results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/stats/scalping-breakdown", (req, res) => {
  const scalpingTrades = state.trades.filter(t => t.strategy === 'Scalping' && t.subStrategy);
  const { netTrades } = summarizeTradesByOutcome(scalpingTrades);
  const bySubStrat = {};
  for (const nt of netTrades) {
    if (!bySubStrat[nt.subStrategy]) bySubStrat[nt.subStrategy] = { total: 0, wins: 0, pnl: 0 };
    bySubStrat[nt.subStrategy].total += 1;
    bySubStrat[nt.subStrategy].pnl += nt.pnl;
    if (nt.pnl >= 0) bySubStrat[nt.subStrategy].wins += 1;
  }
  for (const k in bySubStrat) {
    bySubStrat[k].winRate = Math.round((bySubStrat[k].wins / bySubStrat[k].total) * 1000) / 10;
    bySubStrat[k].pnl = Math.round(bySubStrat[k].pnl * 100) / 100;
  }
  res.json({ success: true, breakdown: bySubStrat });
});

app.get("/stats/all-modes", async (req, res) => {
  try {
    const modes = ['demo', 'testnet', 'real'];
    const result = {};
    for (const mode of modes) {
      const trades = (mode === state.tradingMode)
        ? state.trades
        : (stateCollection ? (await loadFinancialDoc(mode)).trades : []);
      const { total, wins, losses, winRate, pnlTotal } = summarizeTradesByOutcome(trades);
      result[mode] = {
        total, wins, losses, winRate,
        pnlTotal: Math.round(pnlTotal * 100) / 100
      };
    }
    res.json({ success: true, stats: result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Fase 3a: disparar el resumen diario a mano para verificar el formato nuevo sin esperar a la noche.
// NO resetea los contadores del día (dailyPnl/dailyTrades) — solo manda el mensaje.
app.post("/test/daily-summary", async (req, res) => {
  try {
    const savedPnl = state.dailyPnl, savedTrades = state.dailyTrades;
    await sendDailySummaryMsg();
    state.dailyPnl = savedPnl; state.dailyTrades = savedTrades;
    await saveState(state);
    res.json({ success: true, sent: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/state", (req, res) => {
  // Fase 2: se expone la config de tamaño (constantes del código, no editables desde el panel)
  res.json({ ...state, sizing: { mode: SIZING_MODE, riskPerTradePct: SIZING_MODE === 'risk' ? RISK_PER_TRADE_PCT : null, fixedPct: SIZING_MODE === 'fixed' ? (state.positionSizePct || 30) : null, autoTradingLiveEnabled: AUTO_TRADING_LIVE_ENABLED, blockedByMode: autoTradingBlockedByMode() } });
});

app.post("/state/config", async (req, res) => {
  const { autoPairs, autoTFs, minConfidence, requireMTF, maxDailyGainPct, maxDailyLossPct, positionSizePct, subSlThresholdMin, tpAtrMultiplier, cooldownMinutes } = req.body;
  // Registro para cazar el bug de reseteo de config — si minConfidence llega
  // con un valor sospechoso (fuera del rango 50-90 que permite el slider del
  // panel), queda anotado acá con la IP y el user-agent de origen.
  if (minConfidence !== undefined && (minConfidence < 50 || minConfidence > 90)) {
    console.log(`⚠️ SOSPECHOSO: minConfidence=${minConfidence} (fuera del rango 50-90 del slider) — IP: ${req.ip} — UA: ${req.headers['user-agent']} — body completo: ${JSON.stringify(req.body)}`);
  }
  console.log(`/state/config recibido — minConfidence: ${minConfidence}, autoTFs: ${JSON.stringify(autoTFs)}, positionSizePct: ${positionSizePct}`);
  if (autoPairs) state.autoPairs = autoPairs;
  if (autoTFs) state.autoTFs = autoTFs;
  if (minConfidence !== undefined) state.minConfidence = minConfidence;
  if (requireMTF !== undefined) state.requireMTF = requireMTF;
  if (maxDailyGainPct !== undefined) state.maxDailyGainPct = maxDailyGainPct;
  if (maxDailyLossPct !== undefined) state.maxDailyLossPct = maxDailyLossPct;
  if (positionSizePct !== undefined) state.positionSizePct = positionSizePct;
  if (subSlThresholdMin !== undefined) state.subSlThresholdMin = subSlThresholdMin;
  if (tpAtrMultiplier !== undefined) state.tpAtrMultiplier = tpAtrMultiplier;
  if (cooldownMinutes !== undefined) state.cooldownMinutes = cooldownMinutes;
  await saveState(state);
  res.json({ success: true, state });
});

app.post("/state/toggle-auto", async (req, res) => {
  state.autoMode = !state.autoMode;
  if (state.autoMode) state.consecutiveLosses = 0; // reactivación manual = arranque fresco, 3 intentos nuevos
  await saveState(state);
  sendTelegram(state.autoMode ? '▶ Bot automático activado (Servidor 24/7) — contador de pérdidas reiniciado' : '■ Bot automático detenido (Servidor)');
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

// Apertura MANUAL de operaciones (31/8/2026) — a pedido de Juan: abrir una
// posición con TP/SL propios, usando el mismo capital real y quedando
// vigilada por el mismo bot (Telegram, cierre automático), pero sin que la
// haya elegido el algoritmo. A propósito NO tiene breakeven/trailing/límite
// de tiempo automático de Tendencia — esos están afinados para las entradas
// del algoritmo, no para el criterio manual del usuario. Se abre al precio
// de mercado del momento, no a un precio futuro (no es una orden pendiente).
app.post("/state/open-manual-trade", async (req, res) => {
  const { pair, signal, tp, sl, sizePct } = req.body || {};
  if (!pair || !signal || !tp || !sl) {
    return res.status(400).json({ error: "Faltan datos — hacen falta: pair (ej. 'BTCUSDT'), signal ('COMPRAR' o 'VENDER'), tp, sl" });
  }
  if (!['COMPRAR', 'VENDER'].includes(signal)) {
    return res.status(400).json({ error: "signal tiene que ser 'COMPRAR' o 'VENDER'" });
  }
  if (state.openTrades.find(t => t.pair === pair)) {
    return res.status(400).json({ error: `Ya hay una operación abierta en ${pair} — cerrala primero si querés reemplazarla` });
  }
  try {
    const { closes } = await fetchKlines(pair, "1m", 2);
    let entry = closes[closes.length - 1];
    // Chequeo de sentido común: el TP tiene que estar del lado correcto según
    // la dirección, si no la operación no tendría forma de resolverse a favor.
    if (signal === 'COMPRAR' && (tp <= entry || sl >= entry)) {
      return res.status(400).json({ error: `Para COMPRAR, el TP (${tp}) tiene que estar arriba del precio actual (${entry.toFixed(2)}) y el SL abajo` });
    }
    if (signal === 'VENDER' && (tp >= entry || sl <= entry)) {
      return res.status(400).json({ error: `Para VENDER, el TP (${tp}) tiene que estar abajo del precio actual (${entry.toFixed(2)}) y el SL arriba` });
    }
    // Ejecución real en Testnet/Real (5/9/2026) — antes el manual SIEMPRE
    // simulaba internamente, nunca tocaba Binance ni en Testnet ni en Real,
    // aunque el modo global estuviera puesto ahí. Ahora sí ejecuta de verdad
    // cuando corresponde — reusando las mismas funciones ya probadas del
    // lado automático (getRealBalance/placeRealOrder/roundQtyForBinance),
    // pero SOLO para esta operación puntual que el usuario pidió a mano.
    // Esto es completamente independiente del interruptor
    // AUTO_TRADING_LIVE_ENABLED que frena al auto-trading — uno no habilita
    // al otro.
    let capitalBase = state.capital;
    let qty, size;
    if (state.tradingMode !== 'demo') {
      if (state.killSwitchActive) {
        return res.status(400).json({ error: 'Kill switch activo — no se puede abrir ninguna operación real (ni manual) hasta desactivarlo.' });
      }
      try {
        capitalBase = await getRealBalance(state.tradingMode);
      } catch (e) {
        return res.status(400).json({ error: `No se pudo leer el saldo real de ${state.tradingMode.toUpperCase()}: ${e.message}` });
      }
    }
    let riskUsd = null;
    if (SIZING_MODE === 'risk' && !sizePct) {
      // Manual con módulo de riesgo (si el usuario manda sizePct explícito, se respeta el fijo)
      const r = calcPositionSize({ capital: capitalBase, entry, sl, overrides: { riskPerTradePct: RISK_PER_TRADE_PCT } });
      if (!r.ok) return res.status(400).json({ error: `Módulo de riesgo: ${r.reason}` });
      size = r.size; riskUsd = r.riskUsd;
    } else {
      const pct = sizePct || state.positionSizePct || 30;
      size = capitalBase * (pct / 100);
    }
    qty = entry > 0 ? size / entry : 0;

    if (state.tradingMode !== 'demo') {
      qty = roundQtyForBinance(pair, qty);
      if (qty <= 0) {
        return res.status(400).json({ error: 'La cantidad calculada da demasiado chica para que Binance la acepte — probá con un sizePct más alto.' });
      }
      const side = signal === 'COMPRAR' ? 'BUY' : 'SELL';
      try {
        const order = await placeRealOrder(state.tradingMode, pair, side, qty);
        entry = extractFillPrice(order, entry);
        qty = parseFloat(order.executedQty) || qty;
        size = qty * entry;
      } catch (e) {
        return res.status(400).json({ error: `Falló la orden real (${state.tradingMode.toUpperCase()}): ${e.message} — no se abrió ninguna posición.` });
      }
    }

    const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
    const trade = {
      id: Date.now() + '-' + pair, pair, signal, direction: signal === 'COMPRAR' ? 'LARGO' : 'SHORT',
      entry, tp: parseFloat(tp), sl: parseFloat(sl), qty, size, tf: 'manual',
      sizingMode: sizePct ? 'fixed' : SIZING_MODE, riskUsd, // Fase 2
      strategy: 'Manual', subStrategy: null, tp2: null, adxScale: 1.0,
      atr: Math.abs(entry - sl) / 1.5, peakPrice: entry,
      trailingActive: false, partialTaken: false, trendDisagreeCount: 0,
      openTime: formatArgTime(new Date()), openTimestamp: Date.now(),
      confidence: 100, auto: false
    };
    state.openTrades.push(trade);
    await saveState(state);
    const modeTag = state.tradingMode !== 'demo' ? ` [${state.tradingMode.toUpperCase()} — orden real ejecutada]` : '';
    sendTelegram(`✋ OPERACIÓN MANUAL ABIERTA${modeTag} (Servidor)\n📊 ${pair.replace('USDT','/USDT')}\n${signal} · ${trade.direction}\n💵 Entrada: $${entry.toFixed(2)}\n🎯 TP: $${trade.tp.toFixed(2)}\n🛑 SL: $${trade.sl.toFixed(2)}\n📊 R/R: 1:${rr.toFixed(2)}\n💰 Tamaño: ${pct}% del capital\nSin breakeven/trailing automático — TP/SL fijos, tal como los pusiste.`);
    res.json({ success: true, trade, state });
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
// ── Selector de modo (demo/testnet/real), confirmación explícita y kill switch ──
app.post("/mode/confirm", async (req, res) => {
  const { confirmText } = req.body;
  if (confirmText !== 'ENTIENDO EL RIESGO') {
    return res.status(400).json({ error: "Confirmación inválida. Hay que escribir exactamente: ENTIENDO EL RIESGO" });
  }
  state.realModeConfirmed = true;
  await saveState(state);
  sendTelegram('✅ Confirmación de riesgo real registrada. Ya se puede activar testnet/real.');
  res.json({ success: true });
});

app.post("/mode/set", async (req, res) => {
  const { mode } = req.body;
  if (!['demo', 'testnet', 'real'].includes(mode)) return res.status(400).json({ error: "Modo inválido" });
  if (state.killSwitchActive && mode !== 'demo') {
    return res.status(400).json({ error: "El kill switch está activo — reseteálo primero en /kill/reset antes de volver a testnet/real." });
  }
  if (mode !== 'demo' && !state.realModeConfirmed) {
    return res.status(400).json({ error: "Falta la confirmación explícita de riesgo. Llamá primero a /mode/confirm." });
  }
  if (mode !== 'demo') {
    const creds = getBinanceCredentials(mode);
    if (!creds || !creds.apiKey || !creds.apiSecret) {
      return res.status(400).json({ error: `Faltan configurar las variables de entorno de Binance para modo ${mode} en Render.` });
    }
  }
  const previousMode = state.tradingMode;
  if (previousMode !== mode) {
    // Guardamos el financiero del modo viejo antes de irnos, así no se pierde nada
    await saveState(state);
    // Cargamos el financiero SEPARADO del modo nuevo — el capital/historial de
    // cada modo nunca se mezcla con el de los otros dos.
    const financial = stateCollection ? await loadFinancialDoc(mode) : freshFinancialState();
    state = { ...state, ...financial, tradingMode: mode };
  }
  await saveState(state);
  sendTelegram(`⚙️ Modo de operación cambiado a: ${mode.toUpperCase()}\n💰 Capital de este modo: $${state.capital.toFixed(2)}`);
  res.json({ success: true, tradingMode: state.tradingMode, capital: state.capital });
});

app.post("/kill", async (req, res) => {
  state.killSwitchActive = true;
  state.autoMode = false;
  if (state.tradingMode !== 'demo') {
    await saveState(state); // guarda el financiero del modo que se estaba usando
    const demoFinancial = stateCollection ? await loadFinancialDoc('demo') : freshFinancialState();
    state = { ...state, ...demoFinancial, tradingMode: 'demo' };
  }
  await saveState(state);
  sendTelegram('🛑🛑🛑 KILL SWITCH ACTIVADO — todo detenido, modo forzado a DEMO, AUTO pausado.');
  res.json({ success: true });
});

app.post("/kill/reset", async (req, res) => {
  state.killSwitchActive = false;
  await saveState(state);
  sendTelegram('🔓 Kill switch reseteado. Seguís en modo DEMO hasta que elijas otro modo.');
  res.json({ success: true });
});

async function getAllBalances(mode) {
  const creds = getBinanceCredentials(mode);
  if (!creds || !creds.apiKey || !creds.apiSecret) throw new Error(`Faltan las claves de Binance (${mode}) configuradas en el servidor`);
  const timestamp = Date.now();
  const query = `timestamp=${timestamp}`;
  const signature = hmac(creds.apiSecret, query);
  const response = await fetch(`${creds.baseUrl}/api/v3/account?${query}&signature=${signature}`, {
    headers: { "X-MBX-APIKEY": creds.apiKey }
  });
  const data = await response.json();
  if (data.code) throw new Error(data.msg || 'Error de Binance');
  return (data.balances || []).filter(b => parseFloat(b.free) > 0 || parseFloat(b.locked) > 0);
}

app.post("/real-balance", async (req, res) => {
  const { mode } = req.body;
  if (!['testnet', 'real'].includes(mode)) return res.status(400).json({ error: "Modo inválido" });
  try {
    const usdt = await getRealBalance(mode);
    res.json({ success: true, usdt });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/real-balance-all", async (req, res) => {
  const { mode } = req.body;
  if (!['testnet', 'real'].includes(mode)) return res.status(400).json({ error: "Modo inválido" });
  try {
    const balances = await getAllBalances(mode);
    res.json({ success: true, balances });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/real-order-test", async (req, res) => {
  const { mode } = req.body;
  if (!['testnet', 'real'].includes(mode)) return res.status(400).json({ error: "Modo inválido" });
  try {
    // Orden mínima de prueba — si falla por saldo insuficiente, confirma que
    // la conexión/firma/autenticación funcionan perfecto de punta a punta.
    const data = await placeRealOrder(mode, 'BTCUSDT', 'BUY', '0.001');
    res.json({ success: true, data });
  } catch (e) {
    res.json({ success: false, error: e.message, note: 'Si el error dice algo de saldo/balance insuficiente, la conexión SÍ funciona — solo falta plata de prueba en la cuenta.' });
  }
});

// (5/9/2026) Se sacaron acá /balance, /order y /webhook — eran resabios de
// antes de pasar a variables de entorno (ver handoff del 5/9): aceptaban
// apiKey/apiSecret directo en el body del pedido, sin ningún secreto que
// protegiera /balance ni /order. No exponían TU clave (necesitaban que quien
// llame mande la suya propia), pero eran una puerta abierta: cualquiera con
// la URL podía hacer que tu servidor ejecutara órdenes o disparara tu
// Telegram con sus propias claves. Confirmado que el frontend actual no los
// usa (usa /real-balance, que sí está bien armado con mode+env vars) y que
// no hay integración externa activa (webhook confirmado sin uso por Juan).
// /alert se dejó intacto — lo usa el botón de "probar resumen diario" y no
// maneja claves ni ejecuta operaciones, solo manda un mensaje a Telegram.

app.post("/alert", async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: "Falta mensaje" });
  await sendTelegram(message);
  res.json({ success: true });
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
  const { wins, losses, winRate } = summarizeTradesByOutcome(state.trades);
  let motivacion = '';
  if (state.dailyPnl > 0 && winRate >= 60) motivacion = '🚀 Excelente día! Seguí así, campeón!';
  else if (state.dailyPnl > 0) motivacion = '🟢 Buen día! De a poco se llega lejos.';
  else if (state.dailyPnl < 0 && losses >= 3) motivacion = '💪 Dale vos podés! Mañana es otro día.';
  else if (state.dailyPnl < 0) motivacion = '🔴 Día difícil. Revisá las señales y descansá.';
  else motivacion = '⚪ Día tranquilo. El mercado espera su momento.';
  const now = formatArgTime(new Date());
  // Fase 3a: estado del sistema + ADX 4h de cada par, para saber cada mañana POR QUÉ operó o no operó
  let adxLines = '';
  for (const pair of ['BTCUSDT', 'ETHUSDT']) {
    try {
      const { closes: c4, highs: h4, lows: l4 } = await fetchKlines(pair, '4h', 60);
      const adx = calcADX(h4, l4, c4, 14);
      const adxNum = typeof adx === 'number' ? adx : (adx && typeof adx.adx === 'number' ? adx.adx : null);
      adxLines += `\n   ${pair.replace('USDT', '')}: ADX 4h ${adxNum !== null ? adxNum.toFixed(1) : 'n/d'} ${adxNum !== null ? (adxNum >= 25 ? '✅ habilita' : '⏸ menor a 25, sin tendencia') : ''}`;
    } catch (e) { adxLines += `\n   ${pair.replace('USDT', '')}: ADX 4h error (${e.message})`; }
  }
  const bloqueado = autoTradingBlockedByMode() ? '\n⚠️ AUTO-TRADING BLOQUEADO: modo ≠ demo' : '';
  const sistema = `🔧 Modo: ${String(state.tradingMode).toUpperCase()} · autoMode: ${state.autoMode ? 'ON' : 'OFF'} · tamaño: ${SIZING_MODE === 'risk' ? `riesgo ${RISK_PER_TRADE_PCT}%` : `fijo ${state.positionSizePct || 30}%`}${bloqueado}\n📡 Filtro Estructura (umbral ADX 25):${adxLines}`;
  sendTelegram(`📊 RESUMEN DIARIO (Servidor 24/7)\n📅 ${now}\n\n💰 Capital: $${state.capital.toFixed(2)}\n📈 P&L hoy: ${state.dailyPnl>=0?'+':''}$${state.dailyPnl.toFixed(2)}\n🎯 Operaciones hoy: ${state.dailyTrades}\n✅ Ganadas: ${wins}\n❌ Perdidas: ${losses}\n📊 Win Rate: ${winRate}%\n\n${sistema}\n\n${motivacion}`);
  state.dailyPnl = 0; state.dailyTrades = 0;
  await saveState(state);
}

// ── Backtest Engine ───────────────────────────────────────
async function fetchHistoricalCandlesWithVolume(pair, tf, days, capOverride, endTimeOverride) {
  const limit = 1000;
  const tfMs = { '1m': 60000, '5m': 5*60000, '15m': 15*60000, '30m': 30*60000, '1h': 3600000, '4h': 4*3600000, '1d': 86400000 }[tf];
  const totalCandles = Math.min(Math.ceil((days * 86400000) / tfMs), capOverride || 5000);
  let allCandles = [];
  let endTime = endTimeOverride || Date.now();

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
    low: parseFloat(k[3]), close: parseFloat(k[4]), volume: parseFloat(k[5])
  }));
}

// Backtest de Scalping — necesita 3 timeframes alineados en el tiempo (5m
// para entrada, 1m para régimen, 15m para tamaño de ATR), a diferencia de
// las otras estrategias que solo miran uno. Reutiliza analyzeScalping()
// real, la misma que corre en vivo — no una reescritura.
// Backtest de Tendencia-ADX + filtro de sub-timeframe (5/8/2026) — necesita
// dos series alineadas: la principal (4h) para la señal, y una corta (15m)
// para confirmar que el momento de entrada no está en contra. Comisión real
// incluida, mismo criterio que el resto de los backtests de hoy.
// Backtest de Tendencia + contra-trade en paralelo (7/8/2026) — simula la
// idea del usuario: mientras Tendencia sostiene su posición grande (4h/30m),
// un segundo trade CHICO e independiente aprovecha los movimientos cortos
// en contra en 15m (cruce EMA5/EMA13 bajista si la principal es larga, o
// alcista si es corta), con TP/SL ajustados (1x ATR de 15m, simétrico —
// pensado para capturar el "serrucho", no para acompañar tendencia).
// Se cierra solo o cuando la posición principal cierra. PnL combinado.
async function runTendenciaWithCounterBacktest(pair, tf, days, config) {
  const { minConfidence, riskPct, initialCapital } = config;
  const COMMISSION_PCT = 0.001;
  const shortTf = '15m';

  const [candlesMain, candlesShort] = await Promise.all([
    fetchHistoricalCandlesWithVolume(pair, tf, days, 2000),
    fetchHistoricalCandlesWithVolume(pair, shortTf, days, Math.min(Math.ceil(days * 96) + 100, 20000))
  ]);

  let pShort = 0;
  function windowUpTo(arr, ptrStart, targetTime, maxLen) {
    let i = ptrStart;
    while (i < arr.length - 1 && arr[i + 1].time <= targetTime) i++;
    const start = Math.max(0, i - maxLen + 1);
    return { slice: arr.slice(start, i + 1), newPtr: i };
  }

  let capital = initialCapital, mainTrades = [], counterTrades = [];
  let openMain = null, openCounter = null;
  let peakCapital = initialCapital, maxDrawdown = 0;
  const MIN_HISTORY = 70;

  function closeMain(exitPrice, reason, closeTime) {
    const pricePct = openMain.signal === 'COMPRAR' ? (exitPrice - openMain.entry) / openMain.entry : (openMain.entry - exitPrice) / openMain.entry;
    const grossPnl = openMain.size * pricePct;
    const commission = openMain.size * COMMISSION_PCT * 2;
    const pnl = grossPnl - commission;
    capital += pnl;
    mainTrades.push({ ...openMain, exitPrice, pnl, grossPnl, commission, reason, closeTime });
    openMain = null;
  }
  function closeCounter(exitPrice, reason, closeTime) {
    const pricePct = openCounter.signal === 'COMPRAR' ? (exitPrice - openCounter.entry) / openCounter.entry : (openCounter.entry - exitPrice) / openCounter.entry;
    const grossPnl = openCounter.size * pricePct;
    const commission = openCounter.size * COMMISSION_PCT * 2;
    const pnl = grossPnl - commission;
    capital += pnl;
    counterTrades.push({ ...openCounter, exitPrice, pnl, grossPnl, commission, reason, closeTime });
    openCounter = null;
  }

  for (let i = MIN_HISTORY; i < candlesMain.length; i++) {
    const current = candlesMain[i];
    const window = candlesMain.slice(Math.max(0, i - MIN_HISTORY), i + 1);
    const closes = window.map(c => c.close), highs = window.map(c => c.high), lows = window.map(c => c.low);

    const wShort = windowUpTo(candlesShort, pShort, current.time, 30);
    pShort = wShort.newPtr;
    const shortCloses = wShort.slice.map(c => c.close);
    const shortHighs = wShort.slice.map(c => c.high), shortLows = wShort.slice.map(c => c.low);
    const shortCurrent = wShort.slice[wShort.slice.length - 1];

    if (openMain) {
      let closed = false, exitPrice = null, reason = null;
      if (openMain.signal === 'COMPRAR') {
        if (current.high >= openMain.tp) { exitPrice = openMain.tp; reason = 'TP'; closed = true; }
        else if (current.low <= openMain.sl) { exitPrice = openMain.sl; reason = 'SL'; closed = true; }
      } else {
        if (current.low <= openMain.tp) { exitPrice = openMain.tp; reason = 'TP'; closed = true; }
        else if (current.high >= openMain.sl) { exitPrice = openMain.sl; reason = 'SL'; closed = true; }
      }
      if (closed) {
        closeMain(exitPrice, reason, current.time);
        if (openCounter) closeCounter(shortCurrent ? shortCurrent.close : current.close, 'Cierre junto a la principal', current.time);
        if (capital > peakCapital) peakCapital = capital;
        const dd = (peakCapital - capital) / peakCapital;
        if (dd > maxDrawdown) maxDrawdown = dd;
        continue;
      }

      // Posición principal sigue abierta — evaluar el contra-trade chico
      if (openCounter) {
        if (shortCurrent) {
          let cClosed = false, cExit = null, cReason = null;
          if (openCounter.signal === 'COMPRAR') {
            if (shortCurrent.high >= openCounter.tp) { cExit = openCounter.tp; cReason = 'TP'; cClosed = true; }
            else if (shortCurrent.low <= openCounter.sl) { cExit = openCounter.sl; cReason = 'SL'; cClosed = true; }
          } else {
            if (shortCurrent.low <= openCounter.tp) { cExit = openCounter.tp; cReason = 'TP'; cClosed = true; }
            else if (shortCurrent.high >= openCounter.sl) { cExit = openCounter.sl; cReason = 'SL'; cClosed = true; }
          }
          if (cClosed) closeCounter(cExit, cReason, shortCurrent.time);
        }
      } else if (shortCloses.length >= 15) {
        const ema5 = calcEMA(shortCloses, 5), ema13 = calcEMA(shortCloses, 13);
        const atrShort = calcATR(shortHighs, shortLows, shortCloses) || shortCurrent.close * 0.002;
        if (ema5 && ema13) {
          // Si la principal es larga y el corto plazo cruza bajista -> contra-short.
          // Si la principal es corta y el corto plazo cruza alcista -> contra-largo.
          if (openMain.signal === 'COMPRAR' && ema5 < ema13) {
            const size = capital * (riskPct / 2);
            openCounter = { signal: 'VENDER', entry: shortCurrent.close, tp: shortCurrent.close - atrShort, sl: shortCurrent.close + atrShort, size, openTime: shortCurrent.time };
          } else if (openMain.signal === 'VENDER' && ema5 > ema13) {
            const size = capital * (riskPct / 2);
            openCounter = { signal: 'COMPRAR', entry: shortCurrent.close, tp: shortCurrent.close + atrShort, sl: shortCurrent.close - atrShort, size, openTime: shortCurrent.time };
          }
        }
      }
      continue;
    }

    const a = analyzeTrendFollowAdx(closes, highs, lows);
    if (a && a.signal !== 'NEUTRO' && a.confidence >= minConfidence) {
      const size = capital * riskPct;
      openMain = { signal: a.signal, entry: a.entry, tp: a.tp, sl: a.sl, size, openTime: current.time, confidence: a.confidence };
    }
  }

  const allTrades = [...mainTrades, ...counterTrades];
  const wins = allTrades.filter(t => t.pnl > 0).length;
  const losses = allTrades.filter(t => t.pnl < 0).length;
  const totalPnl = capital - initialCapital;
  return {
    mainTrades: mainTrades.length, counterTrades: counterTrades.length,
    totalTrades: allTrades.length, wins, losses,
    winRate: allTrades.length > 0 ? (wins / allTrades.length * 100).toFixed(1) : "0",
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalGrossPnl: allTrades.reduce((s, t) => s + t.grossPnl, 0).toFixed(2),
    totalCommission: allTrades.reduce((s, t) => s + t.commission, 0).toFixed(2),
    mainPnl: mainTrades.reduce((s, t) => s + t.pnl, 0).toFixed(2),
    counterPnl: counterTrades.reduce((s, t) => s + t.pnl, 0).toFixed(2),
    totalReturn: ((totalPnl / initialCapital) * 100).toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2),
    candlesUsed: { main: candlesMain.length, short: candlesShort.length }
  };
}

// Backtest "realista" de Tendencia-ADX (10/8/2026) — a diferencia de todos los
// backtests anteriores (que solo chequean TP/SL con paciencia infinita), este
// simula los DOS mecanismos que existen SOLO en vivo: el trailing stop
// (activa a 0.6%, sigue al precio a 1x ATR) y el límite de tiempo escalado
// por ADX (con el mismo criterio de "no cortar en pérdida neta salvo límite
// duro 3x" que usa el código real). Usa velas de 15m para simular la vida de
// la operación después de abrir (proxy razonable del chequeo real de 1m).
async function runTendenciaRealisticBacktest(pair, tf, days, config) {
  const { minConfidence, riskPct, initialCapital, timeLimitMultiplier } = config;
  const COMMISSION_PCT = 0.001;
  // shortTf configurable (12/8→29/8): '15m' es el proxy liviano de siempre;
  // '1m' es la nueva opción de máxima precisión, igual de fino que el chequeo
  // real en vivo — pero mucho más pesado de traer, por eso el límite de
  // velas se calcula distinto según cuál se pida.
  const shortTf = config.shortTf || '15m';
  const candlesPerDayByTf = { '1m': 1440, '5m': 288, '15m': 96, '30m': 48, '1h': 24, '4h': 6, '1d': 1 };
  const shortCap = Math.min(Math.ceil(days * (candlesPerDayByTf[shortTf] || 96)) + 200, shortTf === '1m' ? 50000 : 20000);
  // Cap de la serie principal (1/9/2026) — antes era un 2000 fijo, que en
  // timeframes finos (30m = 48 velas/día) se quedaba corto para ventanas
  // largas: pedir days=180 en 30m necesita 8640 velas, pero el cap de 2000
  // truncaba en silencio a ~41.7 días reales sin avisar — mismo patrón que
  // el bug del mapeo de '30m' faltante (30/8), esta vez en el cap en vez
  // del mapeo. Mismo cálculo que ya se usaba para shortCap, aplicado acá.
  const mainCap = Math.min(Math.ceil(days * (candlesPerDayByTf[tf] || 24)) + 200, 20000);
  // Fecha de corte específica (30/8/2026) — antes "days" siempre se contaba
  // hacia atrás desde AHORA MISMO, así que nunca se podía backtestear un
  // período puntual del pasado (ej: "los mismos días exactos donde el bot
  // vino mal en vivo"). Con config.endDate ("2026-08-30" o con hora,
  // "2026-08-30T22:00:00Z"), la ventana de "days" se ancla ahí en vez de a hoy.
  const endTimeOverride = config.endDate ? new Date(config.endDate).getTime() : undefined;

  const [candlesMain, candlesShort] = await Promise.all([
    fetchHistoricalCandlesWithVolume(pair, tf, days, mainCap, endTimeOverride),
    fetchHistoricalCandlesWithVolume(pair, shortTf, days, shortCap, endTimeOverride)
  ]);

  let pShort = 0;
  function windowUpTo(arr, ptrStart, targetTime, maxLen) {
    let i = ptrStart;
    while (i < arr.length - 1 && arr[i + 1].time <= targetTime) i++;
    const start = Math.max(0, i - maxLen + 1);
    return { slice: arr.slice(start, i + 1), newPtr: i };
  }
  function idxAtOrAfter(arr, fromIdx, targetTime) {
    let i = fromIdx;
    while (i < arr.length && arr[i].time < targetTime) i++;
    return i;
  }

  let capital = initialCapital, trades = [], peakCapital = initialCapital, maxDrawdown = 0;
  let closedByReason = {};
  let signalsSeenBeforeFilter = 0, signalsSkippedByEdgeFilter = 0;
  const MIN_HISTORY = 70;
  const tfHours = { '15m': 0.25, '30m': 0.5, '1h': 1, '4h': 4 }[tf] || 4;

  for (let i = MIN_HISTORY; i < candlesMain.length; i++) {
    const current = candlesMain[i];
    const window = candlesMain.slice(Math.max(0, i - MIN_HISTORY), i + 1);
    const closes = window.map(c => c.close), highs = window.map(c => c.high), lows = window.map(c => c.low);

    const analyzeFnRealistic = config.tpVariant === 'capped' ? analyzeTrendFollowAdxCapped
      : config.tpVariant === 'small' ? analyzeTrendFollowSmallTp
      : config.tpVariant === 'adx25' ? analyzeTrendFollowAdx25
      : analyzeTrendFollowAdx;
    const a = analyzeFnRealistic(closes, highs, lows);
    if (!a || a.signal === 'NEUTRO' || a.confidence < minConfidence) continue;
    signalsSeenBeforeFilter++;
    // Mismo filtro de comisión mínima que ya corre en vivo para Tendencia
    // (openTrade(), MIN_EDGE_STRATEGIES) — el backtest realista no lo tenía,
    // por eso la comisión se comía el bruto positivo con 83 operaciones.
    if (config.applyMinEdgeFilter !== false) {
      const projectedMovePct = Math.abs(a.tp - a.entry) / a.entry;
      if (projectedMovePct < 0.006) { signalsSkippedByEdgeFilter++; continue; }
    }

    // Filtro estructural (variante C, 12/8/2026) — exige HH+HL (o LH+LL)
    // confirmados en la MISMA ventana ya usada para la señal (sin datos
    // extra ni futuros) y espacio libre ≥2x la distancia al SL antes del
    // próximo swing en contra. Se activa con config.requireStructure.
    if (config.requireStructure) {
      const { swingHighs, swingLows } = detectConfirmedSwings(highs, lows, 3, 3);
      const structure = classifyStructure(swingHighs, swingLows);
      const structureOk = (a.signal === 'COMPRAR' && structure === 'alcista') || (a.signal === 'VENDER' && structure === 'bajista');
      if (!structureOk) { if(!closedByReason['_structFail']) closedByReason['_structFail']=0; closedByReason['_structFail']++; continue; }
      const slDist = Math.abs(a.entry - a.sl);
      const space = checkStructuralSpace(a.entry, slDist, a.direction, swingHighs, swingLows);
      if (!space.hasSpace) { if(!closedByReason['_spaceFail']) closedByReason['_spaceFail']=0; closedByReason['_spaceFail']++; continue; }
    }

    // Simular la vida de esta operación paso a paso en velas de 15m, igual
    // que el chequeo real de 1m en vivo — trailing + tiempo + TP/SL.
    const wShortStart = windowUpTo(candlesShort, pShort, current.time, 1);
    let sIdx = idxAtOrAfter(candlesShort, wShortStart.newPtr, current.time);
    const entry = a.entry, tp = a.tp, sl0 = a.sl, signal = a.signal;
    const atr = Math.abs(entry - sl0) / 1.5;
    const size = capital * riskPct;
    const MAX_HOURS_OPEN = 2 * (a.adxScale || 1.0) * (config.timeLimitMultiplier || 1.0);
    const HARD_MAX_HOURS_OPEN = MAX_HOURS_OPEN * 3;
    const MIN_ACTIVATION_PCT = 0.006;
    const TRAIL_DISTANCE_ATR = config.trailDistanceAtr || 1.0;
    const COMMISSION_ROUNDTRIP_PCT = 0.002;

    let sl = sl0, peakPrice = entry, trailingActive = false;
    let exitPrice = null, reason = null;
    let nextMainIdx = i + 1; // avanzamos el índice principal hasta pasar el cierre

    for (; sIdx < candlesShort.length; sIdx++) {
      const c = candlesShort[sIdx];
      const hoursOpen = (c.time - current.time) / (1000 * 60 * 60);

      // TP/SL por mecha
      if (signal === 'COMPRAR') {
        if (c.high >= tp) { exitPrice = tp; reason = 'TP Auto'; break; }
        if (c.low <= sl) { exitPrice = sl; reason = trailingActive ? 'SL Auto (trailing)' : 'SL Auto'; break; }
      } else {
        if (c.low <= tp) { exitPrice = tp; reason = 'TP Auto'; break; }
        if (c.high >= sl) { exitPrice = sl; reason = trailingActive ? 'SL Auto (trailing)' : 'SL Auto'; break; }
      }

      // Trailing por retroceso desde el pico (config.peakGiveback, 27/8/2026) —
      // a diferencia del breakeven y el trailing normal (que exigen una
      // distancia FIJA de ATR antes de reaccionar), esto mira si el precio ya
      // devolvió una parte importante de lo más que llegó a subir/bajar a
      // favor — sin importar si esa distancia llegó a 1x o 1.5x ATR. Ataca
      // directo el caso real: el precio subió, no llegó al umbral del
      // breakeven/trailing, se dio vuelta, y cerró por tiempo con centavos
      // sin que nada intentara asegurar el pico.
      if (config.peakGiveback && !trailingActive) {
        const GIVEBACK_PCT = config.peakGivebackPct || 0.5;
        const MIN_PEAK_ATR = config.peakGivebackMinAtr || 0.3;
        if (signal === 'COMPRAR') {
          if (c.high > peakPrice) peakPrice = c.high;
          const peakMove = peakPrice - entry;
          if (peakMove >= atr * MIN_PEAK_ATR) {
            const currentMove = c.close - entry;
            const givenBack = peakMove - currentMove;
            if (givenBack >= peakMove * GIVEBACK_PCT && currentMove > 0) {
              exitPrice = c.close; reason = 'Cierre por retroceso desde pico'; break;
            }
          }
        } else {
          if (c.low < peakPrice) peakPrice = c.low;
          const peakMove = entry - peakPrice;
          if (peakMove >= atr * MIN_PEAK_ATR) {
            const currentMove = entry - c.close;
            const givenBack = peakMove - currentMove;
            if (givenBack >= peakMove * GIVEBACK_PCT && currentMove > 0) {
              exitPrice = c.close; reason = 'Cierre por retroceso desde pico'; break;
            }
          }
        }
      }

      // Breakeven temprano (config.earlyBreakeven) — a diferencia del trailing
      // normal (que recién actúa con 0.6% de ganancia real), esto mueve el SL
      // a punto de equilibrio (ni gana ni pierde, solo paga comisión) apenas
      // el precio se mueve un poco a favor — mucho antes. Ataca directo el
      // patrón real que confirmamos con datos: pérdidas completas y frecuentes,
      // ganancias chicas y raras. No reemplaza el trailing, corre antes.
      if (config.earlyBreakeven) {
        const BREAKEVEN_TRIGGER_ATR = config.breakevenTriggerAtr || 0.4;
        if (signal === 'COMPRAR') {
          const favorableMove = c.high - entry;
          if (favorableMove >= atr * BREAKEVEN_TRIGGER_ATR && sl < entry) {
            sl = entry;
          }
        } else {
          const favorableMove = entry - c.low;
          if (favorableMove >= atr * BREAKEVEN_TRIGGER_ATR && sl > entry) {
            sl = entry;
          }
        }
      }

      // Trailing stop (se puede desactivar con config.disableTrailing, para
      // aislar si es esto o el límite de tiempo lo que corta antes de tiempo)
      if (!config.disableTrailing) {
        if (signal === 'COMPRAR') {
          if (c.high > peakPrice) peakPrice = c.high;
          const favorableMove = peakPrice - entry;
          if (favorableMove >= Math.max(atr, entry * MIN_ACTIVATION_PCT)) {
            const candidateSl = Math.max(entry, peakPrice - atr * TRAIL_DISTANCE_ATR);
            if (candidateSl > sl) { sl = candidateSl; trailingActive = true; }
          }
        } else {
          if (c.low < peakPrice) peakPrice = c.low;
          const favorableMove = entry - peakPrice;
          if (favorableMove >= Math.max(atr, entry * MIN_ACTIVATION_PCT)) {
            const candidateSl = Math.min(entry, peakPrice + atr * TRAIL_DISTANCE_ATR);
            if (candidateSl < sl) { sl = candidateSl; trailingActive = true; }
          }
        }
      }

      // Límite de tiempo (mismo criterio que en vivo: no cortar en pérdida neta
      // salvo límite duro) — se puede desactivar del todo con config.noTimeLimit,
      // dejando que la operación corra hasta TP o SL sin importar cuánto tarde.
      if (!config.noTimeLimit && hoursOpen >= MAX_HOURS_OPEN) {
        const pricePct = signal === 'COMPRAR' ? (c.close - entry) / entry : (entry - c.close) / entry;
        const inLoss = pricePct < COMMISSION_ROUNDTRIP_PCT;
        const hitHardCap = hoursOpen >= HARD_MAX_HOURS_OPEN;
        if (!inLoss || hitHardCap) {
          exitPrice = c.close;
          reason = hitHardCap && inLoss ? 'Cierre por tiempo (límite duro)' : 'Cierre por tiempo';
          break;
        }
      }
    }

    if (exitPrice === null) continue; // se quedó sin datos antes de resolver, se descarta

    const pricePct = signal === 'COMPRAR' ? (exitPrice - entry) / entry : (entry - exitPrice) / entry;
    const grossPnl = size * pricePct;
    const commission = size * COMMISSION_PCT * 2;
    const pnl = grossPnl - commission;
    capital += pnl;
    trades.push({ signal, entry, exitPrice, pnl, grossPnl, commission, reason, adx: a.adx, adxScale: a.adxScale });
    closedByReason[reason] = (closedByReason[reason] || 0) + 1;
    if (capital > peakCapital) peakCapital = capital;
    const dd = (peakCapital - capital) / peakCapital;
    if (dd > maxDrawdown) maxDrawdown = dd;

    // Avanzar el índice principal hasta pasar el momento de cierre, para no reabrir en el mismo hueco
    while (nextMainIdx < candlesMain.length && candlesMain[nextMainIdx].time < candlesShort[sIdx].time) nextMainIdx++;
    i = nextMainIdx - 1; // el for principal hace i++ después
    pShort = sIdx;
  }

  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const totalPnl = capital - initialCapital;
  const adxScaleCount = {};
  for (const t of trades) {
    const k = (t.adxScale || 1.0).toString();
    adxScaleCount[k] = (adxScaleCount[k] || 0) + 1;
  }
  return {
    trades: trades.length, wins, losses,
    winRate: trades.length > 0 ? (wins / trades.length * 100).toFixed(1) : "0",
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalGrossPnl: trades.reduce((s, t) => s + t.grossPnl, 0).toFixed(2),
    totalCommission: trades.reduce((s, t) => s + t.commission, 0).toFixed(2),
    totalReturn: ((totalPnl / initialCapital) * 100).toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2),
    closedByReason,
    adxScaleDistribution: adxScaleCount,
    edgeFilterCheck: { signalsSeenBeforeFilter, signalsSkippedByEdgeFilter, filterActive: config.applyMinEdgeFilter !== false },
    candlesUsed: { main: candlesMain.length, short: candlesShort.length }
  };
}

// Motor de backtest para la entrada por estructura (1/9/2026) — walk-forward
// sin lookahead: en cada vela de 30m, calcula swings y régimen 4h SOLO con
// datos hasta ese momento (nunca del futuro). Deliberadamente SIN el
// aparataje de breakeven/trailing/límite-de-tiempo de la entrada vieja —
// esta entrada usa SL/TP estructurales fijos, para testearla en estado puro
// antes de sumarle cualquier gestión de salida encima (evitar repetir el
// error de afinar la salida antes de confirmar que la entrada tiene filo).
async function runStructuralEntryBacktest(pair, tf, days, config) {
  const { minConfidence = 0, riskPct, initialCapital, minAdx4h = 20 } = config;
  const COMMISSION_PCT = 0.001;
  const candlesPerDayByTf = { '15m': 96, '30m': 48, '1h': 24, '4h': 6 };
  const mainCap = Math.min(Math.ceil(days * (candlesPerDayByTf[tf] || 48)) + 200, 20000);
  const cap4h = Math.min(Math.ceil(days * 6) + 100, 5000);
  const endTimeOverride = config.endDate ? new Date(config.endDate).getTime() : undefined;

  const [candlesMain, candles4h] = await Promise.all([
    fetchHistoricalCandlesWithVolume(pair, tf, days, mainCap, endTimeOverride),
    fetchHistoricalCandlesWithVolume(pair, '4h', days, cap4h, endTimeOverride)
  ]);

  let capital = initialCapital, trades = [], peakCapital = initialCapital, maxDrawdown = 0;
  let closedByReason = {};
  const pnlByReason = {};
  let signalsSeenBeforeFilter = 0, signalsSkippedByEdgeFilter = 0, signalsSkippedByRegime = 0, signalsSkippedByAdx = 0, signalsSkippedBySizing = 0;
  const MIN_HISTORY = 60;
  let p4h = 0;

  for (let i = MIN_HISTORY; i < candlesMain.length; i++) {
    const current = candlesMain[i];
    const window = candlesMain.slice(Math.max(0, i - MIN_HISTORY), i + 1);
    const closes = window.map(c => c.close), highs = window.map(c => c.high), lows = window.map(c => c.low);

    // Avanzar el puntero de 4h hasta la última vela YA CERRADA al momento actual (sin lookahead).
    while (p4h < candles4h.length - 1 && candles4h[p4h + 1].time <= current.time) p4h++;
    if (candles4h[p4h].time > current.time || p4h < 19) continue; // todavía no hay suficiente historial 4h real
    const window4h = candles4h.slice(Math.max(0, p4h - 40), p4h + 1);
    const closes4h = window4h.map(c => c.close), highs4h = window4h.map(c => c.high), lows4h = window4h.map(c => c.low);

    const a = analyzeStructuralEntry(closes, highs, lows, closes4h, highs4h, lows4h, minAdx4h);
    if (!a || a.signal === 'NEUTRO') {
      if (a && a.reason === 'Estructura local no confirma el régimen 4h') signalsSkippedByRegime++;
      if (a && a.reason && a.reason.startsWith('ADX 4h insuficiente')) signalsSkippedByAdx++;
      continue;
    }
    if (a.confidence < minConfidence) continue;
    signalsSeenBeforeFilter++;
    const projectedMovePct = Math.abs(a.tp - a.entry) / a.entry;
    if (config.applyMinEdgeFilter !== false && projectedMovePct < 0.006) { signalsSkippedByEdgeFilter++; continue; }

    // Resolver la operación vela a vela en el mismo timeframe principal —
    // SL/TP fijos, estructurales, sin trailing ni límite de tiempo (a propósito).
    const entry = a.entry, tp = a.tp, sl = a.sl, signal = a.signal;
    // Tamaño: 'fixed' = comportamiento de siempre (capital × riskPct);
    // 'risk' = módulo de riesgo (risk.js): arriesga riskPerTradePct del capital según la distancia al SL.
    let size, riskUsd = null;
    if (config.sizingMode === 'risk') {
      const r = calcPositionSize({ capital, entry, sl, overrides: { riskPerTradePct: config.riskPerTradePct ?? 0.5 } });
      if (!r.ok) { signalsSkippedBySizing++; continue; }
      size = r.size; riskUsd = r.riskUsd;
    } else {
      size = capital * riskPct;
    }
    let exitPrice = null, reason = null;
    const MAX_BARS = 96; // ~2 días en 30m como límite de seguridad, no de gestión activa
    let barsOpen = 0;
    // Slippage (castigo de ejecución real, config.slippagePct en %, default 0 = comportamiento de siempre):
    // la entrada a mercado en una ruptura se llena PEOR; el SL a mercado se llena PEOR; el TP se asume
    // exacto (orden límite); el cierre por tiempo a mercado también se llena peor.
    const slip = (config.slippagePct || 0) / 100;
    const dir = signal === 'COMPRAR' ? 1 : -1;
    const entryFill = entry * (1 + dir * slip);
    for (let j = i + 1; j < candlesMain.length && barsOpen < MAX_BARS; j++) {
      const c = candlesMain[j];
      barsOpen++;
      if (signal === 'COMPRAR') {
        if (c.low <= sl) { exitPrice = sl * (1 - slip); reason = 'SL estructural'; break; }
        if (c.high >= tp) { exitPrice = tp; reason = 'TP estructural'; break; }
      } else {
        if (c.high >= sl) { exitPrice = sl * (1 + slip); reason = 'SL estructural'; break; }
        if (c.low <= tp) { exitPrice = tp; reason = 'TP estructural'; break; }
      }
      if (barsOpen === MAX_BARS - 1) { exitPrice = c.close * (1 - dir * slip); reason = 'Cierre por tiempo (2 días, sin resolver)'; }
    }
    if (exitPrice === null) continue;

    const pricePct = signal === 'COMPRAR' ? (exitPrice - entryFill) / entryFill : (entryFill - exitPrice) / entryFill;
    const grossPnl = size * pricePct;
    const commission = size * COMMISSION_PCT * 2;
    const pnl = grossPnl - commission;
    capital += pnl;
    trades.push({ signal, entry, exitPrice, pnl, grossPnl, commission, reason, rr: a.rr, regime4h: a.regime4h, size: Math.round(size * 100) / 100, riskUsd, rMultiple: riskUsd ? Math.round(pnl / riskUsd * 100) / 100 : null });
    closedByReason[reason] = (closedByReason[reason] || 0) + 1;
    pnlByReason[reason] = Math.round(((pnlByReason[reason] || 0) + pnl) * 100) / 100; // P&L separado por tipo de cierre (SL / TP / tiempo)
    if (capital > peakCapital) peakCapital = capital;
    const dd = (peakCapital - capital) / peakCapital;
    if (dd > maxDrawdown) maxDrawdown = dd;
  }

  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const totalPnl = capital - initialCapital;
  const rTrades = trades.filter(t => t.rMultiple !== null);
  const expectancyR = rTrades.length ? (rTrades.reduce((s, t) => s + t.rMultiple, 0) / rTrades.length).toFixed(3) : null;
  return {
    sizing: { mode: config.sizingMode === 'risk' ? 'risk' : 'fixed', riskPerTradePct: config.sizingMode === 'risk' ? (config.riskPerTradePct ?? 0.5) : null, fixedPct: config.sizingMode === 'risk' ? null : riskPct, expectancyR, signalsSkippedBySizing, slippagePct: config.slippagePct || 0 },
    pnlByReason,
    trades: trades.length, wins, losses,
    winRate: trades.length > 0 ? (wins / trades.length * 100).toFixed(1) : "0",
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalGrossPnl: trades.reduce((s, t) => s + t.grossPnl, 0).toFixed(2),
    totalCommission: trades.reduce((s, t) => s + t.commission, 0).toFixed(2),
    totalReturn: ((totalPnl / initialCapital) * 100).toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2),
    closedByReason,
    edgeFilterCheck: { signalsSeenBeforeFilter, signalsSkippedByEdgeFilter, signalsSkippedByRegime, signalsSkippedByAdx, minAdx4h, filterActive: config.applyMinEdgeFilter !== false },
    candlesUsed: { main: candlesMain.length, h4: candles4h.length }
  };
}

async function runTendenciaSubfilterBacktest(pair, tf, days, config) {
  const { minConfidence, riskPct, initialCapital } = config;
  const COMMISSION_PCT = 0.001;
  const tfMs = { '15m': 15*60000, '30m': 30*60000, '1h': 3600000, '4h': 4*3600000 }[tf];
  const shortTf = '15m';

  const [candlesMain, candlesShort] = await Promise.all([
    fetchHistoricalCandlesWithVolume(pair, tf, days, 2000),
    fetchHistoricalCandlesWithVolume(pair, shortTf, days, Math.min(Math.ceil(days * 96) + 100, 20000))
  ]);

  let pShort = 0;
  function windowUpTo(arr, ptrStart, targetTime, maxLen) {
    let i = ptrStart;
    while (i < arr.length - 1 && arr[i + 1].time <= targetTime) i++;
    const start = Math.max(0, i - maxLen + 1);
    return { slice: arr.slice(start, i + 1), newPtr: i };
  }

  let capital = initialCapital, trades = [], openTrade = null, peakCapital = initialCapital, maxDrawdown = 0;
  const MIN_HISTORY = 70;

  for (let i = MIN_HISTORY; i < candlesMain.length; i++) {
    const current = candlesMain[i];
    const window = candlesMain.slice(Math.max(0, i - MIN_HISTORY), i + 1);
    const closes = window.map(c => c.close), highs = window.map(c => c.high), lows = window.map(c => c.low);

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
        const pricePct = openTrade.signal === 'COMPRAR' ? (exitPrice - openTrade.entry) / openTrade.entry : (openTrade.entry - exitPrice) / openTrade.entry;
        const grossPnl = openTrade.size * pricePct;
        const commission = openTrade.size * COMMISSION_PCT * 2;
        const pnl = grossPnl - commission;
        capital += pnl;
        trades.push({ ...openTrade, exitPrice, pnl, grossPnl, commission, reason, closeTime: current.time });
        openTrade = null;
        if (capital > peakCapital) peakCapital = capital;
        const dd = (peakCapital - capital) / peakCapital;
        if (dd > maxDrawdown) maxDrawdown = dd;
      }
      continue;
    }

    const wShort = windowUpTo(candlesShort, pShort, current.time, 20);
    pShort = wShort.newPtr;
    if (wShort.slice.length < 12) continue;

    const a = analyzeTrendFollowAdxSubfilter(closes, highs, lows, wShort.slice.map(c => c.close));
    if (a && a.signal !== 'NEUTRO' && a.confidence >= minConfidence) {
      const size = capital * riskPct;
      openTrade = { signal: a.signal, entry: a.entry, tp: a.tp, sl: a.sl, size, openTime: current.time, confidence: a.confidence };
    }
  }

  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const totalPnl = capital - initialCapital;
  return {
    trades: trades.length, wins, losses,
    winRate: trades.length > 0 ? (wins / trades.length * 100).toFixed(1) : "0",
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalGrossPnl: trades.reduce((s, t) => s + t.grossPnl, 0).toFixed(2),
    totalCommission: trades.reduce((s, t) => s + t.commission, 0).toFixed(2),
    totalReturn: ((totalPnl / initialCapital) * 100).toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2),
    candlesUsed: { main: candlesMain.length, short: candlesShort.length }
  };
}

// Backtest "realista" de Scalping (26/8/2026) — a diferencia del motor
// original (que solo mira TP/SL con paciencia infinita), este simula la
// "Salida por reversión" que en vivo es el motivo de cierre MÁS FRECUENTE
// de Scalping-Tendencia (cruce EMA9/21 en contra en 5m) — y opcionalmente
// un breakeven temprano, para probar si el mismo arreglo que funcionó en
// Tendencia también corrige la asimetría pérdida-grande/ganancia-chica en Scalping.
async function runScalpingRealisticBacktest(pair, days, config) {
  const { minConfidence, riskPct, initialCapital, earlyBreakeven, breakevenTriggerAtr } = config;
  const COMMISSION_PCT = 0.001;

  const [candles5, candles1, candles15] = await Promise.all([
    fetchHistoricalCandlesWithVolume(pair, '5m', days, 6000),
    fetchHistoricalCandlesWithVolume(pair, '1m', days + 0.1, 12000),
    fetchHistoricalCandlesWithVolume(pair, '15m', days + 0.5, 2000)
  ]);

  let p1 = 0, p15 = 0;
  function windowUpTo(arr, ptrStart, targetTime, maxLen) {
    let i = ptrStart;
    while (i < arr.length - 1 && arr[i + 1].time <= targetTime) i++;
    const start = Math.max(0, i - maxLen + 1);
    return { slice: arr.slice(start, i + 1), newPtr: i };
  }

  let capital = initialCapital, trades = [], openTrade = null, peakCapital = initialCapital, maxDrawdown = 0;
  let closedByReason = {};
  const MIN_5M_HISTORY = 60;
  const BREAKEVEN_TRIGGER_ATR = breakevenTriggerAtr || 0.8;

  for (let i = MIN_5M_HISTORY; i < candles5.length; i++) {
    const current = candles5[i];
    const window5 = candles5.slice(Math.max(0, i - MIN_5M_HISTORY), i + 1);
    const closes5 = window5.map(c => c.close);

    if (openTrade) {
      let closed = false, exitPrice = null, reason = null;
      if (openTrade.signal === 'COMPRAR') {
        if (current.high >= openTrade.tp) { exitPrice = openTrade.tp; reason = 'TP'; closed = true; }
        else if (current.low <= openTrade.sl) { exitPrice = openTrade.sl; reason = openTrade.trailingActive ? 'SL (breakeven)' : 'SL'; closed = true; }
      } else {
        if (current.low <= openTrade.tp) { exitPrice = openTrade.tp; reason = 'TP'; closed = true; }
        else if (current.high >= openTrade.sl) { exitPrice = openTrade.sl; reason = openTrade.trailingActive ? 'SL (breakeven)' : 'SL'; closed = true; }
      }
      // Breakeven temprano — solo si todavía no se activó, y solo se aplica a
      // Scalping-Tendencia (la rama que usa reversión, igual que en vivo).
      if (!closed && earlyBreakeven && openTrade.subStrategy === 'Scalping-Tendencia' && !openTrade.trailingActive) {
        const favorableMove = openTrade.signal === 'COMPRAR' ? current.high - openTrade.entry : openTrade.entry - current.high;
        // (arriba: para VENDER, lo favorable es que el precio BAJE, así que comparamos contra current.low)
        const fav = openTrade.signal === 'COMPRAR' ? (current.high - openTrade.entry) : (openTrade.entry - current.low);
        if (fav >= openTrade.atr * BREAKEVEN_TRIGGER_ATR) {
          const candidateSl = openTrade.entry;
          if (openTrade.signal === 'COMPRAR' && candidateSl > openTrade.sl) { openTrade.sl = candidateSl; openTrade.trailingActive = true; }
          else if (openTrade.signal === 'VENDER' && candidateSl < openTrade.sl) { openTrade.sl = candidateSl; openTrade.trailingActive = true; }
        }
      }
      // Salida por reversión — SOLO Scalping-Tendencia, igual que en vivo
      if (!closed && openTrade.subStrategy === 'Scalping-Tendencia' && closes5.length >= 22) {
        const ema9 = calcEMA(closes5, 9), ema21 = calcEMA(closes5, 21);
        const reversedAgainstLong = openTrade.signal === 'COMPRAR' && ema9 < ema21;
        const reversedAgainstShort = openTrade.signal === 'VENDER' && ema9 > ema21;
        if (reversedAgainstLong || reversedAgainstShort) {
          exitPrice = current.close; reason = 'Salida por reversión'; closed = true;
        }
      }
      if (closed) {
        const pricePct = openTrade.signal === 'COMPRAR' ? (exitPrice - openTrade.entry) / openTrade.entry : (openTrade.entry - exitPrice) / openTrade.entry;
        const grossPnl = openTrade.size * pricePct;
        const commission = openTrade.size * COMMISSION_PCT * 2;
        const pnl = grossPnl - commission;
        capital += pnl;
        trades.push({ ...openTrade, exitPrice, pnl, grossPnl, commission, reason, closeTime: current.time });
        closedByReason[reason] = (closedByReason[reason] || 0) + 1;
        openTrade = null;
        if (capital > peakCapital) peakCapital = capital;
        const dd = (peakCapital - capital) / peakCapital;
        if (dd > maxDrawdown) maxDrawdown = dd;
      }
      continue;
    }

    const w1 = windowUpTo(candles1, p1, current.time, 30);
    p1 = w1.newPtr;
    const w15 = windowUpTo(candles15, p15, current.time, 60);
    p15 = w15.newPtr;
    if (w1.slice.length < 20 || w15.slice.length < 20) continue;

    const a = analyzeScalping(
      closes5, window5.map(c => c.high), window5.map(c => c.low),
      w1.slice.map(c => c.open), w1.slice.map(c => c.high), w1.slice.map(c => c.low), w1.slice.map(c => c.close), w1.slice.map(c => c.volume),
      w15.slice.map(c => c.high), w15.slice.map(c => c.low), w15.slice.map(c => c.close)
    );
    if (a && a.signal !== 'NEUTRO' && a.confidence >= minConfidence) {
      const subStrategy = `Scalping-${a.regime.startsWith('Tendencia') ? 'Tendencia' : 'Lateral'}`;
      // Filtro opcional (1/9/2026) para aislar una sola rama al backtestear —
      // necesario para poder probar distintos umbrales de confianza SOLO
      // para Lateral sin que las operaciones de Tendencia se mezclen en el
      // resultado (antes de esto, minConfidence aplicaba a las dos por igual).
      if (config.onlySubStrategy && subStrategy !== config.onlySubStrategy) continue;
      // Mismo filtro de comisión mínima que ya corre en vivo para
      // Scalping-Tendencia (openTrade(), MIN_EDGE_STRATEGIES) — usa el TP1,
      // no el TP2, igual que la validación real.
      if (config.applyMinEdgeFilter !== false && subStrategy === 'Scalping-Tendencia') {
        const projectedMovePct = Math.abs(a.tp - a.entry) / a.entry;
        if (projectedMovePct < 0.006) continue;
      }
      const size = capital * riskPct;
      openTrade = { signal: a.signal, entry: a.entry, tp: a.tp, sl: a.sl, size, openTime: current.time, confidence: a.confidence, subStrategy, atr: a.atr, trailingActive: false };
    }
  }

  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const totalPnl = capital - initialCapital;
  // Desglose por sub-rama (1/9/2026) — necesario para poder juzgar Lateral y
  // Tendencia por separado en vez de un promedio que puede esconder que una
  // rama compensa a la otra.
  const bySubStrategy = {};
  for (const t of trades) {
    if (!bySubStrategy[t.subStrategy]) bySubStrategy[t.subStrategy] = { trades: 0, wins: 0, losses: 0, netPnl: 0 };
    const b = bySubStrategy[t.subStrategy];
    b.trades++; if (t.pnl > 0) b.wins++; else if (t.pnl < 0) b.losses++; b.netPnl += t.pnl;
  }
  for (const k in bySubStrategy) {
    bySubStrategy[k].winRate = (bySubStrategy[k].wins / bySubStrategy[k].trades * 100).toFixed(1);
    bySubStrategy[k].netPnl = bySubStrategy[k].netPnl.toFixed(2);
  }
  return {
    trades: trades.length, wins, losses,
    winRate: trades.length > 0 ? (wins / trades.length * 100).toFixed(1) : "0",
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalGrossPnl: trades.reduce((s, t) => s + t.grossPnl, 0).toFixed(2),
    totalCommission: trades.reduce((s, t) => s + t.commission, 0).toFixed(2),
    totalReturn: ((totalPnl / initialCapital) * 100).toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2),
    closedByReason,
    bySubStrategy,
    candlesUsed: { c5: candles5.length, c1: candles1.length, c15: candles15.length }
  };
}

async function runScalpingBacktest(pair, days, config) {
  const { minConfidence, riskPct, initialCapital } = config;
  const COMMISSION_PCT = 0.001; // 0.1% por lado, igual que el resto de los backtests

  const [candles5, candles1, candles15] = await Promise.all([
    fetchHistoricalCandlesWithVolume(pair, '5m', days, 6000),
    fetchHistoricalCandlesWithVolume(pair, '1m', days + 0.1, 12000), // +buffer para el lookback inicial
    fetchHistoricalCandlesWithVolume(pair, '15m', days + 0.5, 2000)
  ]);

  // Punteros para no recorrer las series 1m/15m desde cero en cada paso
  let p1 = 0, p15 = 0;
  function windowUpTo(arr, ptrStart, targetTime, maxLen) {
    let i = ptrStart;
    while (i < arr.length - 1 && arr[i + 1].time <= targetTime) i++;
    const start = Math.max(0, i - maxLen + 1);
    return { slice: arr.slice(start, i + 1), newPtr: i };
  }

  let capital = initialCapital;
  let trades = [];
  let openTrade = null;
  let peakCapital = initialCapital;
  let maxDrawdown = 0;
  const MIN_5M_HISTORY = 60;

  for (let i = MIN_5M_HISTORY; i < candles5.length; i++) {
    const current = candles5[i];
    const window5 = candles5.slice(Math.max(0, i - MIN_5M_HISTORY), i + 1);

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
        const grossPnl = openTrade.size * pricePct;
        const commission = openTrade.size * COMMISSION_PCT * 2;
        const pnl = grossPnl - commission;
        capital += pnl;
        trades.push({ ...openTrade, exitPrice, pnl, grossPnl, commission, reason, closeTime: current.time });
        openTrade = null;
        if (capital > peakCapital) peakCapital = capital;
        const dd = (peakCapital - capital) / peakCapital;
        if (dd > maxDrawdown) maxDrawdown = dd;
      }
      continue;
    }

    const w1 = windowUpTo(candles1, p1, current.time, 30);
    p1 = w1.newPtr;
    const w15 = windowUpTo(candles15, p15, current.time, 60);
    p15 = w15.newPtr;
    if (w1.slice.length < 20 || w15.slice.length < 20) continue; // sin suficiente historia todavía

    const a = analyzeScalping(
      window5.map(c => c.close), window5.map(c => c.high), window5.map(c => c.low),
      w1.slice.map(c => c.open), w1.slice.map(c => c.high), w1.slice.map(c => c.low), w1.slice.map(c => c.close), w1.slice.map(c => c.volume),
      w15.slice.map(c => c.high), w15.slice.map(c => c.low), w15.slice.map(c => c.close)
    );
    if (a && a.signal !== 'NEUTRO' && a.confidence >= minConfidence) {
      const size = capital * riskPct;
      openTrade = { signal: a.signal, entry: a.entry, tp: a.tp, sl: a.sl, size, openTime: current.time, confidence: a.confidence };
    }
  }

  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const winRate = trades.length > 0 ? (wins / trades.length * 100) : 0;
  const totalPnl = capital - initialCapital;
  const totalGrossPnl = trades.reduce((s, t) => s + t.grossPnl, 0);
  const totalCommission = trades.reduce((s, t) => s + t.commission, 0);

  return {
    trades: trades.length, wins, losses,
    winRate: winRate.toFixed(1),
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalGrossPnl: totalGrossPnl.toFixed(2),
    totalCommission: totalCommission.toFixed(2),
    totalReturn: ((totalPnl / initialCapital) * 100).toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2),
    candlesUsed: { c5: candles5.length, c1: candles1.length, c15: candles15.length }
  };
}

async function fetchHistoricalCandles(pair, tf, days) {
  const limit = 1000;
  const tfMs = { '5m': 5*60000, '15m': 15*60000, '30m': 30*60000, '1h': 3600000, '4h': 4*3600000, '1d': 86400000 }[tf];
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
  const analyzeFn = strategy === 'improved' ? analyzeImproved
    : strategy === 'rebote' ? analyzeRebote
    : strategy === 'tendencia' ? analyzeTrendFollow
    : strategy === 'tendencia-adx' ? analyzeTrendFollowAdx
    : strategy === 'tendencia-inverted-rr' ? analyzeTrendFollowInvertedRR
    : analyze;
  const minHistory = strategy === 'improved' ? 210 : strategy === 'rebote' ? 50 : (strategy === 'tendencia' || strategy === 'tendencia-adx' || strategy === 'tendencia-inverted-rr') ? 70 : 200;
  // Comisión real de Binance: 0.1% por lado (entrada + salida) — el motor
  // original NO la descontaba, dando resultados más optimistas de lo real.
  const COMMISSION_PCT = 0.001;
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
        const grossPnl = openTrade.size * pricePct;
        const commission = openTrade.size * COMMISSION_PCT * 2; // ida + vuelta
        const pnl = grossPnl - commission;
        capital += pnl;
        trades.push({ ...openTrade, exitPrice, pnl, grossPnl, commission, reason, closeTime: current.time });
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
  const totalGrossPnl = trades.reduce((s, t) => s + t.grossPnl, 0);
  const totalCommission = trades.reduce((s, t) => s + t.commission, 0);
  
  return {
    trades: trades.length, wins, losses,
    winRate: winRate.toFixed(1),
    finalCapital: capital.toFixed(2),
    totalPnl: totalPnl.toFixed(2),
    totalGrossPnl: totalGrossPnl.toFixed(2),
    totalCommission: totalCommission.toFixed(2),
    totalReturn: totalReturn.toFixed(2),
    maxDrawdown: (maxDrawdown * 100).toFixed(2)
  };
}

app.post("/backtest", async (req, res) => {
  const { pair = 'BTCUSDT', tf = '15m', days = 30, minConfidence = 70, riskPct = 0.20, initialCapital = 1000, strategy = 'original', timeLimitMultiplier = 1.0, tpVariant = 'default', disableTrailing = false, noTimeLimit = false, earlyBreakeven = false, breakevenTriggerAtr, trailDistanceAtr, requireStructure = false, peakGiveback = false, peakGivebackPct, peakGivebackMinAtr, shortTf, endDate, minAdx4h, onlySubStrategy, sizingMode, riskPerTradePct, slippagePct } = req.body;
  try {
    if (strategy === 'scalping-realistic') {
      const result = await runScalpingRealisticBacktest(pair, days, { minConfidence, riskPct, initialCapital, earlyBreakeven, breakevenTriggerAtr, onlySubStrategy });
      return res.json({
        success: true,
        config: { pair, days, minConfidence, riskPct, initialCapital, strategy, earlyBreakeven, breakevenTriggerAtr, onlySubStrategy: onlySubStrategy || 'ambas' },
        dataRange: { note: 'Simula la salida por reversión (real, la más frecuente en vivo) + breakeven opcional — ver candlesUsed en el resultado' },
        result
      });
    }
    if (strategy === 'scalping') {
      const result = await runScalpingBacktest(pair, days, { minConfidence, riskPct, initialCapital });
      return res.json({
        success: true,
        config: { pair, days, minConfidence, riskPct, initialCapital, strategy },
        dataRange: { note: 'multi-timeframe (5m/1m/15m) — ver candlesUsed en el resultado' },
        result
      });
    }
    if (strategy === 'estructura-realistic') {
      const result = await runStructuralEntryBacktest(pair, tf, days, { minConfidence, riskPct, initialCapital, endDate, minAdx4h, sizingMode, riskPerTradePct, slippagePct });
      return res.json({
        success: true,
        config: { pair, tf, days, minConfidence, riskPct, initialCapital, strategy, minAdx4h: minAdx4h ?? 20, endDate: endDate || 'ahora', sizingMode: sizingMode || 'fixed', riskPerTradePct: sizingMode === 'risk' ? (riskPerTradePct ?? 0.5) : null },
        dataRange: { note: 'Entrada nueva por estructura (régimen 4h + ruptura de swing en ' + tf + ', SL/TP estructurales fijos, sin trailing/breakeven) — ver candlesUsed en el resultado' },
        result
      });
    }
    if (strategy === 'tendencia-realistic') {
      const result = await runTendenciaRealisticBacktest(pair, tf, days, { minConfidence, riskPct, initialCapital, timeLimitMultiplier, tpVariant, disableTrailing, noTimeLimit, earlyBreakeven, breakevenTriggerAtr, trailDistanceAtr, requireStructure, peakGiveback, peakGivebackPct, peakGivebackMinAtr, shortTf, endDate });
      return res.json({
        success: true,
        config: { pair, tf, days, minConfidence, riskPct, initialCapital, strategy, timeLimitMultiplier, tpVariant, disableTrailing, noTimeLimit, earlyBreakeven, breakevenTriggerAtr, trailDistanceAtr, requireStructure, peakGiveback, peakGivebackPct, peakGivebackMinAtr, shortTf: shortTf || '15m', endDate: endDate || 'ahora' },
        dataRange: { note: 'Simula trailing stop + límite de tiempo tal cual corren en vivo (' + tf + '/15m) — ver candlesUsed en el resultado' },
        result
      });
    }
    if (strategy === 'tendencia-subfilter') {
      const result = await runTendenciaSubfilterBacktest(pair, tf, days, { minConfidence, riskPct, initialCapital });
      return res.json({
        success: true,
        config: { pair, tf, days, minConfidence, riskPct, initialCapital, strategy },
        dataRange: { note: 'multi-timeframe (' + tf + '/15m) — ver candlesUsed en el resultado' },
        result
      });
    }
    if (strategy === 'tendencia-counter') {
      const result = await runTendenciaWithCounterBacktest(pair, tf, days, { minConfidence, riskPct, initialCapital });
      return res.json({
        success: true,
        config: { pair, tf, days, minConfidence, riskPct, initialCapital, strategy },
        dataRange: { note: 'Tendencia principal (' + tf + ') + contra-trade en paralelo (15m) — ver candlesUsed en el resultado' },
        result
      });
    }
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
  sendTelegram(`🟢 Signal Bot Backend v2 iniciado\n⏰ ${formatArgTime(new Date())}\n💾 Persistencia: ${stateCollection ? 'MongoDB conectado ✅' : 'Solo memoria ⚠️'}\n🤖 Modo AUTO: ${state.autoMode ? 'Activo' : 'Inactivo'}\n🔧 Modo de operación: ${String(state.tradingMode).toUpperCase()} · tamaño: ${SIZING_MODE === 'risk' ? `riesgo ${RISK_PER_TRADE_PCT}%` : 'fijo'}`);
  checkModeBlockAndWarn(); // Fase 3a: si arranca bloqueado por modo, avisa enseguida
  // Start the auto-check loop (runs every 60 seconds regardless of browser)
  setInterval(runAutoCheck, 60000);
});

