process.on('uncaughtException', (e) => { console.error('UNCAUGHT:', e); });
process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); });

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const { MongoClient } = require("mongodb");

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
  autoTFs: ["4h"], // Validated by backtest: 4h gives best results vs 15m/1h
  minConfidence: 70,
  requireMTF: false, // Only one TF now (4h), so multi-TF confirmation not needed
  maxDailyGainPct: 5,
  maxDailyLossPct: 3,
  positionSizePct: 20, // % del capital por operación individual (probando 10/15/20)
  subSlThresholdMin: 5, // minutos de desacuerdo sostenido en 15m antes de cortar (probando 5/15/30)
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
  // Objetivos chicos a propósito — la idea es resolverse rápido, no esperar horas
  if (signal === 'COMPRAR') { tp = price + atr * 1.2; sl = price - atr * 0.8; }
  else if (signal === 'VENDER') { tp = price - atr * 1.2; sl = price + atr * 0.8; }
  else { tp = price + atr; sl = price - atr; }
  const rr = Math.abs(tp - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence, price, entry, tp, sl, rr, strategy: 'Rebote', atr };
}

// Estrategia SCALPING: pensada para operar como lo hace Juan a mano — varias
// veces por hora, 15-30 minutos por operación, no horas. Dispara con un cruce
// rápido de medias (9/21) en 5m, con RSI confirmando que no está en un extremo
// agotado. Usa DOS niveles de TP explícitos (no trailing): TP1 cierra la mitad
// rápido, TP2 se queda con el resto buscando un poco más antes de cerrar del todo.
function analyzeScalping(closes, highs, lows) {
  if (!closes || closes.length < 31) return null;
  const ema9 = calcEMA(closes, 9);
  const ema21 = calcEMA(closes, 21);
  const closesPrev = closes.slice(0, -1);
  const ema9Prev = calcEMA(closesPrev, 9);
  const ema21Prev = calcEMA(closesPrev, 21);
  // Confirmación de 2 velas: exigimos que HACE una vela YA estuviera cruzado
  // en la misma dirección, así no entramos justo en el pico más picado del
  // serrucho — si el cruce se sostiene, es más probable que sea un giro real.
  const closesPrev2 = closes.slice(0, -2);
  const ema9Prev2 = calcEMA(closesPrev2, 9);
  const ema21Prev2 = calcEMA(closesPrev2, 21);
  if (!ema9 || !ema21 || !ema9Prev || !ema21Prev || !ema9Prev2 || !ema21Prev2) return null;

  const rsiSeries = calcRSISeries(closes, 14);
  const rsi = rsiSeries[rsiSeries.length - 1];
  const price = closes[closes.length - 1];
  const atr = calcATR(highs, lows, closes) || price * 0.005;

  let signal = 'NEUTRO', direction = 'ESPERAR', confidence = 0;
  // Confirmación de 2 velas: el cruce tiene que haber pasado HACE una vela y
  // seguir sostenido ahora — no agarramos el cruce en el instante exacto en
  // que ocurre (eso era lo que nos hacía entrar justo en el pico del serrucho).
  const crossUp = ema9 > ema21 && ema9Prev > ema21Prev && ema9Prev2 <= ema21Prev2;
  const crossDown = ema9 < ema21 && ema9Prev < ema21Prev && ema9Prev2 >= ema21Prev2;

  if (crossUp && rsi < 70) {
    signal = 'COMPRAR'; direction = 'LARGO';
    confidence = Math.round(Math.min(85, 60 + (70 - rsi) / 2));
  } else if (crossDown && rsi > 30) {
    signal = 'VENDER'; direction = 'SHORT';
    confidence = Math.round(Math.min(85, 60 + (rsi - 30) / 2));
  }

  let entry = price, tp1, tp2, sl;
  // Objetivos pensados para 15-30 minutos: TP1 cerca, TP2 un poco más lejos.
  // SL ensanchado a 1x ATR (antes 0.6x) — el más ajustado cortaba por ruido
  // normal de 5m antes de darle tiempo real a la operación de desarrollarse.
  if (signal === 'COMPRAR') {
    tp1 = price + atr * 0.8; tp2 = price + atr * 1.6; sl = price - atr * 1.0;
  } else if (signal === 'VENDER') {
    tp1 = price - atr * 0.8; tp2 = price - atr * 1.6; sl = price + atr * 1.0;
  } else {
    tp1 = price + atr; tp2 = price + atr * 2; sl = price - atr;
  }
  const rr = Math.abs(tp2 - entry) / Math.abs(sl - entry);
  return { signal, direction, confidence, price, entry, tp: tp1, tp2, sl, rr, strategy: 'Scalping', atr };
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
  const size = capitalBase * (pct / 100);
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
    strategy: analysis.strategy || 'Reversión',
    tp2: analysis.tp2 || null, // solo Scalping usa un segundo nivel de TP
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
  sendTelegram(`${emoji} ${analysis.signal} AUTO (Servidor)\n📊 ${pair.replace('USDT','/USDT')} · ${tf.toUpperCase()}\n🧠 Estrategia: ${trade.strategy}${cloudInfo}${volInfo}${adxInfo}\n💵 Entrada: $${realEntry.toFixed(2)}\n🎯 TP: $${analysis.tp.toFixed(2)}\n🛑 SL: $${analysis.sl.toFixed(2)}\n📊 R/R: 1:${analysis.rr.toFixed(2)}\n🎯 Confianza: ${analysis.confidence}%\n💰 Tamaño: ${pct}% del capital`);
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
  const closed = { ...t, exitPrice, pnl, pnlPct, pnlBeforeFees, commission, closeTime: formatArgTime(new Date()), reason };
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
    await runAutoCheckInner();
  } catch (e) {
    console.log('Error en runAutoCheck:', e.message);
  } finally {
    isAutoCheckRunning = false;
  }
}

async function runAutoCheckInner() {
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
      const TRAIL_DISTANCE_ATR = 1.0; // el SL persigue el precio a 1x ATR de distancia del mejor precio alcanzado
      if (t.signal === 'COMPRAR') {
        if (recentHigh > t.peakPrice) t.peakPrice = recentHigh;
        const favorableMove = t.peakPrice - t.entry;
        if (favorableMove >= activationDistance) {
          const candidateSl = Math.max(t.entry, t.peakPrice - atr * TRAIL_DISTANCE_ATR);
          if (candidateSl > t.sl) {
            const wasActive = t.trailingActive;
            t.sl = candidateSl; t.trailingActive = true;
            await saveState(state);
            if (!wasActive) {
              sendTelegram(`🔒 Trailing stop activado\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nSL asegurado en $${candidateSl.toFixed(2)} (protege ganancia mínima)`);
              if (!t.partialTaken) await partialCloseTrade(t, currentPrice);
            }
          }
        }
      } else if (t.signal === 'VENDER') {
        if (recentLow < t.peakPrice) t.peakPrice = recentLow;
        const favorableMove = t.entry - t.peakPrice;
        if (favorableMove >= activationDistance) {
          const candidateSl = Math.min(t.entry, t.peakPrice + atr * TRAIL_DISTANCE_ATR);
          if (candidateSl < t.sl) {
            const wasActive = t.trailingActive;
            t.sl = candidateSl; t.trailingActive = true;
            await saveState(state);
            if (!wasActive) {
              sendTelegram(`🔒 Trailing stop activado\n${t.pair.replace('USDT','/USDT')} · ${t.tf}\nSL asegurado en $${candidateSl.toFixed(2)} (protege ganancia mínima)`);
              if (!t.partialTaken) await partialCloseTrade(t, currentPrice);
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
            t.trendDisagreeCount = 0;
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
      const MAX_HOURS_OPEN = (t.strategy === 'Scalping' || t.strategy === 'Rebote') ? 0.5 : (t.strategy === 'Rango' ? 2 : 48); // Scalping y Rebote apuntan a 15-30 min — necesitan margen real para que el precio se mueva con volumen
      const openTimestamp = t.openTimestamp || Date.now();
      const hoursOpen = (Date.now() - openTimestamp) / (1000 * 60 * 60);

      // TP/SL ahora se detectan por mecha (high/low), pero el cierre se registra
      // al precio EXACTO del TP/SL (así como llenaría una orden real), no al
      // precio de cierre de la vela, que puede ser distinto.
      if (t.strategy === 'Scalping' && t.tp2 && !t.partialTaken) {
        // Scalping usa 2 TPs explícitos: al tocar el primero, asegura la mitad
        // y mueve el objetivo del resto al segundo TP (más lejos).
        const hitTP1 = (t.signal === 'COMPRAR' && recentHigh >= t.tp) || (t.signal === 'VENDER' && recentLow <= t.tp);
        const hitSL = (t.signal === 'COMPRAR' && recentLow <= t.sl) || (t.signal === 'VENDER' && recentHigh >= t.sl);
        if (hitTP1) {
          await partialCloseTrade(t, t.tp);
          t.tp = t.tp2;
          t.tp2 = null;
          await saveState(state);
        } else if (hitSL) {
          await closeTradeById(t.id, t.sl, 'SL Auto');
        } else if (hoursOpen >= MAX_HOURS_OPEN) {
          await closeTradeById(t.id, currentPrice, `Cierre por tiempo (${MAX_HOURS_OPEN}hs)`);
        }
      } else if (t.signal === 'COMPRAR' && recentHigh >= t.tp) await closeTradeById(t.id, t.tp, 'TP Auto');
      else if (t.signal === 'COMPRAR' && recentLow <= t.sl) await closeTradeById(t.id, t.sl, 'SL Auto');
      else if (t.signal === 'VENDER' && recentLow <= t.tp) await closeTradeById(t.id, t.tp, 'TP Auto');
      else if (t.signal === 'VENDER' && recentHigh >= t.sl) await closeTradeById(t.id, t.sl, 'SL Auto');
      else if (hoursOpen >= MAX_HOURS_OPEN) {
        await closeTradeById(t.id, currentPrice, `Cierre por tiempo (${MAX_HOURS_OPEN}hs)`);
        sendTelegram(`⏰ OPERACIÓN CERRADA POR TIEMPO\n${t.pair.replace('USDT','/USDT')} llevaba más de ${MAX_HOURS_OPEN}hs abierta sin tocar TP/SL\nSe cerró al precio de mercado para liberar el capital.`);
      }
    } catch (e) { console.log('Check open trade error:', e.message); }
  }

  // Nota: si el bot está pausado (autoMode false), esta función ni siquiera
  // llega hasta acá — el seguimiento fantasma también queda en pausa, igual
  // que el monitoreo de operaciones abiertas (limitación ya conocida).
  await checkGhostTrades();
  if (!state.autoMode) return;

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
        // Rango PAUSADA desde el 30/7 — 13.3% de aciertos en 15 operaciones reales,
        // evidencia clara de que el diagnóstico "ADX bajo = mercado lateral" no
        // alcanza para detectar un rango operable de verdad. Queda el código
        // intacto para cuando se rediseñe (agregar confirmación de que el precio
        // realmente viene rebotando entre piso y techo, no solo ADX bajo).
        // const c = analyzeRango(closes, highs, lows);
        // if (c) signals.push({ tf, pair, signal: c.signal, confidence: c.confidence, analysis: c });
      } catch (e) { console.log(`Analyze error ${pair} ${tf}:`, e.message); }
    }
    // Rebote opera SIEMPRE en 15m, sin importar qué timeframes estén
    // configurados para las otras estrategias — es la idea de aprovechar el
    // vaivén de corto plazo, no depende de mirar 1h/4h.
    try {
      const { closes: closes15, highs: highs15, lows: lows15 } = await fetchKlines(pair, '15m', 60);
      const d = analyzeRebote(closes15, highs15, lows15);
      if (d) signals.push({ tf: '15m', pair, signal: d.signal, confidence: d.confidence, analysis: d });
    } catch (e) { console.log(`Analyze Rebote error ${pair}:`, e.message); }

    // Scalping opera SIEMPRE en 5m — pensada para operar varias veces por
    // hora, 15-30 minutos por operación, con 2 niveles de TP.
    try {
      const { closes: closes5, highs: highs5, lows: lows5 } = await fetchKlines(pair, '5m', 40);
      const e = analyzeScalping(closes5, highs5, lows5);
      if (e) signals.push({ tf: '5m', pair, signal: e.signal, confidence: e.confidence, analysis: e });
    } catch (e) { console.log(`Analyze Scalping error ${pair}:`, e.message); }
    // Rebote y Scalping usan su PROPIO umbral de confianza (más bajo a propósito)
    // en vez del global — son estrategias distintas, pensadas para operar seguido
    // con objetivos chicos, no tiene sentido exigirles la misma convicción que a Tendencia.
    const passesConfidence = (s) => {
      if (s.analysis.strategy === 'Rebote' || s.analysis.strategy === 'Scalping') return s.confidence >= 60;
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
  for (const best of allSignals) {
    await openTrade(best.pair, best.tf, best.analysis);
  }
}

// ── Routes ────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({ status: "Signal Bot Backend OK", time: new Date().toISOString(), autoMode: state.autoMode });
});

app.get("/stats/all-modes", async (req, res) => {
  try {
    const modes = ['demo', 'testnet', 'real'];
    const result = {};
    for (const mode of modes) {
      const trades = (mode === state.tradingMode)
        ? state.trades
        : (stateCollection ? (await loadFinancialDoc(mode)).trades : []);
      const total = trades.length;
      const wins = trades.filter(t => t.pnl >= 0).length;
      result[mode] = {
        total,
        wins,
        losses: total - wins,
        winRate: total > 0 ? Math.round((wins / total) * 1000) / 10 : 0,
        pnlTotal: Math.round(trades.reduce((s, t) => s + t.pnl, 0) * 100) / 100
      };
    }
    res.json({ success: true, stats: result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/state", (req, res) => {
  res.json(state);
});

app.post("/state/config", async (req, res) => {
  const { autoPairs, autoTFs, minConfidence, requireMTF, maxDailyGainPct, maxDailyLossPct, positionSizePct, subSlThresholdMin, tpAtrMultiplier, cooldownMinutes } = req.body;
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
  const now = formatArgTime(new Date());
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
  sendTelegram(`🟢 Signal Bot Backend v2 iniciado\n⏰ ${formatArgTime(new Date())}\n💾 Persistencia: ${stateCollection ? 'MongoDB conectado ✅' : 'Solo memoria ⚠️'}\n🤖 Modo AUTO: ${state.autoMode ? 'Activo' : 'Inactivo'}`);
  // Start the auto-check loop (runs every 60 seconds regardless of browser)
  setInterval(runAutoCheck, 60000);
});
