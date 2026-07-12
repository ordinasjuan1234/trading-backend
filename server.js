const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;

// ── HMAC ──────────────────────────────────────────────────
function hmac(secret, message) {
  return crypto.createHmac("sha256", secret).update(message).digest("hex");
}

// ── Health check ──────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({ status: "Signal Bot Backend OK", time: new Date().toISOString() });
});

// ── Get balance ───────────────────────────────────────────
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
    const btc = data.balances?.find(b => b.asset === "BTC");
    res.json({ 
      usdt: usdt ? parseFloat(usdt.free) : 0,
      btc: btc ? parseFloat(btc.free) : 0,
      balances: data.balances?.filter(b => parseFloat(b.free) > 0)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Place order ───────────────────────────────────────────
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
    res.json({ 
      success: true, 
      orderId: data.orderId, 
      executedQty: data.executedQty, 
      price: data.fills?.[0]?.price,
      status: data.status
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Cancel order ──────────────────────────────────────────
app.post("/cancel", async (req, res) => {
  const { apiKey, apiSecret, symbol, orderId } = req.body;
  try {
    const timestamp = Date.now();
    const params = `symbol=${symbol}&orderId=${orderId}&timestamp=${timestamp}`;
    const signature = hmac(apiSecret, params);
    const response = await fetch(`https://api.binance.com/api/v3/order?${params}&signature=${signature}`, {
      method: "DELETE",
      headers: { "X-MBX-APIKEY": apiKey }
    });
    const data = await response.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Open orders ───────────────────────────────────────────
app.post("/orders", async (req, res) => {
  const { apiKey, apiSecret, symbol } = req.body;
  try {
    const timestamp = Date.now();
    const query = `symbol=${symbol}&timestamp=${timestamp}`;
    const signature = hmac(apiSecret, query);
    const response = await fetch(`https://api.binance.com/api/v3/openOrders?${query}&signature=${signature}`, {
      headers: { "X-MBX-APIKEY": apiKey }
    });
    const data = await response.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── TradingView webhook ───────────────────────────────────
app.post("/webhook", async (req, res) => {
  const { secret, action, symbol, quantity, apiKey, apiSecret } = req.body;
  if (secret !== process.env.WEBHOOK_SECRET) {
    return res.status(401).json({ error: "Webhook secret inválido" });
  }
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
    res.json({ success: true, order: data });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`Backend corriendo en puerto ${PORT}`));
