async function openTrade(pair, tf, analysis) {
  // --- Filtro de edge mínimo sobre comisión real ---
  // Comisión ida y vuelta confirmada: 0.20% (Spot, market, sin BNB)
  // Margen de seguridad 3x -> movimiento proyectado debe ser >= 0.60%
  const MIN_EDGE_STRATEGIES = ['Tendencia']; // agregar 'Rebote', 'Scalping-Tendencia' de a una, después de juzgar el impacto de esta
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
      return;
    }
  }
  const trade = {
    id: Date.now() + '-' + pair, pair, signal: analysis.signal, direction: analysis.direction,
    entry: realEntry, tp: analysis.tp, sl: analysis.sl, qty, size, tf,
    strategy: analysis.strategy || 'Reversión',
    subStrategy: analysis.regime ? `Scalping-${analysis.regime.startsWith('Tendencia') ? 'Tendencia' : 'Lateral'}` : null,
    tp2: analysis.tp2 || null,
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
  sendTelegram(`${emoji} ${analysis.signal} AUTO (Servidor)\n📊 ${pair.replace('USDT','/USDT')} · ${tf.toUpperCase()}\n🧠 Estrategia: ${trade.strategy}${cloudInfo}${volInfo}${adxInfo}${regimeInfo}\n💵 Entrada: $${realEntry.toFixed(2)}\n🎯 TP: $${analysis.tp.toFixed(2)}\n🛑 SL: $${analysis.sl.toFixed(2)}\n📊 R/R: 1:${analysis.rr.toFixed(2)}\n🎯 Confianza: ${analysis.confidence}%\n💰 Tamaño: ${pct}% del capital`);
}
