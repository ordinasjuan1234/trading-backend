// risk.js — Módulo de riesgo del Signal Bot (Fase 1 del refactor por módulos)
//
// Regla profesional: se decide cuánto se ARRIESGA (un % fijo del capital),
// y de ahí sale cuánto se COMPRA. Si el SL está lejos, se compra menos;
// si está cerca, se compra más. Cada pérdida vale lo mismo en dólares.
//
// Es una función pura: no toca state, no toca Binance, no tiene efectos.
// Se puede probar sola en la consola con: node risk.js

const RISK_DEFAULTS = {
  riskPerTradePct: 0.5,   // % del capital que se pierde si salta el SL (incluida comisión). 0,5% ≈ el 30% fijo actual con SL a 1,5%; subir a 1% recién con track record real
  maxExposurePct: 50,     // techo: nunca comprar por más del 50% del capital (spot, sin apalancamiento)
  minNotionalUsd: 10,     // mínimo que acepta Binance Spot para una orden (BTCUSDT/ETHUSDT: 5 USDT, usamos 10 por margen)
  commissionRtPct: 0.20,  // comisión ida y vuelta, market, sin BNB (confirmada 0,20%)
};

/**
 * Calcula el tamaño de una operación a partir del riesgo.
 *
 * @param {object} p
 * @param {number} p.capital        capital disponible (demo: state.capital; real: saldo de Binance)
 * @param {number} p.entry          precio de entrada previsto
 * @param {number} p.sl             precio del stop loss
 * @param {object} [p.overrides]    cualquier campo de RISK_DEFAULTS para pisar
 * @returns {{ ok:boolean, size:number, qty:number, riskUsd:number, slDistPct:number, exposurePct:number, capped:boolean, reason?:string }}
 *   size        = notional en USD a comprar
 *   qty         = cantidad de moneda (size / entry), SIN redondear al lot size de Binance (eso lo hace roundQtyForBinance)
 *   riskUsd     = pérdida esperada en USD si salta el SL (ya con comisión)
 *   capped      = true si el techo maxExposurePct recortó el tamaño (entonces riskUsd < objetivo)
 */
function calcPositionSize(p) {
  const cfg = { ...RISK_DEFAULTS, ...(p.overrides || {}) };
  const { capital, entry, sl } = p;

  if (!(capital > 0) || !(entry > 0) || !(sl > 0)) {
    return fail('capital/entry/sl inválidos');
  }
  const slDistPct = Math.abs(entry - sl) / entry;      // ej. 0.015 = 1,5%
  if (slDistPct <= 0) return fail('SL igual a la entrada');

  // Pérdida real por USD invertido si salta el SL = distancia al SL + comisión ida y vuelta.
  // Sin sumar la comisión, un SL muy cerca (0,3%) haría comprar una posición enorme
  // cuya comisión sola (0,2%) ya come casi todo el riesgo previsto.
  const lossPerUsd = slDistPct + cfg.commissionRtPct / 100;

  const riskTarget = capital * (cfg.riskPerTradePct / 100);
  let size = riskTarget / lossPerUsd;

  const maxSize = capital * (cfg.maxExposurePct / 100);
  let capped = false;
  if (size > maxSize) { size = maxSize; capped = true; }

  if (size < cfg.minNotionalUsd) {
    return fail(`tamaño ${size.toFixed(2)} USD por debajo del mínimo de Binance (${cfg.minNotionalUsd})`);
  }

  return {
    ok: true,
    size: round2(size),
    qty: size / entry,
    riskUsd: round2(size * lossPerUsd),
    slDistPct: round4(slDistPct * 100),
    exposurePct: round2(size / capital * 100),
    capped,
  };

  function fail(reason) {
    return { ok: false, size: 0, qty: 0, riskUsd: 0, slDistPct: 0, exposurePct: 0, capped: false, reason };
  }
}

function round2(x) { return Math.round(x * 100) / 100; }
function round4(x) { return Math.round(x * 10000) / 10000; }

module.exports = { calcPositionSize, RISK_DEFAULTS };

// ─── Autoprueba: `node risk.js` ────────────────────────────────────────────
if (require.main === module) {
  const cases = [
    { name: 'SL a 1,5% (típico Estructura)', capital: 1000, entry: 80000, sl: 78800 },
    { name: 'SL a 0,5% (muy cerca → topa el techo)', capital: 1000, entry: 80000, sl: 79600 },
    { name: 'SL a 4% (lejos → compra poco)',   capital: 1000, entry: 80000, sl: 76800 },
    { name: 'capital chico (Binance rechaza)',  capital: 30,   entry: 80000, sl: 76800 },
  ];
  for (const c of cases) {
    const r = calcPositionSize(c);
    console.log(`\n${c.name}`);
    console.log(r.ok
      ? `  compra $${r.size} (${r.exposurePct}% del capital) · arriesga $${r.riskUsd} · SL a ${r.slDistPct}%${r.capped ? ' · TOPADO por techo de exposición' : ''}`
      : `  NO OPERA: ${r.reason}`);
  }
  console.log('\nComparación con el 30% fijo actual, capital $1000:');
  for (const slPct of [0.5, 1, 1.5, 2, 3, 4]) {
    const fixedRisk = 300 * (slPct / 100 + 0.002);
    const r = calcPositionSize({ capital: 1000, entry: 100, sl: 100 - slPct });
    console.log(`  SL ${slPct}%  →  fijo 30%: arriesga $${fixedRisk.toFixed(2)}   |   riesgo 0,5%: compra $${r.size}, arriesga $${r.riskUsd}`);
  }
}
