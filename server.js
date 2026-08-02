/*
 =====================================================================
 FILTRO DE EDGE MÍNIMO SOBRE COMISIÓN — para pegar en server.js
 =====================================================================

 DÓNDE PEGARLO:
 Abrí server.js, buscá la función openTrade(pair, tf, analysis).
 Es la que empieza así:

     async function openTrade(pair, tf, analysis) {
       const pct = state.positionSizePct || 20;
       ...

 Pegá el bloque de abajo INMEDIATAMENTE DESPUÉS de la línea:

     async function openTrade(pair, tf, analysis) {

 y ANTES de la línea:

     const pct = state.positionSizePct || 20;

 No toques nada más de la función — el resto queda exactamente igual.
 =====================================================================
*/

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
      return; // no abre operación, no gasta llamada a getRealBalance ni nada más
    }
  }

/*
 =====================================================================
 CÓMO QUEDA LA FUNCIÓN COMPLETA DESPUÉS DEL CAMBIO (referencia):
 =====================================================================

 async function openTrade(pair, tf, analysis) {
   // --- Filtro de edge mínimo sobre comisión real ---
   const MIN_EDGE_STRATEGIES = ['Tendencia'];
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
   // ... el resto de la función sigue exactamente igual que antes,
   // sin ningún otro cambio.
 }

 =====================================================================
 ANTES DE SUBIR A GITHUB — 2 chequeos:
 =====================================================================
 1. Confirmá en tu código cuál es el string exacto que trae
    analysis.strategy para Tendencia (yo asumí 'Tendencia' por el
    fallback que se ve en tu openTrade original — verificalo vos).
 2. Después de desplegar, mirá los logs de Render un rato para
    confirmar que aparece el console.log de descarte cuando
    corresponde — así sabés que el filtro corre de verdad.
 =====================================================================
*/
