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
