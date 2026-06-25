# Stable Release: SMA115 Combinado Funding/Rango/Cruces/Slope

Tag: `stable-sma115-combined-cross-slope-20260625`

## Estado del release

- Estrategia activa: `sma115_stable` con filtros combinados.
- Produccion VM: `/home/ubuntu/bot6rangos`.
- Watcher: `bot6rangos-watcher.service`.
- Trading real: desactivado por `WATCHER_TRADING_DRY_RUN=true`.

## Logica estable

- SMA `115`, timeframe `30m`.
- TP `8%`.
- SL defensivo al crear pending contrario: `2%`.
- SL general de toda operacion abierta: `5%`.
- Filtros base:
  - cierre contra promedio de las `10` velas previas;
  - distancia maxima a SMA: `abs(close / SMA - 1) <= 1%`.
- Filtros de regimen:
  - `funding_abs <= 0.00005`;
  - `range96_pct >= 3.0`;
  - `cross_count_96 <= 10`;
  - `abs(sma_slope_96_pct) > 0.15`.

## Configuracion clave VM

- `BOT6_STRATEGY_MODE=sma115_stable`
- `SMA_STABLE_COMBINED_FILTER_ENABLED=true`
- `SMA_STABLE_FUNDING_ABS_MAX=0.00005`
- `SMA_STABLE_RANGE_WINDOW=96`
- `SMA_STABLE_RANGE_MIN_PCT=3.0`
- `SMA_STABLE_CROSS_DENSITY_WINDOW=96`
- `SMA_STABLE_CROSS_DENSITY_MAX=10`
- `SMA_STABLE_SLOPE_WINDOW=96`
- `SMA_STABLE_SLOPE_MIN_ABS_PCT=0.15`
- `WATCHER_TRADING_DRY_RUN=true`

## Backtest de referencia

Periodo: `2025-01-01` a `2026-06-23`, `ETHUSDT 30m`.

- Trades: `398`
- PnL total: `301.204%`
- Peor racha negativa: `-9.353%`

Referencia local:

`/home/diego/backtest historico bb ranged alt tp/data/sma115_COMBINED_vs_cross96max10_ETHUSDT_202501_20260623_30m_20260624/filter_ablation_fixed_params/summary_filter_ablation.csv`
