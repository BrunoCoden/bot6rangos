# Stable Release: SMA115 Combinado Funding/Rango

Tag: `stable-sma115-combined-funding-range-20260624`

## Estado del release

- Estrategia activa: `sma115_stable` con filtro combinado.
- Produccion VM: `/home/ubuntu/bot6rangos`.
- Watcher: `bot6rangos-watcher.service` activo.
- Trading real: desactivado por `WATCHER_TRADING_DRY_RUN=true`.

## Logica estable

- SMA `115`, timeframe `30m`.
- TP `8%`.
- SL defensivo al crear pending: `2%`.
- SL general de toda operacion abierta: `5%`.
- Filtros base:
  - cierre contra promedio de las `10` velas previas;
  - distancia maxima a SMA: `abs(close / SMA - 1) <= 1%`.
- Filtro combinado:
  - `funding_abs <= 0.00005`;
  - `range96_pct >= 3.0`.

## Configuracion clave VM

- `BOT6_STRATEGY_MODE=sma115_stable`
- `SMA_STABLE_COMBINED_FILTER_ENABLED=true`
- `SMA_STABLE_FUNDING_ABS_MAX=0.00005`
- `SMA_STABLE_RANGE_WINDOW=96`
- `SMA_STABLE_RANGE_MIN_PCT=3.0`
- `WATCHER_TRADING_DRY_RUN=true`

## Backtest de referencia

Periodo: `2025-01-01` a `2026-06-19`, `ETHUSDT 30m`.

- Trades: `458`
- PnL total: `272.926%`
- Peor racha negativa: `-9.321%`

Referencia local:

`/home/diego/backtest historico bb ranged alt tp/data/sma115_STABLE_combined_funding_range_sweep_ETHUSDT_202501_20260619_30m_20260623/funding_5e-05_range_range96_pct_3p0`
