# Stable Release: 2026-01-08 (Gauchito Gil)

Tag: `stable-2026-01-08-gauchito-gil`

## Estado del release

- Watcher: en producción, reinicio validado.
- Trading: habilitado.
- Thresholds: reconstrucción al inicio habilitada.
- Binance/Bybit: posiciones detectadas y thresholds guardados.

## Configuración clave

- `WATCHER_THRESHOLDS_REBUILD_ON_STARTUP=true`
- `WATCHER_THRESHOLDS_CLEAR_ON_STARTUP=false`

## Notional por usuario/exchange

- `diego/binance`: `notional_usdt: 10000.0`
- `diego/bybit`: `notional_usdt: 5000.0`
- `bruno/binance`: `notional_usdt: 5.0`

## Thresholds (estado al reinicio)

- `diego/binance` (ETHUSDT): entry `3127.01`, loss `2970.6595`, gain `3408.4409`
- `diego/bybit` (ETHUSDT): entry `3126.51`, loss `2970.1845`, gain `3407.8959`

## Notas operativas

- Los thresholds se guardan en `backtest/backtestTR/pending_thresholds.json`.
- El watcher reconstruye thresholds desde posiciones abiertas al iniciar.
- En la VM se comentaron alias en `.env` que pisaban claves de Binance; revisar antes de reactivar.
