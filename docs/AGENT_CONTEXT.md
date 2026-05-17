# Agent Context: bot6rangos

`bot6rangos` is a Range3 + Bollinger trading bot scaffold based on the original
`/home/diego/bot` structure. It keeps the Binance and Bybit trading modules,
Telegram alerts, heartbeat monitor, trade ledgers, and Telegram commands, but all
users and exchanges are disabled by default.

## Strategy

The watcher uses closed `30m` ETHUSDT candles and emits `range3_signal` events:

- Range3 channels over 200 candles: upper 25%, middle 50%, lower 25%.
- Bollinger signal type: `Cruce de cierre`.
- SHORT signals are valid only in the upper range.
- LONG signals are valid only in the lower range.
- If the signal candle or the previous 3 candles made a new range max/min, the
  watcher stores a pending entry at `maxfloor` for SHORT or `minroof` for LONG.
- Pending entries are watcher-managed JSON state, not resting exchange orders.
- A pending entry fills with a market order when mark/candle state touches its
  level; it cancels if a new same-side extreme appears first.
- A consecutive same-direction signal without a recent extreme enters directly
  at close and clears the pending.

## Risk Flow

- Normal entries use initial SL 2%.
- Normal entries trail by 1% favorable price steps.
- At +3% favorable price, the SL moves to +0.5% from entry and trailing stops.
- SL-like exits from normal entries open one opposite flip.
- Flip entries use fixed SL 2% and TP 2%.
- Flip SL does not open another flip.
- A new valid strategy signal closes any open flip and resumes normal signal
  handling.

## Operational Defaults

- `.env.example` documents the bot6 defaults and order ID prefix `B6R`.
- `trading/accounts/oci_accounts.yaml` contains Binance/Bybit templates only,
  with every user and exchange disabled.
- Runtime state lives under `backtest/backtestTR/`.
- `RANGE_PENDING_STATE_PATH` stores the watcher-managed pending order.

## Main Files

- `alerts.py`: Range3 + Bollinger signal/state generation.
- `watcher_alertas.py`: trading orchestration, pending entry state, thresholds,
  trailing/profit-lock/flip handling, retries, and ledgers.
- `telegram_bot_commands.py`: Telegram operational commands.
- `trading/exchanges/binance.py` and `trading/exchanges/bybit.py`: exchange
  adapters.
