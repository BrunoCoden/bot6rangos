# Estrategias Activas (VM)

Fecha de referencia: 2026-02-25  
Fuente: configuración y código actuales en `ubuntu@167.126.0.127`.

## Repos activos actualmente

1. `/home/ubuntu/bot`
2. `/home/ubuntu/botDex`
3. `/home/ubuntu/bot4BBBtc`

---

## 1) Repo `/home/ubuntu/bot` (Bollinger ETH)

### Estrategia de señal
- Usa **Bollinger** (`ALERT_ENABLE_BOLLINGER_SIGNALS=true`).
- `SYMBOL=ETHUSDT.P`
- `STREAM_INTERVAL=30m`
- Señal por **rotura + rebote** con `_pending_break`.
- Tiene alternancia global de señal confirmada (`last_signal.json`):
  - Si se repite la misma dirección confirmada, se ignora.

### Ejecución de órdenes
- Si llega señal y ya hay posición en la **misma dirección**, no abre nueva; actualiza threshold con ese precio señal.
- Si hay posición en dirección opuesta, hace **close** y luego **open** en dirección de señal.
- Si no hay posición, abre en dirección de señal.

### SL/TP y thresholds
- En watcher, SL/TP efectivo viene de:
  - `LOSS_PCT = WATCHER_CONTRA_THRESHOLD_PCT` (default `0.02`)
  - `GAIN_PCT = 0.0`
- Como `WATCHER_CONTRA_THRESHOLD_PCT` no está definido en `.env`, el SL efectivo actual es **2%**.
- TP efectivo actual: **sin TP**.
- Evaluación de threshold en tiempo real con precio de exchange (mark/fallback de stream).
- Al tocar SL: cierra posición y hace **flip** (abre contraria) y registra nuevo threshold.

---

## 2) Repo `/home/ubuntu/botDex` (Supertrend ETH, DEX/CEX)

### Estrategia de señal
- Usa **Supertrend** (`ALERT_ENABLE_SUPERTREND_SIGNALS=true`, bollinger desactivado).
- `STREAM_INTERVAL=30m`
- Lógica de entrada de señal es **contraria** al cambio de supertrend (según `alerts.py`):
  - cambio a bajista -> entrada LONG
  - cambio a alcista -> entrada SHORT

### Ejecución de órdenes
- Si llega señal y ya hay posición en esa misma dirección objetivo, no abre nueva; resetea SL desde señal.
- Si hay opuesta, hace close+open.
- Si no hay posición, abre.

### SL/TP y thresholds
- `STRAT_STOP_LOSS_PCT=0.02` => SL efectivo **2%**.
- TP no activo en este repo (no hay `STRAT_TAKE_PROFIT_PCT` aplicado en watcher).
- Evaluación SL en tiempo real con **mark y last**:
  - dispara si toca cualquiera de los dos.
- Al tocar SL:
  - cierra posición
  - hace reversa (`stop_loss_reversal`)
  - registra nuevo threshold.

---

## 3) Repo `/home/ubuntu/bot4BBBtc` (Bollinger BTC)

### Estrategia de señal
- Usa **Bollinger** (`ALERT_ENABLE_BOLLINGER_SIGNALS=true`).
- `SYMBOL=BTCUSDT.P`
- Señal por rotura + rebote con `_pending_break`.
- Tiene alternancia global de señal confirmada (`last_signal.json`), igual que `bot`.

### Ejecución de órdenes
- Igual a `bot`:
  - misma dirección: no abre, actualiza threshold.
  - dirección opuesta: close+open.
  - sin posición: abre.

### SL/TP y thresholds
- En watcher actual, SL/TP efectivo también se toma por:
  - `WATCHER_CONTRA_THRESHOLD_PCT` (default 2%)
  - `GAIN_PCT=0.0`
- **Importante**: aunque en `.env` existe `STRAT_STOP_LOSS_PCT=0.015`, ese valor no gobierna este watcher actual.
- Resultado efectivo hoy: SL **2%**, TP **0%** (sin TP).
- Al tocar SL, cierra y hace flip, registrando nuevo threshold.

---

## Resumen rápido por repo

- `bot`: Bollinger ETH, SL 2%, sin TP, con flip por SL.
- `botDex`: Supertrend contraria ETH, SL 2%, sin TP, con reversa por SL.
- `bot4BBBtc`: Bollinger BTC, SL efectivo 2%, sin TP, con flip por SL.
