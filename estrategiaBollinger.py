# estrategiaBollinger.py
"""
Estrategia basada en señales de Bandas de Bollinger:
  - Consume las señales de `alerts.generate_alerts()`.
  - Cruce al alza de la banda inferior => abre LONG (si no hay posición en esa dirección).
  - Cruce a la baja de la banda superior => abre SHORT.
  - Cada cambio de tendencia cierra la posición vigente antes de abrir la nueva.
  - Solo imprime eventos / recomendaciones (sin ejecución real).
"""

import os
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from alerts import generate_alerts, format_alert_message
from trade_logger import log_trade, send_trade_notification, format_timestamp


POLL_SECONDS = float(os.getenv("STRAT_POLL_SECONDS", os.getenv("ALERT_POLL_SECONDS", "5")))
FEE_RATE = float(os.getenv("STRAT_FEE_RATE", "0.0005"))
SYMBOL_DISPLAY = os.getenv("SYMBOL", "ETHUSDT.P")
STREAM_INTERVAL = os.getenv("STREAM_INTERVAL", "30m").strip()


@dataclass
class Position:
    direction: str   # "long" o "short"
    entry_price: float
    opened_at: pd.Timestamp
    entry_reason: str


class StrategyState:
    def __init__(self):
        self.current_position: Optional[Position] = None

    def close_current(self, exit_price: float, exit_time: pd.Timestamp, exit_reason: str):
        if self.current_position is None:
            return
        pos = self.current_position
        fees = (pos.entry_price + exit_price) * FEE_RATE
        log_trade(
            direction=pos.direction,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            entry_time=pos.opened_at,
            exit_time=exit_time,
            entry_reason=pos.entry_reason,
            exit_reason=exit_reason,
            fees=fees,
            notify=True,
        )
        self.current_position = None

    def open_position(self, direction: str, price: float, ts: pd.Timestamp, reason: str):
        if price <= 0:
            print("[STRAT][WARN] Precio inválido para abrir posición")
            return
        self.current_position = Position(direction, price, ts, reason)
        print(f"[STRAT] Nueva {direction.upper()} @ {price:.2f} (motivo: {reason})")
        try:
            ts_fmt = format_timestamp(ts)
            send_trade_notification(
                f"{SYMBOL_DISPLAY} {STREAM_INTERVAL}\n"
                f"Apertura {direction.upper()}\n"
                f"Precio: {price:.2f}\n"
                f"Hora: {ts_fmt}\n"
                f"Motivo: {reason}"
            )
        except Exception as exc:
            print(f"[STRAT][WARN] Error notificando apertura: {exc}")


def _extract_price_from_alert(alert: dict) -> Optional[float]:
    msg = alert.get("message", "")
    for token in msg.split():
        try:
            val = float(token.replace(",", "."))
            if val > 0:
                return val
        except Exception:
            continue
    return None


def _direction_for_alert(alert: dict) -> Optional[str]:
    explicit = alert.get("direction")
    if isinstance(explicit, str) and explicit.lower() in ("long", "short"):
        return explicit.lower()

    typ = alert.get("type")
    msg = alert.get("message", "").lower()
    if typ == "bollinger_signal":
        if "alcista" in msg:
            return "long"
        if "bajista" in msg:
            return "short"
    return None


def run_strategy():
    state = StrategyState()
    print("[STRAT] Estrategia Bollinger iniciada")

    while True:
        try:
            alerts = generate_alerts()
        except Exception as exc:
            print(f"[STRAT][ERROR] {exc}")
            time.sleep(POLL_SECONDS)
            continue

        if alerts:
            for alert in alerts:
                msg = format_alert_message(alert)
                print(f"[STRAT][ALERTA] {msg}")

            for alert in alerts:
                direction = _direction_for_alert(alert)
                if direction is None:
                    continue

                price = alert.get("price")
                if price is not None:
                    try:
                        price = float(price)
                    except Exception:
                        price = None
                if price is None:
                    price = _extract_price_from_alert(alert)

                ts = alert.get("timestamp")
                if not isinstance(ts, pd.Timestamp):
                    try:
                        ts = pd.Timestamp(ts)
                    except Exception:
                        ts = pd.Timestamp.utcnow()

                if state.current_position is not None:
                    if state.current_position.direction == direction:
                        print(f"[STRAT] Señal {alert['type']} coincide con posición actual {direction.upper()}, se ignora.")
                        continue
                    exit_price = price if price is not None and price > 0 else state.current_position.entry_price
                    state.close_current(exit_price, ts, alert["type"])

                if price is None or price <= 0:
                    price = float(os.getenv("STRAT_FALLBACK_PRICE", "0"))
                if price <= 0:
                    print("[STRAT][WARN] No se pudo determinar precio de entrada; se omite la operación.")
                    continue

                state.open_position(direction, price, ts, alert["type"])

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run_strategy()
