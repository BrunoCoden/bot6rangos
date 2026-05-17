# order_fill_listener.py
"""
Listener dedicado que monitorea órdenes pendientes y registra
el minuto exacto en que se ejecutan según velas de 1 minuto.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from binance.um_futures import UMFutures
from dotenv import load_dotenv
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    from .config import OUTPUT_PRESETS, resolve_profile
    from .realtime_backtest import (
        _compute_risk_levels,
        _ensure_timestamp,
        _load_state,
        _save_state,
        _state_path,
    )
except ImportError:  # ejecución directa fuera del paquete
    CURRENT_DIR = Path(__file__).resolve().parent
    PARENT_DIR = CURRENT_DIR.parent
    if str(PARENT_DIR) not in sys.path:
        sys.path.append(str(PARENT_DIR))
    from config import OUTPUT_PRESETS, resolve_profile
    from realtime_backtest import (  # type: ignore
        _compute_risk_levels,
        _ensure_timestamp,
        _load_state,
        _save_state,
        _state_path,
    )


def _um_client() -> UMFutures:
    base_url = os.getenv("BINANCE_UM_BASE_URL", "https://fapi.binance.com")
    return UMFutures(base_url=base_url)


def _symbol_from_env() -> str:
    symbol = os.getenv("SYMBOL", "ETHUSDT.P")
    return symbol.replace(".P", "")


def _detect_fill(
    client: UMFutures,
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    order_time: pd.Timestamp,
    tolerance: float,
    lookback_minutes: int,
) -> Optional[pd.Timestamp]:
    """
    Devuelve el cierre de la primera vela de 1 minuto que toca el precio objetivo.
    Si aún no se ejecutó, retorna None.
    """
    if direction not in {"long", "short"} or entry_price <= 0:
        return None

    order_time_utc = order_time.tz_convert("UTC")
    start_ts = order_time_utc - pd.Timedelta(minutes=lookback_minutes)
    start_ms = int(max(0, start_ts.timestamp() * 1000))
    now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)

    try:
        klines = client.klines(
            symbol=symbol,
            interval="1m",
            startTime=start_ms,
            endTime=now_ms,
            limit=1000,
        )
    except Exception as exc:
        print(f"[LISTENER][WARN] No se pudieron descargar velas 1m ({exc})")
        return None

    if not klines:
        return None

    tolerance_abs = abs(tolerance)
    target_up = entry_price + tolerance_abs
    target_down = entry_price - tolerance_abs

    for candle in klines:
        open_ms = int(candle[0])
        close_ms = int(candle[6])
        open_ts = pd.Timestamp(open_ms, unit="ms", tz="UTC")
        close_ts = pd.Timestamp(close_ms, unit="ms", tz="UTC")

        if close_ts <= order_time_utc:
            continue

        high = float(candle[2])
        low = float(candle[3])

        if direction == "long":
            if low <= target_up:
                return close_ts
        else:
            if high >= target_down:
                return close_ts

    return None


def _transition_to_open(
    *,
    trades_path: Path,
    state: dict,
    fill_timestamp: pd.Timestamp,
) -> None:
    direction = state.get("direction")
    entry_price = float(state["entry_price"])
    entry_reason = state.get("entry_reason", "signal")
    entry_meta = state.get("entry_meta") or {}
    last_signal = state.get("last_signal_direction", direction)

    stop_price, take_price = _compute_risk_levels(direction, entry_price)
    new_state = {
        "status": "open",
        "direction": direction,
        "entry_price": entry_price,
        "entry_time": fill_timestamp.isoformat(),
        "entry_reason": entry_reason,
        "entry_meta": {
            **entry_meta,
            "order_time": state.get("order_time"),
        },
        "last_signal_direction": last_signal,
    }
    if stop_price is not None:
        new_state["stop_price"] = float(stop_price)
    if take_price is not None:
        new_state["take_price"] = float(take_price)

    _save_state(new_state, trades_path)
    print(f"[LISTENER] Orden '{direction}' ejecutada en {fill_timestamp.isoformat()}")


def run_listener(
    *,
    profile: str,
    poll_seconds: float,
    tolerance: float,
    lookback_minutes: int,
) -> None:
    trades_path = OUTPUT_PRESETS[profile]["trades"]
    state_path = _state_path(trades_path)
    print(f"[LISTENER] Usando perfil {profile} | State: {state_path}")

    client = _um_client()
    symbol = _symbol_from_env()
    tz_name = os.getenv("TZ", "UTC")
    try:
        local_tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        local_tz = ZoneInfo("UTC")

    while True:
        state = _load_state(trades_path)
        if not state:
            time.sleep(poll_seconds)
            continue

        status = state.get("status")
        if status != "pending":
            time.sleep(poll_seconds)
            continue

        try:
            direction = state["direction"]
            entry_price = float(state["entry_price"])
            order_time_raw = state.get("order_time") or state.get("entry_time")
        except (KeyError, ValueError, TypeError):
            time.sleep(poll_seconds)
            continue

        if not order_time_raw:
            time.sleep(poll_seconds)
            continue

        order_time = _ensure_timestamp(order_time_raw)
        fill_ts = _detect_fill(
            client,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            order_time=order_time,
            tolerance=tolerance,
            lookback_minutes=lookback_minutes,
        )

        if fill_ts is None:
            time.sleep(poll_seconds)
            continue

        try:
            fill_ts_local = fill_ts.tz_convert(local_tz)
        except Exception:
            fill_ts_local = fill_ts

        _transition_to_open(
            trades_path=trades_path,
            state=state,
            fill_timestamp=fill_ts_local,
        )
        time.sleep(poll_seconds)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Listener dedicado para captar el minuto exacto de ejecución de órdenes pendientes."
    )
    parser.add_argument(
        "--profile",
        choices=sorted(OUTPUT_PRESETS.keys()),
        default=None,
        help="Perfil de salidas a monitorear (tr/historico).",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=float(os.getenv("ORDER_LISTENER_POLL_SECONDS", "15")),
        help="Segundos de espera entre chequeos.",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=float(os.getenv("ORDER_LISTENER_PRICE_TOL", "0.0")),
        help="Tolerancia absoluta en el match de precio.",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=int(os.getenv("ORDER_LISTENER_LOOKBACK_MINUTES", "120")),
        help="Minutos hacia atrás que se consultan al buscar la vela que llenó la orden.",
    )

    args = parser.parse_args()
    profile = resolve_profile(args.profile)

    run_listener(
        profile=profile,
        poll_seconds=max(1.0, args.poll_seconds),
        tolerance=args.tolerance,
        lookback_minutes=max(1, args.lookback_minutes),
    )


if __name__ == "__main__":
    main()
