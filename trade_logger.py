# trade_logger.py
import os
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from zoneinfo import ZoneInfo
from typing import Any
from datetime import datetime, timezone


DEFAULT_TRADES_PATH = os.getenv("STRAT_TRADES_CSV_PATH", "estrategia_trades.csv").strip()
SYMBOL_DISPLAY = os.getenv("SYMBOL", "ETHUSDT.P")
STREAM_INTERVAL = os.getenv("STREAM_INTERVAL", "30m").strip()
TZ_NAME = os.getenv("TZ", "UTC")
try:
    LOCAL_TZ = ZoneInfo(TZ_NAME)
except Exception:
    LOCAL_TZ = ZoneInfo("UTC")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
_chat_ids_raw = os.getenv("TELEGRAM_CHAT_IDS", "")
TELEGRAM_CHAT_IDS = [part.strip() for part in _chat_ids_raw.replace(";", ",").split(",") if part.strip()]
TRADE_ALERTS_ENABLED = os.getenv("TRADE_ALERTS_ENABLED", "true").lower() == "true"

TRADE_COLUMNS = [
    "EntryTime",
    "OrderTime",
    "ExitTime",
    "Direction",
    "EntryPrice",
    "ExitPrice",
    "EntryReason",
    "ExitReason",
    "PnLAbs",
    "PnLPct",
    "Fees",
    "Outcome",
]

TRADE_TABLE_COLUMNS = [
    "EntryTime",
    "Direction",
    "ReferencePrice",
    "Fees",
    "PnLPct",
    "PnLAbs",
    "Source",
]

TRADE_LOG_SOURCE = (os.getenv("TRADE_LOG_SOURCE", "live") or "live").strip()
if not TRADE_LOG_SOURCE:
    TRADE_LOG_SOURCE = "live"
base_dashboard_dir = os.getenv("TRADES_DASHBOARD_BASE", "trades_dashboard").strip() or "trades_dashboard"
DEFAULT_TRADES_DASHBOARD_DIR = Path(base_dashboard_dir) / TRADE_LOG_SOURCE
TRADE_TABLE_CSV_PATH = Path(os.getenv("TRADE_TABLE_CSV_PATH", DEFAULT_TRADES_DASHBOARD_DIR / "trades_table.csv"))
TRADE_DASHBOARD_HTML_PATH = Path(os.getenv("TRADE_DASHBOARD_HTML_PATH", DEFAULT_TRADES_DASHBOARD_DIR / "trades_dashboard.html"))


def format_timestamp(ts: Any) -> str:
    try:
        if isinstance(ts, pd.Timestamp):
            dt = ts.to_pydatetime()
        elif isinstance(ts, datetime):
            dt = ts
        elif hasattr(ts, "to_pydatetime"):
            dt = ts.to_pydatetime()
        else:
            dt = pd.Timestamp(ts).to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_local = dt.astimezone(LOCAL_TZ)
        return dt_local.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return str(ts)


def _prepare_csv(path: Path):
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=TRADE_COLUMNS).to_csv(path, index=False, encoding="utf-8")
        return

    try:
        existing = pd.read_csv(path)
    except Exception:
        return

    if any(col not in existing.columns for col in TRADE_COLUMNS):
        upgraded = existing.reindex(columns=TRADE_COLUMNS, fill_value=np.nan)
        upgraded.to_csv(path, index=False, encoding="utf-8")


def _ensure_trade_table():
    TRADE_TABLE_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not TRADE_TABLE_CSV_PATH.exists():
        pd.DataFrame(columns=TRADE_TABLE_COLUMNS).to_csv(TRADE_TABLE_CSV_PATH, index=False, encoding="utf-8")


def _append_trade_table(entry_time: str, direction: str, entry_price: float, fees: float, pnl_abs: float, pnl_pct: float, *, source: str):
    try:
        _ensure_trade_table()
        pd.DataFrame(
            [{
                "EntryTime": entry_time,
                "Direction": direction,
                "ReferencePrice": entry_price,
                "Fees": fees,
                "PnLPct": pnl_pct,
                "PnLAbs": pnl_abs,
                "Source": source,
            }]
        ).to_csv(TRADE_TABLE_CSV_PATH, mode="a", header=False, index=False, encoding="utf-8")
    except Exception as exc:
        print(f"[TRADE][WARN] No se pudo actualizar trades_table.csv ({exc})")


def _render_trade_dashboard():
    try:
        if not TRADE_TABLE_CSV_PATH.exists():
            TRADE_DASHBOARD_HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
            TRADE_DASHBOARD_HTML_PATH.write_text("<html><body><h2>Sin operaciones registradas</h2></body></html>", encoding="utf-8")
            return
        df = pd.read_csv(TRADE_TABLE_CSV_PATH)
        total = len(df)
        wins = int((df["PnLAbs"] > 0).sum())
        losses = int((df["PnLAbs"] < 0).sum())
        win_rate = (wins / total * 100) if total else 0.0
        avg_pct = df["PnLPct"].mean() * 100 if total else 0.0
        pnl_total = df["PnLAbs"].sum()
        summary_html = (
            "<table>"
            f"<tr><th>Total trades</th><td>{total}</td></tr>"
            f"<tr><th>Ganadores</th><td>{wins}</td></tr>"
            f"<tr><th>Perdedores</th><td>{losses}</td></tr>"
            f"<tr><th>Win rate</th><td>{win_rate:.2f}%</td></tr>"
            f"<tr><th>PnL promedio</th><td>{avg_pct:.2f}%</td></tr>"
            f"<tr><th>PnL total</th><td>{pnl_total:.2f}</td></tr>"
            "</table>"
        )
        table_html = df.to_html(index=False, classes="trade-table", float_format=lambda x: f"{x:.4f}" if isinstance(x, float) else x)
        html = f"""
        <html>
        <head>
            <meta charset="utf-8" />
            <title>Trade Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 2rem; background: #111; color: #f5f5f5; }}
                h1 {{ color: #facc15; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
                th, td {{ border: 1px solid #333; padding: 0.5rem; text-align: left; }}
                th {{ background: #222; }}
                tr:nth-child(even) {{ background: #1a1a1a; }}
            </style>
        </head>
        <body>
            <h1>Registro de trades</h1>
            {summary_html}
            {table_html}
        </body>
        </html>
        """
        TRADE_DASHBOARD_HTML_PATH.write_text(html, encoding="utf-8")
    except Exception as exc:
        print(f"[TRADE][WARN] No se pudo generar dashboard de trades ({exc})")


def _send_trade_notification(text: str):
    if not TRADE_ALERTS_ENABLED or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chat_id in TELEGRAM_CHAT_IDS:
        payload = {
            "chat_id": chat_id,
            "text": text,
        }
        try:
            requests.post(url, json=payload, timeout=10).raise_for_status()
        except Exception as exc:
            print(f"[TRADE][WARN] No se pudo enviar alerta a Telegram ({chat_id}): {exc}")


def log_trade(
    *,
    direction: str,
    entry_price: float,
    exit_price: float,
    entry_time: pd.Timestamp,
    order_time: Any | None = None,
    exit_time: pd.Timestamp,
    entry_reason: str,
    exit_reason: str,
    fees: float = 0.0,
    notify: bool = False,
    csv_path: str | bool | None = None,
):
    if entry_price is None or exit_price is None:
        return

    path = None if csv_path is False else Path(csv_path or DEFAULT_TRADES_PATH)
    if path is not None:
        _prepare_csv(path)

    pnl_abs = exit_price - entry_price if direction == "long" else entry_price - exit_price
    pnl_abs -= fees
    pnl_pct = pnl_abs / entry_price if entry_price else np.nan
    outcome = "win" if pnl_abs > 0 else ("loss" if pnl_abs < 0 else "flat")
    outcome_label = "GANANCIA" if outcome == "win" else ("PÉRDIDA" if outcome == "loss" else "RESULTADO NEUTRO")

    order_time_obj = order_time or entry_time
    if isinstance(order_time_obj, pd.Timestamp):
        order_time_str = order_time_obj.isoformat()
    elif hasattr(order_time_obj, "isoformat"):
        order_time_str = order_time_obj.isoformat()
    elif order_time_obj is None:
        order_time_str = ""
    else:
        order_time_str = str(order_time_obj)

    data = {
        "EntryTime": entry_time.isoformat() if hasattr(entry_time, "isoformat") else str(entry_time),
        "OrderTime": order_time_str,
        "ExitTime": exit_time.isoformat() if hasattr(exit_time, "isoformat") else str(exit_time),
        "Direction": direction,
        "EntryPrice": entry_price,
        "ExitPrice": exit_price,
        "EntryReason": entry_reason,
        "ExitReason": exit_reason,
        "PnLAbs": pnl_abs,
        "PnLPct": pnl_pct,
        "Fees": fees,
        "Outcome": outcome,
    }

    if path is not None:
        pd.DataFrame([data]).to_csv(path, mode="a", header=False, index=False, encoding="utf-8")
    message = (
        f"[TRADE] {SYMBOL_DISPLAY} {STREAM_INTERVAL} | {direction.upper()} {entry_reason} → {exit_reason} | "
        f"Entry {entry_price:.2f} Exit {exit_price:.2f} | Fees {fees:.2f} | PnL {pnl_abs:.2f} ({pnl_pct*100:.2f}%)"
    )
    try:
        _append_trade_table(
            entry_time=data["EntryTime"],
            direction=direction,
            entry_price=entry_price,
            fees=fees,
            pnl_abs=pnl_abs,
            pnl_pct=pnl_pct,
            source=TRADE_LOG_SOURCE,
        )
        _render_trade_dashboard()
    except Exception as exc:
        print(f"[TRADE][WARN] No se pudo actualizar el tablero de trades ({exc})")
    print(message)
    if notify:
        try:
            ts_entry = format_timestamp(entry_time)
            ts_exit = format_timestamp(exit_time)
            tele_msg = (
                f"{SYMBOL_DISPLAY} {STREAM_INTERVAL}\n"
                f"Cierre {direction.upper()}\n"
                f"Entrada: {entry_price:.2f} ({ts_entry})\n"
                f"Salida: {exit_price:.2f} ({ts_exit})\n"
                f"Fees: {fees:.2f}\n"
                f"Resultado: {outcome_label} {pnl_abs:.2f} ({pnl_pct*100:+.2f}%)"
            )
            _send_trade_notification(tele_msg)
        except Exception as exc:
            print(f"[TRADE][WARN] Error enviando alerta de trade: {exc}")


def send_trade_notification(text: str):
    _send_trade_notification(text)
