# tabla_alertas.py
import os
from pathlib import Path

import pandas as pd

from zoneinfo import ZoneInfo

ALERTS_TABLE_CSV_PATH = os.getenv("ALERTS_TABLE_CSV_PATH", "alerts_stream.csv").strip()
ALERTS_TABLE_TZ = os.getenv("TZ", "UTC")

try:
    _LOCAL_TZ = ZoneInfo(ALERTS_TABLE_TZ)
except Exception:
    _LOCAL_TZ = ZoneInfo("UTC")

CSV_COLUMNS = ["Timestamp", "TimestampUTC", "Open", "High", "Low", "Close", "Volume"]

_last_logged = None


def _ensure_header(path: Path):
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=CSV_COLUMNS).to_csv(path, index=False, encoding="utf-8")


def log_stream_bar(df: pd.DataFrame):
    """
    Registra la última vela disponible en el CSV definido por ALERTS_TABLE_CSV_PATH.
    Evita duplicados por timestamp.
    """
    global _last_logged

    if df is None or df.empty:
        return

    ts = df.index[-1]
    if _last_logged is not None and ts == _last_logged:
        return

    row = df.iloc[-1]
    try:
        ts_local = ts.tz_convert(_LOCAL_TZ) if ts.tzinfo else ts.tz_localize("UTC").tz_convert(_LOCAL_TZ)
    except Exception:
        ts_local = ts
    try:
        ts_utc = ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")
    except Exception:
        ts_utc = ts

    data = {
        "Timestamp": ts_local.isoformat() if hasattr(ts_local, "isoformat") else str(ts_local),
        "TimestampUTC": ts_utc.isoformat() if hasattr(ts_utc, "isoformat") else str(ts_utc),
        "Open": row["Open"],
        "High": row["High"],
        "Low": row["Low"],
        "Close": row["Close"],
        "Volume": row["Volume"],
    }

    path = Path(ALERTS_TABLE_CSV_PATH)
    _ensure_header(path)
    pd.DataFrame([data]).to_csv(path, mode="a", header=False, index=False, encoding="utf-8")
    _last_logged = ts
