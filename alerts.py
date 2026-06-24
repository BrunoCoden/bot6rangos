import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

from backtest.realtime_backtest import evaluate_realtime_risk, process_realtime_signal
from paginado_binance import INTERVAL_MS, KlinesFetchError, fetch_klines_paginado
from tabla_alertas import log_stream_bar
from velas import (
    API_SYMBOL,
    BB_LENGTH,
    BB_MULT,
    STREAM_INTERVAL,
    SYMBOL_DISPLAY,
    compute_bollinger_bands,
)


ALERT_STREAM_BARS = int(os.getenv("ALERT_STREAM_BARS", "5000"))
SIGNAL_ALERTS_ENABLED = os.getenv("ALERT_ENABLE_BOLLINGER_SIGNALS", "false").lower() == "true"
STOP_LOSS_PCT = float(os.getenv("STRAT_STOP_LOSS_PCT", "0.02"))
STRATEGY_MODE = os.getenv("BOT6_STRATEGY_MODE", "sma115_stable").strip().lower()

SMA_STABLE_LEN = int(os.getenv("SMA_STABLE_LEN", "115"))
SMA_STABLE_PREV_AVG_BARS = int(os.getenv("SMA_STABLE_PREV_AVG_BARS", "10"))
SMA_STABLE_MAX_DISTANCE_PCT = float(os.getenv("SMA_STABLE_MAX_DISTANCE_PCT", "0.01"))
SMA_STABLE_TAKE_PROFIT_PCT = float(os.getenv("SMA_STABLE_TAKE_PROFIT_PCT", "0.08"))
SMA_STABLE_COMBINED_FILTER_ENABLED = (
    os.getenv("SMA_STABLE_COMBINED_FILTER_ENABLED", "false").lower() == "true"
)
SMA_STABLE_FUNDING_ABS_MAX = float(os.getenv("SMA_STABLE_FUNDING_ABS_MAX", "0.00005"))
SMA_STABLE_RANGE_WINDOW = int(os.getenv("SMA_STABLE_RANGE_WINDOW", "96"))
SMA_STABLE_RANGE_MIN_PCT = float(os.getenv("SMA_STABLE_RANGE_MIN_PCT", "3.0"))
SMA_STABLE_FUNDING_HTTP_TIMEOUT = float(os.getenv("SMA_STABLE_FUNDING_HTTP_TIMEOUT", "5"))
SMA_STABLE_FUNDING_BASE_URL = os.getenv("BINANCE_UM_BASE_URL", "https://fapi.binance.com").rstrip("/")

RANGE_LOOKBACK_BARS = int(os.getenv("RANGE_LOOKBACK_BARS", "200"))
RANGE_PCT_UPPER = float(os.getenv("RANGE_PCT_UPPER", "25"))
RANGE_PCT_MIDDLE = float(os.getenv("RANGE_PCT_MIDDLE", "50"))
RANGE_PCT_LOWER = float(os.getenv("RANGE_PCT_LOWER", "25"))
RANGE_BB_SIGNAL_TYPE = os.getenv("RANGE_BB_SIGNAL_TYPE", "Cruce de cierre").strip()
RANGE_CLASSIFY_WITH = os.getenv("RANGE_CLASSIFY_WITH", "Mecha").strip()
RANGE_NEW_EXTREME_BARS = int(os.getenv("RANGE_NEW_EXTREME_BARS", "3"))
RANGE_PENDING_ORDER_TYPE = os.getenv("RANGE_PENDING_ORDER_TYPE", "Stop en banda").strip()
RANGE_AVOID_REPEATED_RAW_BB = os.getenv("RANGE_AVOID_REPEATED_RAW_BB", "true").lower() == "true"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
_chat_ids_raw = os.getenv("TELEGRAM_CHAT_IDS", "")
TELEGRAM_CHAT_IDS = [part.strip() for part in _chat_ids_raw.replace(";", ",").split(",") if part.strip()]

_LAST_RANGE_SIGNAL_PATH = Path(os.getenv("RANGE_LAST_SIGNAL_PATH", "backtest/backtestTR/range3_last_signal.json"))
_last_processed_close_ts: str | None = None
_funding_cache: dict[str, tuple[int, float | None]] = {}

LOCAL_TZ_NAME = os.getenv("TZ", "UTC")
try:
    LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)
except Exception:
    LOCAL_TZ = ZoneInfo("UTC")


def _prepare_frames() -> dict | None:
    try:
        df_stream = fetch_klines_paginado(API_SYMBOL, STREAM_INTERVAL, ALERT_STREAM_BARS)
    except KlinesFetchError as exc:
        print(
            f"[WATCHER][KLINES][FAIL] No se pudo descargar stream symbol={API_SYMBOL} "
            f"interval={STREAM_INTERVAL} err={exc}"
        )
        return None
    except Exception as exc:
        print(f"[ALERT][WARN] Falló _prepare_frames ({exc})")
        return None
    if df_stream.empty:
        return None

    ohlc_stream = df_stream[["Open", "High", "Low", "Close", "Volume"]].copy()
    if "CloseTimeDT" in df_stream.columns:
        ohlc_stream["BarCloseTime"] = df_stream["CloseTimeDT"]
    else:
        interval_ms = INTERVAL_MS.get(STREAM_INTERVAL, 0)
        ohlc_stream["BarCloseTime"] = df_stream.index + pd.to_timedelta(interval_ms, unit="ms")
    bb = compute_bollinger_bands(ohlc_stream, BB_LENGTH, BB_MULT).reindex(ohlc_stream.index).ffill()
    channels = _compute_range3_channels(ohlc_stream)
    return {"stream": ohlc_stream, "bollinger": bb, "channels": channels}


def _compute_range3_channels(ohlc: pd.DataFrame) -> pd.DataFrame:
    high = ohlc["High"].astype("float64")
    low = ohlc["Low"].astype("float64")
    lookback = max(int(RANGE_LOOKBACK_BARS), 2)
    max_line = high.rolling(lookback, min_periods=lookback).max()
    min_line = low.rolling(lookback, min_periods=lookback).min()
    range_size = max_line - min_line
    total = RANGE_PCT_UPPER + RANGE_PCT_MIDDLE + RANGE_PCT_LOWER
    if total <= 0:
        total = 100.0
    w_upper = RANGE_PCT_UPPER / total
    w_middle = RANGE_PCT_MIDDLE / total
    maxfloor = max_line - range_size * w_upper
    minroof = maxfloor - range_size * w_middle
    return pd.DataFrame(
        {
            "max": max_line,
            "maxfloor": maxfloor,
            "minroof": minroof,
            "min": min_line,
            "range": range_size,
        },
        index=ohlc.index,
    )


def _raw_bb_signals(ohlc: pd.DataFrame, bb: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    close = ohlc["Close"].astype("float64")
    high = ohlc["High"].astype("float64")
    low = ohlc["Low"].astype("float64")
    upper = bb["upper"].astype("float64")
    lower = bb["lower"].astype("float64")

    if RANGE_BB_SIGNAL_TYPE == "Mecha + cierre":
        lower_raw = (low <= lower) & (close > lower)
        upper_raw = (high >= upper) & (close < upper)
    elif RANGE_BB_SIGNAL_TYPE == "Toque simple":
        lower_raw = low <= lower
        upper_raw = high >= upper
    else:
        lower_raw = (close > lower) & (close.shift(1) <= lower.shift(1))
        upper_raw = (close < upper) & (close.shift(1) >= upper.shift(1))

    if RANGE_AVOID_REPEATED_RAW_BB:
        lower_sig = lower_raw & (~lower_raw.shift(1, fill_value=False))
        upper_sig = upper_raw & (~upper_raw.shift(1, fill_value=False))
    else:
        lower_sig = lower_raw
        upper_sig = upper_raw
    return lower_sig.fillna(False), upper_sig.fillna(False), (lower_sig | upper_sig).fillna(False)


def _load_last_processed_close_ts() -> str | None:
    try:
        data = json.loads(_LAST_RANGE_SIGNAL_PATH.read_text(encoding="utf-8"))
        value = data.get("last_processed_close_ts")
        return str(value) if value else None
    except Exception:
        return None


def _save_last_processed_close_ts(close_ts: pd.Timestamp) -> None:
    try:
        _LAST_RANGE_SIGNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LAST_RANGE_SIGNAL_PATH.write_text(
            json.dumps({"last_processed_close_ts": close_ts.isoformat()}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"[RANGE3][WARN] No se pudo guardar last close ts: {exc}")


def _timestamp_ms(ts: pd.Timestamp) -> int:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.tz_convert("UTC").timestamp() * 1000)


def _fetch_last_funding_abs(symbol: str, close_ts: pd.Timestamp) -> float | None:
    end_ms = _timestamp_ms(close_ts)
    cache_key = f"{symbol}|{end_ms // 28_800_000}"
    cached = _funding_cache.get(cache_key)
    if cached and cached[0] <= end_ms:
        return cached[1]
    try:
        resp = requests.get(
            f"{SMA_STABLE_FUNDING_BASE_URL}/fapi/v1/fundingRate",
            params={"symbol": symbol, "endTime": end_ms, "limit": 1},
            timeout=SMA_STABLE_FUNDING_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            _funding_cache[cache_key] = (end_ms, None)
            return None
        funding_abs = abs(float(data[-1].get("fundingRate")))
        funding_time = int(data[-1].get("fundingTime") or end_ms)
        _funding_cache[cache_key] = (funding_time, funding_abs)
        return funding_abs
    except Exception as exc:
        print(f"[WATCHER][SMA115][COMBO][WARN] funding_fetch_failed err={exc}")
        return None


def _range3_events(bb: pd.DataFrame, channels: pd.DataFrame, ohlc: pd.DataFrame) -> list[dict]:
    global _last_processed_close_ts
    if bb.empty or channels.empty or len(ohlc) < max(RANGE_LOOKBACK_BARS, BB_LENGTH) + 2:
        return []

    # Usa sólo la última vela cerrada; la última fila es vela en formación.
    i = len(ohlc) - 2
    row = ohlc.iloc[i]
    close_ts = row.get("BarCloseTime", ohlc.index[i])
    close_ts = close_ts if isinstance(close_ts, pd.Timestamp) else pd.Timestamp(close_ts)
    close_key = close_ts.isoformat()
    if _last_processed_close_ts is None:
        _last_processed_close_ts = _load_last_processed_close_ts()
    if _last_processed_close_ts == close_key:
        return []
    _last_processed_close_ts = close_key
    _save_last_processed_close_ts(close_ts)

    lower_sig, upper_sig, bb_sig = _raw_bb_signals(ohlc, bb)
    h = float(row["High"])
    l = float(row["Low"])
    c = float(row["Close"])
    ch = channels.iloc[i]
    max_line = float(ch["max"]) if not pd.isna(ch["max"]) else np.nan
    maxfloor = float(ch["maxfloor"]) if not pd.isna(ch["maxfloor"]) else np.nan
    minroof = float(ch["minroof"]) if not pd.isna(ch["minroof"]) else np.nan
    min_line = float(ch["min"]) if not pd.isna(ch["min"]) else np.nan
    range_size = float(ch["range"]) if not pd.isna(ch["range"]) else np.nan

    if np.isnan(range_size) or range_size <= 0:
        return []

    new_max_now = bool(h >= max_line)
    new_min_now = bool(l <= min_line)
    state_event = {
        "type": "range3_state",
        "timestamp": close_ts,
        "symbol": API_SYMBOL,
        "price": c,
        "close_price": c,
        "high": h,
        "low": l,
        "new_max_now": new_max_now,
        "new_min_now": new_min_now,
        "max": max_line,
        "maxfloor": maxfloor,
        "minroof": minroof,
        "min": min_line,
    }

    ref_short = h if RANGE_CLASSIFY_WITH == "Mecha" else c
    ref_long = l if RANGE_CLASSIFY_WITH == "Mecha" else c
    in_short_zone = ref_short <= max_line and ref_short >= maxfloor
    in_long_zone = ref_long >= min_line and ref_long <= minroof
    close_in_short_zone = c <= max_line and c >= maxfloor
    close_in_long_zone = c >= min_line and c <= minroof

    short_signal = bool(upper_sig.iloc[i] and close_in_short_zone and in_short_zone and not in_long_zone)
    long_signal = bool(lower_sig.iloc[i] and close_in_long_zone and in_long_zone and not in_short_zone)
    if not short_signal and not long_signal:
        return [state_event]

    side = 1 if long_signal else -1
    direction = "long" if side == 1 else "short"
    start_i = max(0, i - max(0, RANGE_NEW_EXTREME_BARS))
    recent_max = bool((ohlc["High"].iloc[start_i : i + 1].astype("float64") >= channels["max"].iloc[start_i : i + 1].astype("float64")).any())
    recent_min = bool((ohlc["Low"].iloc[start_i : i + 1].astype("float64") <= channels["min"].iloc[start_i : i + 1].astype("float64")).any())
    recent_extreme = recent_max or recent_min
    pending_price = minroof if side == 1 else maxfloor
    entry_mode = "pending" if recent_extreme else "direct"
    entry_price = pending_price if recent_extreme else c
    stop_loss = entry_price * (1 - STOP_LOSS_PCT) if direction == "long" else entry_price * (1 + STOP_LOSS_PCT)
    trend = "alcista" if direction == "long" else "bajista"
    msg_extra = (
        f"orden pendiente en {pending_price:.2f}"
        if entry_mode == "pending"
        else f"entrada directa en close {c:.2f}"
    )
    signal_event = {
        "type": "range3_signal",
        "timestamp": close_ts,
        "message": (
            f"📶 [RANGE3+BB] {SYMBOL_DISPLAY} {STREAM_INTERVAL}: Señal {trend} "
            f"en {c:.2f} ({msg_extra})"
        ),
        "symbol": API_SYMBOL,
        "price": float(entry_price),
        "entry_price": float(entry_price),
        "close_price": c,
        "direction": direction,
        "range_entry_mode": entry_mode,
        "pending_price": float(pending_price),
        "pending_order_type": RANGE_PENDING_ORDER_TYPE,
        "recent_extreme": recent_extreme,
        "recent_max": recent_max,
        "recent_min": recent_min,
        "max": max_line,
        "maxfloor": maxfloor,
        "minroof": minroof,
        "min": min_line,
        "stop_loss": stop_loss,
        "take_profit": None,
        "sl": stop_loss,
        "tp": None,
    }
    return [state_event, signal_event]


def _sma115_events(ohlc: pd.DataFrame) -> list[dict]:
    global _last_processed_close_ts
    min_rows = max(SMA_STABLE_LEN, SMA_STABLE_PREV_AVG_BARS, SMA_STABLE_RANGE_WINDOW) + 3
    if ohlc.empty or len(ohlc) < min_rows:
        return []

    # Usa sólo la última vela cerrada; la última fila es vela en formación.
    i = len(ohlc) - 2
    row = ohlc.iloc[i]
    close_ts = row.get("BarCloseTime", ohlc.index[i])
    close_ts = close_ts if isinstance(close_ts, pd.Timestamp) else pd.Timestamp(close_ts)
    close_key = close_ts.isoformat()
    if _last_processed_close_ts is None:
        _last_processed_close_ts = _load_last_processed_close_ts()
    if _last_processed_close_ts == close_key:
        return []
    _last_processed_close_ts = close_key
    _save_last_processed_close_ts(close_ts)

    close_series = ohlc["Close"].astype("float64")
    sma = close_series.rolling(SMA_STABLE_LEN, min_periods=SMA_STABLE_LEN).mean()
    range_window = max(int(SMA_STABLE_RANGE_WINDOW), 2)
    range_high = ohlc["High"].astype("float64").rolling(range_window, min_periods=range_window).max()
    range_low = ohlc["Low"].astype("float64").rolling(range_window, min_periods=range_window).min()
    range_pct = ((range_high - range_low) / sma) * 100.0
    ohlc4 = (
        ohlc["Open"].astype("float64")
        + ohlc["High"].astype("float64")
        + ohlc["Low"].astype("float64")
        + ohlc["Close"].astype("float64")
    ) / 4.0
    prev_avg = ohlc4.shift(1).rolling(
        SMA_STABLE_PREV_AVG_BARS,
        min_periods=SMA_STABLE_PREV_AVG_BARS,
    ).mean()

    sma_now = float(sma.iloc[i]) if not pd.isna(sma.iloc[i]) else np.nan
    sma_prev = float(sma.iloc[i - 1]) if i > 0 and not pd.isna(sma.iloc[i - 1]) else np.nan
    prev10_now = float(prev_avg.iloc[i]) if not pd.isna(prev_avg.iloc[i]) else np.nan
    range_pct_now = float(range_pct.iloc[i]) if not pd.isna(range_pct.iloc[i]) else np.nan
    if np.isnan(sma_now) or np.isnan(sma_prev) or np.isnan(prev10_now) or sma_now <= 0:
        return []

    c = float(row["Close"])
    h = float(row["High"])
    l = float(row["Low"])
    prev_close = float(close_series.iloc[i - 1])
    prev_side = "long" if prev_close > sma_prev else "short" if prev_close < sma_prev else None
    side = "long" if c > sma_now else "short" if c < sma_now else None
    distance_pct = abs(c / sma_now - 1.0)
    prev10_ok = bool((side == "long" and c > prev10_now) or (side == "short" and c < prev10_now))
    distance_ok = bool(distance_pct <= SMA_STABLE_MAX_DISTANCE_PCT)
    funding_abs = _fetch_last_funding_abs(API_SYMBOL, close_ts) if SMA_STABLE_COMBINED_FILTER_ENABLED else None
    funding_ok = (
        True
        if not SMA_STABLE_COMBINED_FILTER_ENABLED
        else funding_abs is not None and funding_abs <= SMA_STABLE_FUNDING_ABS_MAX
    )
    range_ok = (
        True
        if not SMA_STABLE_COMBINED_FILTER_ENABLED
        else (not np.isnan(range_pct_now)) and range_pct_now >= SMA_STABLE_RANGE_MIN_PCT
    )
    combo_ok = bool(funding_ok and range_ok)

    state_event = {
        "type": "sma115_state",
        "timestamp": close_ts,
        "symbol": API_SYMBOL,
        "price": c,
        "close_price": c,
        "high": h,
        "low": l,
        "sma": sma_now,
        "prev10_avg": prev10_now,
        "side": side,
        "prev10_ok": prev10_ok,
        "distance_ok": distance_ok,
        "distance_pct": distance_pct,
        "entry_filter_ok": bool(prev10_ok and distance_ok),
        "funding_abs": funding_abs,
        "funding_ok": funding_ok,
        "range_window": range_window,
        "range_pct": None if np.isnan(range_pct_now) else range_pct_now,
        "range_ok": range_ok,
        "combo_ok": combo_ok,
    }
    if side is None or prev_side is None or side == prev_side:
        return [state_event]

    if not combo_ok:
        entry_mode = "ignored_combo"
    elif prev10_ok and distance_ok:
        entry_mode = "direct"
    elif prev10_ok and not distance_ok:
        entry_mode = "pending"
    else:
        entry_mode = "ignored_prev10"
    direction = side
    tp = c * (1.0 + SMA_STABLE_TAKE_PROFIT_PCT) if direction == "long" else c * (1.0 - SMA_STABLE_TAKE_PROFIT_PCT)
    sl = c * (1.0 - STOP_LOSS_PCT) if direction == "long" else c * (1.0 + STOP_LOSS_PCT)
    signal_event = {
        "type": "sma115_signal",
        "timestamp": close_ts,
        "message": (
            f"📶 [SMA115] {SYMBOL_DISPLAY} {STREAM_INTERVAL}: Señal {direction.upper()} "
            f"close={c:.2f} SMA={sma_now:.2f} prev10={prev10_now:.2f} "
            f"dist={distance_pct * 100:.3f}% modo={entry_mode} "
            f"funding_abs={funding_abs if funding_abs is not None else 'NA'} "
            f"range{range_window}={range_pct_now if not np.isnan(range_pct_now) else 'NA'}"
        ),
        "symbol": API_SYMBOL,
        "price": c,
        "entry_price": c,
        "close_price": c,
        "direction": direction,
        "sma": sma_now,
        "prev10_avg": prev10_now,
        "prev10_ok": prev10_ok,
        "distance_ok": distance_ok,
        "distance_pct": distance_pct,
        "entry_filter_ok": bool(prev10_ok and distance_ok),
        "entry_mode": entry_mode,
        "funding_abs": funding_abs,
        "funding_ok": funding_ok,
        "range_window": range_window,
        "range_pct": None if np.isnan(range_pct_now) else range_pct_now,
        "range_ok": range_ok,
        "combo_ok": combo_ok,
        "stop_loss": sl,
        "take_profit": tp,
        "sl": sl,
        "tp": tp,
    }
    return [state_event, signal_event]


def generate_alerts() -> list[dict]:
    frames = _prepare_frames()
    if not frames:
        return []

    log_stream_bar(frames["stream"])
    try:
        evaluate_realtime_risk(frames["stream"], profile="tr")
    except Exception as exc:
        print(f"[ALERT][WARN] No se pudo evaluar SL/TP en tiempo real ({exc})")
    if not SIGNAL_ALERTS_ENABLED:
        return []

    if STRATEGY_MODE == "sma115_stable":
        events = _sma115_events(frames["stream"])
    else:
        events = _range3_events(frames["bollinger"], frames["channels"], frames["stream"])
    for evt in events:
        if evt.get("type") == "range3_signal":
            try:
                process_realtime_signal(evt, profile="tr")
            except Exception as exc:
                print(f"[ALERT][WARN] No se pudo actualizar el backtest en tiempo real ({exc})")
    return events


def format_alert_message(alert: dict) -> str:
    ts = alert.get("timestamp")
    ts_str = ""
    if isinstance(ts, pd.Timestamp):
        try:
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            ts_str = ts.tz_convert(LOCAL_TZ).isoformat()
        except Exception:
            ts_str = str(ts)
    elif hasattr(ts, "astimezone"):
        try:
            ts_str = ts.astimezone(LOCAL_TZ).isoformat()
        except Exception:
            ts_str = str(ts)
    elif hasattr(ts, "isoformat"):
        ts_str = ts.isoformat()
    else:
        ts_str = str(ts)

    base = f"{ts_str}\n{alert.get('message', '')}"
    parts = []
    try:
        tp = alert.get("take_profit")
        sl = alert.get("stop_loss")
        if tp is not None:
            parts.append(f"TP: {float(tp):.2f}")
        if sl is not None:
            parts.append(f"SL: {float(sl):.2f}")
    except Exception:
        pass
    return f"{base}\n" + " | ".join(parts) if parts else base


def send_alerts(alerts: list[dict]) -> int:
    user_alerts = [a for a in alerts or [] if not str(a.get("type") or "").startswith("range3_state")]
    if not user_alerts or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        return 0
    base_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    sent = 0
    for alert in user_alerts:
        text = format_alert_message(alert)
        for chat_id in TELEGRAM_CHAT_IDS:
            try:
                resp = requests.post(base_url, json={"chat_id": chat_id, "text": text}, timeout=10)
                resp.raise_for_status()
                sent += 1
            except Exception as exc:
                details = ""
                if isinstance(exc, requests.HTTPError) and exc.response is not None:
                    try:
                        details = f" | Response: {exc.response.json()}"
                    except ValueError:
                        details = f" | Response: {exc.response.text}"
                print(f"[ERROR] Telegram send failed ({chat_id}): {exc}{details}")
    return sent


if __name__ == "__main__":
    alerts = generate_alerts()
    for alert in alerts:
        print(f"[ALERTA] {format_alert_message(alert)}")
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS:
        sent = send_alerts(alerts)
        print(f"[INFO] Alertas enviadas a Telegram: {sent}")
    else:
        print("[WARN] TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_IDS no configurados; no se enviaron mensajes.")
