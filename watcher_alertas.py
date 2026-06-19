# watcher_alertas.py
import hashlib
import json
import math
import os
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from multiprocessing import Process, Queue
from pathlib import Path
from queue import Empty as QueueEmpty
from queue import Full as QueueFull
import random
import time
from datetime import datetime, timezone, timedelta

from binance.um_futures import UMFutures

from alerts import generate_alerts, send_alerts, format_alert_message
from balance_ledger import BalanceLedger, BalanceLedgerConfig, normalize_close_ts
from trades_table_ledger import TradesTableLedger, TradesTableLedgerConfig
from trade_logger import send_trade_notification, format_timestamp
from velas import SYMBOL_DISPLAY, STREAM_INTERVAL
from trading.accounts.manager import AccountManager
from trading.accounts.models import ExchangeEnvironment, ExchangeCredential
from trading.orders.executor import OrderExecutor
from trading.orders.models import OrderRequest, OrderSide, OrderType, TimeInForce

TRADING_ENABLED = os.getenv("WATCHER_ENABLE_TRADING", "false").lower() == "true"
TRADING_ACCOUNTS_FILE = os.getenv("WATCHER_ACCOUNTS_FILE", "trading/accounts/sample_accounts.yaml")
TRADING_USER_ID = os.getenv("WATCHER_TRADING_USER", "").strip()
TRADING_EXCHANGE = os.getenv("WATCHER_TRADING_EXCHANGE", "").strip()
TRADING_DRY_RUN = os.getenv("WATCHER_TRADING_DRY_RUN", "true").lower() != "false"
TRADING_MIN_PRICE = float(os.getenv("WATCHER_TRADING_MIN_PRICE", "0"))

_executor: OrderExecutor | None = None
_account_manager: AccountManager | None = None
_accounts_mtime: float | None = None
_last_order_direction: dict[tuple[str, str], str] = {}
_thresholds: list[dict] = []
THRESHOLDS_PATH = Path("backtest/backtestTR/pending_thresholds.json")
BALANCE_LEDGER_PATH = Path(os.getenv("BALANCE_LEDGER_PATH", "backtest/backtestTR/balance_ledger.jsonl"))
BALANCE_BACKFILL_STATE_PATH = Path(
    os.getenv("BALANCE_BACKFILL_STATE_PATH", "backtest/backtestTR/balance_backfill_state.json")
)
BALANCE_LEDGER_SOURCE = os.getenv("BALANCE_LEDGER_SOURCE", "live")
BALANCE_BACKFILL_LOG_PATH = os.getenv("WATCHER_BALANCE_BACKFILL_LOG", "").strip()
BALANCE_LEDGER = BalanceLedger(
    BalanceLedgerConfig(
        ledger_path=BALANCE_LEDGER_PATH,
        state_path=BALANCE_BACKFILL_STATE_PATH,
        source=BALANCE_LEDGER_SOURCE,
    )
)
TRADES_TABLE_LEDGER_PATH = Path(
    os.getenv("TRADES_TABLE_LEDGER_PATH", "backtest/backtestTR/trades_table_ledger.jsonl")
)
OPEN_TRADE_STATE_PATH = Path(
    os.getenv("OPEN_TRADE_STATE_PATH", "backtest/backtestTR/open_trade_state.json")
)
PENDING_EXECUTION_RESOLUTIONS_PATH = Path(
    os.getenv("PENDING_EXECUTION_RESOLUTIONS_PATH", "backtest/backtestTR/pending_execution_resolutions.json")
)
TRADES_TABLE_MIN_START_LOCAL = os.getenv("TRADES_TABLE_MIN_START_LOCAL", "2026-04-01T00:00:00-03:00")
ORDER_ID_PREFIX = os.getenv("WATCHER_ORDER_ID_PREFIX", "BOT1")
TRADES_FILL_RESOLVE_WAIT_SECONDS = float(os.getenv("TRADES_FILL_RESOLVE_WAIT_SECONDS", "45"))
TRADES_FILL_RESOLVE_POLL_SECONDS = float(os.getenv("TRADES_FILL_RESOLVE_POLL_SECONDS", "1"))
TRADES_FILL_RESOLVE_TTL_SECONDS = float(os.getenv("TRADES_FILL_RESOLVE_TTL_SECONDS", "86400"))
TRADES_FILL_RESOLVE_WORKER_POLL_SECONDS = float(os.getenv("TRADES_FILL_RESOLVE_WORKER_POLL_SECONDS", "3"))
TRADES_FILL_RESOLVE_ATTEMPT_SECONDS = float(os.getenv("TRADES_FILL_RESOLVE_ATTEMPT_SECONDS", "8"))
TRADES_TABLE_LEDGER = TradesTableLedger(TradesTableLedgerConfig(ledger_path=TRADES_TABLE_LEDGER_PATH))
_open_trade_state: dict[str, dict] = {}


def _resolve_sl_pct() -> tuple[float, str]:
    primary = os.getenv("STRAT_STOP_LOSS_PCT")
    legacy = os.getenv("WATCHER_CONTRA_THRESHOLD_PCT")
    for source, raw in (("STRAT_STOP_LOSS_PCT", primary), ("WATCHER_CONTRA_THRESHOLD_PCT", legacy)):
        if raw is None or str(raw).strip() == "":
            continue
        try:
            value = float(raw)
            if value > 0:
                return value, source
            print(f"[WATCHER][WARN] {source} inválido (<=0): {raw}; fallback.")
        except Exception:
            print(f"[WATCHER][WARN] {source} inválido (no numérico): {raw}; fallback.")
    return 0.02, "DEFAULT_0.02"


LOSS_PCT, LOSS_PCT_SOURCE = _resolve_sl_pct()
PARTIAL_TP_PCT = 0.04
PARTIAL_TP_CLOSE_PCT = 0.5
POST_TP_SL_PCT = 0.03
THRESHOLDS_CLEAR_ON_STARTUP = os.getenv("WATCHER_THRESHOLDS_CLEAR_ON_STARTUP", "false").lower() == "true"
THRESHOLDS_REBUILD_ON_STARTUP = os.getenv("WATCHER_THRESHOLDS_REBUILD_ON_STARTUP", "false").lower() == "true"
ACCOUNTS_AUTO_RELOAD = os.getenv("WATCHER_ACCOUNTS_AUTO_RELOAD", "false").lower() == "true"
DISABLED_ACCOUNTS_AUTO_CLOSE = os.getenv("WATCHER_DISABLED_AUTO_CLOSE", "true").lower() == "true"
DISABLED_ACCOUNTS_CLOSE_POLL_SECONDS = float(os.getenv("WATCHER_DISABLED_CLOSE_POLL_SECONDS", "30"))
CLOSE_OPPOSITE_TIMEOUT_SECONDS = float(os.getenv("WATCHER_CLOSE_OPPOSITE_TIMEOUT_SECONDS", "10"))
CLOSE_OPPOSITE_POLL_SECONDS = float(os.getenv("WATCHER_CLOSE_OPPOSITE_POLL_SECONDS", "0.5"))
POSITION_RETRY_COUNT = int(os.getenv("WATCHER_POSITION_RETRY_COUNT", "3"))
POSITION_RETRY_DELAY = float(os.getenv("WATCHER_POSITION_RETRY_DELAY", "0.5"))
POSITION_GRACE_SECONDS = float(os.getenv("WATCHER_POSITION_GRACE_SECONDS", "20"))
POSITION_GUARD_RETRY_ENABLED = os.getenv("WATCHER_POSITION_GUARD_RETRY_ENABLED", "true").lower() == "true"
POSITION_GUARD_RETRY_BASE_SECONDS = float(os.getenv("WATCHER_POSITION_GUARD_RETRY_BASE_SECONDS", "1"))
POSITION_GUARD_RETRY_MAX_SECONDS = float(os.getenv("WATCHER_POSITION_GUARD_RETRY_MAX_SECONDS", "60"))
POSITION_GUARD_RETRY_JITTER_SECONDS = float(os.getenv("WATCHER_POSITION_GUARD_RETRY_JITTER_SECONDS", "0.3"))
POSITION_GUARD_RETRY_TTL_SECONDS = float(os.getenv("WATCHER_POSITION_GUARD_RETRY_TTL_SECONDS", "21600"))
POSITION_GUARD_WORKER_POLL_SECONDS = float(os.getenv("WATCHER_POSITION_GUARD_WORKER_POLL_SECONDS", "1"))
POSITION_GUARD_QUEUE_PATH = Path(
    os.getenv("WATCHER_POSITION_GUARD_QUEUE_FILE", "backtest/backtestTR/pending_trade_retries.json")
)
POSITION_MONITOR_ALERTS_ENABLED = os.getenv("WATCHER_POSITION_MONITOR_ALERTS", "true").lower() == "true"
POSITION_MONITOR_COOLDOWN_SECONDS = float(os.getenv("WATCHER_POSITION_MONITOR_COOLDOWN_SECONDS", "300"))
POSITION_MONITOR_UNKNOWN_ALERT_EVERY = int(os.getenv("WATCHER_POSITION_MONITOR_UNKNOWN_ALERT_EVERY", "5"))
POSITION_MONITOR_POLL_SECONDS = float(os.getenv("WATCHER_POSITION_MONITOR_POLL_SECONDS", "10"))
POSITION_MONITOR_REQUIRE_OPEN = os.getenv("WATCHER_POSITION_MONITOR_REQUIRE_OPEN", "true").lower() == "true"
POSITION_MONITOR_FLAT_ALERT_SECONDS = float(os.getenv("WATCHER_POSITION_MONITOR_FLAT_ALERT_SECONDS", "90"))
RANGE_PENDING_PATH = Path(os.getenv("RANGE_PENDING_STATE_PATH", "backtest/backtestTR/range3_pending_order.json"))
RANGE_FLIP_STOP_LOSS_PCT = float(os.getenv("RANGE_FLIP_STOP_LOSS_PCT", "0.02"))
RANGE_FLIP_TAKE_PROFIT_PCT = float(os.getenv("RANGE_FLIP_TAKE_PROFIT_PCT", "0.02"))
RANGE_TRAILING_STEP_PCT = float(os.getenv("RANGE_TRAILING_STEP_PCT", "0.01"))
RANGE_PROFIT_LOCK_TRIGGER_PCT = float(os.getenv("RANGE_PROFIT_LOCK_TRIGGER_PCT", "0.03"))
RANGE_PROFIT_LOCK_SL_PCT = float(os.getenv("RANGE_PROFIT_LOCK_SL_PCT", "0.005"))
SMA_STABLE_TAKE_PROFIT_PCT = float(os.getenv("SMA_STABLE_TAKE_PROFIT_PCT", "0.08"))
SMA_STABLE_PENDING_SL_PCT = float(os.getenv("SMA_STABLE_PENDING_SL_PCT", "0.02"))
BINANCE_RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", "20000"))
BINANCE_HTTP_TIMEOUT = float(os.getenv("BINANCE_HTTP_TIMEOUT", "10"))
BINANCE_TIMESTAMP_RETRY_COUNT = int(os.getenv("BINANCE_TIMESTAMP_RETRY_COUNT", "2"))
BINANCE_TIMESTAMP_RETRY_DELAY = float(os.getenv("BINANCE_TIMESTAMP_RETRY_DELAY", "0.25"))
BYBIT_RECV_WINDOW_MS = int(os.getenv("BYBIT_RECV_WINDOW_MS", "20000"))
BYBIT_HTTP_TIMEOUT = float(os.getenv("BYBIT_HTTP_TIMEOUT", "10"))
BYBIT_TIMESTAMP_RETRY_COUNT = int(os.getenv("BYBIT_TIMESTAMP_RETRY_COUNT", "2"))
BYBIT_TIMESTAMP_RETRY_DELAY = float(os.getenv("BYBIT_TIMESTAMP_RETRY_DELAY", "0.25"))

_last_disabled_close_attempt: dict[tuple[str, str, str], float] = {}
_trade_retries: list[dict] = []
_pending_execution_resolutions: list[dict] = []
_ops_alert_last_sent: dict[str, float] = {}
_position_monitor_state: dict[tuple[str, str, str], dict] = {}
_binance_lot_rules_cache: dict[tuple[str, str], tuple[float, float, int, float, float]] = {}


def _resolve_balance_backfill_log_path() -> Path | None:
    candidates: list[Path] = []
    if BALANCE_BACKFILL_LOG_PATH:
        candidates.append(Path(BALANCE_BACKFILL_LOG_PATH))
    candidates.extend(
        [
            Path("/var/log/stratbot/watcher.log"),
            Path("watcher.log"),
        ]
    )
    for path in candidates:
        try:
            if path.exists() and path.is_file():
                return path
        except Exception:
            continue
    return None


def _trade_state_key(user_id: str, exchange: str, symbol: str) -> str:
    return f"{str(user_id)}|{str(exchange).lower()}|{str(symbol)}"


def _load_open_trade_state() -> None:
    global _open_trade_state
    try:
        if OPEN_TRADE_STATE_PATH.exists():
            data = json.loads(OPEN_TRADE_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _open_trade_state = data
                return
    except Exception as exc:
        print(f"[WATCHER][TRADES_TABLE][WARN] No se pudo cargar open_trade_state: {exc}")
    _open_trade_state = {}


def _save_open_trade_state() -> None:
    try:
        OPEN_TRADE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        OPEN_TRADE_STATE_PATH.write_text(json.dumps(_open_trade_state, indent=2), encoding="utf-8")
    except Exception as exc:
        print(f"[WATCHER][TRADES_TABLE][WARN] No se pudo guardar open_trade_state: {exc}")


def _min_trades_table_start_utc() -> datetime:
    try:
        parsed = normalize_close_ts(TRADES_TABLE_MIN_START_LOCAL)
        if parsed is not None:
            return parsed
    except Exception:
        pass
    return datetime(2026, 4, 1, 3, 0, 0, tzinfo=timezone.utc)


def _ts_from_exchange_payload(payload: dict | None) -> datetime:
    if not isinstance(payload, dict):
        return datetime.now(timezone.utc)
    for key in ("updateTime", "transactTime", "time", "createdTime", "updatedTime"):
        raw = payload.get(key)
        if raw in (None, ""):
            continue
        try:
            ms = int(float(raw))
            if ms > 0:
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        except Exception:
            continue
    return datetime.now(timezone.utc)


def _safe_exec_price(value) -> float | None:
    try:
        v = float(value)
        if not math.isfinite(v) or v <= 0:
            return None
        return v
    except Exception:
        return None


def _safe_exec_qty(value) -> float | None:
    try:
        v = float(value)
        if not math.isfinite(v) or v <= 0:
            return None
        return v
    except Exception:
        return None


def _pending_resolution_id(
    *,
    kind: str,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    order_id: str | None,
    client_order_id: str | None,
    close_reason: str | None,
) -> str:
    payload = "|".join(
        [
            str(kind or ""),
            str(user_id or ""),
            str(exchange or "").lower(),
            str(symbol or ""),
            str(direction or "").lower(),
            str(order_id or ""),
            str(client_order_id or ""),
            str(close_reason or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _load_pending_execution_resolutions() -> None:
    global _pending_execution_resolutions
    try:
        if PENDING_EXECUTION_RESOLUTIONS_PATH.exists():
            data = json.loads(PENDING_EXECUTION_RESOLUTIONS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                _pending_execution_resolutions = data
                return
    except Exception as exc:
        print(f"[WATCHER][TRADES_TABLE][WARN] No se pudo cargar pending_execution_resolutions: {exc}")
    _pending_execution_resolutions = []


def _save_pending_execution_resolutions() -> None:
    try:
        PENDING_EXECUTION_RESOLUTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        PENDING_EXECUTION_RESOLUTIONS_PATH.write_text(
            json.dumps(_pending_execution_resolutions, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"[WATCHER][TRADES_TABLE][WARN] No se pudo guardar pending_execution_resolutions: {exc}")


def _resolve_binance_fill_sync(
    *,
    cred: ExchangeCredential,
    symbol: str,
    order_id: str | None,
    client_order_id: str | None,
    side: str | None,
    qty_hint: float | None,
    wait_seconds: float,
) -> dict:
    deadline = time.time() + max(wait_seconds, 0.0)
    api_key, api_secret = cred.resolve_keys(os.environ)
    base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
    client = (
        UMFutures(key=api_key, secret=api_secret, base_url=base_url, timeout=BINANCE_HTTP_TIMEOUT)
        if base_url
        else UMFutures(key=api_key, secret=api_secret, timeout=BINANCE_HTTP_TIMEOUT)
    )
    oid_int = None
    if order_id not in (None, ""):
        try:
            oid_int = int(float(order_id))
        except Exception:
            oid_int = None

    while True:
        order_payload = None
        try:
            query_kwargs = {"symbol": symbol, "recvWindow": BINANCE_RECV_WINDOW_MS}
            if oid_int is not None:
                query_kwargs["orderId"] = oid_int
            elif client_order_id:
                query_kwargs["origClientOrderId"] = str(client_order_id)
            if "orderId" in query_kwargs or "origClientOrderId" in query_kwargs:
                order_payload = _retry_binance_timestamp(
                    lambda: client.query_order(**query_kwargs),
                    "fill_query_order",
                )
        except Exception as exc:
            return {"ok": False, "pending": False, "detail": f"binance_query_order_error:{exc}"}

        fills: list[dict] = []
        try:
            trades_kwargs = {"symbol": symbol, "recvWindow": BINANCE_RECV_WINDOW_MS}
            if oid_int is not None:
                trades_kwargs["orderId"] = oid_int
            trades_raw = _retry_binance_timestamp(
                lambda: client.get_account_trades(**trades_kwargs),
                "fill_account_trades",
            )
            if isinstance(trades_raw, list):
                for row in trades_raw:
                    if not isinstance(row, dict):
                        continue
                    row_oid = str(row.get("orderId") or "")
                    row_coid = str(row.get("clientOrderId") or "")
                    if order_id and row_oid and row_oid == str(order_id):
                        fills.append(row)
                        continue
                    if client_order_id and row_coid and row_coid == str(client_order_id):
                        fills.append(row)
                        continue
        except Exception as exc:
            return {"ok": False, "pending": False, "detail": f"binance_account_trades_error:{exc}"}

        if fills:
            qty_total = 0.0
            quote_total = 0.0
            fee_total = 0.0
            ts_max = None
            for row in fills:
                qty = _safe_exec_qty(row.get("qty"))
                px = _safe_exec_price(row.get("price"))
                if qty is None or px is None:
                    continue
                qty_total += qty
                quote_total += px * qty
                fee = _safe_exec_price(row.get("commission"))
                if fee is not None:
                    fee_total += fee
                t_raw = row.get("time")
                try:
                    t_ms = int(float(t_raw))
                    ts_row = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)
                    ts_max = ts_row if ts_max is None else max(ts_max, ts_row)
                except Exception:
                    pass
            if qty_total > 0 and quote_total > 0:
                return {
                    "ok": True,
                    "pending": False,
                    "price": quote_total / qty_total,
                    "qty": qty_total,
                    "ts": ts_max or datetime.now(timezone.utc),
                    "fees_usdt": fee_total,
                    "detail": "binance_account_trades",
                }

        if isinstance(order_payload, dict):
            status = str(order_payload.get("status") or "").upper()
            avg_price = _safe_exec_price(order_payload.get("avgPrice"))
            exec_qty = _safe_exec_qty(order_payload.get("executedQty"))
            if status == "FILLED" and avg_price is not None and exec_qty is not None:
                return {
                    "ok": True,
                    "pending": False,
                    "price": avg_price,
                    "qty": exec_qty,
                    "ts": _ts_from_exchange_payload(order_payload),
                    "fees_usdt": 0.0,
                    "detail": "binance_query_order_filled",
                }

        if time.time() >= deadline:
            return {"ok": False, "pending": True, "detail": "binance_fill_not_confirmed"}
        time.sleep(max(TRADES_FILL_RESOLVE_POLL_SECONDS, 0.2))


def _resolve_bybit_fill_sync(
    *,
    cred: ExchangeCredential,
    symbol: str,
    order_id: str | None,
    client_order_id: str | None,
    side: str | None,
    qty_hint: float | None,
    wait_seconds: float,
) -> dict:
    from pybit.unified_trading import HTTP  # type: ignore

    deadline = time.time() + max(wait_seconds, 0.0)
    api_key, api_secret = cred.resolve_keys(os.environ)
    is_testnet = cred.environment != ExchangeEnvironment.LIVE
    domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
    client = (
        HTTP(
            api_key=api_key,
            api_secret=api_secret,
            testnet=False,
            domain=domain_env,
            recv_window=BYBIT_RECV_WINDOW_MS,
            timeout=BYBIT_HTTP_TIMEOUT,
        )
        if domain_env
        else HTTP(
            api_key=api_key,
            api_secret=api_secret,
            testnet=is_testnet,
            recv_window=BYBIT_RECV_WINDOW_MS,
            timeout=BYBIT_HTTP_TIMEOUT,
        )
    )

    while True:
        fills: list[dict] = []
        try:
            exec_kwargs = {"category": "linear", "symbol": symbol, "limit": 200}
            if order_id:
                exec_kwargs["orderId"] = str(order_id)
            if client_order_id:
                exec_kwargs["orderLinkId"] = str(client_order_id)
            raw = _retry_bybit_timestamp(
                lambda: client.get_executions(**exec_kwargs),
                "fill_get_executions",
            )
            rows = (raw or {}).get("result", {}).get("list") or []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    row_oid = str(row.get("orderId") or "")
                    row_coid = str(row.get("orderLinkId") or "")
                    if order_id and row_oid == str(order_id):
                        fills.append(row)
                        continue
                    if client_order_id and row_coid == str(client_order_id):
                        fills.append(row)
                        continue
        except Exception as exc:
            return {"ok": False, "pending": False, "detail": f"bybit_get_executions_error:{exc}"}

        if fills:
            qty_total = 0.0
            quote_total = 0.0
            fee_total = 0.0
            ts_max = None
            for row in fills:
                qty = _safe_exec_qty(row.get("execQty"))
                px = _safe_exec_price(row.get("execPrice"))
                if qty is None or px is None:
                    continue
                qty_total += qty
                quote_total += px * qty
                fee = _safe_exec_price(row.get("execFee"))
                if fee is not None:
                    fee_total += fee
                t_raw = row.get("execTime")
                try:
                    t_ms = int(float(t_raw))
                    ts_row = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)
                    ts_max = ts_row if ts_max is None else max(ts_max, ts_row)
                except Exception:
                    pass
            if qty_total > 0 and quote_total > 0:
                return {
                    "ok": True,
                    "pending": False,
                    "price": quote_total / qty_total,
                    "qty": qty_total,
                    "ts": ts_max or datetime.now(timezone.utc),
                    "fees_usdt": fee_total,
                    "detail": "bybit_get_executions",
                }

        if time.time() >= deadline:
            return {"ok": False, "pending": True, "detail": "bybit_fill_not_confirmed"}
        time.sleep(max(TRADES_FILL_RESOLVE_POLL_SECONDS, 0.2))


def _resolve_order_fill_sync(
    *,
    user_id: str,
    exchange: str,
    cred: ExchangeCredential,
    symbol: str,
    side: str | None,
    order_id: str | None,
    client_order_id: str | None,
    qty_hint: float | None,
    wait_seconds: float,
    phase: str,
) -> dict:
    ex_l = str(exchange).lower()
    print(
        f"[WATCHER][TRADES_FILL_RESOLVE_START] phase={phase} user={user_id} ex={exchange} "
        f"symbol={symbol} order_id={order_id} client_order_id={client_order_id}"
    )
    try:
        if ex_l == "binance":
            out = _resolve_binance_fill_sync(
                cred=cred,
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                side=side,
                qty_hint=qty_hint,
                wait_seconds=wait_seconds,
            )
        elif ex_l == "bybit":
            out = _resolve_bybit_fill_sync(
                cred=cred,
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                side=side,
                qty_hint=qty_hint,
                wait_seconds=wait_seconds,
            )
        else:
            out = {"ok": False, "pending": False, "detail": f"unsupported_exchange:{exchange}"}
    except Exception as exc:
        out = {"ok": False, "pending": False, "detail": f"resolver_exception:{exc}"}

    tag = "OK" if out.get("ok") else ("PENDING" if out.get("pending") else "FAIL")
    print(
        f"[WATCHER][TRADES_FILL_RESOLVE_{tag}] phase={phase} user={user_id} ex={exchange} "
        f"symbol={symbol} order_id={order_id} detail={out.get('detail')}"
    )
    return out


def _enqueue_pending_execution_resolution(
    *,
    kind: str,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    side: str | None,
    order_id: str | None,
    client_order_id: str | None,
    source_event: str | None,
    close_reason: str | None,
    event_ts,
    qty_hint: float | None,
) -> None:
    rid = _pending_resolution_id(
        kind=kind,
        user_id=user_id,
        exchange=exchange,
        symbol=symbol,
        direction=direction,
        order_id=order_id,
        client_order_id=client_order_id,
        close_reason=close_reason,
    )
    now_ts = time.time()
    existing = next((x for x in _pending_execution_resolutions if x.get("id") == rid), None)
    if existing is not None:
        existing["updated_ts"] = now_ts
        existing["next_retry_ts"] = min(
            float(existing.get("next_retry_ts") or now_ts),
            now_ts + max(TRADES_FILL_RESOLVE_POLL_SECONDS, 0.5),
        )
        _save_pending_execution_resolutions()
        return

    item = {
        "id": rid,
        "kind": str(kind),
        "user_id": str(user_id),
        "exchange": str(exchange).lower(),
        "symbol": str(symbol),
        "direction": str(direction).lower(),
        "side": str(side or ""),
        "order_id": str(order_id or ""),
        "client_order_id": str(client_order_id or ""),
        "source_event": str(source_event or ""),
        "close_reason": str(close_reason or ""),
        "event_ts": str(event_ts or ""),
        "qty_hint": float(qty_hint) if qty_hint is not None else None,
        "attempt": 0,
        "created_ts": now_ts,
        "updated_ts": now_ts,
        "next_retry_ts": now_ts + max(TRADES_FILL_RESOLVE_POLL_SECONDS, 0.5),
    }
    _pending_execution_resolutions.append(item)
    _save_pending_execution_resolutions()
    print(
        f"[WATCHER][TRADES_FILL_RESOLVE_PENDING] kind={kind} user={user_id} ex={exchange} "
        f"symbol={symbol} order_id={order_id} client_order_id={client_order_id}"
    )


def _set_open_trade_state(
    *,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    entry_price_real: float | None,
    entry_qty_real: float | None,
    entry_ts_real: datetime | None,
    entry_order_id: str | None,
    entry_client_order_id: str | None,
    source_event: str | None,
) -> bool:
    if entry_price_real is None or entry_qty_real is None or entry_ts_real is None:
        return False
    key = _trade_state_key(user_id, exchange, symbol)
    _open_trade_state[key] = {
        "user_id": str(user_id),
        "exchange": str(exchange).lower(),
        "symbol": str(symbol),
        "direction": str(direction).lower(),
        "entry_price_real": float(entry_price_real),
        "entry_qty_real": float(entry_qty_real),
        "entry_ts_real": entry_ts_real.astimezone(timezone.utc).isoformat(),
        "entry_order_id": str(entry_order_id or ""),
        "entry_client_order_id": str(entry_client_order_id or ""),
        "source_event": str(source_event or ""),
        "updated_ts": datetime.now(timezone.utc).isoformat(),
    }
    _save_open_trade_state()
    return True


def _clear_open_trade_state(user_id: str, exchange: str, symbol: str) -> None:
    key = _trade_state_key(user_id, exchange, symbol)
    if key in _open_trade_state:
        _open_trade_state.pop(key, None)
        _save_open_trade_state()


def _register_trades_table_close(
    *,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    close_reason: str,
    exit_price: float | None,
    exit_qty: float | None,
    exit_ts: datetime | None,
    fees_usdt: float | None,
) -> None:
    key = _trade_state_key(user_id, exchange, symbol)
    state = _open_trade_state.get(key)
    if not isinstance(state, dict):
        print(
            f"[WATCHER][TRADES_TABLE_SKIP_INCOMPLETE] user={user_id} ex={exchange} symbol={symbol} "
            f"reason=open_state_missing"
        )
        return
    entry_price = _safe_exec_price(state.get("entry_price_real"))
    entry_qty = _safe_exec_qty(state.get("entry_qty_real"))
    entry_ts = normalize_close_ts(state.get("entry_ts_real"))
    exit_price_v = _safe_exec_price(exit_price)
    exit_qty_v = _safe_exec_qty(exit_qty)
    exit_ts_v = normalize_close_ts(exit_ts) if isinstance(exit_ts, (str, datetime)) else None
    if exit_ts_v is None:
        exit_ts_v = datetime.now(timezone.utc)
    fees_v = float(fees_usdt or 0.0)

    if None in (entry_price, entry_qty, entry_ts, exit_price_v, exit_qty_v):
        print(
            f"[WATCHER][TRADES_TABLE_SKIP_INCOMPLETE] user={user_id} ex={exchange} symbol={symbol} "
            f"reason=missing_fill_data"
        )
        return
    if exit_ts_v < _min_trades_table_start_utc():
        return

    qty_closed = min(float(entry_qty), float(exit_qty_v))
    if qty_closed <= 0:
        print(
            f"[WATCHER][TRADES_TABLE_SKIP_INCOMPLETE] user={user_id} ex={exchange} symbol={symbol} "
            f"reason=qty_non_positive"
        )
        return

    dir_l = str(direction).lower()
    if dir_l == "long":
        pnl_pct = (float(exit_price_v) - float(entry_price)) / float(entry_price)
    elif dir_l == "short":
        pnl_pct = (float(entry_price) - float(exit_price_v)) / float(entry_price)
    else:
        print(
            f"[WATCHER][TRADES_TABLE_SKIP_INCOMPLETE] user={user_id} ex={exchange} symbol={symbol} "
            f"reason=direction_invalid"
        )
        return
    pnl_usdt = (pnl_pct * float(entry_price) * qty_closed) - fees_v
    appended = TRADES_TABLE_LEDGER.append_trade(
        user_id=user_id,
        exchange=exchange,
        symbol=symbol,
        entry_ts=entry_ts,
        exit_ts=exit_ts_v,
        entry_price=float(entry_price),
        exit_price=float(exit_price_v),
        quantity=qty_closed,
        pnl_pct=pnl_pct,
        pnl_usdt=pnl_usdt,
        fees_usdt=fees_v,
        close_reason=close_reason,
        source="live",
        confidence="strict",
    )
    if not appended:
        print(
            f"[WATCHER][TRADES_TABLE_SKIP_INCOMPLETE] user={user_id} ex={exchange} symbol={symbol} "
            f"reason=append_rejected"
        )
        return

    remaining_qty = max(float(entry_qty) - qty_closed, 0.0)
    if remaining_qty > 0:
        state["entry_qty_real"] = remaining_qty
        state["updated_ts"] = datetime.now(timezone.utc).isoformat()
        _open_trade_state[key] = state
    else:
        _open_trade_state.pop(key, None)
    _save_open_trade_state()


def _build_client_order_id(prefix: str, user_id: str, exchange: str, symbol: str, side: str) -> str:
    seed = int(time.time() * 1000)
    digest = hashlib.sha1(f"{user_id}|{exchange}|{symbol}|{side}|{seed}".encode("utf-8")).hexdigest()[:8]
    short_user = "".join(ch for ch in str(user_id).lower() if ch.isalnum())[:6] or "usr"
    short_sym = "".join(ch for ch in str(symbol).upper() if ch.isalnum())[:6] or "SYM"
    side_token = "B" if str(side).upper().startswith("B") else "S"
    return f"{prefix}-{short_user}-{short_sym}-{side_token}-{seed % 1000000:06d}{digest}"


def _record_balance_close(
    *,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    reason: str,
    close_ts=None,
    source: str = "live",
) -> None:
    ts = normalize_close_ts(close_ts) or datetime.now(timezone.utc)
    try:
        ok = BALANCE_LEDGER.append_close(
            close_ts=ts,
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            direction=direction,
            entry_price=float(entry_price),
            exit_price=float(exit_price),
            reason=reason,
            source=source,
        )
    except Exception as exc:
        print(
            f"[WATCHER][BALANCE][WARN] No se pudo registrar cierre user={user_id} ex={exchange} "
            f"symbol={symbol} reason={reason}: {exc}"
        )
        return
    if ok:
        print(
            f"[WATCHER][BALANCE][LEDGER] user={user_id} ex={exchange} symbol={symbol} "
            f"dir={direction} entry={entry_price:.6f} exit={exit_price:.6f} reason={reason}"
        )


def _run_balance_backfill_once() -> None:
    path = _resolve_balance_backfill_log_path()
    if path is None:
        print("[WATCHER][BALANCE][BACKFILL] log_path=none skipped")
        return
    try:
        stats = BALANCE_LEDGER.backfill_from_log(path)
        print(
            f"[WATCHER][BALANCE][BACKFILL] log={path} processed={stats.get('processed', 0)} "
            f"appended={stats.get('appended', 0)} skipped={stats.get('skipped', 0)}"
        )
    except Exception as exc:
        print(f"[WATCHER][BALANCE][BACKFILL][WARN] Falló backfill desde {path}: {exc}")


def _is_binance_timestamp_error(exc: Exception) -> bool:
    msg = str(exc)
    return "-1021" in msg or "outside of the recvWindow" in msg or "recvWindow" in msg


def _is_bybit_timestamp_error(exc: Exception) -> bool:
    msg = str(exc)
    return "ErrCode: 10002" in msg or "recv_window" in msg.lower() or "server timestamp" in msg.lower()


def _retry_timestamp_call(func, retries: int, delay: float, checker, tag: str):
    retries = max(int(retries), 0)
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as exc:
            if not checker(exc):
                raise
            if attempt < retries:
                print(f"[EXCHANGE][TIME][{tag}][RETRY] attempt={attempt + 1}/{retries + 1} err={exc}")
                time.sleep(max(float(delay), 0.0))
                continue
            print(f"[EXCHANGE][TIME][{tag}][FAIL] err={exc}")
            raise
    return func()


def _retry_binance_timestamp(func, action: str):
    return _retry_timestamp_call(
        func,
        BINANCE_TIMESTAMP_RETRY_COUNT,
        BINANCE_TIMESTAMP_RETRY_DELAY,
        _is_binance_timestamp_error,
        f"BINANCE::{action}",
    )


def _retry_bybit_timestamp(func, action: str):
    return _retry_timestamp_call(
        func,
        BYBIT_TIMESTAMP_RETRY_COUNT,
        BYBIT_TIMESTAMP_RETRY_DELAY,
        _is_bybit_timestamp_error,
        f"BYBIT::{action}",
    )


def _is_transient_market_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and (status_code == 429 or status_code >= 500):
        return True
    msg = str(exc).lower()
    markers = (
        "timeout",
        "timed out",
        "read timed out",
        "connection reset",
        "remote disconnected",
        "temporary failure in name resolution",
        "name or service not known",
        "too many requests",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "http 429",
        "http 500",
        "http 502",
        "http 503",
        "http 504",
    )
    return any(token in msg for token in markers)


def _retry_market_call(func, *, exchange: str, action: str):
    retries = max(MARK_PRICE_RETRY_COUNT, 0)
    base_delay = max(MARK_PRICE_RETRY_DELAY, 0.05)
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as exc:
            retryable = _is_transient_market_error(exc) or _is_binance_timestamp_error(exc) or _is_bybit_timestamp_error(exc)
            if retryable and attempt < retries:
                delay = base_delay * (attempt + 1)
                print(
                    f"[WATCHER][MARK][RETRY] ex={exchange} action={action} "
                    f"attempt={attempt + 1}/{retries + 1} delay={delay:.2f}s err={exc}"
                )
                time.sleep(delay)
                continue
            print(
                f"[WATCHER][MARK][FAIL] ex={exchange} action={action} "
                f"attempts={attempt + 1} retryable={retryable} err={exc}"
            )
            return None


def _floor_to_step(value: float, step: float) -> float:
    try:
        dv = Decimal(str(value))
        ds = Decimal(str(step))
        if ds <= 0:
            return float(value)
        out = (dv / ds).to_integral_value(rounding=ROUND_DOWN) * ds
        return float(out)
    except Exception:
        if step <= 0:
            return value
        return math.floor(value / step) * step


def _ceil_to_step(value: float, step: float) -> float:
    try:
        dv = Decimal(str(value))
        ds = Decimal(str(step))
        if ds <= 0:
            return float(value)
        out = (dv / ds).to_integral_value(rounding=ROUND_UP) * ds
        return float(out)
    except Exception:
        if step <= 0:
            return value
        return math.ceil(value / step) * step


def _notional_to_qty_binance(
    *,
    target_notional: float,
    price: float,
    step: float,
    min_qty: float,
    min_notional: float,
    overshoot_cap: float = 0.03,
) -> dict:
    if target_notional <= 0:
        raise ValueError("target_notional inválido")
    if price <= 0:
        raise ValueError("price inválido")
    if step <= 0:
        raise ValueError("step inválido")

    raw_qty = target_notional / price
    floor_qty = _floor_to_step(raw_qty, step)
    ceil_qty = _ceil_to_step(raw_qty, step)

    effective_min_notional = min_notional if min_notional > 0 else 0.0
    min_notional_qty = _ceil_to_step(effective_min_notional / price, step) if effective_min_notional > 0 else 0.0
    effective_min_qty = max(min_qty, min_notional_qty)

    candidate_qtys = []
    for q in (floor_qty, ceil_qty):
        if q >= effective_min_qty - 1e-12:
            candidate_qtys.append(q)
    candidate_qtys = sorted(set(candidate_qtys))

    if not candidate_qtys:
        selected_qty = effective_min_qty
        selected_notional = selected_qty * price
        deviation_pct = ((selected_notional - target_notional) / target_notional) if target_notional > 0 else 0.0
        return {
            "raw_qty": raw_qty,
            "qty_floor": floor_qty,
            "qty_ceil": ceil_qty,
            "qty_selected": selected_qty,
            "selected_notional": selected_notional,
            "deviation_pct": deviation_pct,
            "effective_min_qty": effective_min_qty,
            "min_notional": effective_min_notional,
            "overshoot_for_constraints": True,
        }

    candidates = []
    for q in candidate_qtys:
        n = q * price
        candidates.append(
            {
                "qty": q,
                "notional": n,
                "abs_error": abs(n - target_notional),
            }
        )
    candidates.sort(key=lambda c: (c["abs_error"], c["notional"]))
    selected = candidates[0]

    cap_notional = target_notional * (1 + max(overshoot_cap, 0.0))
    overshoot_for_constraints = False
    if selected["notional"] > cap_notional + 1e-12:
        within_cap = [c for c in candidates if c["notional"] <= cap_notional + 1e-12]
        if within_cap:
            within_cap.sort(key=lambda c: (c["abs_error"], c["notional"]))
            selected = within_cap[0]
        else:
            selected = min(candidates, key=lambda c: c["notional"])
            overshoot_for_constraints = True

    deviation_pct = ((selected["notional"] - target_notional) / target_notional) if target_notional > 0 else 0.0
    return {
        "raw_qty": raw_qty,
        "qty_floor": floor_qty,
        "qty_ceil": ceil_qty,
        "qty_selected": selected["qty"],
        "selected_notional": selected["notional"],
        "deviation_pct": deviation_pct,
        "effective_min_qty": effective_min_qty,
        "min_notional": effective_min_notional,
        "overshoot_for_constraints": overshoot_for_constraints,
    }


def _step_decimals(step_raw) -> int:
    try:
        s = str(step_raw).strip()
        if not s:
            return 3
        if "." not in s:
            return 0
        frac = s.rstrip("0").split(".", 1)[1]
        return max(len(frac), 0)
    except Exception:
        return 3


def _binance_lot_rules(cred: ExchangeCredential, symbol: str) -> tuple[float, float, int, float]:
    """
    Devuelve (step_size, min_qty, decimals, min_notional) para un símbolo de Binance Futures.
    Usa cache corta para evitar exchange_info en cada orden.
    """
    env_key = "testnet" if cred.environment == ExchangeEnvironment.TESTNET else "live"
    key = (env_key, str(symbol).upper())
    now = time.time()
    cached = _binance_lot_rules_cache.get(key)
    if cached and (now - cached[4]) < 300:
        return cached[0], cached[1], cached[2], cached[3]

    default_step, default_min, default_decimals, default_min_notional = 0.001, 0.001, 3, 100.0
    try:
        api_key, api_secret = cred.resolve_keys(os.environ)
        base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
        client = UMFutures(key=api_key, secret=api_secret, base_url=base_url, timeout=BINANCE_HTTP_TIMEOUT) if base_url else UMFutures(
            key=api_key, secret=api_secret, timeout=BINANCE_HTTP_TIMEOUT
        )
        info = _retry_market_call(
            lambda: client.exchange_info(),
            exchange="binance",
            action="exchange_info",
        )
        symbols = (info or {}).get("symbols") or []
        row = next((s for s in symbols if str(s.get("symbol", "")).upper() == str(symbol).upper()), None)
        if not row:
            raise RuntimeError(f"symbol_not_found:{symbol}")
        filters = row.get("filters") or []
        lot = next((f for f in filters if f.get("filterType") == "MARKET_LOT_SIZE"), None) or next(
            (f for f in filters if f.get("filterType") == "LOT_SIZE"),
            None,
        )
        if not lot:
            raise RuntimeError("lot_filter_missing")
        notional_filter = next(
            (f for f in filters if f.get("filterType") in {"MIN_NOTIONAL", "NOTIONAL"}),
            None,
        )
        step_raw = lot.get("stepSize") or default_step
        min_raw = lot.get("minQty") or default_min
        min_notional_raw = default_min_notional
        if isinstance(notional_filter, dict):
            min_notional_raw = (
                notional_filter.get("notional")
                or notional_filter.get("minNotional")
                or default_min_notional
            )
        step = float(step_raw)
        min_qty = float(min_raw)
        min_notional = float(min_notional_raw)
        if step <= 0:
            step = default_step
        if min_qty <= 0:
            min_qty = default_min
        if min_notional <= 0:
            min_notional = default_min_notional
        decimals = _step_decimals(step_raw)
        _binance_lot_rules_cache[key] = (step, min_qty, decimals, min_notional, now)
        return step, min_qty, decimals, min_notional
    except Exception as exc:
        print(
            f"[WATCHER][WARN] Binance lot rules fallback user_env={env_key} symbol={symbol} "
            f"step={default_step} min={default_min} min_notional={default_min_notional} reason={exc}"
        )
        return default_step, default_min, default_decimals, default_min_notional


def _load_manager() -> AccountManager | None:
    global _account_manager, _accounts_mtime, _executor
    if not TRADING_ENABLED:
        return None
    try:
        path = Path(TRADING_ACCOUNTS_FILE)
        try:
            current_mtime = path.stat().st_mtime
        except FileNotFoundError:
            current_mtime = None

        if not ACCOUNTS_AUTO_RELOAD and _account_manager is not None:
            return _account_manager

        if _account_manager is not None and _accounts_mtime is not None and current_mtime == _accounts_mtime:
            return _account_manager

        _account_manager = AccountManager.from_file(path)
        _accounts_mtime = current_mtime
        _executor = None
        print(f"[WATCHER][INFO] Cuentas recargadas desde {path} (mtime={_accounts_mtime})")
        return _account_manager
    except Exception as exc:
        print(f"[WATCHER][WARN] No se pudo inicializar AccountManager ({exc}); modo trading deshabilitado.")
        return None


def _bybit_position_amount(cred: ExchangeCredential, symbol: str) -> float | None:
    """
    Devuelve cantidad firmada (long >0, short <0) para Bybit linear.
    Si hay error en la API devuelve None (posición desconocida).
    """
    try:
        from pybit.unified_trading import HTTP  # type: ignore

        api_key, api_secret = cred.resolve_keys(os.environ)
        is_testnet = cred.environment != ExchangeEnvironment.LIVE
        domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
        client = (
            HTTP(
                api_key=api_key,
                api_secret=api_secret,
                testnet=False,
                domain=domain_env,
                recv_window=BYBIT_RECV_WINDOW_MS,
                timeout=BYBIT_HTTP_TIMEOUT,
            )
            if domain_env
            else HTTP(
                api_key=api_key,
                api_secret=api_secret,
                testnet=is_testnet,
                recv_window=BYBIT_RECV_WINDOW_MS,
                timeout=BYBIT_HTTP_TIMEOUT,
            )
        )
        raw = _retry_bybit_timestamp(
            lambda: client.get_positions(category="linear", symbol=symbol),
            "get_positions",
        )
        items = raw.get("result", {}).get("list") or []
        if not items:
            return 0.0
        # Unified: size + side
        pos = items[0]
        size = float(pos.get("size") or 0.0)
        side = str(pos.get("side") or "").lower()
        if size == 0:
            return 0.0
        return size if side == "buy" else -size
    except Exception:
        return None


def _bybit_mark_price(cred: ExchangeCredential, symbol: str) -> float | None:
    """
    Obtiene mark price desde Bybit (fallback: last price).
    """
    try:
        from pybit.unified_trading import HTTP  # type: ignore

        api_key, api_secret = cred.resolve_keys(os.environ)
        is_testnet = cred.environment != ExchangeEnvironment.LIVE
        domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
        client = (
            HTTP(
                api_key=api_key,
                api_secret=api_secret,
                testnet=False,
                domain=domain_env,
                recv_window=BYBIT_RECV_WINDOW_MS,
                timeout=BYBIT_HTTP_TIMEOUT,
            )
            if domain_env
            else HTTP(
                api_key=api_key,
                api_secret=api_secret,
                testnet=is_testnet,
                recv_window=BYBIT_RECV_WINDOW_MS,
                timeout=BYBIT_HTTP_TIMEOUT,
            )
        )
        raw = _retry_market_call(
            lambda: client.get_tickers(category="linear", symbol=symbol),
            exchange="bybit",
            action="get_tickers",
        )
        if not raw:
            return None
        items = raw.get("result", {}).get("list") or []
        if not items:
            return None
        data = items[0] or {}
        for key in ("markPrice", "indexPrice", "lastPrice"):
            val = data.get(key)
            if val:
                return float(val)
    except Exception:
        return None
    return None


def _binance_mark_price(cred: ExchangeCredential, symbol: str) -> float | None:
    """
    Obtiene mark price desde Binance (preferido para triggers en tiempo real).
    """
    try:
        api_key, api_secret = cred.resolve_keys(os.environ)
    except Exception:
        api_key = None
        api_secret = None
    try:
        base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
        if api_key and api_secret:
            client = UMFutures(key=api_key, secret=api_secret, base_url=base_url, timeout=BINANCE_HTTP_TIMEOUT) if base_url else UMFutures(
                key=api_key, secret=api_secret, timeout=BINANCE_HTTP_TIMEOUT
            )
        else:
            client = UMFutures(base_url=base_url, timeout=BINANCE_HTTP_TIMEOUT) if base_url else UMFutures(timeout=BINANCE_HTTP_TIMEOUT)
        data = _retry_market_call(
            lambda: client.mark_price(symbol=symbol),
            exchange="binance",
            action="mark_price",
        )
        if isinstance(data, dict):
            price = data.get("markPrice") or data.get("indexPrice")
            if price:
                return float(price)
    except Exception:
        return None
    return None


def _close_disabled_accounts_positions() -> None:
    """
    Si un usuario está enabled=false, intenta cerrar posiciones abiertas en todos sus exchanges.
    Solo actúa si WATCHER_DISABLED_AUTO_CLOSE=true.
    """
    if not TRADING_ENABLED or not DISABLED_ACCOUNTS_AUTO_CLOSE:
        return
    manager = _load_manager()
    if manager is None:
        return
    executor = _resolve_executor()
    if executor is None:
        return

    now = time.time()
    for account in manager.list_accounts():
        if account.enabled:
            continue
        for exchange, cred in (account.exchanges or {}).items():
            if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                continue
            symbol = (cred.extra or {}).get("symbol") or SYMBOL_DISPLAY.replace(".P", "")
            try:
                pos_amt = _coerce_position(_current_position(account.user_id, exchange, symbol))
            except Exception:
                pos_amt = None
            if pos_amt is None or abs(pos_amt) < 1e-8:
                continue

            key = (account.user_id, exchange.lower(), str(symbol))
            last = _last_disabled_close_attempt.get(key, 0.0)
            if now - last < max(DISABLED_ACCOUNTS_CLOSE_POLL_SECONDS, 10.0):
                continue
            _last_disabled_close_attempt[key] = now

            qty = abs(pos_amt)
            side = OrderSide.SELL if pos_amt > 0 else OrderSide.BUY
            order = OrderRequest(
                symbol=symbol,
                side=side,
                type=OrderType.MARKET,
                quantity=qty,
                price=None,
                time_in_force=TimeInForce.GTC,
                reduce_only=True,
                extra_params={
                    "source_event": "disabled_auto_close",
                    "account": account.user_id,
                    "exchange": exchange,
                },
            )
            try:
                resp = executor.execute(account.user_id, exchange, order, dry_run=TRADING_DRY_RUN)
                print(
                    f"[WATCHER][AUTO_CLOSE_DISABLED] user={account.user_id} ex={exchange} symbol={symbol} "
                    f"qty={qty} side={side.value} success={resp.success} status={resp.status}"
                )
            except Exception as exc:
                print(f"[WATCHER][WARN] Auto-cierre por disabled falló user={account.user_id} ex={exchange}: {exc}")


def _resolve_executor() -> OrderExecutor | None:
    global _executor
    manager = _load_manager()
    if manager is None:
        return None
    if _executor is not None:
        return _executor
    _executor = OrderExecutor(manager)
    return _executor


def _load_thresholds():
    """
    Carga umbrales pendientes desde disco (si existe).
    """
    global _thresholds
    try:
        if THRESHOLDS_PATH.exists():
            data = json.loads(THRESHOLDS_PATH.read_text())
            if isinstance(data, list):
                _thresholds = data
    except Exception:
        _thresholds = []


def _save_thresholds():
    """
    Guarda umbrales pendientes en disco para persistencia simple.
    """
    try:
        THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
        THRESHOLDS_PATH.write_text(json.dumps(_thresholds, indent=2))
    except Exception:
        pass


_load_thresholds()
_load_open_trade_state()
_load_pending_execution_resolutions()


def _load_trade_retries() -> None:
    global _trade_retries
    try:
        if POSITION_GUARD_QUEUE_PATH.exists():
            data = json.loads(POSITION_GUARD_QUEUE_PATH.read_text())
            if isinstance(data, list):
                _trade_retries = data
                return
    except Exception as exc:
        print(f"[WATCHER][RETRY][WARN] No se pudo cargar cola de reintentos: {exc}")
    _trade_retries = []


def _save_trade_retries() -> None:
    try:
        POSITION_GUARD_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
        POSITION_GUARD_QUEUE_PATH.write_text(json.dumps(_trade_retries, indent=2))
    except Exception as exc:
        print(f"[WATCHER][RETRY][WARN] No se pudo guardar cola de reintentos: {exc}")


_load_trade_retries()


def _ops_alert_key(code: str, user_id: str | None = None, exchange: str | None = None, symbol: str | None = None) -> str:
    return "|".join([code or "ops", user_id or "-", exchange or "-", symbol or "-"])


def _send_ops_alert(
    *,
    code: str,
    message: str,
    user_id: str | None = None,
    exchange: str | None = None,
    symbol: str | None = None,
    force: bool = False,
) -> None:
    if not POSITION_MONITOR_ALERTS_ENABLED:
        return
    key = _ops_alert_key(code, user_id, exchange, symbol)
    now_ts = time.time()
    cooldown = max(POSITION_MONITOR_COOLDOWN_SECONDS, 5.0)
    last_sent = _ops_alert_last_sent.get(key, 0.0)
    if not force and (now_ts - last_sent) < cooldown:
        return
    _ops_alert_last_sent[key] = now_ts
    alert = {
        "type": "ops_warning",
        "timestamp": datetime.now(timezone.utc),
        "message": message,
        "user_id": user_id,
        "exchange": exchange,
        "symbol": symbol,
    }
    try:
        send_alerts([alert])
    except Exception as exc:
        print(f"[ALERT][WARN] Falló envío de alerta operativa code={code} ({exc})")


def _monitor_positions_health(now_ts: float) -> None:
    """
    Monitor de salud de posición por target habilitado.
    Envía alertas operativas cuando:
      - la posición queda UNKNOWN repetidamente
      - la cuenta queda FLAT más allá del grace configurado (si REQUIRE_OPEN=true)
    """
    if not TRADING_ENABLED:
        return
    manager = _load_manager()
    if manager is None:
        return

    for account in manager.list_accounts():
        if not account.enabled:
            continue
        for exchange, cred in (account.exchanges or {}).items():
            if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                continue
            symbol = (cred.extra or {}).get("symbol") or SYMBOL_DISPLAY.replace(".P", "")
            key = (account.user_id, exchange, symbol)
            state = _position_monitor_state.setdefault(
                key,
                {"unknown_streak": 0, "flat_since": None, "last_side": "unknown"},
            )

            pos_amt = _coerce_position(_current_position(account.user_id, exchange, symbol))
            side = _position_side(pos_amt)
            state["last_side"] = side

            if side == "unknown":
                state["unknown_streak"] = int(state.get("unknown_streak", 0)) + 1
                unknown_every = max(POSITION_MONITOR_UNKNOWN_ALERT_EVERY, 1)
                if state["unknown_streak"] % unknown_every == 0:
                    _send_ops_alert(
                        code="position_unknown_streak",
                        user_id=account.user_id,
                        exchange=exchange,
                        symbol=symbol,
                        message=(
                            f"⚠️ [OPS] Lectura de posición UNKNOWN repetida\n"
                            f"Cuenta: {account.user_id}/{exchange}\n"
                            f"Símbolo: {symbol}\n"
                            f"Streak: {state['unknown_streak']}\n"
                            f"Acción: se mantiene retry/guard activo."
                        ),
                    )
                continue

            state["unknown_streak"] = 0

            if side == "flat":
                if state.get("flat_since") is None:
                    state["flat_since"] = now_ts
                flat_age = now_ts - float(state["flat_since"])
                if POSITION_MONITOR_REQUIRE_OPEN and flat_age >= max(POSITION_MONITOR_FLAT_ALERT_SECONDS, 5.0):
                    _send_ops_alert(
                        code="flat_without_position",
                        user_id=account.user_id,
                        exchange=exchange,
                        symbol=symbol,
                        message=(
                            f"🚨 [OPS] Cuenta sin posición abierta\n"
                            f"Cuenta: {account.user_id}/{exchange}\n"
                            f"Símbolo: {symbol}\n"
                            f"Flat hace: {flat_age:.0f}s\n"
                            f"Acción: revisar retries/ejecución."
                        ),
                    )
            else:
                state["flat_since"] = None


def _clear_thresholds_file() -> None:
    global _thresholds
    _thresholds = []
    try:
        THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
        _save_thresholds()
        print(f"[WATCHER][THRESHOLDS] Limpiado archivo de umbrales ([]) en: {THRESHOLDS_PATH}")
    except Exception as exc:
        print(f"[WATCHER][WARN] No se pudo limpiar archivo de umbrales {THRESHOLDS_PATH}: {exc}")


def _resolve_targets() -> list[tuple[str, str]]:
    """
    Devuelve lista de (user_id, exchange) habilitados.
    Si se configuró WATCHER_TRADING_USER/EXCHANGE se usa como filtro.
    """
    manager = _load_manager()
    if manager is None:
        return []

    user_filter = TRADING_USER_ID.lower()
    if user_filter in {"", "default"}:
        user_filter = None
    exchange_filter = TRADING_EXCHANGE.lower() if TRADING_EXCHANGE else None

    targets: list[tuple[str, str]] = []
    for account in manager.list_accounts():
        if not account.enabled:
            continue
        if user_filter and account.user_id.lower() != user_filter:
            continue
        for ex_name, cred in account.exchanges.items():
            if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                continue
            if exchange_filter and ex_name.lower() != exchange_filter:
                continue
            targets.append((account.user_id, ex_name))
    return targets


def _direction_to_side(direction: str | None) -> OrderSide:
    mapping = {
        "long": OrderSide.BUY,
        "short": OrderSide.SELL,
    }
    key = (direction or "").lower()
    if key not in mapping:
        raise ValueError(f"Dirección inválida para operar: {direction}")
    return mapping[key]


def _opposite_direction(direction: str) -> str:
    if direction == "long":
        return "short"
    if direction == "short":
        return "long"
    raise ValueError(f"Dirección inválida para invertir: {direction}")


def _resolve_quantity(event: dict, notional_usdt: float | None = None) -> float:
    price = _price_from_event(event)
    # Prioridad: cantidad explícita en evento -> notional USDT (desde DashCRUD/YAML por usuario/exchange)
    qty_raw = event.get("quantity")
    if qty_raw:
        qty = float(str(qty_raw).replace(",", "."))
        if qty <= 0:
            raise ValueError("quantity debe ser > 0")
        return qty

    if notional_usdt is None or notional_usdt <= 0:
        raise ValueError("Sin notional_usdt (configurarlo por usuario/exchange en DashCRUD).")
    if price is None or price <= 0:
        raise ValueError("No se puede calcular qty desde notional: precio ausente/ inválido.")
    qty = float(notional_usdt) / float(price)
    if qty <= 0:
        raise ValueError("quantity calculada debe ser > 0")
    return qty


def _price_from_event(event: dict) -> float | None:
    # Prioridad: entry/price explícitos, luego banda de referencia y, por último, close
    for key in ("entry_price", "price", "reference_band"):
        val = event.get(key)
        if val is None:
            continue
        try:
            price = float(val)
            if price > 0:
                return price
        except Exception:
            continue
    close_price = event.get("close_price")
    if close_price is not None:
        try:
            price = float(close_price)
            if price > 0:
                return price
        except Exception:
            pass
    return None


def _compute_thresholds(
    direction: str, entry_price: float, entry_source: str = "signal"
) -> tuple[float, float | None, bool]:
    """
    Calcula SL/TP para la stable activa de bot6rangos.
    - Señal normal: SL general configurado por STRAT_STOP_LOSS_PCT y TP fijo 8%.
    - Flip legacy: conserva SL/TP de flip si alguna cola vieja lo usa.
    """
    source = str(entry_source).lower()
    sl_pct = RANGE_FLIP_STOP_LOSS_PCT if source == "flip" else LOSS_PCT
    if direction == "long":
        loss_price = entry_price * (1 - sl_pct)
    else:
        loss_price = entry_price * (1 + sl_pct)
    if source == "flip":
        partial_tp_enabled = False
        if direction == "long":
            gain_price = entry_price * (1 + RANGE_FLIP_TAKE_PROFIT_PCT)
        else:
            gain_price = entry_price * (1 - RANGE_FLIP_TAKE_PROFIT_PCT)
    else:
        partial_tp_enabled = False
        if direction == "long":
            gain_price = entry_price * (1 + SMA_STABLE_TAKE_PROFIT_PCT)
        else:
            gain_price = entry_price * (1 - SMA_STABLE_TAKE_PROFIT_PCT)
    return loss_price, gain_price, partial_tp_enabled


def _compute_post_tp_loss_price(direction: str, base_price: float) -> float:
    """
    SL para remanente tras TP parcial:
    - Long: -3% desde base_price
    - Short: +3% desde base_price
    """
    if direction == "long":
        return base_price * (1 - POST_TP_SL_PCT)
    return base_price * (1 + POST_TP_SL_PCT)


def _register_threshold(
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    entry_price: float,
    signal_direction: str | None,
    entry_source: str = "signal",
    position_qty_ref: float | None = None,
):
    """
    Registra umbral de cierre (-2%) para una nueva operación.
    Reemplaza cualquier registro previo del mismo usuario/exchange/símbolo.
    """
    global _thresholds
    loss_price, gain_price, partial_tp_enabled = _compute_thresholds(direction, entry_price, entry_source)
    # filtra previos
    _thresholds = [
        th
        for th in _thresholds
        if not (
            th.get("user_id") == user_id
            and th.get("exchange") == exchange
            and th.get("symbol") == symbol
        )
    ]
    threshold = {
        "user_id": user_id,
        "exchange": exchange,
        "symbol": symbol,
        "direction": direction,
        "signal_direction": signal_direction,
        "entry_source": str(entry_source).lower(),
        "entry_price": entry_price,
        "loss_price": loss_price,
        "gain_price": gain_price,
        "partial_tp_enabled": partial_tp_enabled,
        "partial_tp_done": False,
        "partial_tp_pct": PARTIAL_TP_PCT,
        "partial_tp_close_pct": PARTIAL_TP_CLOSE_PCT,
        "post_tp_sl_active": False,
        "post_tp_sl_pct": POST_TP_SL_PCT,
        "post_tp_sl_base_price": None,
        "profit_lock_done": False,
        "trailing_step_pct": RANGE_TRAILING_STEP_PCT,
        "profit_lock_trigger_pct": RANGE_PROFIT_LOCK_TRIGGER_PCT,
        "profit_lock_sl_pct": RANGE_PROFIT_LOCK_SL_PCT,
        "position_qty_ref": float(position_qty_ref) if position_qty_ref else None,
        "fired_loss": False,
        "last_open_ts": time.time(),
        "fired_gain": False,
    }
    _thresholds.append(threshold)
    print(
        f"[WATCHER][THRESHOLDS][REGISTER] user={user_id} ex={exchange} symbol={symbol} dir={direction} "
        f"entry={entry_price:.6f} loss={loss_price:.6f} tp_partial={gain_price} "
        f"entry_source={entry_source}"
    )
    _save_thresholds()
    return threshold


def _update_threshold_from_signal(
    user_id: str,
    exchange: str,
    symbol: str,
    position_direction: str,
    signal_direction: str,
    entry_price: float,
) -> dict:
    global _thresholds
    prev = None
    for th in _thresholds:
        if (
            th.get("user_id") == user_id
            and th.get("exchange") == exchange
            and th.get("symbol") == symbol
        ):
            prev = th
            break
    prev_entry = float(prev.get("entry_price")) if prev and prev.get("entry_price") is not None else None
    prev_loss = float(prev.get("loss_price")) if prev and prev.get("loss_price") is not None else None
    loss_price, gain_price, partial_tp_enabled = _compute_thresholds(position_direction, entry_price, "signal")
    _thresholds = [
        th
        for th in _thresholds
        if not (
            th.get("user_id") == user_id
            and th.get("exchange") == exchange
            and th.get("symbol") == symbol
        )
    ]
    _thresholds.append(
        {
            "user_id": user_id,
            "exchange": exchange,
            "symbol": symbol,
            "direction": position_direction,
            "signal_direction": signal_direction,
            "entry_source": "signal",
            "entry_price": entry_price,
            "loss_price": loss_price,
            "gain_price": gain_price,
            "partial_tp_enabled": partial_tp_enabled,
            "partial_tp_done": False,
            "partial_tp_pct": PARTIAL_TP_PCT,
            "partial_tp_close_pct": PARTIAL_TP_CLOSE_PCT,
            "post_tp_sl_active": False,
            "post_tp_sl_pct": POST_TP_SL_PCT,
            "post_tp_sl_base_price": None,
            "position_qty_ref": None,
            "fired_loss": False,
            "last_open_ts": time.time(),
            "fired_gain": False,
        }
    )
    print(
        f"[WATCHER][THRESHOLDS][UPDATE] user={user_id} ex={exchange} symbol={symbol} dir={position_direction} "
        f"entry={entry_price:.6f} loss={loss_price:.6f}"
    )
    try:
        _save_thresholds()
        return {
            "ok": True,
            "user_id": user_id,
            "exchange": exchange,
            "symbol": symbol,
            "direction": position_direction,
            "signal_direction": signal_direction,
            "entry_price": entry_price,
            "loss_price": loss_price,
            "prev_entry_price": prev_entry,
            "prev_loss_price": prev_loss,
        }
    except Exception as exc:
        print(
            f"[WATCHER][WARN] Falló persistencia de threshold actualizado "
            f"user={user_id} ex={exchange} symbol={symbol}: {exc}"
        )
        return {
            "ok": False,
            "user_id": user_id,
            "exchange": exchange,
            "symbol": symbol,
            "direction": position_direction,
            "signal_direction": signal_direction,
            "entry_price": entry_price,
            "loss_price": loss_price,
            "prev_entry_price": prev_entry,
            "prev_loss_price": prev_loss,
            "error": str(exc),
        }


def _update_threshold_loss_only_from_signal(
    user_id: str,
    exchange: str,
    symbol: str,
    position_direction: str,
    signal_direction: str,
    entry_price: float,
) -> dict:
    """
    Actualiza solo SL de un threshold existente usando precio de señal.
    Mantiene TP/source/estado de TP parcial intactos.
    """
    global _thresholds
    prev_idx = None
    prev = None
    for idx, th in enumerate(_thresholds):
        if (
            th.get("user_id") == user_id
            and th.get("exchange") == exchange
            and th.get("symbol") == symbol
        ):
            prev_idx = idx
            prev = th
            break

    if prev is None or prev_idx is None:
        return {
            "ok": False,
            "user_id": user_id,
            "exchange": exchange,
            "symbol": symbol,
            "direction": position_direction,
            "signal_direction": signal_direction,
            "entry_price": entry_price,
            "error": "threshold_not_found",
        }

    prev_loss = float(prev.get("loss_price")) if prev.get("loss_price") is not None else None
    threshold_source = str(prev.get("entry_source") or "signal").lower()
    loss_price, _, _ = _compute_thresholds(position_direction, entry_price, threshold_source)

    updated = dict(prev)
    updated["direction"] = position_direction
    updated["signal_direction"] = signal_direction
    updated["loss_price"] = loss_price
    updated["fired_loss"] = False
    updated["last_open_ts"] = time.time()
    _thresholds[prev_idx] = updated
    print(
        f"[WATCHER][THRESHOLDS][SL-ONLY-UPDATE] user={user_id} ex={exchange} symbol={symbol} "
        f"dir={position_direction} src={threshold_source} loss={loss_price:.6f}"
    )
    try:
        _save_thresholds()
        return {
            "ok": True,
            "user_id": user_id,
            "exchange": exchange,
            "symbol": symbol,
            "direction": position_direction,
            "signal_direction": signal_direction,
            "entry_price": entry_price,
            "loss_price": loss_price,
            "prev_loss_price": prev_loss,
        }
    except Exception as exc:
        return {
            "ok": False,
            "user_id": user_id,
            "exchange": exchange,
            "symbol": symbol,
            "direction": position_direction,
            "signal_direction": signal_direction,
            "entry_price": entry_price,
            "loss_price": loss_price,
            "prev_loss_price": prev_loss,
            "error": str(exc),
        }


def _execute_trade_for_target(
    user_id: str,
    exchange: str,
    direction: str,
    symbol: str,
    price: float,
    source_event: str,
    signal_direction: str | None = None,
    event_ts: str | None = None,
    post_trade_alerts: list[dict] | None = None,
    enqueue_on_fail: bool = True,
    quantity_override: float | None = None,
) -> tuple[bool, float | None]:
    executor = _resolve_executor()
    if executor is None or _account_manager is None:
        return False, None

    try:
        account = _account_manager.get_account(user_id)
        cred = account.get_exchange(exchange)
    except Exception as exc:
        print(f"[WATCHER][WARN] No se pudo resolver cuenta {user_id}/{exchange}: {exc}")
        return False, None

    notional = cred.notional_usdt
    if cred.extra:
        symbol = cred.extra.get("symbol", symbol)

    is_signal_event = str(source_event or "").lower() != "threshold_flip"
    pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
    if pos_amt is None:
        print(
            f"[WATCHER][POS][UNKNOWN_INIT] user={user_id} ex={exchange} "
            f"symbol={symbol} where=pre_trade_check"
        )
        _send_ops_alert(
            code="pre_trade_position_unknown",
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            message=(
                f"⚠️ [OPS] No se pudo validar posición antes de operar\n"
                f"Cuenta: {user_id}/{exchange}\n"
                f"Símbolo: {symbol}\n"
                f"Motivo: pre_trade_check=unknown\n"
                f"Acción: señal enviada a cola de retry."
            ),
        )
        if enqueue_on_fail:
            _enqueue_trade_retry(
                user_id=user_id,
                exchange=exchange,
                symbol=symbol,
                direction=direction,
                signal_direction=signal_direction,
                price=price,
                source_event=source_event,
                event_ts=event_ts,
                reason="position_unknown",
                detail="pre_trade_check",
                quantity_override=quantity_override,
            )
        return False, None

    just_closed_for_signal = False
    if abs(pos_amt) >= 1e-8:
        pos_dir = "long" if pos_amt > 0 else "short"
        if not is_signal_event and pos_dir == direction:
            print(
                f"[WATCHER][INFO] Orden {direction} ya está abierta en {exchange}; "
                f"se omite reapertura por {source_event}."
            )
            return True, None

        close_entry = None
        current_th = None
        for th in _thresholds:
            if (
                str(th.get("user_id")) == str(user_id)
                and str(th.get("exchange")).lower() == str(exchange).lower()
                and str(th.get("symbol")) == str(symbol)
                and str(th.get("direction")).lower() == str(pos_dir)
            ):
                current_th = th
                try:
                    close_entry = float(th.get("entry_price"))
                except Exception:
                    close_entry = None
                break

        if is_signal_event:
            th_entry_source = str((current_th or {}).get("entry_source") or "signal").lower()
            th_partial_tp_done = bool((current_th or {}).get("partial_tp_done", False))
            # En bot6rangos, una señal nueva cierra cualquier flip abierto y retoma
            # el flujo normal de la estrategia. El flip no se mantiene con SL-only.
            is_post_sl_full_state = False

            if direction == pos_dir and is_post_sl_full_state:
                update_result = _update_threshold_loss_only_from_signal(
                    user_id,
                    exchange,
                    symbol,
                    position_direction=pos_dir,
                    signal_direction=signal_direction or direction,
                    entry_price=price,
                )
                if post_trade_alerts is not None:
                    prev_loss = update_result.get("prev_loss_price")
                    new_loss = update_result.get("loss_price")
                    prev_loss_txt = f"{prev_loss:.2f}" if isinstance(prev_loss, (int, float)) else "N/A"
                    new_loss_txt = f"{new_loss:.2f}" if isinstance(new_loss, (int, float)) else "N/A"
                    ts = event_ts or datetime.now(timezone.utc)
                    if update_result.get("ok"):
                        post_trade_alerts.append(
                            {
                                "type": "sl_updated",
                                "timestamp": ts,
                                "message": (
                                    f"{symbol} {STREAM_INTERVAL}\n"
                                    f"🛡️ [SL-UPDATE] SL actualizado (post-SL + señal misma dirección)\n"
                                    f"Cuenta: {user_id}/{exchange}\n"
                                    f"Dirección: {pos_dir.upper()}\n"
                                    f"SL anterior: {prev_loss_txt}\n"
                                    f"SL nuevo: {new_loss_txt}"
                                ),
                                "stop_loss": new_loss,
                            }
                        )
                    else:
                        post_trade_alerts.append(
                            {
                                "type": "sl_update_error",
                                "timestamp": ts,
                                "message": (
                                    f"{symbol} {STREAM_INTERVAL}\n"
                                    f"❌ [SL-UPDATE-ERROR] ERROR al actualizar SL (post-SL + misma dirección)\n"
                                    f"Cuenta: {user_id}/{exchange}\n"
                                    f"Dirección: {pos_dir.upper()}\n"
                                    f"Motivo: {update_result.get('error', 'desconocido')}"
                                ),
                            }
                        )
                print(
                    f"[WATCHER][INFO] Post-SL full + señal misma dirección; se mantiene posición y solo se actualiza SL."
                )
                return True, None

            close_result = _close_position_result(user_id, exchange, symbol, pos_dir)
            close_ok = bool(close_result.get("ok"))
            if not close_ok:
                reason = "close_current_failed"
                detail = f"signal_reset pos_dir={pos_dir} detail={close_result.get('detail')}"
                print(
                    f"[WATCHER][WARN] No se pudo cerrar posición actual en {symbol}; "
                    f"se omite señal. reason={reason} detail={detail}"
                )
                _send_ops_alert(
                    code="close_current_failed",
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    message=(
                        f"⚠️ [OPS] Falló cierre de posición actual\n"
                        f"Cuenta: {user_id}/{exchange}\n"
                        f"Símbolo: {symbol}\n"
                        f"Reason: {reason}\n"
                        f"Detail: {detail}\n"
                        f"Acción: señal enviada a cola de retry."
                    ),
                )
                if enqueue_on_fail:
                    _enqueue_trade_retry(
                        user_id=user_id,
                        exchange=exchange,
                        symbol=symbol,
                        direction=direction,
                        signal_direction=signal_direction,
                        price=price,
                        source_event=source_event,
                        event_ts=event_ts,
                        reason=reason,
                        detail=detail,
                        quantity_override=quantity_override,
                    )
                return False, None
            close_reason = "signal_reset"
            just_closed_for_signal = True
            print(
                f"[WATCHER][INFO] Señal nueva detectada; se cerró posición previa "
                f"user={user_id} ex={exchange} symbol={symbol} prev_dir={pos_dir} new_dir={direction}"
            )
        else:
            close_result = _close_opposite_position_result(user_id, exchange, direction, symbol, price)
            if not close_result.get("ok"):
                reason = str(close_result.get("reason") or "close_opposite_failed")
                detail = str(close_result.get("detail") or "")
                print(
                    f"[WATCHER][WARN] No se pudo cerrar posición opuesta en {symbol}; "
                    f"se omite señal. reason={reason} detail={detail}"
                )
                _send_ops_alert(
                    code="close_opposite_failed",
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    message=(
                        f"⚠️ [OPS] Falló cierre de posición opuesta\n"
                        f"Cuenta: {user_id}/{exchange}\n"
                        f"Símbolo: {symbol}\n"
                        f"Reason: {reason}\n"
                        f"Detail: {detail or 'N/A'}\n"
                        f"Acción: señal enviada a cola de retry."
                    ),
                )
                if enqueue_on_fail:
                    _enqueue_trade_retry(
                        user_id=user_id,
                        exchange=exchange,
                        symbol=symbol,
                        direction=direction,
                        signal_direction=signal_direction,
                        price=price,
                        source_event=source_event,
                        event_ts=event_ts,
                        reason=reason,
                        detail=detail,
                        quantity_override=quantity_override,
                    )
                return False, None
            close_reason = "signal_flip"
        close_entry = close_entry if close_entry and close_entry > 0 else float(price)
        close_exec_price = _safe_exec_price(close_result.get("exit_price")) if is_signal_event else None
        close_exit_ts = close_result.get("exit_ts") if is_signal_event else None
        close_exit_qty = _safe_exec_qty(close_result.get("exit_qty")) if is_signal_event else None
        close_exit_price_for_balance = float(close_exec_price if close_exec_price is not None else float(price))
        _record_balance_close(
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            direction=pos_dir,
            entry_price=close_entry,
            exit_price=close_exit_price_for_balance,
            reason=close_reason,
            close_ts=close_exit_ts or event_ts,
            source="live",
        )
        _register_trades_table_close(
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            direction=pos_dir,
            close_reason=close_reason,
            exit_price=close_exec_price,
            exit_qty=close_exit_qty,
            exit_ts=close_exit_ts or normalize_close_ts(event_ts) or datetime.now(timezone.utc),
            fees_usdt=0.0,
        )
        _maybe_enqueue_pending_close_resolution(
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            direction=pos_dir,
            close_reason=close_reason,
            close_result=close_result,
            event_ts=close_exit_ts or event_ts,
        )
        pos_amt = 0.0

    last_dir = _last_order_direction.get((user_id, exchange))
    if abs(pos_amt) < 1e-8 and direction and direction == (last_dir or "").lower() and not just_closed_for_signal:
        print(f"[WATCHER][INFO] Orden {direction} ya colocada en {exchange}; se ignora señal.")
        return True, None

    if quantity_override is not None:
        try:
            quantity = float(quantity_override)
        except Exception as exc:
            print(
                f"[WATCHER][WARN] quantity_override inválida ({exc}) "
                f"usuario={user_id} exchange={exchange}"
            )
            return False, None
    else:
        try:
            quantity = _resolve_quantity({"price": price}, notional_usdt=notional)
        except Exception as exc:
            print(
                f"[WATCHER][WARN] Cantidad inválida para trading ({exc}) usuario={user_id} exchange={exchange}"
            )
            return False, None
    ex_l = str(exchange).lower()
    if ex_l == "binance":
        step, min_qty, _, min_notional = _binance_lot_rules(cred, symbol)
        target_notional = float(notional) if isinstance(notional, (int, float)) and float(notional) > 0 else float(quantity) * float(price)
        sizing = _notional_to_qty_binance(
            target_notional=target_notional,
            price=float(price),
            step=step,
            min_qty=min_qty,
            min_notional=min_notional,
            overshoot_cap=0.03,
        )
        quantity = float(sizing["qty_selected"])
        print(
            f"[WATCHER][SIZING][BINANCE] user={user_id} ex={exchange} symbol={symbol} "
            f"target_notional={target_notional:.4f} price_ref={float(price):.6f} "
            f"qty_floor={float(sizing['qty_floor']):.8f} qty_ceil={float(sizing['qty_ceil']):.8f} "
            f"qty_selected={quantity:.8f} selected_notional={float(sizing['selected_notional']):.4f} "
            f"deviation_pct={float(sizing['deviation_pct']) * 100:.3f}% min_notional={float(sizing['min_notional']):.4f} "
            f"overshoot_for_constraints={bool(sizing['overshoot_for_constraints'])}"
        )
        if quantity < min_qty:
            print(
                f"[WATCHER][WARN] Qty calculada por debajo del mínimo; no se abre posición "
                f"user={user_id} ex={exchange} symbol={symbol} qty={quantity:.8f} min={min_qty}"
            )
            _send_ops_alert(
                code="min_qty_skip",
                user_id=user_id,
                exchange=exchange,
                symbol=symbol,
                message=(
                    f"⚠️ [OPS] No se abrió posición por qty mínima\n"
                    f"Cuenta: {user_id}/{exchange}\n"
                    f"Símbolo: {symbol}\n"
                    f"Dirección objetivo: {direction}\n"
                    f"Qty calculada: {quantity:.8f}\n"
                    f"Mínimo requerido: {min_qty}"
                ),
            )
            return False, None
    elif ex_l == "bybit":
        step = 0.001
        quantity = math.floor(quantity / step) * step
        if quantity < step:
            print(
                f"[WATCHER][WARN] Qty calculada por debajo del mínimo; no se abre posición "
                f"user={user_id} ex={exchange} symbol={symbol} qty={quantity:.8f} min={step}"
            )
            _send_ops_alert(
                code="min_qty_skip",
                user_id=user_id,
                exchange=exchange,
                symbol=symbol,
                message=(
                    f"⚠️ [OPS] No se abrió posición por qty mínima\n"
                    f"Cuenta: {user_id}/{exchange}\n"
                    f"Símbolo: {symbol}\n"
                    f"Dirección objetivo: {direction}\n"
                    f"Qty calculada: {quantity:.8f}\n"
                    f"Mínimo requerido: {step}"
                ),
            )
            return False, None
    side = _direction_to_side(direction)
    extra = {
        "source_event": source_event,
        "account": user_id,
        "exchange": exchange,
        "signal_direction": signal_direction,
    }
    client_order_id = _build_client_order_id(ORDER_ID_PREFIX, user_id, exchange, symbol, side.value)
    order = OrderRequest(
        symbol=symbol,
        side=side,
        type=OrderType.MARKET,
        quantity=quantity,
        price=None,
        time_in_force=TimeInForce.GTC,
        client_order_id=client_order_id,
        extra_params=extra,
    )

    try:
        response = executor.execute(user_id, exchange, order, dry_run=TRADING_DRY_RUN)
        err = getattr(response, "error", None)
        err_text = f" error={err}" if err else ""
        print(
            f"[WATCHER][TRADE] user={user_id} ex={exchange} success={response.success} status={response.status}{err_text} raw={response.raw}"
        )
        if response.success:
            _last_order_direction[(user_id, exchange)] = direction
            exchange_order_id = str(getattr(response, "exchange_order_id", "") or "")
            fill = _resolve_order_fill_sync(
                user_id=user_id,
                exchange=exchange,
                cred=cred,
                symbol=symbol,
                side=side.value,
                order_id=exchange_order_id,
                client_order_id=client_order_id,
                qty_hint=_safe_exec_qty(response.filled_quantity) or _safe_exec_qty(quantity),
                wait_seconds=TRADES_FILL_RESOLVE_WAIT_SECONDS,
                phase="open",
            )
            exec_price = _safe_exec_price(fill.get("price")) if fill.get("ok") else None
            exec_qty = _safe_exec_qty(fill.get("qty")) if fill.get("ok") else None
            exec_ts = normalize_close_ts(fill.get("ts")) if fill.get("ok") else None
            if exec_price is not None and exec_qty is not None and exec_ts is not None:
                _set_open_trade_state(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    entry_price_real=exec_price,
                    entry_qty_real=exec_qty,
                    entry_ts_real=exec_ts,
                    entry_order_id=exchange_order_id,
                    entry_client_order_id=client_order_id,
                    source_event=source_event,
                )
            else:
                _enqueue_pending_execution_resolution(
                    kind="open",
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    side=side.value,
                    order_id=exchange_order_id,
                    client_order_id=client_order_id,
                    source_event=source_event,
                    close_reason=None,
                    event_ts=event_ts,
                    qty_hint=_safe_exec_qty(response.filled_quantity) or _safe_exec_qty(quantity),
                )
            return True, float(exec_price or price)
        _send_ops_alert(
            code="order_error",
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            message=(
                f"⚠️ [OPS] Orden rechazada/no exitosa\n"
                f"Cuenta: {user_id}/{exchange}\n"
                f"Símbolo: {symbol}\n"
                f"Status: {response.status}\n"
                f"Error: {err or 'N/A'}"
            ),
        )
        if enqueue_on_fail:
            _enqueue_trade_retry(
                user_id=user_id,
                exchange=exchange,
                symbol=symbol,
                direction=direction,
                signal_direction=signal_direction,
                price=price,
                source_event=source_event,
                event_ts=event_ts,
                reason="order_error",
                detail=str(err) if err else None,
                quantity_override=quantity_override,
            )
        return False, None
    except RuntimeError as exc:
        print(f"[WATCHER][WARN] Credenciales/config faltantes para {user_id}/{exchange}: {exc}")
        _send_ops_alert(
            code="credentials_error",
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            message=(
                f"🚨 [OPS] Error de credenciales/config\n"
                f"Cuenta: {user_id}/{exchange}\n"
                f"Símbolo: {symbol}\n"
                f"Detalle: {exc}"
            ),
        )
        return False, None
    except Exception as exc:
        print(f"[WATCHER][ERROR] Falló la ejecución de orden usuario={user_id} exchange={exchange} ({exc})")
        _send_ops_alert(
            code="execution_exception",
            user_id=user_id,
            exchange=exchange,
            symbol=symbol,
            message=(
                f"🚨 [OPS] Excepción al ejecutar orden\n"
                f"Cuenta: {user_id}/{exchange}\n"
                f"Símbolo: {symbol}\n"
                f"Detalle: {exc}"
            ),
        )
        if enqueue_on_fail:
            _enqueue_trade_retry(
                user_id=user_id,
                exchange=exchange,
                symbol=symbol,
                direction=direction,
                signal_direction=signal_direction,
                price=price,
                source_event=source_event,
                event_ts=event_ts,
                reason="execution_exception",
                detail=str(exc),
                quantity_override=quantity_override,
            )
        return False, None


def _binance_position_details(cred: ExchangeCredential, symbol: str) -> tuple[float, float | None]:
    """
    Devuelve (position_amt_signed, entry_price) para Binance Futures.
    """
    try:
        api_key, api_secret = cred.resolve_keys(os.environ)
        base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
        client = UMFutures(key=api_key, secret=api_secret, base_url=base_url) if base_url else UMFutures(
            key=api_key, secret=api_secret, timeout=BINANCE_HTTP_TIMEOUT
        )
        pos = _retry_binance_timestamp(
            lambda: client.get_position_risk(symbol=symbol, recvWindow=BINANCE_RECV_WINDOW_MS),
            "get_position_risk",
        )
        if not pos:
            return 0.0, None
        row = pos[0] or {}
        amt = float(row.get("positionAmt") or 0.0)
        entry = row.get("entryPrice")
        try:
            entry_price = float(entry) if entry is not None else None
        except Exception:
            entry_price = None
        if entry_price is not None and entry_price <= 0:
            entry_price = None
        return amt, entry_price
    except Exception:
        return 0.0, None


def _bybit_position_details(cred: ExchangeCredential, symbol: str) -> tuple[float, float | None]:
    """
    Devuelve (position_amt_signed, entry_price) para Bybit linear.
    """
    try:
        from pybit.unified_trading import HTTP  # type: ignore

        api_key, api_secret = cred.resolve_keys(os.environ)
        is_testnet = cred.environment != ExchangeEnvironment.LIVE
        domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
        client = (
            HTTP(
                api_key=api_key,
                api_secret=api_secret,
                testnet=False,
                domain=domain_env,
                recv_window=BYBIT_RECV_WINDOW_MS,
                timeout=BYBIT_HTTP_TIMEOUT,
            )
            if domain_env
            else HTTP(
                api_key=api_key,
                api_secret=api_secret,
                testnet=is_testnet,
                recv_window=BYBIT_RECV_WINDOW_MS,
                timeout=BYBIT_HTTP_TIMEOUT,
            )
        )
        raw = _retry_bybit_timestamp(
            lambda: client.get_positions(category="linear", symbol=symbol),
            "get_positions",
        )
        items = raw.get("result", {}).get("list") or []
        if not items:
            return 0.0, None
        pos = items[0] or {}
        size = float(pos.get("size") or 0.0)
        side = str(pos.get("side") or "").lower()
        if size == 0:
            return 0.0, None
        signed_amt = size if side == "buy" else -size
        entry_price = None
        for k in ("avgPrice", "entryPrice", "avgEntryPrice"):
            v = pos.get(k)
            if v is None:
                continue
            try:
                fv = float(v)
                if fv > 0:
                    entry_price = fv
                    break
            except Exception:
                continue
        return signed_amt, entry_price
    except Exception:
        return 0.0, None


def _rebuild_thresholds_from_open_positions() -> None:
    """
    Recalcula umbrales (-2%) en base a las posiciones abiertas actuales.
    - Requiere acceso a exchanges (no dry-run).
    - Si no se puede obtener entry_price, omite ese par y deja log.
    """
    if not TRADING_ENABLED:
        print("[WATCHER][THRESHOLDS][REBUILD] Trading deshabilitado; no se reconstruyen umbrales.")
        return
    manager = _load_manager()
    if manager is None:
        print("[WATCHER][THRESHOLDS][REBUILD] No hay AccountManager; no se reconstruyen umbrales.")
        return

    global _thresholds
    rebuilt = 0
    skipped = 0
    missing_entry = 0
    kept_existing = 0
    scanned = 0

    existing = {}
    for th in _thresholds:
        key = (th.get("user_id"), th.get("exchange"), th.get("symbol"))
        if all(key):
            existing[key] = th

    new_thresholds: list[dict] = []

    for account in manager.list_accounts():
        if not account.enabled:
            continue
        for exchange, cred in (account.exchanges or {}).items():
            if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                continue
            scanned += 1
            symbol = (cred.extra or {}).get("symbol") or SYMBOL_DISPLAY.replace(".P", "")
            pos_amt = 0.0
            entry_price = None
            ex_l = exchange.lower()
            try:
                if ex_l == "binance":
                    pos_amt, entry_price = _binance_position_details(cred, symbol)
                elif ex_l == "bybit":
                    pos_amt, entry_price = _bybit_position_details(cred, symbol)
                else:
                    continue
            except Exception as exc:
                print(
                    f"[WATCHER][THRESHOLDS][REBUILD][ERR] user={account.user_id} ex={exchange} symbol={symbol} "
                    f"err={exc}"
                )
                skipped += 1
                continue

            if not pos_amt:
                print(
                    f"[WATCHER][THRESHOLDS][REBUILD][NO_POS] user={account.user_id} ex={exchange} symbol={symbol}"
                )
                continue
            direction = "long" if pos_amt > 0 else "short"
            key = (account.user_id, exchange, symbol)
            if entry_price is None or entry_price <= 0:
                missing_entry += 1
                prev = existing.get(key)
                prev_entry = float(prev.get("entry_price") or 0) if prev else 0.0
                if prev_entry > 0:
                    loss_price, gain_price, _ = _compute_thresholds(direction, prev_entry, "signal")
                    new_thresholds.append(
                        {
                            "user_id": account.user_id,
                            "exchange": exchange,
                            "symbol": symbol,
                            "direction": direction,
                            "signal_direction": prev.get("signal_direction") if prev else direction,
                            "entry_source": "signal",
                            "entry_price": prev_entry,
                            "loss_price": loss_price,
                            "gain_price": gain_price,
                            "partial_tp_enabled": False,
                            "partial_tp_done": False,
                            "partial_tp_pct": PARTIAL_TP_PCT,
                            "partial_tp_close_pct": PARTIAL_TP_CLOSE_PCT,
                            "position_qty_ref": None,
                            "fired_loss": False,
                            "fired_gain": False,
                        }
                    )
                    kept_existing += 1
                    print(
                        f"[WATCHER][THRESHOLDS][REBUILD][KEEP] user={account.user_id} ex={exchange} "
                        f"symbol={symbol} entry={prev_entry:.6f} reason=no_entry_price"
                    )
                else:
                    print(
                        f"[WATCHER][THRESHOLDS][REBUILD][SKIP] user={account.user_id} ex={exchange} "
                        f"symbol={symbol} pos_amt={pos_amt} reason=no_entry_price"
                    )
                    skipped += 1
                continue

            entry_val = float(entry_price)
            loss_price, gain_price, _ = _compute_thresholds(direction, entry_val, "signal")
            new_thresholds.append(
                {
                    "user_id": account.user_id,
                    "exchange": exchange,
                    "symbol": symbol,
                    "direction": direction,
                    "signal_direction": direction,
                    "entry_source": "signal",
                    "entry_price": entry_val,
                    "loss_price": loss_price,
                    "gain_price": gain_price,
                    "partial_tp_enabled": False,
                    "partial_tp_done": False,
                    "partial_tp_pct": PARTIAL_TP_PCT,
                    "partial_tp_close_pct": PARTIAL_TP_CLOSE_PCT,
                    "position_qty_ref": None,
                    "fired_loss": False,
                    "fired_gain": False,
                }
            )
            rebuilt += 1

    _thresholds = new_thresholds
    _save_thresholds()
    removed = max(len(existing) - len(new_thresholds), 0)
    print(
        f"[WATCHER][THRESHOLDS][REBUILD] done scanned={scanned} rebuilt={rebuilt} kept={kept_existing} "
        f"missing_entry={missing_entry} skipped={skipped} removed={removed}"
    )


def _threshold_guard_rebuild_missing() -> None:
    """
    Autocuración: garantiza que toda posición abierta tenga threshold válido.
    No limpia thresholds en caso de posición desconocida; sólo reconstruye faltantes/inválidos.
    """
    if not TRADING_ENABLED:
        print("[WATCHER][THRESHOLDS][GUARD][DONE] scanned=0 rebuilt=0 skipped=0 reason=trading_disabled")
        return
    manager = _load_manager()
    if manager is None:
        print("[WATCHER][THRESHOLDS][GUARD][DONE] scanned=0 rebuilt=0 skipped=0 reason=no_manager")
        return

    global _thresholds
    scanned = 0
    rebuilt = 0
    skipped = 0
    print("[WATCHER][THRESHOLDS][GUARD][START]")

    existing: dict[tuple[str, str, str], dict] = {}
    for th in _thresholds:
        key = (th.get("user_id"), th.get("exchange"), th.get("symbol"))
        if all(key):
            existing[key] = th

    for account in manager.list_accounts():
        if not account.enabled:
            continue
        for exchange, cred in (account.exchanges or {}).items():
            if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                continue
            symbol = (cred.extra or {}).get("symbol") or SYMBOL_DISPLAY.replace(".P", "")
            scanned += 1
            ex_l = exchange.lower()
            try:
                if ex_l == "binance":
                    pos_amt, entry_price = _binance_position_details(cred, symbol)
                elif ex_l == "bybit":
                    pos_amt, entry_price = _bybit_position_details(cred, symbol)
                else:
                    continue
            except Exception as exc:
                skipped += 1
                print(
                    f"[WATCHER][THRESHOLDS][GUARD][SKIP] user={account.user_id} ex={exchange} "
                    f"symbol={symbol} reason=position_unknown err={exc}"
                )
                continue

            if pos_amt is None:
                skipped += 1
                print(
                    f"[WATCHER][THRESHOLDS][GUARD][SKIP] user={account.user_id} ex={exchange} "
                    f"symbol={symbol} reason=position_unknown"
                )
                continue
            if not pos_amt:
                continue

            direction = "long" if pos_amt > 0 else "short"
            key = (account.user_id, exchange, symbol)
            th = existing.get(key)
            th_entry = float(th.get("entry_price") or 0) if th else 0.0
            th_loss = float(th.get("loss_price") or 0) if th else 0.0
            th_dir = (th.get("direction") or "").lower() if th else ""
            missing_or_invalid = (
                th is None
                or th_entry <= 0
                or th_loss <= 0
                or th_dir not in {"long", "short"}
                or th_dir != direction
            )
            if not missing_or_invalid:
                continue

            print(
                f"[WATCHER][THRESHOLDS][GUARD][MISSING] user={account.user_id} ex={exchange} "
                f"symbol={symbol}"
            )
            if entry_price is None or entry_price <= 0:
                skipped += 1
                print(
                    f"[WATCHER][THRESHOLDS][GUARD][SKIP] user={account.user_id} ex={exchange} "
                    f"symbol={symbol} reason=entry_unknown"
                )
                continue

            new_th = _register_threshold(
                account.user_id,
                exchange,
                symbol,
                direction,
                float(entry_price),
                signal_direction=direction,
            )
            existing[key] = new_th
            rebuilt += 1
            print(
                f"[WATCHER][THRESHOLDS][GUARD][REBUILT] user={account.user_id} ex={exchange} "
                f"symbol={symbol} entry={float(new_th.get('entry_price') or 0):.6f} "
                f"loss={float(new_th.get('loss_price') or 0):.6f}"
            )

    print(
        f"[WATCHER][THRESHOLDS][GUARD][DONE] scanned={scanned} rebuilt={rebuilt} skipped={skipped}"
    )


def _current_position(user_id: str, exchange: str, symbol: str) -> float | None:
    """
    Devuelve cantidad firmada de la posición actual (long >0, short <0).
    Implementado para binance/bybit; si falla devuelve None.
    """
    try:
        # En dry-run no consultamos exchanges (evita requests reales en simulaciones).
        if TRADING_DRY_RUN:
            return 0.0
        if _account_manager is None:
            return None
        account = _account_manager.get_account(user_id)
        cred = account.get_exchange(exchange)
        if exchange.lower() == "binance":
            api_key, api_secret = cred.resolve_keys(os.environ)
            base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
            client = UMFutures(key=api_key, secret=api_secret, base_url=base_url) if base_url else UMFutures(
                key=api_key, secret=api_secret, timeout=BINANCE_HTTP_TIMEOUT
            )
            pos = _retry_binance_timestamp(
                lambda: client.get_position_risk(symbol=symbol, recvWindow=BINANCE_RECV_WINDOW_MS),
                "_current_position",
            )
            if not pos:
                return 0.0
            return float(pos[0].get("positionAmt") or 0.0)
        elif exchange.lower() == "bybit":
            return _bybit_position_amount(cred, symbol)
        return 0.0
    except Exception:
        return None


def _coerce_position(raw) -> float | None:
    if raw is None:
        return None
    try:
        val = float(raw)
    except Exception:
        return None
    if math.isnan(val) or math.isinf(val):
        return None
    return val


def _position_is_flat(pos: float | None, eps: float = 1e-8) -> bool | None:
    pos_n = _coerce_position(pos)
    if pos_n is None:
        return None
    return abs(pos_n) < eps


def _position_side(pos: float | None, eps: float = 1e-8) -> str:
    pos_n = _coerce_position(pos)
    if pos_n is None:
        return "unknown"
    if abs(pos_n) < eps:
        return "flat"
    return "long" if pos_n > 0 else "short"


def _format_pos(pos: float | None) -> str:
    pos_n = _coerce_position(pos)
    if pos_n is None:
        return "unknown"
    return f"{pos_n:.8f}"


def _confirm_no_position(user_id: str, exchange: str, symbol: str) -> bool:
    """
    Verifica ausencia de posición con reintentos para evitar limpiar thresholds por errores transitorios.
    Devuelve True si confirma 0, False si detecta posición o falla la consulta.
    """
    for attempt in range(max(POSITION_RETRY_COUNT, 1)):
        pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
        if pos_amt is None:
            print(
                f"[WATCHER][THRESHOLDS][SKIP] user={user_id} ex={exchange} symbol={symbol} "
                f"reason=position_unknown"
            )
            return False
        if abs(pos_amt) >= 1e-8:
            return False
        if attempt < POSITION_RETRY_COUNT - 1:
            time.sleep(max(POSITION_RETRY_DELAY, 0.1))
    return True


def _close_position_result(
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    quantity: float | None = None,
) -> dict:
    """
    Cierra posición (completa o parcial) usando orden reduceOnly MARKET.
    Devuelve resultado estructurado con datos de ejecución para ledger estricto.
    """
    result = {
        "ok": False,
        "exit_price": None,
        "exit_qty": None,
        "exit_ts": None,
        "fees_usdt": 0.0,
        "exit_order_id": None,
        "exit_client_order_id": None,
        "fill_resolved": False,
        "detail": None,
    }
    if _account_manager is None:
        result["detail"] = "account_manager_none"
        return result
    if TRADING_DRY_RUN:
        result.update(
            {
                "ok": True,
                "exit_price": None,
                "exit_qty": float(quantity) if quantity else None,
                "exit_ts": datetime.now(timezone.utc),
                "detail": "dry_run",
            }
        )
        return result
    try:
        account = _account_manager.get_account(user_id)
        cred = account.get_exchange(exchange)
        pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
        if pos_amt is None:
            result["detail"] = "position_unknown"
            return result
        if abs(pos_amt) < 1e-8:
            result["detail"] = "already_flat"
            return result
        current_qty = abs(pos_amt)
        if quantity is not None:
            try:
                req_qty = float(quantity)
            except Exception:
                result["detail"] = "invalid_quantity"
                return result
            if req_qty <= 0:
                result["detail"] = "invalid_quantity_non_positive"
                return result
            qty = min(current_qty, req_qty)
        else:
            qty = current_qty
        if qty <= 0:
            result["detail"] = "qty_non_positive"
            return result
        is_partial = qty + 1e-8 < current_qty
        side = "SELL" if pos_amt > 0 else "BUY"
        ex_l = exchange.lower()
        if ex_l == "binance":
            step, min_qty, decimals, _ = _binance_lot_rules(cred, symbol)
            qty = _floor_to_step(qty, step)
            if qty < min_qty:
                result["detail"] = "qty_below_min"
                return result
            api_key, api_secret = cred.resolve_keys(os.environ)
            base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
            client = UMFutures(key=api_key, secret=api_secret, base_url=base_url) if base_url else UMFutures(
                key=api_key, secret=api_secret
            )
            client_order_id = _build_client_order_id(ORDER_ID_PREFIX, user_id, exchange, symbol, side)
            raw = client.new_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=f"{qty:.{decimals}f}",
                reduceOnly="true",
                newClientOrderId=client_order_id,
            )
            result["exit_order_id"] = str(raw.get("orderId") or "")
            result["exit_client_order_id"] = str(raw.get("clientOrderId") or client_order_id)
            fill = _resolve_order_fill_sync(
                user_id=user_id,
                exchange=exchange,
                cred=cred,
                symbol=symbol,
                side=side,
                order_id=result["exit_order_id"],
                client_order_id=result["exit_client_order_id"],
                qty_hint=qty,
                wait_seconds=TRADES_FILL_RESOLVE_WAIT_SECONDS,
                phase="close",
            )
            if fill.get("ok"):
                result["exit_price"] = _safe_exec_price(fill.get("price"))
                result["exit_qty"] = _safe_exec_qty(fill.get("qty")) or qty
                result["exit_ts"] = normalize_close_ts(fill.get("ts")) or datetime.now(timezone.utc)
                result["fees_usdt"] = float(fill.get("fees_usdt") or 0.0)
                result["fill_resolved"] = True
                result["detail"] = f"order_id={result['exit_order_id']} fill={fill.get('detail')}"
            else:
                result["detail"] = f"order_id={result['exit_order_id']} fill_pending={fill.get('detail')}"
        elif ex_l == "bybit":
            from decimal import Decimal, ROUND_DOWN, ROUND_UP
            import re
            from pybit.unified_trading import HTTP  # type: ignore

            api_key, api_secret = cred.resolve_keys(os.environ)
            is_testnet = cred.environment != ExchangeEnvironment.LIVE
            domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
            client = (
                HTTP(
                    api_key=api_key,
                    api_secret=api_secret,
                    testnet=False,
                    domain=domain_env,
                    recv_window=BYBIT_RECV_WINDOW_MS,
                    timeout=BYBIT_HTTP_TIMEOUT,
                )
                if domain_env
                else HTTP(
                    api_key=api_key,
                    api_secret=api_secret,
                    testnet=is_testnet,
                    recv_window=BYBIT_RECV_WINDOW_MS,
                    timeout=BYBIT_HTTP_TIMEOUT,
                )
            )
            side = "Sell" if pos_amt > 0 else "Buy"

            def _quantize(v: float, step: str) -> str:
                dv = Decimal(str(v)).quantize(Decimal(step), rounding=ROUND_DOWN)
                if dv <= 0:
                    dv = Decimal(step)
                return format(dv, "f")

            def _ceil_to_step(value: float, step: str) -> str:
                dv = Decimal(str(value))
                ds = Decimal(step)
                if ds <= 0:
                    return format(dv, "f")
                q = (dv / ds).to_integral_value(rounding=ROUND_UP)
                out = q * ds
                if out <= 0:
                    out = ds
                return format(out, "f")

            def _autocorrect_qty(
                symbol_: str,
                qty_s: str,
                *,
                is_partial_order: bool,
                current_qty_value: float,
            ) -> str | None:
                try:
                    raw_info = client.get_instruments_info(category="linear", symbol=str(symbol_).upper())
                    items = raw_info.get("result", {}).get("list") or []
                    first = items[0] if items else {}
                    lot = first.get("lotSizeFilter") or {}
                    min_qty_s = str(lot.get("minOrderQty") or "")
                    step_s = str(lot.get("qtyStep") or "")
                    if not min_qty_s or not step_s:
                        return None
                    current = float(qty_s)
                    min_qty = float(min_qty_s)
                    if is_partial_order:
                        target = max(current, min_qty)
                        down = Decimal(str(target)).quantize(Decimal(step_s), rounding=ROUND_DOWN)
                        if down < Decimal(str(min_qty)):
                            down = Decimal(str(min_qty))
                        max_current = Decimal(str(current_qty_value)).quantize(Decimal(step_s), rounding=ROUND_DOWN)
                        if down > max_current:
                            down = max_current
                        if down <= 0:
                            return None
                        return format(down, "f")
                    target = max(current, min_qty)
                    return _ceil_to_step(target, step_s)
                except Exception:
                    return None

            qty_step = "0.001"
            min_qty = 0.001
            try:
                raw_info = client.get_instruments_info(category="linear", symbol=str(symbol).upper())
                items_info = raw_info.get("result", {}).get("list") or []
                first_info = items_info[0] if items_info else {}
                lot_info = first_info.get("lotSizeFilter") or {}
                qty_step = str(lot_info.get("qtyStep") or qty_step)
                min_qty = float(lot_info.get("minOrderQty") or min_qty)
            except Exception:
                pass
            if qty < min_qty:
                result["detail"] = "qty_below_min"
                return result
            qty_s = _quantize(qty, qty_step)
            client_order_id = _build_client_order_id(ORDER_ID_PREFIX, user_id, exchange, symbol, side)
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": qty_s,
                "reduceOnly": True,
                "orderLinkId": client_order_id,
            }
            try:
                raw = client.place_order(**params)
            except Exception as exc:
                msg = str(exc)
                err_code = None
                m = re.search(r"ErrCode:\\s*(\\d+)", msg)
                if m:
                    try:
                        err_code = int(m.group(1))
                    except Exception:
                        err_code = None
                looks_like_qty_error = (
                    (err_code == 10001)
                    or ("minimum limit" in msg.lower())
                    or ("qty" in msg.lower() and "invalid" in msg.lower())
                    or ("precision" in msg.lower())
                )
                if looks_like_qty_error:
                    corrected = _autocorrect_qty(
                        symbol,
                        qty_s,
                        is_partial_order=is_partial,
                        current_qty_value=current_qty,
                    )
                    if corrected and corrected != qty_s:
                        params2 = {**params, "qty": corrected}
                        raw = client.place_order(**params2)
                        params = params2
                    else:
                        raise
                else:
                    raise
            ret_code = raw.get("retCode")
            if ret_code not in (None, 0, "0"):
                msg = raw.get("retMsg") or "BYBIT_ERROR"
                result["detail"] = f"Bybit retCode={ret_code} retMsg={msg}"
                return result
            raw_result = raw.get("result", {}) if isinstance(raw, dict) else {}
            result["exit_order_id"] = str(raw_result.get("orderId") or "")
            result["exit_client_order_id"] = str(raw_result.get("orderLinkId") or params.get("orderLinkId") or "")
            fill = _resolve_order_fill_sync(
                user_id=user_id,
                exchange=exchange,
                cred=cred,
                symbol=symbol,
                side=side,
                order_id=result["exit_order_id"],
                client_order_id=result["exit_client_order_id"],
                qty_hint=qty,
                wait_seconds=TRADES_FILL_RESOLVE_WAIT_SECONDS,
                phase="close",
            )
            if fill.get("ok"):
                result["exit_price"] = _safe_exec_price(fill.get("price"))
                result["exit_qty"] = _safe_exec_qty(fill.get("qty")) or _safe_exec_qty(params.get("qty")) or qty
                result["exit_ts"] = normalize_close_ts(fill.get("ts")) or datetime.now(timezone.utc)
                result["fees_usdt"] = float(fill.get("fees_usdt") or 0.0)
                result["fill_resolved"] = True
                result["detail"] = f"order_id={result['exit_order_id']} fill={fill.get('detail')}"
            else:
                result["detail"] = f"order_id={result['exit_order_id']} fill_pending={fill.get('detail')}"
        else:
            result["detail"] = "exchange_unsupported"
            return result

        result["ok"] = True
        print(
            f"[WATCHER][INFO] Cierre reduceOnly MARKET user={user_id} ex={exchange} "
            f"symbol={symbol} qty={qty} side={side} mode={'partial' if is_partial else 'full'}"
        )
        return result
    except Exception as exc:
        result["detail"] = str(exc)
        print(f"[WATCHER][WARN] No se pudo cerrar posición user={user_id} ex={exchange} symbol={symbol}: {exc}")
        return result


def _close_position(
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    quantity: float | None = None,
) -> bool:
    return bool(_close_position_result(user_id, exchange, symbol, direction, quantity=quantity).get("ok"))


def _maybe_enqueue_pending_close_resolution(
    *,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    close_reason: str,
    close_result: dict,
    event_ts,
) -> None:
    if not isinstance(close_result, dict) or not close_result.get("ok"):
        return
    if _safe_exec_price(close_result.get("exit_price")) is not None and _safe_exec_qty(close_result.get("exit_qty")) is not None:
        return
    order_id = str(close_result.get("exit_order_id") or "")
    client_order_id = str(close_result.get("exit_client_order_id") or "")
    if not order_id and not client_order_id:
        return
    side = "SELL" if str(direction).lower() == "long" else "BUY"
    _enqueue_pending_execution_resolution(
        kind="close",
        user_id=user_id,
        exchange=exchange,
        symbol=symbol,
        direction=direction,
        side=side,
        order_id=order_id,
        client_order_id=client_order_id,
        source_event="close",
        close_reason=close_reason,
        event_ts=event_ts,
        qty_hint=_safe_exec_qty(close_result.get("exit_qty")),
    )


def _evaluate_thresholds(current_price: float, ts) -> list[dict]:
    """
    Evalúa si el precio actual dispara algún cierre por pérdida/ganancia.
    Devuelve lista de alertas a emitir y ejecuta cierre reduceOnly MARKET cuando corresponde.
    """
    alerts = []
    updated = False
    keep_thresholds = []

    price_cache: dict[tuple[str, str], float | None] = {}
    for th in _thresholds:
        user_id = th.get("user_id")
        exchange = th.get("exchange")
        symbol = th.get("symbol", SYMBOL_DISPLAY.replace(".P", ""))
        direction = th.get("direction")
        signal_direction = th.get("signal_direction") or None
        entry = float(th.get("entry_price") or 0)
        loss_price = float(th.get("loss_price") or 0)
        gain_raw = th.get("gain_price")
        gain_price = float(gain_raw) if gain_raw not in (None, "") else None
        fired_loss = th.get("fired_loss", False)
        fired_gain = th.get("fired_gain", False)
        entry_source = str(th.get("entry_source") or "signal").lower()
        partial_tp_enabled = bool(th.get("partial_tp_enabled", entry_source == "flip"))
        partial_tp_done = bool(th.get("partial_tp_done", False))
        partial_tp_close_pct = float(th.get("partial_tp_close_pct") or PARTIAL_TP_CLOSE_PCT)
        post_tp_sl_active = bool(th.get("post_tp_sl_active", False))
        post_tp_sl_pct = float(th.get("post_tp_sl_pct") or POST_TP_SL_PCT)
        post_tp_sl_base_price_raw = th.get("post_tp_sl_base_price")
        post_tp_sl_base_price = (
            float(post_tp_sl_base_price_raw)
            if post_tp_sl_base_price_raw not in (None, "")
            else None
        )
        triggered_kind = th.get("triggered_kind")
        last_attempt = float(th.get("last_close_attempt") or 0.0)
        now_ts = time.time()

        if entry <= 0:
            continue
        # Si ya no hay posición, limpiar registro
        pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
        if pos_amt is None:
            print(
                f"[WATCHER][THRESHOLDS][SKIP] user={user_id} ex={exchange} symbol={symbol} "
                f"reason=position_unknown"
            )
            keep_thresholds.append(th)
            continue
        if abs(pos_amt) < 1e-8:
            last_open_ts = float(th.get("last_open_ts") or 0.0)
            if last_open_ts and (time.time() - last_open_ts) < POSITION_GRACE_SECONDS:
                keep_thresholds.append(th)
                continue
            if not _confirm_no_position(user_id, exchange, symbol):
                keep_thresholds.append(th)
                continue
            print(
                f"[WATCHER][THRESHOLDS][CLEAN] user={user_id} ex={exchange} symbol={symbol} "
                f"reason=no_position"
            )
            _clear_open_trade_state(str(user_id), str(exchange), str(symbol))
            updated = True
            continue

        used_price = None
        price_key = (str(exchange).lower(), symbol)
        if price_key in price_cache:
            used_price = price_cache[price_key]
        else:
            if exchange:
                ex_l = str(exchange).lower()
                try:
                    manager = _account_manager or _load_manager()
                    if manager is not None and user_id:
                        account = manager.get_account(user_id)
                        cred = account.get_exchange(exchange)
                        if ex_l == "binance":
                            used_price = _binance_mark_price(cred, symbol)
                        elif ex_l == "bybit":
                            used_price = _bybit_mark_price(cred, symbol)
                except Exception:
                    used_price = None
            price_cache[price_key] = used_price
        if used_price is None:
            used_price = current_price
        try:
            used_price = float(used_price)
        except Exception:
            continue
        if used_price <= 0:
            continue

        if entry_source != "flip" and not post_tp_sl_active:
            profit_lock_done = bool(th.get("profit_lock_done", False))
            trailing_step_pct = float(th.get("trailing_step_pct") or RANGE_TRAILING_STEP_PCT)
            profit_lock_trigger_pct = float(th.get("profit_lock_trigger_pct") or RANGE_PROFIT_LOCK_TRIGGER_PCT)
            profit_lock_sl_pct = float(th.get("profit_lock_sl_pct") or RANGE_PROFIT_LOCK_SL_PCT)
            new_loss_price = None
            if direction == "long":
                if (not profit_lock_done) and used_price >= entry * (1 + profit_lock_trigger_pct):
                    new_loss_price = entry * (1 + profit_lock_sl_pct)
                    th["profit_lock_done"] = True
                    print(
                        f"[WATCHER][RANGE3][PROFIT-LOCK] user={user_id} ex={exchange} symbol={symbol} "
                        f"dir={direction} entry={entry:.6f} loss={new_loss_price:.6f}"
                    )
                elif not profit_lock_done and trailing_step_pct > 0:
                    steps = int(math.floor(max(0.0, (used_price / entry - 1.0)) / trailing_step_pct))
                    if steps > 0:
                        candidate = entry * (1 - LOSS_PCT + steps * trailing_step_pct)
                        if candidate > loss_price:
                            new_loss_price = candidate
            else:
                if (not profit_lock_done) and used_price <= entry * (1 - profit_lock_trigger_pct):
                    new_loss_price = entry * (1 - profit_lock_sl_pct)
                    th["profit_lock_done"] = True
                    print(
                        f"[WATCHER][RANGE3][PROFIT-LOCK] user={user_id} ex={exchange} symbol={symbol} "
                        f"dir={direction} entry={entry:.6f} loss={new_loss_price:.6f}"
                    )
                elif not profit_lock_done and trailing_step_pct > 0:
                    steps = int(math.floor(max(0.0, (1.0 - used_price / entry)) / trailing_step_pct))
                    if steps > 0:
                        candidate = entry * (1 + LOSS_PCT - steps * trailing_step_pct)
                        if candidate < loss_price:
                            new_loss_price = candidate
            if new_loss_price is not None and new_loss_price > 0:
                th["loss_price"] = float(new_loss_price)
                loss_price = float(new_loss_price)
                updated = True
                print(
                    f"[WATCHER][RANGE3][TRAIL] user={user_id} ex={exchange} symbol={symbol} "
                    f"dir={direction} used={used_price:.6f} loss={loss_price:.6f}"
                )

        if triggered_kind:
            if now_ts - last_attempt < THRESHOLDS_RETRY_SECONDS:
                keep_thresholds.append(th)
                continue
            close_result = _close_position_result(user_id, exchange, symbol, direction)
            close_ok = bool(close_result.get("ok"))
            th["last_close_attempt"] = now_ts
            if close_ok:
                close_exec_price = _safe_exec_price(close_result.get("exit_price"))
                close_exit_qty = _safe_exec_qty(close_result.get("exit_qty")) or abs(pos_amt)
                close_exit_ts = close_result.get("exit_ts") or normalize_close_ts(ts) or datetime.now(timezone.utc)
                close_exit_price_for_balance = float(close_exec_price if close_exec_price is not None else used_price)
                _record_balance_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry,
                    exit_price=close_exit_price_for_balance,
                    reason=str(triggered_kind),
                    close_ts=close_exit_ts,
                    source="live",
                )
                _register_trades_table_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=str(triggered_kind),
                    exit_price=close_exec_price,
                    exit_qty=close_exit_qty,
                    exit_ts=close_exit_ts,
                    fees_usdt=0.0,
                )
                _maybe_enqueue_pending_close_resolution(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=str(triggered_kind),
                    close_result=close_result,
                    event_ts=close_exit_ts,
                )
                print(
                    f"[WATCHER][THRESHOLDS][CLOSE] user={user_id} ex={exchange} symbol={symbol} "
                    f"ok=True kind={triggered_kind}"
                )
                alerts.append(
                    {
                        "type": "auto_close",
                        "timestamp": ts,
                        "message": (
                            f"{symbol} {STREAM_INTERVAL}\n"
                            f"🚨 [SL-HIT] Cierre {direction.upper()} por {triggered_kind}\n"
                            f"Estado: FLAT hasta nueva señal/pending\n"
                            f"Entrada: {entry:.2f}\n"
                            f"Último: {used_price:.2f}"
                        ),
                        "direction": direction,
                        "user_id": user_id,
                        "exchange": exchange,
                    }
                )
                updated = True
                continue
            print(
                f"[WATCHER][THRESHOLDS][RETRY] user={user_id} ex={exchange} symbol={symbol} "
                f"kind={triggered_kind} next_in={THRESHOLDS_RETRY_SECONDS}s"
            )
            updated = True
            keep_thresholds.append(th)
            continue

        hit_loss = False
        hit_take_profit = False
        hit_partial_tp = False
        if direction == "long":
            hit_loss = (not fired_loss) and used_price <= loss_price
            hit_take_profit = (
                (not partial_tp_enabled) and gain_price is not None and (not fired_gain)
                and used_price >= gain_price
            )
            hit_partial_tp = (
                partial_tp_enabled and (not partial_tp_done) and gain_price is not None and (not fired_gain)
                and used_price >= gain_price
            )
        else:  # short
            hit_loss = (not fired_loss) and used_price >= loss_price
            hit_take_profit = (
                (not partial_tp_enabled) and gain_price is not None and (not fired_gain)
                and used_price <= gain_price
            )
            hit_partial_tp = (
                partial_tp_enabled and (not partial_tp_done) and gain_price is not None and (not fired_gain)
                and used_price <= gain_price
            )

        if hit_take_profit:
            tp_reason = f"take_profit_{SMA_STABLE_TAKE_PROFIT_PCT * 100:g}pct"
            print(
                f"[WATCHER][THRESHOLDS][TP][TRIGGER] user={user_id} ex={exchange} symbol={symbol} "
                f"dir={direction} last={used_price:.6f} entry={entry:.6f} tp={gain_price:.6f}"
            )
            tp_result = _close_position_result(user_id, exchange, symbol, direction)
            tp_ok = bool(tp_result.get("ok"))
            if tp_ok:
                tp_exec_price = _safe_exec_price(tp_result.get("exit_price"))
                tp_exit_qty = _safe_exec_qty(tp_result.get("exit_qty")) or abs(pos_amt)
                tp_exit_ts = tp_result.get("exit_ts") or normalize_close_ts(ts) or datetime.now(timezone.utc)
                close_exit_price_for_balance = float(tp_exec_price if tp_exec_price is not None else used_price)
                _record_balance_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry,
                    exit_price=close_exit_price_for_balance,
                    reason=tp_reason,
                    close_ts=tp_exit_ts,
                    source="live",
                )
                _register_trades_table_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=tp_reason,
                    exit_price=tp_exec_price,
                    exit_qty=tp_exit_qty,
                    exit_ts=tp_exit_ts,
                    fees_usdt=0.0,
                )
                _maybe_enqueue_pending_close_resolution(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=tp_reason,
                    close_result=tp_result,
                    event_ts=tp_exit_ts,
                )
                alerts.append(
                    {
                        "type": "take_profit",
                        "timestamp": ts,
                        "message": (
                            f"{symbol} {STREAM_INTERVAL}\n"
                            f"✅ [TP-HIT] Cierre {direction.upper()} por TP +{SMA_STABLE_TAKE_PROFIT_PCT * 100:g}%\n"
                            f"Entrada: {entry:.2f}\n"
                            f"Último: {used_price:.2f}"
                        ),
                        "direction": direction,
                        "user_id": user_id,
                        "exchange": exchange,
                    }
                )
                updated = True
                continue
            th["triggered_kind"] = tp_reason
            th["last_close_attempt"] = now_ts
            keep_thresholds.append(th)
            updated = True
            continue

        if hit_partial_tp:
            if not partial_tp_enabled:
                print(
                    f"[WATCHER][THRESHOLDS][TP][TRIGGER] user={user_id} ex={exchange} symbol={symbol} "
                    f"dir={direction} last={used_price:.6f} entry={entry:.6f} tp={gain_price:.6f}"
                )
                tp_result = _close_position_result(user_id, exchange, symbol, direction)
                tp_ok = bool(tp_result.get("ok"))
                if tp_ok:
                    tp_exec_price = _safe_exec_price(tp_result.get("exit_price"))
                    tp_exit_qty = _safe_exec_qty(tp_result.get("exit_qty")) or abs(pos_amt)
                    tp_exit_ts = tp_result.get("exit_ts") or normalize_close_ts(ts) or datetime.now(timezone.utc)
                    close_exit_price_for_balance = float(tp_exec_price if tp_exec_price is not None else used_price)
                    _record_balance_close(
                        user_id=user_id,
                        exchange=exchange,
                        symbol=symbol,
                        direction=direction,
                        entry_price=entry,
                        exit_price=close_exit_price_for_balance,
                        reason="flip_take_profit",
                        close_ts=tp_exit_ts,
                        source="live",
                    )
                    _register_trades_table_close(
                        user_id=user_id,
                        exchange=exchange,
                        symbol=symbol,
                        direction=direction,
                        close_reason="flip_take_profit",
                        exit_price=tp_exec_price,
                        exit_qty=tp_exit_qty,
                        exit_ts=tp_exit_ts,
                        fees_usdt=0.0,
                    )
                    _maybe_enqueue_pending_close_resolution(
                        user_id=user_id,
                        exchange=exchange,
                        symbol=symbol,
                        direction=direction,
                        close_reason="flip_take_profit",
                        close_result=tp_result,
                        event_ts=tp_exit_ts,
                    )
                    alerts.append(
                        {
                            "type": "take_profit",
                            "timestamp": ts,
                            "message": (
                                f"{symbol} {STREAM_INTERVAL}\n"
                                f"✅ [TP-HIT] Cierre {direction.upper()} por ganancia +{RANGE_FLIP_TAKE_PROFIT_PCT * 100:g}%\n"
                                f"Entrada: {entry:.2f}\n"
                                f"Último: {used_price:.2f}"
                            ),
                            "direction": direction,
                            "user_id": user_id,
                            "exchange": exchange,
                        }
                    )
                    updated = True
                    continue
                th["triggered_kind"] = "flip_take_profit"
                th["last_close_attempt"] = now_ts
                keep_thresholds.append(th)
                updated = True
                continue

            close_qty = abs(pos_amt) * max(min(partial_tp_close_pct, 1.0), 0.0)
            print(
                f"[WATCHER][THRESHOLDS][TP-PARTIAL][TRIGGER] user={user_id} ex={exchange} symbol={symbol} "
                f"dir={direction} last={used_price:.6f} entry={entry:.6f} tp={gain_price:.6f} "
                f"close_qty={close_qty:.8f}"
            )
            partial_result = _close_position_result(user_id, exchange, symbol, direction, quantity=close_qty)
            partial_ok = bool(partial_result.get("ok"))
            if partial_ok:
                partial_exec_price = _safe_exec_price(partial_result.get("exit_price"))
                partial_exit_qty = _safe_exec_qty(partial_result.get("exit_qty")) or close_qty
                partial_exit_ts = partial_result.get("exit_ts") or normalize_close_ts(ts) or datetime.now(timezone.utc)
                _register_trades_table_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=f"partial_tp_{PARTIAL_TP_PCT * 100:g}pct",
                    exit_price=partial_exec_price,
                    exit_qty=partial_exit_qty,
                    exit_ts=partial_exit_ts,
                    fees_usdt=0.0,
                )
                _maybe_enqueue_pending_close_resolution(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=f"partial_tp_{PARTIAL_TP_PCT * 100:g}pct",
                    close_result=partial_result,
                    event_ts=partial_exit_ts,
                )
                pos_after = _coerce_position(_current_position(user_id, exchange, symbol))
                remaining_qty = abs(pos_after) if pos_after is not None else max(abs(pos_amt) - close_qty, 0.0)
                post_tp_base = float(used_price)
                post_tp_loss = _compute_post_tp_loss_price(direction, post_tp_base)
                th["partial_tp_done"] = True
                th["fired_gain"] = True
                th["gain_price"] = None
                th["loss_price"] = post_tp_loss
                th["post_tp_sl_active"] = True
                th["post_tp_sl_pct"] = POST_TP_SL_PCT
                th["post_tp_sl_base_price"] = post_tp_base
                th["position_qty_ref"] = remaining_qty if remaining_qty > 0 else None
                th["last_open_ts"] = now_ts
                print(
                    f"[WATCHER][THRESHOLDS][TP-PARTIAL][SL3-ARMED] user={user_id} ex={exchange} symbol={symbol} "
                    f"dir={direction} base={post_tp_base:.6f} loss={post_tp_loss:.6f} pct={POST_TP_SL_PCT}"
                )
                keep_thresholds.append(th)
                alerts.append(
                    {
                        "type": "partial_tp",
                        "timestamp": ts,
                        "message": (
                            f"{symbol} {STREAM_INTERVAL}\n"
                            f"🎯 [TP-PARTIAL] Toma parcial +{PARTIAL_TP_PCT * 100:g}%\n"
                            f"Cuenta: {user_id}/{exchange}\n"
                            f"Dirección: {direction.upper()}\n"
                            f"Cierre parcial: {partial_tp_close_pct * 100:g}%\n"
                            f"Remanente aprox: {remaining_qty:.6f}\n"
                            f"🛡️ Nuevo SL remanente: {post_tp_loss:.2f} (-{POST_TP_SL_PCT * 100:g}% desde TP)"
                        ),
                        "direction": direction,
                        "user_id": user_id,
                        "exchange": exchange,
                    }
                )
                updated = True
                continue
            print(
                f"[WATCHER][THRESHOLDS][TP-PARTIAL][RETRY] user={user_id} ex={exchange} symbol={symbol} "
                f"next_in={THRESHOLDS_RETRY_SECONDS}s"
            )
            keep_thresholds.append(th)
            continue

        if hit_loss:
            loss_pct_used = post_tp_sl_pct if post_tp_sl_active else LOSS_PCT
            kind = str(th.get("loss_reason") or f"stop_loss_{loss_pct_used * 100:g}pct")
            # Ejecuta cierre reduceOnly MARKET del tamaño actual
            print(
                f"[WATCHER][THRESHOLDS][TRIGGER] user={user_id} ex={exchange} symbol={symbol} dir={direction} "
                f"last={used_price:.6f} entry={entry:.6f} loss={loss_price:.6f} gain={gain_price} kind={kind}"
            )
            close_result = _close_position_result(user_id, exchange, symbol, direction)
            close_ok = bool(close_result.get("ok"))
            th["last_close_attempt"] = now_ts
            if close_ok:
                flip_qty_override = None if post_tp_sl_active else (abs(pos_amt) if partial_tp_done else None)
                close_exec_price = _safe_exec_price(close_result.get("exit_price"))
                close_exit_qty = _safe_exec_qty(close_result.get("exit_qty")) or abs(pos_amt)
                close_exit_ts = close_result.get("exit_ts") or normalize_close_ts(ts) or datetime.now(timezone.utc)
                close_exit_price_for_balance = float(close_exec_price if close_exec_price is not None else used_price)
                _record_balance_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry,
                    exit_price=close_exit_price_for_balance,
                    reason=str(kind),
                    close_ts=close_exit_ts,
                    source="live",
                )
                _register_trades_table_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=str(kind),
                    exit_price=close_exec_price,
                    exit_qty=close_exit_qty,
                    exit_ts=close_exit_ts,
                    fees_usdt=0.0,
                )
                _maybe_enqueue_pending_close_resolution(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=str(kind),
                    close_result=close_result,
                    event_ts=close_exit_ts,
                )
                print(
                    f"[WATCHER][THRESHOLDS][CLOSE] user={user_id} ex={exchange} symbol={symbol} ok=True kind={kind}"
                )
                alerts.append(
                    {
                        "type": "auto_close",
                        "timestamp": ts,
                        "message": (
                            f"{symbol} {STREAM_INTERVAL}\n"
                            f"🚨 [SL-HIT] Cierre {direction.upper()} por {kind}\n"
                            f"Estado: FLAT hasta nueva señal/pending\n"
                            f"Entrada: {entry:.2f}\n"
                            f"Último: {used_price:.2f}"
                        ),
                        "direction": direction,
                        "user_id": user_id,
                        "exchange": exchange,
                    }
                )
                updated = True
                # una vez disparado, removemos el registro (se reemplaza con la próxima operación)
                continue
            th["triggered_kind"] = kind
            th["fired_loss"] = hit_loss or fired_loss
            th["fired_gain"] = fired_gain
            print(
                f"[WATCHER][THRESHOLDS][RETRY] user={user_id} ex={exchange} symbol={symbol} "
                f"kind={kind} next_in={THRESHOLDS_RETRY_SECONDS}s"
            )
            updated = True
            keep_thresholds.append(th)
            continue

        keep_thresholds.append(th)

    if updated:
        _thresholds[:] = keep_thresholds
        _save_thresholds()

    return alerts


def _has_open_position_same_direction(user_id: str, exchange: str, direction: str, symbol: str) -> bool:
    """
    Devuelve True si ya hay posición abierta en la misma dirección para el símbolo.
    Solo aplica a binance; si falla la consulta no bloquea (retorna False).
    """
    try:
        if _account_manager is None:
            return False
        account = _account_manager.get_account(user_id)
        cred = account.get_exchange(exchange)
        pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
        if pos_amt is None:
            print(
                f"[WATCHER][POS][UNKNOWN_INIT] user={user_id} ex={exchange} "
                f"symbol={symbol} where=same_direction"
            )
            return False
        if direction == "long" and pos_amt > 0:
            return True
        if direction == "short" and pos_amt < 0:
            return True
        return False
    except Exception as exc:  # pragma: no cover - externo
        print(f"[WATCHER][WARN] No se pudo obtener posición para {user_id}/{exchange}: {exc}")
        return False


def _has_opposite_position(user_id: str, exchange: str, direction: str, symbol: str) -> bool:
    """
    True si hay posición abierta en el sentido contrario.
    """
    try:
        if _account_manager is None:
            return False
        account = _account_manager.get_account(user_id)
        cred = account.get_exchange(exchange)
        pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
        if pos_amt is None:
            print(
                f"[WATCHER][POS][UNKNOWN_INIT] user={user_id} ex={exchange} "
                f"symbol={symbol} where=opposite_check"
            )
            return False
        if direction == "long" and pos_amt < 0:
            return True
        if direction == "short" and pos_amt > 0:
            return True
        return False
    except Exception:
        return False


def _interval_seconds(interval: str) -> int:
    unit = interval[-1].lower()
    value = int(interval[:-1])
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    raise ValueError(f"Intervalo no soportado: {interval}")


def _retry_backoff_seconds(attempt: int) -> float:
    base = max(POSITION_GUARD_RETRY_BASE_SECONDS, 0.1)
    max_delay = max(POSITION_GUARD_RETRY_MAX_SECONDS, base)
    exp = min(base * (2 ** max(attempt, 0)), max_delay)
    jitter_max = max(POSITION_GUARD_RETRY_JITTER_SECONDS, 0.0)
    jitter = random.uniform(0, jitter_max) if jitter_max > 0 else 0.0
    return exp + jitter


def _make_retry_id(
    *,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    source_event: str,
    event_ts: str | None,
) -> str:
    payload = "|".join(
        [
            str(user_id),
            str(exchange).lower(),
            str(symbol),
            str(direction).lower(),
            str(source_event or ""),
            str(event_ts or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _is_non_retryable_reason(reason: str) -> bool:
    return reason in {
        "credentials_error",
        "account_resolve_error",
        "invalid_direction",
        "invalid_quantity",
        "invalid_price",
        "below_min_price",
    }


def _enqueue_trade_retry(
    *,
    user_id: str,
    exchange: str,
    symbol: str,
    direction: str,
    signal_direction: str | None,
    price: float,
    source_event: str,
    event_ts: str | None,
    reason: str,
    detail: str | None = None,
    quantity_override: float | None = None,
) -> None:
    if not POSITION_GUARD_RETRY_ENABLED or _is_non_retryable_reason(reason):
        return
    now_ts = time.time()
    retry_id = _make_retry_id(
        user_id=user_id,
        exchange=exchange,
        symbol=symbol,
        direction=direction,
        source_event=source_event,
        event_ts=event_ts,
    )
    existing = next((x for x in _trade_retries if x.get("id") == retry_id), None)
    if existing:
        existing["updated_ts"] = now_ts
        existing["last_reason"] = reason
        existing["detail"] = detail
        next_retry = now_ts + _retry_backoff_seconds(int(existing.get("attempt", 0)))
        existing["next_retry_ts"] = min(float(existing.get("next_retry_ts") or next_retry), next_retry)
        _save_trade_retries()
        print(
            f"[WATCHER][RETRY][ENQUEUE] id={retry_id} user={user_id} ex={exchange} "
            f"symbol={symbol} dir={direction} reason={reason} action=update"
        )
        return

    item = {
        "id": retry_id,
        "created_ts": now_ts,
        "updated_ts": now_ts,
        "next_retry_ts": now_ts + _retry_backoff_seconds(0),
        "attempt": 0,
        "last_reason": reason,
        "detail": detail,
        "event": {
            "type": source_event,
            "timestamp": event_ts,
            "symbol": symbol,
            "direction": direction,
            "signal_direction": signal_direction,
            "price": float(price),
            "quantity_override": float(quantity_override) if quantity_override is not None else None,
        },
        "target": {
            "user_id": user_id,
            "exchange": exchange,
        },
    }
    _trade_retries.append(item)
    _save_trade_retries()
    print(
        f"[WATCHER][RETRY][ENQUEUE] id={retry_id} user={user_id} ex={exchange} "
        f"symbol={symbol} dir={direction} reason={reason} action=create"
    )


def _close_opposite_position_result(user_id: str, exchange: str, direction: str, symbol: str, price: float) -> dict:
    """
    Cierra posición opuesta con resultado estructurado, robusto ante position unknown.
    """
    try:
        if _account_manager is None:
            return {"ok": True, "reason": "no_opposite", "detail": "manager_none"}
        if TRADING_DRY_RUN:
            print(
                f"[WATCHER][INFO] Dry-run activo: se omite cierre real de opuesta user={user_id} ex={exchange} symbol={symbol}"
            )
            return {"ok": True, "reason": "no_opposite", "detail": "dry_run"}

        account = _account_manager.get_account(user_id)
        cred = account.get_exchange(exchange)
        pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
        if pos_amt is None:
            print(
                f"[WATCHER][POS][UNKNOWN_INIT] user={user_id} ex={exchange} "
                f"symbol={symbol} where=close_opposite"
            )
            return {"ok": False, "reason": "position_unknown", "detail": "initial_position_unknown"}
        if abs(pos_amt) < 1e-8:
            return {"ok": True, "reason": "no_opposite", "detail": "flat_position"}
        if direction == "long" and pos_amt > 0:
            return {"ok": True, "reason": "no_opposite", "detail": "same_side_long"}
        if direction == "short" and pos_amt < 0:
            return {"ok": True, "reason": "no_opposite", "detail": "same_side_short"}

        qty = abs(pos_amt)
        side = "BUY" if pos_amt < 0 else "SELL"
        if exchange.lower() == "binance":
            api_key, api_secret = cred.resolve_keys(os.environ)
            base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
            client = UMFutures(key=api_key, secret=api_secret, base_url=base_url) if base_url else UMFutures(
                key=api_key, secret=api_secret
            )
            step, min_qty, decimals, _ = _binance_lot_rules(cred, symbol)
            qty = _floor_to_step(qty, step)
            if qty < min_qty:
                return {
                    "ok": False,
                    "reason": "close_rejected",
                    "detail": f"binance_qty_below_min qty={qty:.8f} min={min_qty}",
                }
            client.new_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=f"{qty:.{decimals}f}",
                reduceOnly="true",
            )
        elif exchange.lower() == "bybit":
            from decimal import Decimal, ROUND_DOWN, ROUND_UP
            import re
            from pybit.unified_trading import HTTP  # type: ignore

            api_key, api_secret = cred.resolve_keys(os.environ)
            is_testnet = cred.environment != ExchangeEnvironment.LIVE
            domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
            client = (
                HTTP(
                    api_key=api_key,
                    api_secret=api_secret,
                    testnet=False,
                    domain=domain_env,
                    recv_window=BYBIT_RECV_WINDOW_MS,
                    timeout=BYBIT_HTTP_TIMEOUT,
                )
                if domain_env
                else HTTP(
                    api_key=api_key,
                    api_secret=api_secret,
                    testnet=is_testnet,
                    recv_window=BYBIT_RECV_WINDOW_MS,
                    timeout=BYBIT_HTTP_TIMEOUT,
                )
            )
            side = "Sell" if pos_amt > 0 else "Buy"

            def _quantize(v: float, step: str) -> str:
                dv = Decimal(str(v)).quantize(Decimal(step), rounding=ROUND_DOWN)
                if dv <= 0:
                    dv = Decimal(step)
                return format(dv, "f")

            def _ceil_to_step(value: float, step: str) -> str:
                dv = Decimal(str(value))
                ds = Decimal(step)
                if ds <= 0:
                    return format(dv, "f")
                q = (dv / ds).to_integral_value(rounding=ROUND_UP)
                out = q * ds
                if out <= 0:
                    out = ds
                return format(out, "f")

            def _autocorrect_qty(symbol_: str, qty_s: str) -> str | None:
                try:
                    raw = client.get_instruments_info(category="linear", symbol=str(symbol_).upper())
                    items = raw.get("result", {}).get("list") or []
                    first = items[0] if items else {}
                    lot = first.get("lotSizeFilter") or {}
                    min_qty_s = str(lot.get("minOrderQty") or "")
                    step_s = str(lot.get("qtyStep") or "")
                    if not min_qty_s or not step_s:
                        return None
                    current = float(qty_s)
                    min_qty = float(min_qty_s)
                    target = max(current, min_qty)
                    return _ceil_to_step(target, step_s)
                except Exception:
                    return None

            qty_step = "0.001"
            try:
                raw_info = client.get_instruments_info(category="linear", symbol=str(symbol).upper())
                items_info = raw_info.get("result", {}).get("list") or []
                first_info = items_info[0] if items_info else {}
                lot_info = first_info.get("lotSizeFilter") or {}
                qty_step = str(lot_info.get("qtyStep") or qty_step)
            except Exception as exc:
                print(
                    f"[WATCHER][WARN] No se pudo leer qtyStep de Bybit "
                    f"user={user_id} symbol={symbol}: {exc}. Fallback step={qty_step}"
                )

            qty_s = _quantize(qty, qty_step)
            params = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": qty_s,
                "reduceOnly": True,
            }
            try:
                raw = client.place_order(**params)
            except Exception as exc:
                msg = str(exc)
                err_code = None
                m = re.search(r"ErrCode:\\s*(\\d+)", msg)
                if m:
                    try:
                        err_code = int(m.group(1))
                    except Exception:
                        err_code = None
                looks_like_qty_error = (
                    (err_code == 10001)
                    or ("minimum limit" in msg.lower())
                    or ("qty" in msg.lower() and "invalid" in msg.lower())
                    or ("precision" in msg.lower())
                )
                if looks_like_qty_error:
                    corrected = _autocorrect_qty(symbol, qty_s)
                    if corrected and corrected != qty_s:
                        print(
                            f"[WATCHER][WARN] Bybit rechazó qty en cierre; reintentando symbol={symbol} "
                            f"qty={qty_s} -> {corrected} err={msg}"
                        )
                        raw = client.place_order(**{**params, "qty": corrected})
                    else:
                        raise
                else:
                    raise
            ret_code = raw.get("retCode")
            if ret_code not in (None, 0, "0"):
                msg = raw.get("retMsg") or "BYBIT_ERROR"
                raise RuntimeError(f"Bybit retCode={ret_code} retMsg={msg}")
        else:
            return {"ok": False, "reason": "close_rejected", "detail": "unsupported_exchange"}

        print(
            f"[WATCHER][INFO] Cierre reduceOnly (MARKET) de posición opuesta qty={qty} side={side} en {symbol} ex={exchange}"
        )
        if CLOSE_OPPOSITE_TIMEOUT_SECONDS <= 0:
            return {"ok": True, "reason": "closed", "detail": "close_sent_no_wait"}
        print(
            f"[WATCHER][INFO] Esperando cierre completo de posición opuesta user={user_id} ex={exchange} symbol={symbol}"
        )
        deadline = time.time() + CLOSE_OPPOSITE_TIMEOUT_SECONDS
        while True:
            pos_now = _coerce_position(_current_position(user_id, exchange, symbol))
            if pos_now is None:
                print(
                    f"[WATCHER][POS][UNKNOWN_POLL] user={user_id} ex={exchange} "
                    f"symbol={symbol} where=close_opposite_confirm"
                )
            elif abs(pos_now) < 1e-8:
                return {"ok": True, "reason": "closed", "detail": "confirmed_flat"}
            if time.time() >= deadline:
                print(
                    f"[WATCHER][POS][CONFIRM_TIMEOUT] user={user_id} ex={exchange} "
                    f"symbol={symbol} pos={_format_pos(pos_now)}"
                )
                return {
                    "ok": False,
                    "reason": "confirm_timeout_unknown_or_open",
                    "detail": f"final_pos={_format_pos(pos_now)}",
                }
            time.sleep(max(CLOSE_OPPOSITE_POLL_SECONDS, 0.1))
    except Exception as exc:  # pragma: no cover - externo
        print(f"[WATCHER][WARN] No se pudo verificar/cerrar posición opuesta: {exc}")
        return {"ok": False, "reason": "exception", "detail": str(exc)}


def _close_opposite_position(user_id: str, exchange: str, direction: str, symbol: str, price: float) -> bool:
    return bool(_close_opposite_position_result(user_id, exchange, direction, symbol, price).get("ok"))


def _process_trade_retry_queue(now_ts: float) -> None:
    if not POSITION_GUARD_RETRY_ENABLED:
        return
    if not _trade_retries:
        return

    ttl = max(POSITION_GUARD_RETRY_TTL_SECONDS, 60.0)
    changed = False
    keep: list[dict] = []
    for item in _trade_retries:
        if not isinstance(item, dict):
            changed = True
            continue
        retry_id = str(item.get("id") or "")
        created_ts = float(item.get("created_ts") or now_ts)
        age = max(0.0, now_ts - created_ts)
        if age > ttl:
            print(f"[WATCHER][RETRY][EXPIRED] id={retry_id} age={age:.1f}s ttl={ttl:.1f}s")
            changed = True
            continue

        next_retry_ts = float(item.get("next_retry_ts") or 0.0)
        if now_ts < next_retry_ts:
            keep.append(item)
            continue

        event = item.get("event") or {}
        target = item.get("target") or {}
        user_id = str(target.get("user_id") or "")
        exchange = str(target.get("exchange") or "")
        direction = str(event.get("direction") or "")
        signal_direction = event.get("signal_direction")
        symbol = str(event.get("symbol") or "")
        source_event = str(event.get("type") or "retry_signal")
        event_ts = str(event.get("timestamp")) if event.get("timestamp") is not None else None
        price = float(event.get("price") or 0.0)
        qty_override_raw = event.get("quantity_override")
        qty_override = float(qty_override_raw) if qty_override_raw not in (None, "") else None
        attempt = int(item.get("attempt") or 0)
        print(
            f"[WATCHER][RETRY][PROCESS] id={retry_id} attempt={attempt + 1} "
            f"user={user_id} ex={exchange} symbol={symbol} dir={direction}"
        )
        ok, _ = _execute_trade_for_target(
            user_id=user_id,
            exchange=exchange,
            direction=direction,
            symbol=symbol,
            price=price,
            source_event=source_event,
            signal_direction=signal_direction,
            event_ts=event_ts,
            enqueue_on_fail=False,
            quantity_override=qty_override,
        )
        if ok:
            print(
                f"[WATCHER][RETRY][SUCCESS] id={retry_id} user={user_id} ex={exchange} "
                f"symbol={symbol} dir={direction}"
            )
            changed = True
            continue

        attempt += 1
        delay = _retry_backoff_seconds(attempt)
        item["attempt"] = attempt
        item["updated_ts"] = now_ts
        item["last_reason"] = "retry_failed"
        item["next_retry_ts"] = now_ts + delay
        keep.append(item)
        changed = True
        print(
            f"[WATCHER][RETRY][FAIL] id={retry_id} user={user_id} ex={exchange} "
            f"symbol={symbol} dir={direction} next={item['next_retry_ts']:.3f}"
        )
        if attempt in {1, 3, 5} or attempt % 10 == 0:
            _send_ops_alert(
                code="retry_fail",
                user_id=user_id,
                exchange=exchange,
                symbol=symbol,
                message=(
                    f"⚠️ [OPS] Reintento de trade falló\n"
                    f"Cuenta: {user_id}/{exchange}\n"
                    f"Símbolo: {symbol}\n"
                    f"Intento: {attempt}\n"
                    f"Reason: {item.get('last_reason', 'retry_failed')}\n"
                    f"Próximo retry en ~{delay:.1f}s"
                ),
            )

    if changed:
        _trade_retries[:] = keep
        _save_trade_retries()


def _process_pending_execution_resolutions(now_ts: float) -> None:
    if not _pending_execution_resolutions:
        return
    manager = _load_manager()
    if manager is None:
        return

    changed = False
    keep: list[dict] = []
    ttl = max(TRADES_FILL_RESOLVE_TTL_SECONDS, 60.0)
    for item in _pending_execution_resolutions:
        if not isinstance(item, dict):
            changed = True
            continue
        rid = str(item.get("id") or "")
        created_ts = float(item.get("created_ts") or now_ts)
        if now_ts - created_ts > ttl:
            print(f"[WATCHER][TRADES_FILL_RESOLVE_FAIL] id={rid} reason=expired ttl={ttl:.1f}s")
            changed = True
            continue

        next_retry_ts = float(item.get("next_retry_ts") or 0.0)
        if now_ts < next_retry_ts:
            keep.append(item)
            continue

        user_id = str(item.get("user_id") or "")
        exchange = str(item.get("exchange") or "")
        symbol = str(item.get("symbol") or "")
        kind = str(item.get("kind") or "")
        direction = str(item.get("direction") or "")
        side = str(item.get("side") or "")
        order_id = str(item.get("order_id") or "")
        client_order_id = str(item.get("client_order_id") or "")
        qty_hint = _safe_exec_qty(item.get("qty_hint"))
        close_reason = str(item.get("close_reason") or "")
        source_event = str(item.get("source_event") or "")
        event_ts = item.get("event_ts")
        attempt = int(item.get("attempt") or 0)

        try:
            account = manager.get_account(user_id)
            cred = account.get_exchange(exchange)
        except Exception as exc:
            print(f"[WATCHER][TRADES_FILL_RESOLVE_FAIL] id={rid} reason=account_resolve err={exc}")
            changed = True
            continue

        out = _resolve_order_fill_sync(
            user_id=user_id,
            exchange=exchange,
            cred=cred,
            symbol=symbol,
            side=side,
            order_id=order_id,
            client_order_id=client_order_id,
            qty_hint=qty_hint,
            wait_seconds=TRADES_FILL_RESOLVE_ATTEMPT_SECONDS,
            phase=f"pending_{kind}",
        )
        if out.get("ok"):
            exec_price = _safe_exec_price(out.get("price"))
            exec_qty = _safe_exec_qty(out.get("qty"))
            exec_ts = normalize_close_ts(out.get("ts")) or datetime.now(timezone.utc)
            fees = float(out.get("fees_usdt") or 0.0)
            if kind == "open":
                ok_state = _set_open_trade_state(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    entry_price_real=exec_price,
                    entry_qty_real=exec_qty,
                    entry_ts_real=exec_ts,
                    entry_order_id=order_id,
                    entry_client_order_id=client_order_id,
                    source_event=source_event,
                )
                if ok_state:
                    print(
                        f"[WATCHER][TRADES_FILL_RESOLVE_OK] id={rid} kind=open "
                        f"user={user_id} ex={exchange} symbol={symbol}"
                    )
                    changed = True
                    continue
            elif kind == "close":
                key = _trade_state_key(user_id, exchange, symbol)
                if key not in _open_trade_state:
                    item["attempt"] = attempt + 1
                    item["updated_ts"] = now_ts
                    item["next_retry_ts"] = now_ts + _retry_backoff_seconds(attempt + 1)
                    keep.append(item)
                    changed = True
                    continue
                _register_trades_table_close(
                    user_id=user_id,
                    exchange=exchange,
                    symbol=symbol,
                    direction=direction,
                    close_reason=close_reason or "pending_close",
                    exit_price=exec_price,
                    exit_qty=exec_qty,
                    exit_ts=exec_ts,
                    fees_usdt=fees,
                )
                print(
                    f"[WATCHER][TRADES_FILL_RESOLVE_OK] id={rid} kind=close "
                    f"user={user_id} ex={exchange} symbol={symbol}"
                )
                changed = True
                continue

        item["attempt"] = attempt + 1
        item["updated_ts"] = now_ts
        item["next_retry_ts"] = now_ts + _retry_backoff_seconds(attempt + 1)
        item["last_reason"] = str(out.get("detail") or "fill_unresolved")
        keep.append(item)
        changed = True

    if changed:
        _pending_execution_resolutions[:] = keep
        _save_pending_execution_resolutions()


def _submit_trade(event: dict) -> list[dict]:
    post_trade_alerts: list[dict] = []
    if _resolve_executor() is None:
        return post_trade_alerts
    targets = _resolve_targets()
    if not targets:
        print("[WATCHER][WARN] No hay cuentas habilitadas/filtradas para operar; se omite trading.")
        return post_trade_alerts
    try:
        signal_direction = (event.get("direction") or "").lower()
        order_direction = signal_direction
        _direction_to_side(order_direction)
    except Exception as exc:
        print(f"[WATCHER][WARN] No se pudo determinar dirección para trading: {exc}")
        return post_trade_alerts

    price = _price_from_event(event)
    if price is None or price <= 0:
        print("[WATCHER][WARN] Evento sin precio de referencia, se omite trading.")
        return post_trade_alerts
    if TRADING_MIN_PRICE > 0 and price < TRADING_MIN_PRICE:
        print(f"[WATCHER][INFO] Precio {price:.2f} < mínimo configurado ({TRADING_MIN_PRICE}); no se opera.")
        return post_trade_alerts
    event_ts = str(event.get("timestamp")) if event.get("timestamp") is not None else None
    symbol = event.get("symbol") or SYMBOL_DISPLAY.replace(".P", "")

    for user_id, exchange in targets:
        ok, entry = _execute_trade_for_target(
            user_id=user_id,
            exchange=exchange,
            direction=order_direction,
            symbol=symbol,
            price=price,
            source_event=event.get("type", "unknown"),
            signal_direction=signal_direction,
            event_ts=event_ts,
            post_trade_alerts=post_trade_alerts,
        )
        if ok and entry:
            _register_threshold(
                user_id,
                exchange,
                symbol,
                order_direction,
                float(entry),
                signal_direction,
                entry_source="signal",
            )
    return post_trade_alerts

POLL_SECONDS = float(os.getenv("ALERT_POLL_SECONDS", "5"))
MAX_SEEN = int(os.getenv("ALERT_MAX_SEEN", "500"))
SEND_STARTUP_TEST = os.getenv("WATCHER_STARTUP_TEST_ALERT", "true").lower() == "true"
THRESHOLD_POLL_SECONDS = float(os.getenv("THRESHOLD_POLL_SECONDS", "1"))
THRESHOLDS_DUMP_SECONDS = float(os.getenv("THRESHOLDS_DUMP_SECONDS", "300"))
THRESHOLDS_RETRY_SECONDS = float(os.getenv("THRESHOLDS_RETRY_SECONDS", "10"))
THRESHOLDS_DUMP_SECONDS = float(os.getenv("THRESHOLDS_DUMP_SECONDS", "300"))
THRESHOLD_GUARD_INTERVAL = float(os.getenv("WATCHER_THRESHOLD_GUARD_INTERVAL", "180"))
ALERT_SIGNAL_WORKER_ENABLED = os.getenv("ALERT_SIGNAL_WORKER_ENABLED", "true").lower() == "true"
ALERT_SIGNAL_WORKER_STALE_SECONDS = float(os.getenv("ALERT_SIGNAL_WORKER_STALE_SECONDS", "30"))
ALERT_SIGNAL_WORKER_RESTART_BACKOFF_SECONDS = float(
    os.getenv("ALERT_SIGNAL_WORKER_RESTART_BACKOFF_SECONDS", "3")
)
ALERT_SIGNAL_WORKER_QUEUE_MAX = int(os.getenv("ALERT_SIGNAL_WORKER_QUEUE_MAX", "200"))
MARK_PRICE_RETRY_COUNT = int(os.getenv("WATCHER_MARK_PRICE_RETRY_COUNT", "2"))
MARK_PRICE_RETRY_DELAY = float(os.getenv("WATCHER_MARK_PRICE_RETRY_DELAY", "0.25"))
SIGNAL_BUS_EXPORT_ENABLED = os.getenv("SIGNAL_BUS_EXPORT_ENABLED", "true").lower() == "true"
SIGNAL_BUS_EXPORT_FILE = Path(os.getenv("SIGNAL_BUS_EXPORT_FILE", "/home/ubuntu/shared/bot1_signals.jsonl"))
SIGNAL_BUS_EXPORT_TYPES = {
    part.strip().lower()
    for part in os.getenv("SIGNAL_BUS_EXPORT_TYPES", "bollinger_signal").replace(";", ",").split(",")
    if part.strip()
}


def _notify_startup():
    if not SEND_STARTUP_TEST:
        return
    try:
        now_utc = datetime.now(timezone.utc)
        entry_time = now_utc - timedelta(minutes=45)
        ts_entry = format_timestamp(entry_time)
        ts_exit = format_timestamp(now_utc)

        opening_msg = (
            f"{SYMBOL_DISPLAY} {STREAM_INTERVAL}\n"
            f"[PRUEBA] Apertura LONG\n"
            f"Precio: 3500.00\n"
            f"Hora: {ts_entry}\n"
            f"Motivo: ALERTA_DE_PRUEBA"
        )

        closing_msg = (
            f"{SYMBOL_DISPLAY} {STREAM_INTERVAL}\n"
            f"[PRUEBA] Cierre LONG\n"
            f"Entrada: 3500.00 ({ts_entry})\n"
            f"Salida: 3600.00 ({ts_exit})\n"
            f"Fees: 3.50\n"
            f"Resultado: GANANCIA 96.50 (+2.76%)"
        )

        send_trade_notification(opening_msg)
        send_trade_notification(closing_msg)

        sample_alert = {
            "type": "heartbeat_test",
            "timestamp": now_utc,
            "message": (
                f"{SYMBOL_DISPLAY} {STREAM_INTERVAL}: [PRUEBA] Señal formateada\n"
                f"Entrada simulada 3500 → 3600 (+2.76%)"
            ),
            "direction": "long",
        }
        send_alerts([sample_alert])
    except Exception as exc:
        print(f"[WATCHER][WARN] No se pudo enviar la alerta de prueba: {exc}")


def _dump_thresholds(ts: datetime) -> None:
    manager = _account_manager or _load_manager()
    print(f"[WATCHER][THRESHOLDS][DUMP] ts={ts.isoformat()} count={len(_thresholds)}")
    if not _thresholds:
        return
    price_cache: dict[tuple[str, str], float | None] = {}
    for th in _thresholds:
        user_id = th.get("user_id")
        exchange = th.get("exchange")
        symbol = th.get("symbol", SYMBOL_DISPLAY.replace(".P", ""))
        signal_direction = th.get("signal_direction")
        entry = float(th.get("entry_price") or 0)
        loss_price = float(th.get("loss_price") or 0)
        gain_raw = th.get("gain_price")
        gain_price = float(gain_raw) if gain_raw not in (None, "") else None
        mark = None
        triggered_kind = th.get("triggered_kind")
        last_attempt = th.get("last_close_attempt")
        ex_key = str(exchange).lower() if exchange else ""
        cache_key = (ex_key, symbol)
        if cache_key in price_cache:
            mark = price_cache[cache_key]
        else:
            if manager is not None and user_id and exchange:
                try:
                    account = manager.get_account(user_id)
                    cred = account.get_exchange(exchange)
                    if ex_key == "binance":
                        mark = _binance_mark_price(cred, symbol)
                    elif ex_key == "bybit":
                        mark = _bybit_mark_price(cred, symbol)
                except Exception:
                    mark = None
            price_cache[cache_key] = mark
        print(
            f"[WATCHER][THRESHOLDS][DUMP] user={user_id} ex={exchange} symbol={symbol} "
            f"entry={entry:.6f} loss={loss_price:.6f} gain={gain_price} mark={mark} "
            f"signal_dir={signal_direction} "
            f"triggered={triggered_kind} last_attempt={last_attempt}"
        )


def _signal_worker_put(signal_queue: Queue, payload: dict) -> None:
    try:
        signal_queue.put_nowait(payload)
    except QueueFull:
        try:
            _ = signal_queue.get_nowait()
            signal_queue.put_nowait(payload)
        except Exception:
            pass


def _signal_worker_loop(signal_queue: Queue, poll_seconds: float) -> None:
    pid = os.getpid()
    loop_sleep = max(float(poll_seconds), 0.2)
    print(f"[WATCHER][SIGNAL-WORKER][START] pid={pid} poll={loop_sleep}")
    while True:
        _signal_worker_put(signal_queue, {"kind": "heartbeat", "ts": time.time(), "pid": pid})
        try:
            events = generate_alerts()
            if events:
                _signal_worker_put(signal_queue, {"kind": "events", "ts": time.time(), "pid": pid, "events": events})
        except Exception as exc:
            _signal_worker_put(signal_queue, {"kind": "fail", "ts": time.time(), "pid": pid, "error": str(exc)})
        _signal_worker_put(signal_queue, {"kind": "heartbeat", "ts": time.time(), "pid": pid})
        time.sleep(loop_sleep)


def _signal_bus_ts_iso(value) -> str:
    if isinstance(value, datetime):
        try:
            return value.astimezone(timezone.utc).isoformat()
        except Exception:
            return value.isoformat()
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _signal_bus_export_event(evt: dict) -> None:
    if not SIGNAL_BUS_EXPORT_ENABLED:
        return
    event_type = str(evt.get("type") or "").strip().lower()
    if event_type not in SIGNAL_BUS_EXPORT_TYPES:
        return
    try:
        ts_iso = _signal_bus_ts_iso(evt.get("timestamp"))
        symbol = str(evt.get("symbol") or SYMBOL_DISPLAY.replace(".P", ""))
        direction = str(evt.get("direction") or "").strip().lower()
        price = _price_from_event(evt)
        raw = "|".join(
            [
                event_type,
                ts_iso,
                symbol,
                direction,
                "" if price is None else f"{float(price):.8f}",
            ]
        )
        event_id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
        payload = {
            "event_id": event_id,
            "type": event_type,
            "timestamp_iso": ts_iso,
            "symbol": symbol,
            "direction": direction,
            "price": None if price is None else float(price),
            "producer_repo": "bot",
            "exported_at": datetime.now(timezone.utc).isoformat(),
        }
        SIGNAL_BUS_EXPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with SIGNAL_BUS_EXPORT_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False))
            fh.write("\n")
        print(
            f"[SIGNAL-BUS][WRITE][OK] id={event_id} type={event_type} "
            f"symbol={symbol} dir={direction} file={SIGNAL_BUS_EXPORT_FILE}"
        )
    except Exception as exc:
        print(f"[SIGNAL-BUS][WRITE][WARN] err={exc}")


def _range3_load_pending() -> dict | None:
    try:
        if not RANGE_PENDING_PATH.exists():
            return None
        data = json.loads(RANGE_PENDING_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) and data.get("direction") else None
    except Exception as exc:
        print(f"[WATCHER][RANGE3][PENDING][WARN] load_failed err={exc}")
        return None


def _range3_save_pending(pending: dict | None) -> None:
    try:
        RANGE_PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not pending:
            if RANGE_PENDING_PATH.exists():
                RANGE_PENDING_PATH.unlink()
            return
        RANGE_PENDING_PATH.write_text(json.dumps(pending, ensure_ascii=False, default=str), encoding="utf-8")
    except Exception as exc:
        print(f"[WATCHER][RANGE3][PENDING][WARN] save_failed err={exc}")


def _apply_sma_pending_defensive_sl(pending_direction: str) -> None:
    """
    Stable SMA115: cuando queda un pending contrario y hay posición abierta,
    ajusta el SL de esa posición al 2% defensivo. El pending sigue activo.
    """
    manager = _load_manager()
    if manager is None:
        return
    changed = False
    for user_id, exchange in _resolve_targets():
        try:
            account = manager.get_account(user_id)
            cred = account.get_exchange(exchange)
            symbol = (cred.extra or {}).get("symbol") or SYMBOL_DISPLAY.replace(".P", "")
            pos_amt = _coerce_position(_current_position(user_id, exchange, symbol))
        except Exception:
            continue
        if pos_amt is None or abs(pos_amt) < 1e-8:
            continue
        pos_dir = "long" if pos_amt > 0 else "short"
        if pos_dir == pending_direction:
            continue
        for th in _thresholds:
            if (
                str(th.get("user_id")) == str(user_id)
                and str(th.get("exchange")).lower() == str(exchange).lower()
                and str(th.get("symbol")) == str(symbol)
                and str(th.get("direction")).lower() == str(pos_dir)
            ):
                entry = float(th.get("entry_price") or 0)
                if entry <= 0:
                    continue
                if pos_dir == "long":
                    th["loss_price"] = entry * (1.0 - SMA_STABLE_PENDING_SL_PCT)
                else:
                    th["loss_price"] = entry * (1.0 + SMA_STABLE_PENDING_SL_PCT)
                th["loss_reason"] = f"pending_defensive_stop_loss_{SMA_STABLE_PENDING_SL_PCT * 100:g}pct"
                th["pending_defensive_active"] = True
                th["fired_loss"] = False
                changed = True
                print(
                    f"[WATCHER][SMA115][PENDING-SL] user={user_id} ex={exchange} symbol={symbol} "
                    f"pos={pos_dir} pending={pending_direction} loss={float(th['loss_price']):.6f}"
                )
                break
    if changed:
        _save_thresholds()


def _sma115_pending_confirmed(pending: dict, state_evt: dict) -> bool:
    direction = str(pending.get("direction") or "").lower()
    side = str(state_evt.get("side") or "").lower()
    if direction not in {"long", "short"} or side != direction:
        return False
    # Reproduce la stable final: el pending se llena cuando el precio vuelve
    # a estar dentro del 1% de la SMA. El prev10 del pending queda trazado
    # desde la señal original.
    return bool(state_evt.get("distance_ok"))


def _sma115_execute_pending(pending: dict, state_evt: dict) -> None:
    event = dict(pending.get("event") or {})
    direction = str(pending.get("direction") or event.get("direction") or "").lower()
    price = float(state_evt.get("close_price") or state_evt.get("price") or event.get("price") or 0)
    if direction not in {"long", "short"} or price <= 0:
        _range3_save_pending(None)
        return
    event.update(
        {
            "type": "sma115_pending_fill",
            "timestamp": state_evt.get("timestamp") or datetime.now(timezone.utc),
            "direction": direction,
            "price": price,
            "entry_price": price,
            "close_price": price,
            "entry_mode": "pending_fill",
            "sma": state_evt.get("sma"),
            "distance_pct": state_evt.get("distance_pct"),
            "message": (
                f"📥 [SMA115] Pending ejecutado {direction.upper()} "
                f"{event.get('symbol', SYMBOL_DISPLAY.replace('.P', ''))} en {price:.2f}"
            ),
        }
    )
    _range3_save_pending(None)
    print(
        f"[WATCHER][SMA115][PENDING][FILL] dir={direction} price={price:.6f} "
        f"ts={event.get('timestamp')}"
    )
    alerts_to_send = [event]
    if TRADING_ENABLED:
        alerts_to_send.extend(_submit_trade(event) or [])
    try:
        send_alerts(alerts_to_send)
    except Exception as exc:
        print(f"[WATCHER][SMA115][PENDING][WARN] fill_alert_failed err={exc}")


def _sma115_process_state(evt: dict) -> None:
    pending = _range3_load_pending()
    if not pending or str(pending.get("strategy") or "") != "sma115_stable":
        return
    if _sma115_pending_confirmed(pending, evt):
        _sma115_execute_pending(pending, evt)


def _sma115_process_signal(evt: dict) -> bool:
    direction = str(evt.get("direction") or "").lower()
    if direction not in {"long", "short"}:
        return False
    pending = _range3_load_pending()
    if pending:
        print(
            f"[WATCHER][SMA115][PENDING][REPLACE] prev_dir={pending.get('direction')} "
            f"new_dir={direction}"
        )
        _range3_save_pending(None)
    if evt.get("entry_mode") != "pending":
        return False
    pending_payload = {
        "strategy": "sma115_stable",
        "direction": direction,
        "symbol": evt.get("symbol") or SYMBOL_DISPLAY.replace(".P", ""),
        "created_ts": datetime.now(timezone.utc).isoformat(),
        "signal_ts": str(evt.get("timestamp")),
        "event": evt,
    }
    _range3_save_pending(pending_payload)
    _apply_sma_pending_defensive_sl(direction)
    _signal_bus_export_event(evt)
    print(
        f"[WATCHER][SMA115][PENDING][SET] dir={direction} "
        f"symbol={pending_payload['symbol']} close={evt.get('close_price')}"
    )
    try:
        send_alerts([evt])
    except Exception as exc:
        print(f"[WATCHER][SMA115][PENDING][WARN] alert_failed err={exc}")
    return True


def _range3_pending_hit(pending: dict, high: float, low: float) -> bool:
    try:
        side = int(pending.get("side") or 0)
        price = float(pending.get("pending_price") or 0)
        order_type = str(pending.get("pending_order_type") or "Stop en banda")
    except Exception:
        return False
    if side == 1:
        return high >= price if order_type == "Stop en banda" else low <= price
    if side == -1:
        return low <= price if order_type == "Stop en banda" else high >= price
    return False


def _range3_execute_pending(pending: dict, ts) -> None:
    event = dict(pending.get("event") or {})
    direction = str(pending.get("direction") or event.get("direction") or "").lower()
    price = float(pending.get("pending_price") or event.get("price") or 0)
    if direction not in {"long", "short"} or price <= 0:
        _range3_save_pending(None)
        return
    event.update(
        {
            "type": "range3_pending_fill",
            "timestamp": ts,
            "direction": direction,
            "price": price,
            "entry_price": price,
            "close_price": price,
            "range_entry_mode": "pending_fill",
            "message": (
                f"📥 [RANGE3+BB] Pendiente ejecutada {direction.upper()} "
                f"{event.get('symbol', SYMBOL_DISPLAY.replace('.P', ''))} en {price:.2f}"
            ),
        }
    )
    _range3_save_pending(None)
    print(
        f"[WATCHER][RANGE3][PENDING][FILL] dir={direction} price={price:.6f} "
        f"ts={ts}"
    )
    alerts_to_send = [event]
    if TRADING_ENABLED:
        alerts_to_send.extend(_submit_trade(event) or [])
    try:
        send_alerts(alerts_to_send)
    except Exception as exc:
        print(f"[WATCHER][RANGE3][PENDING][WARN] fill_alert_failed err={exc}")


def _range3_process_state(evt: dict) -> None:
    pending = _range3_load_pending()
    if not pending:
        return
    try:
        high = float(evt.get("high") or evt.get("price") or 0)
        low = float(evt.get("low") or evt.get("price") or 0)
    except Exception:
        return
    if _range3_pending_hit(pending, high, low):
        _range3_execute_pending(pending, evt.get("timestamp") or datetime.now(timezone.utc))
        return
    side = int(pending.get("side") or 0)
    if (side == -1 and evt.get("new_max_now")) or (side == 1 and evt.get("new_min_now")):
        print(
            f"[WATCHER][RANGE3][PENDING][CANCEL] dir={pending.get('direction')} "
            f"price={pending.get('pending_price')} reason=new_extreme"
        )
        _range3_save_pending(None)


def _range3_process_signal(evt: dict) -> bool:
    pending = _range3_load_pending()
    direction = str(evt.get("direction") or "").lower()
    side = 1 if direction == "long" else -1 if direction == "short" else 0
    if side == 0:
        return False

    if pending:
        pending_side = int(pending.get("side") or 0)
        if pending_side != side:
            print(
                f"[WATCHER][RANGE3][PENDING][CANCEL] prev_dir={pending.get('direction')} "
                f"new_dir={direction} reason=opposite_signal"
            )
            _range3_save_pending(None)
            return True
        if evt.get("range_entry_mode") != "pending":
            print(
                f"[WATCHER][RANGE3][PENDING][CONSECUTIVE-CLOSE] dir={direction} "
                f"price={evt.get('close_price') or evt.get('price')}"
            )
            _range3_save_pending(None)
            evt["entry_price"] = evt.get("close_price") or evt.get("price")
            evt["price"] = evt["entry_price"]
            evt["range_entry_mode"] = "direct"
            evt["message"] = (
                f"📶 [RANGE3+BB] Señal consecutiva {direction.upper()} sin nuevo extremo; "
                f"entrada directa en {float(evt['price']):.2f}"
            )
            return False

    if evt.get("range_entry_mode") != "pending":
        return False

    pending_price = float(evt.get("pending_price") or evt.get("price") or 0)
    pending_payload = {
        "side": side,
        "direction": direction,
        "symbol": evt.get("symbol") or SYMBOL_DISPLAY.replace(".P", ""),
        "pending_price": pending_price,
        "pending_order_type": evt.get("pending_order_type") or "Stop en banda",
        "created_ts": datetime.now(timezone.utc).isoformat(),
        "signal_ts": str(evt.get("timestamp")),
        "event": evt,
    }
    _range3_save_pending(pending_payload)
    _signal_bus_export_event(evt)
    print(
        f"[WATCHER][RANGE3][PENDING][SET] dir={direction} price={pending_price:.6f} "
        f"symbol={pending_payload['symbol']}"
    )
    try:
        send_alerts([evt])
    except Exception as exc:
        print(f"[WATCHER][RANGE3][PENDING][WARN] alert_failed err={exc}")
    return True


def _process_events(events: list[dict], seen: list[tuple[str, object]]) -> None:
    for evt in events or []:
        if evt.get("type") == "sma115_state":
            _sma115_process_state(evt)
            continue
        if evt.get("type") == "range3_state":
            _range3_process_state(evt)
            continue
        ts = evt.get("timestamp")
        key = (evt.get("type"), ts)
        if key in seen:
            continue
        seen.append(key)
        seen[:] = seen[-MAX_SEEN:]
        if evt.get("type") == "sma115_signal" and _sma115_process_signal(evt):
            continue
        if evt.get("type") == "range3_signal" and _range3_process_signal(evt):
            continue
        _signal_bus_export_event(evt)
        alerts_to_send = [evt]
        try:
            current_price = _price_from_event(evt)
            if current_price:
                ts_eval = evt.get("timestamp", datetime.now(timezone.utc))
                alerts_to_send.extend(_evaluate_thresholds(current_price, ts_eval))
        except Exception:
            pass
        print(f"[ALERTA] {format_alert_message(evt)}")
        try:
            send_alerts(alerts_to_send)
        except Exception as exc:
            print(f"[ALERT][WARN] Falló envío de alertas ({exc})")
        if TRADING_ENABLED:
            post_trade_alerts = _submit_trade(evt)
            if post_trade_alerts:
                try:
                    send_alerts(post_trade_alerts)
                except Exception as exc:
                    print(f"[ALERT][WARN] Falló envío de alertas post-trade ({exc})")


def main():
    seen: list[tuple[str, object]] = []
    _notify_startup()
    _run_balance_backfill_once()
    print(f"[WATCHER][CONFIG] LOSS_PCT={LOSS_PCT} source={LOSS_PCT_SOURCE}")
    print(
        f"[WATCHER][CONFIG] BALANCE_LEDGER_PATH={BALANCE_LEDGER_PATH} "
        f"BALANCE_BACKFILL_STATE_PATH={BALANCE_BACKFILL_STATE_PATH}"
    )
    print(
        f"[WATCHER][CONFIG] TRADES_TABLE_LEDGER_PATH={TRADES_TABLE_LEDGER_PATH} "
        f"OPEN_TRADE_STATE_PATH={OPEN_TRADE_STATE_PATH} "
        f"PENDING_EXECUTION_RESOLUTIONS_PATH={PENDING_EXECUTION_RESOLUTIONS_PATH} "
        f"ORDER_ID_PREFIX={ORDER_ID_PREFIX}"
    )
    print(
        f"[WATCHER][CONFIG] TRADES_FILL_RESOLVE_WAIT_SECONDS={TRADES_FILL_RESOLVE_WAIT_SECONDS} "
        f"TRADES_FILL_RESOLVE_TTL_SECONDS={TRADES_FILL_RESOLVE_TTL_SECONDS} "
        f"TRADES_FILL_RESOLVE_WORKER_POLL_SECONDS={TRADES_FILL_RESOLVE_WORKER_POLL_SECONDS}"
    )
    print(
        f"[WATCHER][CONFIG] BINANCE_RECV_WINDOW_MS={BINANCE_RECV_WINDOW_MS} "
        f"BINANCE_HTTP_TIMEOUT={BINANCE_HTTP_TIMEOUT}"
    )
    print(
        f"[WATCHER][CONFIG] BYBIT_RECV_WINDOW_MS={BYBIT_RECV_WINDOW_MS} "
        f"BYBIT_HTTP_TIMEOUT={BYBIT_HTTP_TIMEOUT}"
    )
    print(
        f"[WATCHER][CONFIG] SIGNAL_WORKER enabled={ALERT_SIGNAL_WORKER_ENABLED} "
        f"stale={ALERT_SIGNAL_WORKER_STALE_SECONDS}s backoff={ALERT_SIGNAL_WORKER_RESTART_BACKOFF_SECONDS}s "
        f"queue_max={ALERT_SIGNAL_WORKER_QUEUE_MAX}"
    )
    print(
        f"[WATCHER][CONFIG] SIGNAL_BUS export_enabled={SIGNAL_BUS_EXPORT_ENABLED} "
        f"file={SIGNAL_BUS_EXPORT_FILE} types={sorted(SIGNAL_BUS_EXPORT_TYPES)}"
    )

    last_threshold_check = 0.0
    last_threshold_guard_check = 0.0
    last_disabled_check = 0.0
    last_threshold_dump = 0.0
    last_retry_worker_check = 0.0
    last_position_monitor_check = 0.0
    last_pending_execution_check = 0.0

    signal_queue: Queue | None = None
    signal_worker: Process | None = None
    signal_last_heartbeat = 0.0
    signal_next_restart_ts = 0.0

    def _start_signal_worker(now_ts: float) -> None:
        nonlocal signal_queue, signal_worker, signal_last_heartbeat
        if not ALERT_SIGNAL_WORKER_ENABLED:
            return
        if signal_worker is not None and signal_worker.is_alive():
            return
        if signal_queue is None:
            signal_queue = Queue(maxsize=max(ALERT_SIGNAL_WORKER_QUEUE_MAX, 1))
        signal_worker = Process(
            target=_signal_worker_loop,
            args=(signal_queue, POLL_SECONDS),
            daemon=True,
            name="watcher-signal-worker",
        )
        signal_worker.start()
        signal_last_heartbeat = now_ts
        print(f"[WATCHER][SIGNAL-WORKER][START] pid={signal_worker.pid}")

    def _stop_signal_worker(reason: str) -> None:
        nonlocal signal_worker
        worker = signal_worker
        if worker is None:
            return
        try:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=3)
        except Exception:
            pass
        signal_worker = None
        print(f"[WATCHER][SIGNAL-WORKER][RESTART] reason={reason}")

    if THRESHOLDS_CLEAR_ON_STARTUP:
        _clear_thresholds_file()
    if THRESHOLDS_REBUILD_ON_STARTUP:
        try:
            _rebuild_thresholds_from_open_positions()
        except Exception as exc:
            print(f"[WATCHER][WARN] Reconstrucción de umbrales falló: {exc}")
    try:
        _threshold_guard_rebuild_missing()
    except Exception as exc:
        print(f"[WATCHER][THRESHOLDS][GUARD][WARN] startup guard failed: {exc}")
    try:
        _close_disabled_accounts_positions()
    except Exception:
        pass

    if ALERT_SIGNAL_WORKER_ENABLED:
        _start_signal_worker(time.time())

    while True:
        now_ts = time.time()
        if ALERT_SIGNAL_WORKER_ENABLED:
            if signal_worker is None:
                if now_ts >= signal_next_restart_ts:
                    _start_signal_worker(now_ts)
            elif not signal_worker.is_alive():
                print("[WATCHER][SIGNAL-WORKER][FAIL] worker_dead=1")
                _stop_signal_worker("dead_process")
                signal_next_restart_ts = time.time() + max(ALERT_SIGNAL_WORKER_RESTART_BACKOFF_SECONDS, 0.1)
            else:
                if now_ts - signal_last_heartbeat > max(ALERT_SIGNAL_WORKER_STALE_SECONDS, 1.0):
                    print(
                        f"[WATCHER][SIGNAL-WORKER][STALE] no_heartbeat_for={now_ts - signal_last_heartbeat:.1f}s "
                        f"threshold={ALERT_SIGNAL_WORKER_STALE_SECONDS:.1f}s"
                    )
                    _stop_signal_worker("stale_heartbeat")
                    signal_next_restart_ts = time.time() + max(ALERT_SIGNAL_WORKER_RESTART_BACKOFF_SECONDS, 0.1)

            if signal_queue is not None:
                while True:
                    try:
                        payload = signal_queue.get_nowait()
                    except QueueEmpty:
                        break
                    kind = payload.get("kind")
                    ts = float(payload.get("ts") or time.time())
                    signal_last_heartbeat = max(signal_last_heartbeat, ts)
                    if kind == "events":
                        events = payload.get("events") or []
                        _process_events(events, seen)
                    elif kind == "fail":
                        print(f"[WATCHER][SIGNAL-WORKER][FAIL] err={payload.get('error')}")
        else:
            try:
                events = generate_alerts()
            except Exception as exc:
                print(f"[ERROR] {exc}")
                time.sleep(POLL_SECONDS)
                continue
            _process_events(events, seen)

        now_ts = time.time()
        if now_ts - last_disabled_check >= DISABLED_ACCOUNTS_CLOSE_POLL_SECONDS:
            last_disabled_check = now_ts
            try:
                _close_disabled_accounts_positions()
            except Exception:
                pass
        if THRESHOLD_GUARD_INTERVAL > 0 and now_ts - last_threshold_guard_check >= THRESHOLD_GUARD_INTERVAL:
            last_threshold_guard_check = now_ts
            try:
                _threshold_guard_rebuild_missing()
            except Exception as exc:
                print(f"[WATCHER][THRESHOLDS][GUARD][WARN] periodic guard failed: {exc}")
        if POSITION_GUARD_WORKER_POLL_SECONDS > 0 and now_ts - last_retry_worker_check >= POSITION_GUARD_WORKER_POLL_SECONDS:
            last_retry_worker_check = now_ts
            try:
                _process_trade_retry_queue(now_ts)
            except Exception as exc:
                print(f"[WATCHER][RETRY][WARN] Worker falló: {exc}")
        if (
            TRADES_FILL_RESOLVE_WORKER_POLL_SECONDS > 0
            and now_ts - last_pending_execution_check >= TRADES_FILL_RESOLVE_WORKER_POLL_SECONDS
        ):
            last_pending_execution_check = now_ts
            try:
                _process_pending_execution_resolutions(now_ts)
            except Exception as exc:
                print(f"[WATCHER][TRADES_FILL_RESOLVE_FAIL] reason=worker_exception err={exc}")
        if POSITION_MONITOR_POLL_SECONDS > 0 and now_ts - last_position_monitor_check >= POSITION_MONITOR_POLL_SECONDS:
            last_position_monitor_check = now_ts
            try:
                _monitor_positions_health(now_ts)
            except Exception as exc:
                print(f"[WATCHER][POSMON][WARN] monitor falló: {exc}")
        if now_ts - last_threshold_check >= THRESHOLD_POLL_SECONDS:
            last_threshold_check = now_ts
            try:
                current_price = None
                try:
                    from velas import LAST_KLINES_CACHE  # type: ignore
                    df = LAST_KLINES_CACHE.get("stream") if isinstance(LAST_KLINES_CACHE, dict) else None
                    if df is not None and not df.empty:
                        current_price = float(df["Close"].iloc[-1])
                except Exception:
                    pass
                ts_eval = datetime.now(timezone.utc)
                try:
                    extra_alerts = _evaluate_thresholds(current_price or 0.0, ts_eval)
                    if extra_alerts:
                        send_alerts(extra_alerts)
                except Exception as exc:
                    print(f"[ALERT][WARN] Falló evaluación periódica de umbrales ({exc})")
            except Exception:
                pass

        if THRESHOLDS_DUMP_SECONDS > 0 and now_ts - last_threshold_dump >= THRESHOLDS_DUMP_SECONDS:
            last_threshold_dump = now_ts
            try:
                _dump_thresholds(datetime.now(timezone.utc))
            except Exception as exc:
                print(f"[WATCHER][WARN] Falló dump de umbrales ({exc})")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
