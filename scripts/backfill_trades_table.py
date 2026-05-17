#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from binance.um_futures import UMFutures
try:
    from pybit.unified_trading import HTTP
except Exception:
    HTTP = None  # type: ignore[assignment]

from balance_ledger import normalize_close_ts
from trades_table_ledger import TradesTableLedger, TradesTableLedgerConfig, parse_iso_ts, _safe_float  # type: ignore
from trading.accounts.manager import AccountManager
from trading.accounts.models import ExchangeEnvironment


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            txt = line.strip()
            if not txt:
                continue
            try:
                obj = json.loads(txt)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def _default_start() -> datetime:
    return datetime(2026, 4, 1, 3, 0, 0, tzinfo=timezone.utc)


def _ts_ms_to_dt(ts_ms) -> datetime | None:
    try:
        v = int(float(ts_ms))
        if v <= 0:
            return None
        return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def _fetch_binance_fills(*, cred, symbol: str, start_utc: datetime, prefix: str) -> list[dict]:
    api_key, api_secret = cred.resolve_keys(os.environ)
    base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
    client = UMFutures(key=api_key, secret=api_secret, base_url=base_url) if base_url else UMFutures(key=api_key, secret=api_secret)

    start_ms = int(start_utc.timestamp() * 1000)
    now_ms = int(time.time() * 1000)
    # Binance account trades suele trabajar por ventanas temporales acotadas.
    window_ms = int(7 * 24 * 60 * 60 * 1000) - 1
    rows: list[dict] = []
    seen_guard = set()
    window_start = start_ms
    guard = 0
    while window_start <= now_ms and guard < 5000:
        guard += 1
        window_end = min(window_start + window_ms, now_ms)
        cursor_ms = window_start
        for _ in range(2000):
            data = client.get_account_trades(
                symbol=symbol,
                limit=1000,
                startTime=cursor_ms,
                endTime=window_end,
            )
            if not isinstance(data, list) or not data:
                break

            ordered = sorted(
                [r for r in data if isinstance(r, dict)],
                key=lambda x: (int(float(x.get("time") or 0)), int(float(x.get("id") or 0))),
            )
            max_time_ms = cursor_ms
            for r in ordered:
                tid = r.get("id")
                try:
                    tid_i = int(tid)
                except Exception:
                    continue
                t_ms = int(float(r.get("time") or 0))
                if t_ms <= 0:
                    continue
                max_time_ms = max(max_time_ms, t_ms)
                if t_ms < start_ms:
                    continue
                tdt = _ts_ms_to_dt(t_ms)
                if tdt is None or tdt < start_utc:
                    continue
                coid = str(r.get("clientOrderId") or "")
                if prefix and not coid.startswith(prefix):
                    continue
                key = f"{symbol}|{tid_i}"
                if key in seen_guard:
                    continue
                seen_guard.add(key)
                side = "buy" if bool(r.get("buyer")) else "sell"
                qty = _safe_float(r.get("qty"))
                price = _safe_float(r.get("price"))
                fee = _safe_float(r.get("commission")) or 0.0
                if qty is None or price is None or qty <= 0 or price <= 0:
                    continue
                rows.append(
                    {
                        "ts": tdt,
                        "symbol": symbol,
                        "side": side,
                        "qty": float(qty),
                        "price": float(price),
                        "fee": float(fee),
                        "realized_pnl": _safe_float(r.get("realizedPnl")),
                        "order_id": str(r.get("orderId") or ""),
                        "client_order_id": coid,
                    }
                )

            if len(ordered) < 1000:
                break
            if max_time_ms <= cursor_ms:
                break
            cursor_ms = max_time_ms + 1
            if cursor_ms > window_end:
                break
        window_start = window_end + 1
    rows.sort(key=lambda x: x["ts"])
    return rows


def _fetch_bybit_fills(*, cred, symbol: str, start_utc: datetime, prefix: str) -> list[dict]:
    if HTTP is None:
        raise RuntimeError("pybit no instalado")
    api_key, api_secret = cred.resolve_keys(os.environ)
    is_testnet = cred.environment != ExchangeEnvironment.LIVE
    domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
    client = (
        HTTP(api_key=api_key, api_secret=api_secret, testnet=False, domain=domain_env)
        if domain_env
        else HTTP(api_key=api_key, api_secret=api_secret, testnet=is_testnet)
    )

    start_ms = int(start_utc.timestamp() * 1000)
    now_ms = int(time.time() * 1000)
    window_ms = int(7 * 24 * 60 * 60 * 1000) - 1
    rows: list[dict] = []
    seen = set()
    window_start = start_ms
    while window_start <= now_ms:
        window_end = min(window_start + window_ms, now_ms)
        cursor = None
        for _ in range(5000):
            kwargs = {
                "category": "linear",
                "symbol": symbol,
                "limit": 200,
                "startTime": window_start,
                "endTime": window_end,
            }
            if cursor:
                kwargs["cursor"] = cursor
            raw = client.get_executions(**kwargs)
            data = (raw or {}).get("result", {}).get("list") or []
            if not isinstance(data, list) or not data:
                break
            for r in data:
                if not isinstance(r, dict):
                    continue
                t_ms = int(float(r.get("execTime") or 0))
                if t_ms <= 0 or t_ms < start_ms or t_ms > window_end:
                    continue
                tdt = _ts_ms_to_dt(t_ms)
                if tdt is None:
                    continue
                coid = str(r.get("orderLinkId") or "")
                if prefix and not coid.startswith(prefix):
                    continue
                key = f"{symbol}|{r.get('execId') or ''}|{r.get('orderId') or ''}|{r.get('execTime') or ''}"
                if key in seen:
                    continue
                seen.add(key)
                side = str(r.get("side") or "").lower()
                qty = _safe_float(r.get("execQty"))
                price = _safe_float(r.get("execPrice"))
                fee = _safe_float(r.get("execFee")) or 0.0
                if qty is None or price is None or qty <= 0 or price <= 0:
                    continue
                if side not in {"buy", "sell"}:
                    continue
                rows.append(
                    {
                        "ts": tdt,
                        "symbol": symbol,
                        "side": side,
                        "qty": float(qty),
                        "price": float(price),
                        "fee": float(fee),
                        "order_id": str(r.get("orderId") or ""),
                        "client_order_id": coid,
                    }
                )
            nxt = (raw or {}).get("result", {}).get("nextPageCursor")
            if not nxt or nxt == cursor:
                break
            cursor = nxt
        window_start = window_end + 1
    rows.sort(key=lambda x: x["ts"])
    return rows


def _fetch_binance_funding(*, cred, symbol: str, start_utc: datetime) -> list[dict]:
    api_key, api_secret = cred.resolve_keys(os.environ)
    base_url = "https://testnet.binancefuture.com" if cred.environment == ExchangeEnvironment.TESTNET else None
    client = UMFutures(key=api_key, secret=api_secret, base_url=base_url) if base_url else UMFutures(key=api_key, secret=api_secret)

    start_ms = int(start_utc.timestamp() * 1000)
    rows: list[dict] = []
    seen: set[str] = set()
    cursor = start_ms
    for _ in range(2000):
        data = client.get_income_history(
            symbol=symbol,
            incomeType="FUNDING_FEE",
            startTime=cursor,
            limit=1000,
        )
        if not isinstance(data, list) or not data:
            break
        ordered = sorted(
            [r for r in data if isinstance(r, dict)],
            key=lambda x: (int(float(x.get("time") or 0)), str(x.get("tranId") or "")),
        )
        max_time = cursor
        for r in ordered:
            ts = _ts_ms_to_dt(r.get("time"))
            if ts is None or ts < start_utc:
                continue
            amount = _safe_float(r.get("income"))
            if amount is None or amount == 0.0:
                continue
            key = f"{r.get('tranId')}|{r.get('time')}|{amount}"
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "ts": ts,
                    "amount": float(amount),
                    "exchange": "binance",
                    "symbol": symbol,
                }
            )
            t_ms = int(float(r.get("time") or 0))
            max_time = max(max_time, t_ms)
        if len(ordered) < 1000 or max_time <= cursor:
            break
        cursor = max_time + 1
    rows.sort(key=lambda x: x["ts"])
    return rows


def _fetch_bybit_funding(*, cred, symbol: str, start_utc: datetime) -> list[dict]:
    if HTTP is None:
        raise RuntimeError("pybit no instalado")
    api_key, api_secret = cred.resolve_keys(os.environ)
    is_testnet = cred.environment != ExchangeEnvironment.LIVE
    domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
    client = (
        HTTP(api_key=api_key, api_secret=api_secret, testnet=False, domain=domain_env)
        if domain_env
        else HTTP(api_key=api_key, api_secret=api_secret, testnet=is_testnet)
    )

    start_ms = int(start_utc.timestamp() * 1000)
    now_ms = int(time.time() * 1000)
    window_ms = int(7 * 24 * 60 * 60 * 1000) - 1
    rows: list[dict] = []
    seen: set[str] = set()
    window_start = start_ms
    while window_start <= now_ms:
        window_end = min(window_start + window_ms, now_ms)
        cursor = None
        for _ in range(5000):
            kwargs = {
                "accountType": "UNIFIED",
                "category": "linear",
                "symbol": symbol,
                "limit": 50,
                "startTime": window_start,
                "endTime": window_end,
            }
            if cursor:
                kwargs["cursor"] = cursor
            raw = client.get_transaction_log(**kwargs)
            data = (raw or {}).get("result", {}).get("list") or []
            if not isinstance(data, list) or not data:
                break
            for r in data:
                if not isinstance(r, dict):
                    continue
                ts = _ts_ms_to_dt(r.get("transactionTime"))
                if ts is None:
                    continue
                t_ms = int(ts.timestamp() * 1000)
                if t_ms < start_ms or t_ms > window_end:
                    continue
                if str(r.get("symbol") or "").upper() not in {"", symbol.upper()}:
                    continue
                funding = _safe_float(r.get("funding"))
                if funding is None and str(r.get("type") or "").upper() == "SETTLEMENT":
                    funding = _safe_float(r.get("cashFlow"))
                if funding is None or funding == 0.0:
                    continue
                key = f"{r.get('id') or ''}|{r.get('transactionTime') or ''}|{funding}"
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "ts": ts,
                        "amount": float(funding),
                        "exchange": "bybit",
                        "symbol": symbol,
                    }
                )
            nxt = (raw or {}).get("result", {}).get("nextPageCursor")
            if not nxt or nxt == cursor:
                break
            cursor = nxt
        window_start = window_end + 1
    rows.sort(key=lambda x: x["ts"])
    return rows


def _fetch_bybit_closed_pnl(*, cred, symbol: str, start_utc: datetime) -> list[dict]:
    if HTTP is None:
        raise RuntimeError("pybit no instalado")
    api_key, api_secret = cred.resolve_keys(os.environ)
    is_testnet = cred.environment != ExchangeEnvironment.LIVE
    domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
    client = (
        HTTP(api_key=api_key, api_secret=api_secret, testnet=False, domain=domain_env)
        if domain_env
        else HTTP(api_key=api_key, api_secret=api_secret, testnet=is_testnet)
    )

    start_ms = int(start_utc.timestamp() * 1000)
    now_ms = int(time.time() * 1000)
    window_ms = int(7 * 24 * 60 * 60 * 1000) - 1
    rows: list[dict] = []
    seen: set[str] = set()
    window_start = start_ms
    while window_start <= now_ms:
        window_end = min(window_start + window_ms, now_ms)
        cursor = None
        for _ in range(5000):
            kwargs = {
                "category": "linear",
                "symbol": symbol,
                "limit": 100,
                "startTime": window_start,
                "endTime": window_end,
            }
            if cursor:
                kwargs["cursor"] = cursor
            raw = client.get_closed_pnl(**kwargs)
            data = (raw or {}).get("result", {}).get("list") or []
            if not isinstance(data, list) or not data:
                break
            for r in data:
                if not isinstance(r, dict):
                    continue
                order_id = str(r.get("orderId") or "")
                if not order_id:
                    continue
                updated_ms = int(float(r.get("updatedTime") or 0))
                created_ms = int(float(r.get("createdTime") or 0))
                ts_ms = updated_ms if updated_ms > 0 else created_ms
                if ts_ms <= 0 or ts_ms < start_ms or ts_ms > window_end:
                    continue
                ts = _ts_ms_to_dt(ts_ms)
                if ts is None:
                    continue
                key = f"{symbol}|{order_id}|{ts_ms}"
                if key in seen:
                    continue
                seen.add(key)
                rows.append(r)
            nxt = (raw or {}).get("result", {}).get("nextPageCursor")
            if not nxt or nxt == cursor:
                break
            cursor = nxt
        window_start = window_end + 1
    rows.sort(
        key=lambda x: (
            int(float(x.get("updatedTime") or 0)),
            int(float(x.get("createdTime") or 0)),
            str(x.get("orderId") or ""),
        )
    )
    return rows


def _allocate_funding_to_trades(trades: list[dict], funding_rows: list[dict]) -> tuple[float, int]:
    if not trades or not funding_rows:
        for row in trades:
            row.setdefault("funding_usdt", 0.0)
            base = _safe_float(row.get("pnl_trade_usdt"))
            if base is None:
                base = _safe_float(row.get("pnl_usdt")) or 0.0
            row["pnl_trade_usdt"] = float(base)
            row["pnl_usdt"] = float(base)
            qty = _safe_float(row.get("quantity")) or 0.0
            entry = _safe_float(row.get("entry_price")) or 0.0
            denom = entry * qty
            row["pnl_pct"] = (float(base) / denom) if denom > 0 else 0.0
        return 0.0, 0

    windows: list[dict] = []
    for idx, row in enumerate(trades):
        ent = parse_iso_ts(row.get("entry_ts"))
        ext = parse_iso_ts(row.get("exit_ts"))
        qty = _safe_float(row.get("quantity")) or 0.0
        if ent is None or ext is None or qty <= 0:
            continue
        windows.append({"idx": idx, "entry": ent, "exit": ext, "qty": qty})

    alloc = [0.0 for _ in trades]
    matched_events = 0
    for f in funding_rows:
        ts = f.get("ts")
        amount = _safe_float(f.get("amount"))
        if not isinstance(ts, datetime) or amount is None or amount == 0.0:
            continue
        candidates = [w for w in windows if w["entry"] <= ts <= w["exit"]]
        if not candidates:
            continue
        total_qty = sum(float(c["qty"]) for c in candidates)
        if total_qty <= 0:
            continue
        matched_events += 1
        for c in candidates:
            share = float(amount) * (float(c["qty"]) / total_qty)
            alloc[int(c["idx"])] += share

    total_funding = 0.0
    for idx, row in enumerate(trades):
        funding = float(alloc[idx])
        base = _safe_float(row.get("pnl_trade_usdt"))
        if base is None:
            base = _safe_float(row.get("pnl_usdt")) or 0.0
        net = float(base) + funding
        row["pnl_trade_usdt"] = float(base)
        row["funding_usdt"] = funding
        row["pnl_usdt"] = net
        qty = _safe_float(row.get("quantity")) or 0.0
        entry = _safe_float(row.get("entry_price")) or 0.0
        denom = entry * qty
        row["pnl_pct"] = (net / denom) if denom > 0 else 0.0
        total_funding += funding
    return total_funding, matched_events


def _build_trades_from_bybit_closed_pnl(
    *,
    closed_rows: list[dict],
    user_id: str,
    exchange: str,
    symbol: str,
) -> list[dict]:
    out: list[dict] = []
    for row in closed_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("symbol") or "").upper() != symbol.upper():
            continue
        exec_type = str(row.get("execType") or "").lower()
        if exec_type and exec_type not in {"trade", "settle"}:
            continue
        qty = _safe_float(row.get("closedSize"))
        if qty is None:
            qty = _safe_float(row.get("qty"))
        entry = _safe_float(row.get("avgEntryPrice"))
        exit_ = _safe_float(row.get("avgExitPrice"))
        pnl = _safe_float(row.get("closedPnl"))
        open_fee = _safe_float(row.get("openFee")) or 0.0
        close_fee = _safe_float(row.get("closeFee")) or 0.0
        created_ts = _ts_ms_to_dt(row.get("createdTime"))
        updated_ts = _ts_ms_to_dt(row.get("updatedTime"))
        exit_ts = updated_ts or created_ts
        entry_ts = created_ts or updated_ts
        if (
            qty is None
            or qty <= 0
            or entry is None
            or entry <= 0
            or exit_ is None
            or exit_ <= 0
            or pnl is None
            or entry_ts is None
            or exit_ts is None
        ):
            continue
        denom = float(entry) * float(qty)
        pnl_pct = (float(pnl) / denom) if denom > 0 else 0.0
        out.append(
            {
                "user_id": user_id,
                "exchange": exchange,
                "symbol": symbol,
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_price": float(entry),
                "exit_price": float(exit_),
                "quantity": float(qty),
                "pnl_pct": float(pnl_pct),
                "pnl_usdt": float(pnl),
                "pnl_trade_usdt": float(pnl),
                "funding_usdt": 0.0,
                "fees_usdt": float(open_fee + close_fee),
                "close_reason": "backfill_real_execution",
                "source": "live",
                "confidence": "strict",
            }
        )
    out.sort(key=lambda x: x["exit_ts"])
    return out


def _build_trades_from_fills(
    *,
    fills: list[dict],
    user_id: str,
    exchange: str,
    symbol: str,
) -> list[dict]:
    def _group_key(row: dict) -> tuple:
        oid = str(row.get("order_id") or "")
        coid = str(row.get("client_order_id") or "")
        side = str(row.get("side") or "").lower()
        if oid:
            return ("oid", oid, side)
        if coid:
            return ("coid", coid, side)
        ts = row.get("ts")
        bucket = int(ts.timestamp()) if isinstance(ts, datetime) else 0
        px = round(float(row.get("price") or 0.0), 2)
        return ("fallback", side, bucket, px)

    aggregated: dict[tuple, dict] = {}
    ordered_keys: list[tuple] = []
    for f in sorted(fills, key=lambda x: x.get("ts") or datetime.now(timezone.utc)):
        side = str(f.get("side") or "").lower()
        qty = float(f.get("qty") or 0.0)
        px = float(f.get("price") or 0.0)
        fee = float(f.get("fee") or 0.0)
        realized_raw = f.get("realized_pnl")
        realized = float(realized_raw) if realized_raw is not None else None
        ts = f.get("ts")
        if side not in {"buy", "sell"} or qty <= 0 or px <= 0 or not isinstance(ts, datetime):
            continue
        key = _group_key(f)
        if key not in aggregated:
            aggregated[key] = {
                "ts": ts,
                "side": side,
                "qty": 0.0,
                "quote": 0.0,
                "fee": 0.0,
                "realized_pnl": 0.0,
                "has_realized": False,
            }
            ordered_keys.append(key)
        agg = aggregated[key]
        agg["qty"] += qty
        agg["quote"] += px * qty
        agg["fee"] += fee
        if realized is not None:
            agg["realized_pnl"] += realized
            agg["has_realized"] = True
        if ts > agg["ts"]:
            agg["ts"] = ts

    executions: list[dict] = []
    for key in ordered_keys:
        agg = aggregated.get(key) or {}
        qty = float(agg.get("qty") or 0.0)
        quote = float(agg.get("quote") or 0.0)
        if qty <= 0 or quote <= 0:
            continue
        executions.append(
            {
                "ts": agg["ts"],
                "side": agg["side"],
                "qty": qty,
                "price": quote / qty,
                "fee": float(agg.get("fee") or 0.0),
                "realized_pnl": float(agg.get("realized_pnl") or 0.0) if bool(agg.get("has_realized")) else None,
            }
        )
    executions.sort(key=lambda x: x["ts"])

    out: list[dict] = []
    eps = 1e-12
    long_lots: list[dict] = []
    short_lots: list[dict] = []

    def _lots_qty(lots: list[dict]) -> float:
        return sum(float(l.get("qty") or 0.0) for l in lots)

    def _push_lot(lots: list[dict], qty: float, price: float, ts: datetime, fee_share: float) -> None:
        if qty <= eps:
            return
        lots.append({"qty": float(qty), "price": float(price), "ts": ts, "fee": float(max(fee_share, 0.0))})

    def _consume_lots(lots: list[dict], qty: float) -> list[dict]:
        consumed: list[dict] = []
        remaining = float(max(qty, 0.0))
        while remaining > eps and lots:
            lot = lots[0]
            lot_qty = float(lot.get("qty") or 0.0)
            if lot_qty <= eps:
                lots.pop(0)
                continue
            take = min(lot_qty, remaining)
            ratio = take / lot_qty
            lot_fee = float(lot.get("fee") or 0.0)
            fee_take = lot_fee * ratio
            consumed.append(
                {
                    "qty": take,
                    "price": float(lot.get("price") or 0.0),
                    "ts": lot.get("ts"),
                    "fee": fee_take,
                }
            )
            lot["qty"] = lot_qty - take
            lot["fee"] = max(lot_fee - fee_take, 0.0)
            remaining -= take
            if float(lot.get("qty") or 0.0) <= eps:
                lots.pop(0)
        return consumed

    def _append_close(
        *,
        direction: str,
        qty: float,
        exit_price: float,
        exit_ts: datetime,
        consumed: list[dict],
        realized_close: float | None,
        close_fee: float,
    ) -> None:
        if qty <= eps or exit_price <= 0:
            return
        consumed_qty = sum(float(c.get("qty") or 0.0) for c in consumed)
        consumed_qty = float(consumed_qty if consumed_qty > eps else qty)
        if consumed_qty <= eps:
            return
        consumed_quote = sum(float(c.get("qty") or 0.0) * float(c.get("price") or 0.0) for c in consumed)
        consumed_entry_fee = sum(float(c.get("fee") or 0.0) for c in consumed)
        if consumed_quote <= eps:
            if realized_close is None:
                return
            if direction == "long":
                inferred_entry = exit_price - (realized_close / consumed_qty)
            else:
                inferred_entry = exit_price + (realized_close / consumed_qty)
            entry_price = max(float(inferred_entry), eps)
        else:
            entry_price = consumed_quote / consumed_qty
        ts_values = [c.get("ts") for c in consumed if isinstance(c.get("ts"), datetime)]
        entry_ts = min(ts_values) if ts_values else exit_ts
        if realized_close is None:
            if direction == "long":
                gross = (exit_price - entry_price) * consumed_qty
            else:
                gross = (entry_price - exit_price) * consumed_qty
            pnl_usdt = gross - close_fee - consumed_entry_fee
        else:
            pnl_usdt = float(realized_close) - close_fee - consumed_entry_fee
        denom = entry_price * consumed_qty
        pnl_pct = (pnl_usdt / denom) if denom > eps else 0.0
        out.append(
            {
                "user_id": user_id,
                "exchange": exchange,
                "symbol": symbol,
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "quantity": consumed_qty,
                "pnl_pct": pnl_pct,
                "pnl_usdt": pnl_usdt,
                "pnl_trade_usdt": pnl_usdt,
                "funding_usdt": 0.0,
                "fees_usdt": close_fee + consumed_entry_fee,
                "close_reason": "backfill_real_execution",
                "source": "live",
                "confidence": "strict",
            }
        )

    for f in executions:
        side = str(f.get("side") or "").lower()
        qty = float(f.get("qty") or 0.0)
        px = float(f.get("price") or 0.0)
        fee = float(f.get("fee") or 0.0)
        ts = f.get("ts")
        realized = f.get("realized_pnl")
        realized_v = float(realized) if realized is not None else None
        if side not in {"buy", "sell"} or qty <= eps or px <= 0 or not isinstance(ts, datetime):
            continue

        if side == "buy" and _lots_qty(short_lots) <= eps and realized_v is not None and abs(realized_v) > eps:
            continue
        if side == "sell" and _lots_qty(long_lots) <= eps and realized_v is not None and abs(realized_v) > eps:
            continue

        remaining = qty
        remaining_fee = fee
        if side == "sell":
            closable = min(remaining, _lots_qty(long_lots))
            if closable > eps:
                ratio = closable / qty
                close_fee = remaining_fee * ratio
                close_realized = (realized_v * ratio) if realized_v is not None else None
                consumed = _consume_lots(long_lots, closable)
                _append_close(
                    direction="long",
                    qty=closable,
                    exit_price=px,
                    exit_ts=ts,
                    consumed=consumed,
                    realized_close=close_realized,
                    close_fee=close_fee,
                )
                remaining -= closable
                remaining_fee = max(remaining_fee - close_fee, 0.0)
            if remaining > eps:
                _push_lot(short_lots, remaining, px, ts, remaining_fee)
        else:  # buy
            closable = min(remaining, _lots_qty(short_lots))
            if closable > eps:
                ratio = closable / qty
                close_fee = remaining_fee * ratio
                close_realized = (realized_v * ratio) if realized_v is not None else None
                consumed = _consume_lots(short_lots, closable)
                _append_close(
                    direction="short",
                    qty=closable,
                    exit_price=px,
                    exit_ts=ts,
                    consumed=consumed,
                    realized_close=close_realized,
                    close_fee=close_fee,
                )
                remaining -= closable
                remaining_fee = max(remaining_fee - close_fee, 0.0)
            if remaining > eps:
                _push_lot(long_lots, remaining, px, ts, remaining_fee)

    return out


def _keep_rows_before_start(rows: list[dict], start_utc: datetime) -> list[dict]:
    keep: list[dict] = []
    for row in rows:
        ts = parse_iso_ts(row.get("exit_ts"))
        if ts is None:
            continue
        if ts < start_utc:
            keep.append(row)
    return keep


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill real (fills de exchange) de trades_table_ledger.")
    parser.add_argument("--target", default="backtest/backtestTR/trades_table_ledger.jsonl")
    parser.add_argument("--accounts", default=os.getenv("WATCHER_ACCOUNTS_FILE", "trading/accounts/oci_accounts.yaml"))
    parser.add_argument("--from-local", default="2026-04-01T00:00:00-03:00")
    parser.add_argument("--user", default="")
    parser.add_argument("--exchange", default="")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--prefix", default=os.getenv("WATCHER_ORDER_ID_PREFIX", "BOT1"))
    args = parser.parse_args()

    start_utc = normalize_close_ts(args.from_local) or _default_start()
    target_path = Path(args.target)
    manager = AccountManager.from_file(Path(args.accounts))

    existing_rows = _read_jsonl(target_path)
    keep_rows = _keep_rows_before_start(existing_rows, start_utc)

    generated: list[dict] = []
    processed_accounts = 0
    skipped_accounts = 0

    user_filter = args.user.strip().lower() if args.user else ""
    exchange_filter = args.exchange.strip().lower() if args.exchange else ""
    symbol_filter = args.symbol.strip().upper() if args.symbol else ""

    for account in manager.list_accounts():
        if not account.enabled:
            continue
        if user_filter and account.user_id.lower() != user_filter:
            continue
        for ex_name, cred in (account.exchanges or {}).items():
            if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                continue
            ex_l = str(ex_name).lower()
            if exchange_filter and ex_l != exchange_filter:
                continue
            if ex_l not in {"binance", "bybit"}:
                continue
            symbol = str((cred.extra or {}).get("symbol") or "").upper()
            if not symbol:
                continue
            if symbol_filter and symbol != symbol_filter:
                continue

            processed_accounts += 1
            try:
                if ex_l == "binance":
                    fills = _fetch_binance_fills(
                        cred=cred,
                        symbol=symbol,
                        start_utc=start_utc,
                        prefix=args.prefix,
                    )
                    if args.prefix and not fills:
                        fallback_prefix = ""
                        fills = _fetch_binance_fills(
                            cred=cred,
                            symbol=symbol,
                            start_utc=start_utc,
                            prefix=fallback_prefix,
                        )
                        if fills:
                            print(
                                f"[BACKFILL_REAL][WARN] user={account.user_id} ex={ex_l} symbol={symbol} "
                                f"prefix_no_match={args.prefix!r} fallback_prefix=''"
                            )
                    trades = _build_trades_from_fills(
                        fills=fills,
                        user_id=account.user_id,
                        exchange=ex_l,
                        symbol=symbol,
                    )
                else:
                    closed = _fetch_bybit_closed_pnl(
                        cred=cred,
                        symbol=symbol,
                        start_utc=start_utc,
                    )
                    trades = _build_trades_from_bybit_closed_pnl(
                        closed_rows=closed,
                        user_id=account.user_id,
                        exchange=ex_l,
                        symbol=symbol,
                    )
                funding_rows: list[dict] = []
                if trades and ex_l == "binance":
                    try:
                        if ex_l == "binance":
                            funding_rows = _fetch_binance_funding(
                                cred=cred,
                                symbol=symbol,
                                start_utc=start_utc,
                            )
                        else:
                            funding_rows = _fetch_bybit_funding(
                                cred=cred,
                                symbol=symbol,
                                start_utc=start_utc,
                            )
                    except Exception as funding_exc:
                        print(
                            f"[BACKFILL_REAL][WARN] funding_fetch user={account.user_id} ex={ex_l} "
                            f"symbol={symbol} err={funding_exc}"
                        )
                    total_funding, matched = _allocate_funding_to_trades(trades, funding_rows)
                    print(
                        f"[BACKFILL_REAL][FUNDING] user={account.user_id} ex={ex_l} symbol={symbol} "
                        f"events={len(funding_rows)} matched={matched} funding_usdt={total_funding:.8f}"
                    )
                generated.extend(trades)
            except Exception as exc:
                skipped_accounts += 1
                print(
                    f"[BACKFILL_REAL][WARN] user={account.user_id} ex={ex_l} symbol={symbol} err={exc}"
                )

    backup_path = target_path.with_name(
        f"{target_path.stem}.pre_real_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}{target_path.suffix}"
    )
    if target_path.exists():
        backup_path.write_text(target_path.read_text(encoding="utf-8"), encoding="utf-8")

    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    ledger = TradesTableLedger(TradesTableLedgerConfig(ledger_path=tmp_path))

    inserted_keep = 0
    for row in keep_rows:
        ok = ledger.append_trade(
            user_id=str(row.get("user_id") or ""),
            exchange=str(row.get("exchange") or ""),
            symbol=str(row.get("symbol") or ""),
            entry_ts=row.get("entry_ts"),
            exit_ts=row.get("exit_ts"),
            entry_price=float(row.get("entry_price") or 0.0),
            exit_price=float(row.get("exit_price") or 0.0),
            quantity=float(row.get("quantity") or 0.0),
            pnl_pct=float(row.get("pnl_pct") or 0.0),
            pnl_usdt=float(row.get("pnl_usdt") or 0.0),
            fees_usdt=float(row.get("fees_usdt") or 0.0),
            funding_usdt=float(row.get("funding_usdt") or 0.0),
            pnl_trade_usdt=float(row.get("pnl_trade_usdt") or row.get("pnl_usdt") or 0.0),
            close_reason=str(row.get("close_reason") or ""),
            source=str(row.get("source") or "live"),
            confidence=str(row.get("confidence") or "strict"),
        )
        if ok:
            inserted_keep += 1

    inserted_generated = 0
    skipped_generated = 0
    for row in sorted(generated, key=lambda x: x.get("exit_ts") or datetime.now(timezone.utc)):
        ok = ledger.append_trade(
            user_id=row["user_id"],
            exchange=row["exchange"],
            symbol=row["symbol"],
            entry_ts=row["entry_ts"],
            exit_ts=row["exit_ts"],
            entry_price=row["entry_price"],
            exit_price=row["exit_price"],
            quantity=row["quantity"],
            pnl_pct=row["pnl_pct"],
            pnl_usdt=row["pnl_usdt"],
            fees_usdt=row["fees_usdt"],
            funding_usdt=row.get("funding_usdt", 0.0),
            pnl_trade_usdt=row.get("pnl_trade_usdt", row.get("pnl_usdt", 0.0)),
            close_reason=row["close_reason"],
            source="live",
            confidence="strict",
        )
        if ok:
            inserted_generated += 1
        else:
            skipped_generated += 1

    if not tmp_path.exists():
        tmp_path.write_text("", encoding="utf-8")
    tmp_path.replace(target_path)
    print(
        "[BACKFILL_REAL] "
        f"target={target_path} backup={backup_path} start={start_utc.isoformat()} "
        f"processed_accounts={processed_accounts} skipped_accounts={skipped_accounts} "
        f"kept={inserted_keep} inserted={inserted_generated} skipped={skipped_generated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
