from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable

LOCAL_TZ = timezone(timedelta(hours=-3))


def _safe_float(value) -> float | None:
    try:
        out = float(value)
        if out != out:  # NaN
            return None
        return out
    except Exception:
        return None


def parse_iso_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        txt = str(value).strip()
        if not txt:
            return None
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(txt)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(timezone.utc)


@dataclass
class TradesTableLedgerConfig:
    ledger_path: Path


class TradesTableLedger:
    def __init__(self, config: TradesTableLedgerConfig):
        self.config = config
        self._ids: set[str] = set()
        self._load_ids()

    def _load_ids(self) -> None:
        self._ids.clear()
        for row in read_trades_rows(self.config.ledger_path):
            tid = row.get("trade_id")
            if isinstance(tid, str) and tid:
                self._ids.add(tid)

    def _build_trade_id(
        self,
        *,
        user_id: str,
        exchange: str,
        symbol: str,
        entry_ts: str,
        exit_ts: str,
        entry_price: float,
        exit_price: float,
        quantity: float,
        close_reason: str,
    ) -> str:
        payload = "|".join(
            [
                user_id,
                exchange,
                symbol,
                entry_ts,
                exit_ts,
                f"{entry_price:.10f}",
                f"{exit_price:.10f}",
                f"{quantity:.10f}",
                close_reason,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]

    def append_trade(
        self,
        *,
        user_id: str,
        exchange: str,
        symbol: str,
        entry_ts,
        exit_ts,
        entry_price: float,
        exit_price: float,
        quantity: float,
        pnl_pct: float,
        pnl_usdt: float,
        fees_usdt: float,
        funding_usdt: float = 0.0,
        pnl_trade_usdt: float | None = None,
        close_reason: str,
        source: str = "live",
        confidence: str = "strict",
    ) -> bool:
        ent = _safe_float(entry_price)
        ext = _safe_float(exit_price)
        qty = _safe_float(quantity)
        pnlp = _safe_float(pnl_pct)
        pnlu = _safe_float(pnl_usdt)
        feeu = _safe_float(fees_usdt)
        fndu = _safe_float(funding_usdt)
        pnl_trade = _safe_float(pnl_trade_usdt)
        ent_ts = parse_iso_ts(entry_ts)
        ext_ts = parse_iso_ts(exit_ts)
        if fndu is None:
            fndu = 0.0
        if pnl_trade is None and pnlu is not None:
            pnl_trade = pnlu - fndu
        if (
            ent is None
            or ext is None
            or qty is None
            or pnlp is None
            or pnlu is None
            or feeu is None
            or pnl_trade is None
            or ent_ts is None
            or ext_ts is None
            or ent <= 0
            or ext <= 0
            or qty <= 0
        ):
            return False

        entry_txt = ent_ts.astimezone(timezone.utc).isoformat()
        exit_txt = ext_ts.astimezone(timezone.utc).isoformat()
        trade_id = self._build_trade_id(
            user_id=str(user_id),
            exchange=str(exchange).lower(),
            symbol=str(symbol),
            entry_ts=entry_txt,
            exit_ts=exit_txt,
            entry_price=ent,
            exit_price=ext,
            quantity=qty,
            close_reason=str(close_reason),
        )
        if trade_id in self._ids:
            return False

        row = {
            "trade_id": trade_id,
            "user_id": str(user_id),
            "exchange": str(exchange).lower(),
            "symbol": str(symbol),
            "entry_ts": entry_txt,
            "exit_ts": exit_txt,
            "entry_price": ent,
            "exit_price": ext,
            "quantity": qty,
            "pnl_pct": pnlp,
            "pnl_usdt": pnlu,
            "pnl_trade_usdt": pnl_trade,
            "funding_usdt": fndu,
            "fees_usdt": feeu,
            "close_reason": str(close_reason),
            "source": str(source),
            "confidence": str(confidence),
        }
        self.config.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config.ledger_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        self._ids.add(trade_id)
        return True


def read_trades_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    try:
        with path.open("r", encoding="utf-8") as fh:
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
    except Exception:
        return rows
    return rows


def filter_trades_rows(
    rows: list[dict],
    *,
    user_id: str | None = None,
    exchange: str | None = None,
    month_period: tuple[datetime, datetime] | None = None,
    enabled_pairs: set[tuple[str, str]] | None = None,
    start_from_utc: datetime | None = None,
) -> list[dict]:
    out: list[dict] = []
    user_filter = user_id.strip().lower() if isinstance(user_id, str) and user_id.strip() else None
    ex_filter = exchange.strip().lower() if isinstance(exchange, str) and exchange.strip() else None

    start = start_from_utc.astimezone(timezone.utc) if isinstance(start_from_utc, datetime) else None
    period_start = period_end = None
    if month_period:
        period_start = month_period[0].astimezone(timezone.utc)
        period_end = month_period[1].astimezone(timezone.utc)

    for row in rows:
        r_user = str(row.get("user_id") or "").strip()
        r_ex = str(row.get("exchange") or "").strip().lower()
        if not r_user or not r_ex:
            continue

        if user_filter and r_user.lower() != user_filter:
            continue
        if ex_filter and r_ex != ex_filter:
            continue
        if enabled_pairs is not None and not user_filter:
            if (r_user, r_ex) not in enabled_pairs:
                continue

        ts = parse_iso_ts(row.get("exit_ts"))
        if ts is None:
            continue
        if start and ts < start:
            continue
        if period_start and period_end and not (period_start <= ts < period_end):
            continue

        out.append(row)

    out.sort(key=lambda x: str(x.get("exit_ts") or ""))
    return out


def summarize_trades_rows(rows: list[dict]) -> dict:
    trades = 0
    wins = 0
    losses = 0
    pnl_sum_pct = 0.0
    pnl_sum_usdt = 0.0
    for row in rows:
        p = _safe_float(row.get("pnl_pct"))
        u = _safe_float(row.get("pnl_usdt"))
        if p is None or u is None:
            continue
        trades += 1
        pnl_sum_pct += p
        pnl_sum_usdt += u
        if p > 0:
            wins += 1
        elif p < 0:
            losses += 1
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "pnl_sum_pct": pnl_sum_pct,
        "pnl_sum_usdt": pnl_sum_usdt,
    }


def consolidate_overlapping_rows(
    rows: list[dict],
    *,
    max_entry_gap_seconds: float = 5.0,
    max_exit_gap_seconds: float = 5.0,
) -> list[dict]:
    """
    Consolida filas "hermanas" casi simultaneas que representan un mismo ciclo
    (mismo user/exchange/symbol y ventanas entry/exit superpuestas).
    Esto evita duplicados en export cuando un mismo evento se ejecuta en dos ordenes
    muy cercanas.
    """
    if not rows:
        return []

    def _sort_key(row: dict):
        ent = parse_iso_ts(row.get("entry_ts"))
        ext = parse_iso_ts(row.get("exit_ts"))
        return (
            str(row.get("user_id") or ""),
            str(row.get("exchange") or ""),
            str(row.get("symbol") or ""),
            ent or datetime.min.replace(tzinfo=timezone.utc),
            ext or datetime.min.replace(tzinfo=timezone.utc),
        )

    ordered = sorted(rows, key=_sort_key)
    out: list[dict] = []

    for row in ordered:
        ent = parse_iso_ts(row.get("entry_ts"))
        ext = parse_iso_ts(row.get("exit_ts"))
        qty = _safe_float(row.get("quantity"))
        ent_px = _safe_float(row.get("entry_price"))
        ext_px = _safe_float(row.get("exit_price"))
        pnl_usdt = _safe_float(row.get("pnl_usdt"))
        pnl_trade_usdt = _safe_float(row.get("pnl_trade_usdt"))
        funding_usdt = _safe_float(row.get("funding_usdt")) or 0.0
        fees = _safe_float(row.get("fees_usdt")) or 0.0
        if pnl_trade_usdt is None:
            pnl_trade_usdt = pnl_usdt
        if (
            ent is None
            or ext is None
            or qty is None
            or ent_px is None
            or ext_px is None
            or pnl_usdt is None
            or pnl_trade_usdt is None
        ):
            continue
        if qty <= 0 or ent_px <= 0 or ext_px <= 0:
            continue

        if not out:
            out.append(dict(row))
            continue

        prev = out[-1]
        p_ent = parse_iso_ts(prev.get("entry_ts"))
        p_ext = parse_iso_ts(prev.get("exit_ts"))
        p_qty = _safe_float(prev.get("quantity"))
        p_ent_px = _safe_float(prev.get("entry_price"))
        p_ext_px = _safe_float(prev.get("exit_price"))
        p_pnl_usdt = _safe_float(prev.get("pnl_usdt"))
        p_pnl_trade_usdt = _safe_float(prev.get("pnl_trade_usdt"))
        p_funding_usdt = _safe_float(prev.get("funding_usdt")) or 0.0
        p_fees = _safe_float(prev.get("fees_usdt")) or 0.0
        if p_pnl_trade_usdt is None:
            p_pnl_trade_usdt = p_pnl_usdt
        if (
            p_ent is None
            or p_ext is None
            or p_qty is None
            or p_ent_px is None
            or p_ext_px is None
            or p_pnl_usdt is None
            or p_pnl_trade_usdt is None
            or p_qty <= 0
        ):
            out.append(dict(row))
            continue

        same_key = (
            str(prev.get("user_id") or "") == str(row.get("user_id") or "")
            and str(prev.get("exchange") or "") == str(row.get("exchange") or "")
            and str(prev.get("symbol") or "") == str(row.get("symbol") or "")
            and str(prev.get("close_reason") or "") == str(row.get("close_reason") or "")
            and str(prev.get("source") or "") == str(row.get("source") or "")
            and str(prev.get("confidence") or "") == str(row.get("confidence") or "")
        )
        near_entry = abs((ent - p_ent).total_seconds()) <= max_entry_gap_seconds
        near_exit = abs((ext - p_ext).total_seconds()) <= max_exit_gap_seconds
        overlap = ent <= (p_ext + timedelta(seconds=max_exit_gap_seconds))
        if not (same_key and near_entry and near_exit and overlap):
            out.append(dict(row))
            continue

        merged_qty = p_qty + qty
        if merged_qty <= 0:
            out.append(dict(row))
            continue
        merged_entry = ((p_ent_px * p_qty) + (ent_px * qty)) / merged_qty
        merged_exit = ((p_ext_px * p_qty) + (ext_px * qty)) / merged_qty
        merged_pnl_usdt = p_pnl_usdt + pnl_usdt
        merged_pnl_trade_usdt = p_pnl_trade_usdt + pnl_trade_usdt
        merged_funding = p_funding_usdt + funding_usdt
        merged_fees = p_fees + fees
        denom = merged_entry * merged_qty
        merged_pnl_pct = (merged_pnl_usdt / denom) if denom > 0 else 0.0

        prev["entry_ts"] = min(p_ent, ent).astimezone(timezone.utc).isoformat()
        prev["exit_ts"] = max(p_ext, ext).astimezone(timezone.utc).isoformat()
        prev["quantity"] = merged_qty
        prev["entry_price"] = merged_entry
        prev["exit_price"] = merged_exit
        prev["pnl_usdt"] = merged_pnl_usdt
        prev["pnl_trade_usdt"] = merged_pnl_trade_usdt
        prev["funding_usdt"] = merged_funding
        prev["fees_usdt"] = merged_fees
        prev["pnl_pct"] = merged_pnl_pct
        prev["trade_id"] = ""

    return out


def write_trades_csv(rows: list[dict], output_path: Path, *, tz=LOCAL_TZ) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    def _trunc(value: float, digits: int = 2) -> float:
        scale = 10 ** digits
        if value >= 0:
            return int(value * scale) / scale
        return -int(abs(value) * scale) / scale

    headers = [
        "Precio de entrada",
        "Dia y Hora de entrada",
        "Precio de salida",
        "Dia y Hora de salida",
        "cantidad",
        "PNL%",
        "PNL en USDT",
        "Funding USDT",
        "PNL neto USDT",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        wr = csv.writer(fh, delimiter=";")
        # Hint para Excel/LibreOffice: forzar separador ';'
        wr.writerow(["sep=;"])
        wr.writerow(headers)
        sum_pnl_pct = 0.0
        sum_trade_usdt = 0.0
        sum_funding_usdt = 0.0
        sum_pnl_net_usdt = 0.0
        for row in rows:
            ent_ts = parse_iso_ts(row.get("entry_ts"))
            ext_ts = parse_iso_ts(row.get("exit_ts"))
            if ent_ts is None or ext_ts is None:
                continue
            entry_price = _safe_float(row.get("entry_price"))
            exit_price = _safe_float(row.get("exit_price"))
            qty = _safe_float(row.get("quantity"))
            pnl_pct = _safe_float(row.get("pnl_pct"))
            pnl_net_usdt = _safe_float(row.get("pnl_usdt"))
            pnl_trade_usdt = _safe_float(row.get("pnl_trade_usdt"))
            funding_usdt = _safe_float(row.get("funding_usdt"))
            if pnl_trade_usdt is None and pnl_net_usdt is not None:
                pnl_trade_usdt = pnl_net_usdt - (funding_usdt or 0.0)
            if funding_usdt is None:
                funding_usdt = 0.0
            if None in (entry_price, exit_price, qty, pnl_pct, pnl_net_usdt, pnl_trade_usdt):
                continue
            entry_price = _trunc(float(entry_price), 1)
            exit_price = _trunc(float(exit_price), 1)
            qty = _trunc(float(qty), 3)
            pnl_pct_val = _trunc(float(pnl_pct) * 100.0, 2)
            pnl_trade_usdt = _trunc(float(pnl_trade_usdt), 1)
            funding_usdt = _trunc(float(funding_usdt), 1)
            pnl_net_usdt = _trunc(float(pnl_net_usdt), 1)
            pnl_pct_fmt = f"{pnl_pct_val:.2f}".replace(".", ",")
            pnl_trade_fmt = f"{pnl_trade_usdt:.1f}".replace(".", ",")
            funding_fmt = f"{funding_usdt:.1f}".replace(".", ",")
            pnl_net_fmt = f"{pnl_net_usdt:.1f}".replace(".", ",")
            sum_pnl_pct += pnl_pct_val
            sum_trade_usdt += float(pnl_trade_usdt)
            sum_funding_usdt += float(funding_usdt)
            sum_pnl_net_usdt += float(pnl_net_usdt)
            wr.writerow(
                [
                    f"{entry_price:.1f}",
                    ent_ts.astimezone(tz).strftime("%y/%m/%d %H:%M:%S"),
                    f"{exit_price:.1f}",
                    ext_ts.astimezone(tz).strftime("%y/%m/%d %H:%M:%S"),
                    f"{qty:.3f}".replace(".", ","),
                    pnl_pct_fmt,
                    pnl_trade_fmt,
                    funding_fmt,
                    pnl_net_fmt,
                ]
            )
        wr.writerow(
            [
                "totales",
                "",
                "",
                "",
                "",
                f"{_trunc(sum_pnl_pct, 2):.2f}".replace(".", ","),
                f"{_trunc(sum_trade_usdt, 1):.1f}".replace(".", ","),
                f"{_trunc(sum_funding_usdt, 1):.1f}".replace(".", ","),
                f"{_trunc(sum_pnl_net_usdt, 1):.1f}".replace(".", ","),
            ]
        )
    return output_path


def utc_from_local_iso(text: str, fallback: datetime | None = None) -> datetime:
    ts = parse_iso_ts(text)
    if ts is not None:
        return ts
    if fallback is not None:
        return fallback.astimezone(timezone.utc)
    return datetime.now(timezone.utc)
