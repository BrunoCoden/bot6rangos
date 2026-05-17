from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable

LOCAL_TZ = timezone(timedelta(hours=-3))

MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

_TS_PREFIX_PATTERNS = [
    re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"),
    re.compile(r"^\[(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\]"),
]
_TS_ANY_RE = re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)")

_TRIGGER_RE = re.compile(
    r"THRESHOLDS\]\[TRIGGER\]\s+user=(?P<user>[^\s]+)\s+ex=(?P<ex>[^\s]+)\s+symbol=(?P<symbol>[^\s]+)\s+dir=(?P<dir>[^\s]+)\s+"
    r"last=(?P<last>[-+]?\d+(?:\.\d+)?)\s+entry=(?P<entry>[-+]?\d+(?:\.\d+)?)\s+.*kind=(?P<kind>.+)$"
)

_CLOSE_RE = re.compile(
    r"THRESHOLDS\]\[CLOSE\]\s+user=(?P<user>[^\s]+)\s+ex=(?P<ex>[^\s]+)\s+symbol=(?P<symbol>[^\s]+)\s+ok=True\s+kind=(?P<kind>.+)$"
)


def _safe_float(value) -> float | None:
    try:
        out = float(value)
        if out != out:  # NaN
            return None
        return out
    except Exception:
        return None


def _parse_iso_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    txt = str(raw).strip()
    if not txt:
        return None
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    # soporta +0300 -> +03:00
    if re.search(r"[+-]\d{4}$", txt):
        txt = txt[:-5] + txt[-5:-2] + ":" + txt[-2:]
    try:
        dt = datetime.fromisoformat(txt)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(timezone.utc)


def _extract_ts_from_line(line: str) -> datetime | None:
    for pattern in _TS_PREFIX_PATTERNS:
        m = pattern.search(line)
        if m:
            return _parse_iso_ts(m.group("ts"))
    m_any = _TS_ANY_RE.search(line)
    if m_any:
        return _parse_iso_ts(m_any.group("ts"))
    return None


def _month_period(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def parse_month_token(token: str) -> tuple[datetime, datetime] | None:
    if not token:
        return None
    text = token.strip().lower()

    m = re.fullmatch(r"(\d{4})[-/](\d{1,2})", text)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        if 1 <= month <= 12:
            return _month_period(year, month)
        return None

    m = re.fullmatch(r"(\d{1,2})/(\d{4})", text)
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        if 1 <= month <= 12:
            return _month_period(year, month)
        return None

    m = re.fullmatch(r"([a-záéíóúñ]+)[-_]?(\d{4})", text)
    if m:
        month_name = m.group(1)
        month = MONTHS_ES.get(month_name)
        year = int(m.group(2))
        if month:
            return _month_period(year, month)
        return None

    return None


def parse_month_from_tokens(tokens: list[str]) -> tuple[tuple[datetime, datetime] | None, set[int]]:
    if not tokens:
        return None, set()

    for i, token in enumerate(tokens):
        period = parse_month_token(token)
        if period:
            return period, {i}

    # soporte "febrero 2026"
    for i in range(len(tokens) - 1):
        joined = f"{tokens[i]}{tokens[i+1]}"
        period = parse_month_token(joined)
        if period:
            return period, {i, i + 1}

    return None, set()


def read_ledger_rows(path: Path) -> list[dict]:
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


@dataclass
class BalanceLedgerConfig:
    ledger_path: Path
    state_path: Path
    source: str = "live"


class BalanceLedger:
    def __init__(self, config: BalanceLedgerConfig):
        self.config = config
        self._ids: set[str] = set()
        self._load_ids()

    def _load_ids(self) -> None:
        self._ids.clear()
        for row in read_ledger_rows(self.config.ledger_path):
            trade_id = row.get("trade_id")
            if isinstance(trade_id, str) and trade_id:
                self._ids.add(trade_id)

    def _build_trade_id(
        self,
        *,
        close_ts: str,
        user_id: str,
        exchange: str,
        symbol: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        reason: str,
    ) -> str:
        payload = "|".join(
            [
                close_ts,
                user_id,
                exchange,
                symbol,
                direction,
                f"{entry_price:.10f}",
                f"{exit_price:.10f}",
                reason,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]

    def append_close(
        self,
        *,
        close_ts: datetime,
        user_id: str,
        exchange: str,
        symbol: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        reason: str,
        source: str | None = None,
    ) -> bool:
        entry = _safe_float(entry_price)
        exit_v = _safe_float(exit_price)
        if entry is None or exit_v is None or entry <= 0:
            return False

        close_utc = close_ts.astimezone(timezone.utc)
        close_txt = close_utc.isoformat()
        pnl_pct = (exit_v - entry) / entry if str(direction).lower() == "long" else (entry - exit_v) / entry
        trade_id = self._build_trade_id(
            close_ts=close_txt,
            user_id=str(user_id),
            exchange=str(exchange).lower(),
            symbol=str(symbol),
            direction=str(direction).lower(),
            entry_price=entry,
            exit_price=exit_v,
            reason=str(reason),
        )
        if trade_id in self._ids:
            return False

        row = {
            "trade_id": trade_id,
            "close_ts": close_txt,
            "user_id": str(user_id),
            "exchange": str(exchange).lower(),
            "symbol": str(symbol),
            "direction": str(direction).lower(),
            "entry_price": entry,
            "exit_price": exit_v,
            "pnl_pct": pnl_pct,
            "reason": str(reason),
            "source": str(source or self.config.source),
        }
        self.config.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config.ledger_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        self._ids.add(trade_id)
        return True

    def _load_state(self) -> dict:
        path = self.config.state_path
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    def _save_state(self, state: dict) -> None:
        self.config.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def backfill_from_log(self, log_path: Path) -> dict:
        stats = {"processed": 0, "appended": 0, "skipped": 0}
        if not log_path.exists():
            return stats

        st = log_path.stat()
        state = self._load_state()
        inode = state.get("inode")
        offset = int(state.get("offset") or 0)
        if inode != st.st_ino:
            offset = 0

        pending: dict[tuple[str, str, str, str], dict] = {}
        last_seen_ts: datetime | None = None

        with log_path.open("r", encoding="utf-8", errors="ignore") as fh:
            if offset > 0:
                try:
                    fh.seek(offset)
                except Exception:
                    offset = 0
                    fh.seek(0)
            for line in fh:
                stats["processed"] += 1
                line_ts = _extract_ts_from_line(line)
                if line_ts is not None:
                    last_seen_ts = line_ts
                trg = _TRIGGER_RE.search(line)
                if trg:
                    key = (
                        str(trg.group("user")),
                        str(trg.group("ex")).lower(),
                        str(trg.group("symbol")),
                        str(trg.group("dir")).lower(),
                    )
                    ts = line_ts or last_seen_ts
                    pending[key] = {
                        "ts": ts,
                        "entry": _safe_float(trg.group("entry")),
                        "last": _safe_float(trg.group("last")),
                        "kind": str(trg.group("kind")).strip(),
                    }
                    continue

                cls = _CLOSE_RE.search(line)
                if not cls:
                    continue

                user = str(cls.group("user"))
                ex = str(cls.group("ex")).lower()
                symbol = str(cls.group("symbol"))
                kind = str(cls.group("kind")).strip()
                # intenta casar por cualquier dirección pendiente más reciente
                candidates = [k for k in pending.keys() if k[0] == user and k[1] == ex and k[2] == symbol]
                if not candidates:
                    stats["skipped"] += 1
                    continue
                key = candidates[-1]
                info = pending.pop(key)
                close_ts = line_ts or info.get("ts") or last_seen_ts
                entry = _safe_float(info.get("entry"))
                exit_v = _safe_float(info.get("last"))
                if close_ts is None or entry is None or exit_v is None:
                    stats["skipped"] += 1
                    continue
                ok = self.append_close(
                    close_ts=close_ts,
                    user_id=user,
                    exchange=ex,
                    symbol=symbol,
                    direction=key[3],
                    entry_price=entry,
                    exit_price=exit_v,
                    reason=kind,
                    source="log_backfill",
                )
                if ok:
                    stats["appended"] += 1
                else:
                    stats["skipped"] += 1

            offset = fh.tell()

        self._save_state({"inode": st.st_ino, "offset": offset, "path": str(log_path)})
        return stats


def normalize_close_ts(value) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(value, str):
        return _parse_iso_ts(value)
    return None


def filter_rows(
    rows: Iterable[dict],
    *,
    user_id: str | None = None,
    month_period: tuple[datetime, datetime] | None = None,
    enabled_pairs: set[tuple[str, str]] | None = None,
) -> list[dict]:
    out: list[dict] = []
    start = end = None
    if month_period:
        start, end = month_period
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_user = str(row.get("user_id") or "").strip()
        row_ex = str(row.get("exchange") or "").strip().lower()
        if not row_user or not row_ex:
            continue
        if user_id and row_user.lower() != user_id.lower():
            continue
        if enabled_pairs is not None and (row_user, row_ex) not in enabled_pairs:
            continue
        ts = normalize_close_ts(row.get("close_ts"))
        if ts is None:
            continue
        if start and ts < start:
            continue
        if end and ts >= end:
            continue
        out.append(row)
    return out


def summarize_rows(rows: Iterable[dict]) -> dict:
    trades = 0
    wins = 0
    losses = 0
    pnl_sum = 0.0
    breakdown: dict[tuple[str, str], dict] = {}
    for row in rows:
        pnl = _safe_float(row.get("pnl_pct"))
        if pnl is None:
            continue
        trades += 1
        pnl_sum += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        key = (str(row.get("user_id") or ""), str(row.get("exchange") or "").lower())
        if key not in breakdown:
            breakdown[key] = {"trades": 0, "pnl_sum": 0.0}
        breakdown[key]["trades"] += 1
        breakdown[key]["pnl_sum"] += pnl
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "pnl_sum": pnl_sum,
        "breakdown": breakdown,
    }
