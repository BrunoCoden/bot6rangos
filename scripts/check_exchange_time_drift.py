#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass

import requests


@dataclass
class DriftResult:
    name: str
    drift_ms: float
    rtt_ms: float
    ok: bool
    detail: str = ""


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return float(raw)
    except Exception:
        return default


def _check_binance(timeout: float) -> DriftResult:
    url = "https://fapi.binance.com/fapi/v1/time"
    try:
        t0 = time.time() * 1000.0
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        t1 = time.time() * 1000.0
        payload = resp.json()
        server_ms = float(payload.get("serverTime"))
        local_mid_ms = (t0 + t1) / 2.0
        return DriftResult(
            name="binance",
            drift_ms=local_mid_ms - server_ms,
            rtt_ms=t1 - t0,
            ok=True,
        )
    except Exception as exc:
        return DriftResult(name="binance", drift_ms=0.0, rtt_ms=0.0, ok=False, detail=str(exc))


def _check_bybit(timeout: float) -> DriftResult:
    domain = os.getenv("BYBIT_DOMAIN", "api.bybit.com").strip() or "api.bybit.com"
    url = f"https://{domain}/v5/market/time"
    try:
        t0 = time.time() * 1000.0
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        t1 = time.time() * 1000.0
        payload = resp.json() or {}
        result = payload.get("result") or {}
        time_nano = result.get("timeNano")
        time_second = result.get("timeSecond")
        if time_nano is not None:
            server_ms = float(int(time_nano) / 1_000_000.0)
        elif time_second is not None:
            server_ms = float(time_second) * 1000.0
        else:
            raise RuntimeError(f"unexpected payload: {payload}")
        local_mid_ms = (t0 + t1) / 2.0
        return DriftResult(
            name="bybit",
            drift_ms=local_mid_ms - server_ms,
            rtt_ms=t1 - t0,
            ok=True,
        )
    except Exception as exc:
        return DriftResult(name="bybit", drift_ms=0.0, rtt_ms=0.0, ok=False, detail=str(exc))


def main() -> int:
    warn_drift_ms = _env_float("EXCHANGE_TIME_WARN_DRIFT_MS", 1500.0)
    timeout = _env_float("BINANCE_HTTP_TIMEOUT", 10.0)

    checks = [_check_binance(timeout), _check_bybit(timeout)]
    any_ok = False
    has_drift_violation = False

    for item in checks:
        if item.ok:
            any_ok = True
            abs_drift = abs(item.drift_ms)
            print(
                f"[TIME-DRIFT] {item.name} ok drift_ms={item.drift_ms:.2f} "
                f"abs_drift_ms={abs_drift:.2f} rtt_ms={item.rtt_ms:.2f}"
            )
            if abs_drift > warn_drift_ms:
                has_drift_violation = True
                print(
                    f"[TIME-DRIFT][WARN] {item.name} drift exceeded threshold "
                    f"({abs_drift:.2f} > {warn_drift_ms:.2f})"
                )
        else:
            print(f"[TIME-DRIFT] {item.name} error={item.detail}")

    if has_drift_violation:
        return 1
    if not any_ok:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
