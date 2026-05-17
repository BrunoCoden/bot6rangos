from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional, List
from decimal import Decimal, ROUND_DOWN
import time

from pybit.unified_trading import HTTP
try:  # pybit puede variar entre versiones
    from pybit.exceptions import InvalidRequestError
except Exception:  # pragma: no cover
    InvalidRequestError = None  # type: ignore[assignment]

from .base import ExchangeClient, ExchangeRegistry
from ..accounts.models import AccountConfig, ExchangeCredential, ExchangeEnvironment
from ..orders.models import OrderRequest, OrderResponse, CancelRequest, CancelResponse, OrderSide, OrderType
from ..utils.logging import get_logger

logger = get_logger("trading.exchanges.bybit")


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return float(raw)
    except Exception:
        return default


BYBIT_RECV_WINDOW_MS = _int_env("BYBIT_RECV_WINDOW_MS", 20000)
BYBIT_HTTP_TIMEOUT = _float_env("BYBIT_HTTP_TIMEOUT", 10.0)
BYBIT_TIMESTAMP_RETRY_COUNT = _int_env("BYBIT_TIMESTAMP_RETRY_COUNT", 2)
BYBIT_TIMESTAMP_RETRY_DELAY = _float_env("BYBIT_TIMESTAMP_RETRY_DELAY", 0.25)


class BybitClient(ExchangeClient):
    name = "bybit"

    def __init__(self):
        self._lot_size_cache: Dict[str, Dict[str, str]] = {}

    def _build_client(self, credential: ExchangeCredential):
        api_key, api_secret = credential.resolve_keys(os.environ)
        is_testnet = credential.environment != ExchangeEnvironment.LIVE
        # pybit v5 unified trading; allow optional custom domain.
        domain_env = os.getenv("BYBIT_DOMAIN_TESTNET" if is_testnet else "BYBIT_DOMAIN")
        if domain_env:
            return HTTP(
                api_key=api_key,
                api_secret=api_secret,
                testnet=False,
                domain=domain_env,
                recv_window=BYBIT_RECV_WINDOW_MS,
                timeout=BYBIT_HTTP_TIMEOUT,
            )
        return HTTP(
            api_key=api_key,
            api_secret=api_secret,
            testnet=is_testnet,
            recv_window=BYBIT_RECV_WINDOW_MS,
            timeout=BYBIT_HTTP_TIMEOUT,
        )

    @staticmethod
    def _quantize(value: float, step: str) -> str:
        dv = Decimal(str(value)).quantize(Decimal(step), rounding=ROUND_DOWN)
        if dv <= 0:
            dv = Decimal(step)
        return format(dv, "f")

    @staticmethod
    def _extract_err_code(exc: Exception) -> Optional[int]:
        m = re.search(r"ErrCode:\\s*(\\d+)", str(exc))
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    @staticmethod
    def _is_bybit_timestamp_error(exc: Exception) -> bool:
        msg = str(exc)
        return "ErrCode: 10002" in msg or "recv_window" in msg.lower() or "server timestamp" in msg.lower()

    def _call_with_timestamp_retry(self, func, action: str):
        retries = max(BYBIT_TIMESTAMP_RETRY_COUNT, 0)
        for attempt in range(retries + 1):
            try:
                return func()
            except Exception as exc:
                if not self._is_bybit_timestamp_error(exc):
                    raise
                if attempt < retries:
                    logger.warning(
                        "[EXCHANGE][TIME][BYBIT][RETRY] action=%s attempt=%s/%s recv_window=%s err=%s",
                        action,
                        attempt + 1,
                        retries + 1,
                        BYBIT_RECV_WINDOW_MS,
                        exc,
                    )
                    time.sleep(max(BYBIT_TIMESTAMP_RETRY_DELAY, 0.0))
                    continue
                logger.error(
                    "[EXCHANGE][TIME][BYBIT][FAIL] action=%s recv_window=%s err=%s",
                    action,
                    BYBIT_RECV_WINDOW_MS,
                    exc,
                )
                raise
        return func()

    def _get_lot_size_filter(self, client: HTTP, symbol: str) -> Dict[str, str]:
        """
        Devuelve filtros de qty del instrumento (min + step) para poder autocorregir.
        Cachea por símbolo para evitar llamadas repetidas.
        """
        key = str(symbol).upper()
        cached = self._lot_size_cache.get(key)
        if cached:
            return cached
        raw = client.get_instruments_info(category="linear", symbol=key)
        items = raw.get("result", {}).get("list") or []
        first = items[0] if items else {}
        lot = first.get("lotSizeFilter") or {}
        # Según Bybit v5: minOrderQty / qtyStep
        out = {
            "minOrderQty": str(lot.get("minOrderQty") or ""),
            "qtyStep": str(lot.get("qtyStep") or ""),
        }
        self._lot_size_cache[key] = out
        return out

    @staticmethod
    def _ceil_to_step(value: float, step: str) -> str:
        dv = Decimal(str(value))
        ds = Decimal(step)
        if ds <= 0:
            return format(dv, "f")
        q = (dv / ds).to_integral_value(rounding="ROUND_UP")
        out = q * ds
        if out <= 0:
            out = ds
        return format(out, "f")

    def _autocorrect_qty(self, client: HTTP, symbol: str, qty: str) -> Optional[str]:
        """
        Corrige qty para cumplir minQty y step, devolviendo un string listo para enviar.
        """
        try:
            lot = self._get_lot_size_filter(client, symbol)
            min_qty_s = lot.get("minOrderQty") or ""
            step_s = lot.get("qtyStep") or ""
            if not min_qty_s or not step_s:
                return None
            current = float(qty)
            min_qty = float(min_qty_s)
            target = max(current, min_qty)
            return self._ceil_to_step(target, step_s)
        except Exception:
            return None

    def _format_order_params(self, order: OrderRequest) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "category": "linear",  # USDT Perp
            "symbol": order.symbol,
            "side": "Buy" if order.side == OrderSide.BUY else "Sell",
            "orderType": "Market" if order.type == OrderType.MARKET else "Limit",
            "qty": self._quantize(order.quantity, "0.001"),
            "reduceOnly": order.reduce_only,
        }
        if order.client_order_id:
            params["orderLinkId"] = str(order.client_order_id)
        if order.type == OrderType.LIMIT and order.price:
            params["price"] = self._quantize(order.price, "0.1")
            params["timeInForce"] = "GTC"
        return params

    def place_order(
        self,
        account: AccountConfig,
        credential: ExchangeCredential,
        order: OrderRequest,
        *,
        dry_run: bool = False,
    ) -> OrderResponse:
        order.validate()
        logger.info(
            "Procesando orden (dry_run=%s) usuario=%s exchange=%s symbol=%s side=%s qty=%s type=%s price=%s",
            dry_run,
            account.user_id,
            credential.exchange,
            order.symbol,
            order.side.value,
            order.quantity,
            order.type.value,
            order.price,
        )
        if dry_run:
            return OrderResponse(
                success=True,
                status="SIMULATED",
                exchange_order_id=None,
                filled_quantity=order.quantity,
                avg_price=order.price,
                raw={"dry_run": True},
            )
        try:
            client = self._build_client(credential)
            params = self._format_order_params(order)
            try:
                raw = self._call_with_timestamp_retry(
                    lambda: client.place_order(**params),
                    action="place_order",
                )
            except Exception as exc:
                # Autocorrección runtime para qty/min/precision: reintenta una sola vez.
                err_code = self._extract_err_code(exc)
                msg = str(exc)
                looks_like_qty_error = (
                    (err_code == 10001)
                    or ("minimum limit" in msg.lower())
                    or ("qty" in msg.lower() and "invalid" in msg.lower())
                    or ("precision" in msg.lower())
                )
                if looks_like_qty_error and params.get("qty"):
                    corrected = self._autocorrect_qty(client, str(params.get("symbol")), str(params.get("qty")))
                    if corrected and corrected != str(params.get("qty")):
                        logger.warning(
                            "Bybit rechazó qty; reintentando con qty corregida symbol=%s qty=%s -> %s err=%s",
                            params.get("symbol"),
                            params.get("qty"),
                            corrected,
                            msg,
                        )
                        params2 = {**params, "qty": corrected}
                        raw = self._call_with_timestamp_retry(
                            lambda: client.place_order(**params2),
                            action="place_order_retry_qty",
                        )
                        raw = {"retry": {"attempt": 2, "from_qty": params.get("qty"), "to_qty": corrected}, **raw}
                        params = params2
                    else:
                        raise
                else:
                    raise
            ret_code = raw.get("retCode")
            if ret_code not in (None, 0, "0"):
                msg = raw.get("retMsg") or "BYBIT_ERROR"
                return OrderResponse(
                    success=False,
                    status=str(msg),
                    error=f"Bybit retCode={ret_code} retMsg={msg}",
                    raw={"entry": raw, "params": params},
                )
            order_id = str(raw.get("result", {}).get("orderId") or "")
            status = raw.get("result", {}).get("orderStatus") or raw.get("retMsg") or "NEW"
            return OrderResponse(
                success=True,
                status=status,
                exchange_order_id=order_id,
                filled_quantity=order.quantity,
                avg_price=order.price,
                raw={"entry": raw, "params": params},
            )
        except Exception as exc:  # pragma: no cover - externo
            logger.exception("Error enviando orden a Bybit: %s", exc)
            return OrderResponse(success=False, status="ERROR", error=str(exc))

    def cancel_order(
        self,
        account: AccountConfig,
        credential: ExchangeCredential,
        request: CancelRequest,
        *,
        dry_run: bool = False,
    ) -> CancelResponse:
        request.validate()
        if dry_run:
            return CancelResponse(success=True, raw={"dry_run": True})
        try:
            client = self._build_client(credential)
            raw = self._call_with_timestamp_retry(
                lambda: client.cancel_order(
                    category="linear",
                    symbol=request.symbol,
                    orderId=request.exchange_order_id,
                    orderLinkId=request.client_order_id,
                ),
                action="cancel_order",
            )
            return CancelResponse(success=True, raw={"resp": raw})
        except Exception as exc:  # pragma: no cover - externo
            logger.exception("Error cancelando orden en Bybit: %s", exc)
            return CancelResponse(success=False, error=str(exc), raw={"error": str(exc)})

    def fetch_account_balance(
        self,
        account: AccountConfig,
        credential: ExchangeCredential,
    ) -> Dict[str, float]:
        try:
            client = self._build_client(credential)
            raw = client.get_wallet_balance(accountType="UNIFIED", coin="USDT")
            bal = raw.get("result", {}).get("list", [{}])[0].get("coin", [{}])[0].get("walletBalance")
            return {"USDT": float(bal) if bal is not None else 0.0}
        except Exception:  # pragma: no cover - externo
            return {"USDT": 0.0}
        return {"USDT": 0.0}


ExchangeRegistry.register(BybitClient)
