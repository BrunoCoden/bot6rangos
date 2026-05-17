from __future__ import annotations

import os
import math
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, List
import time

from binance.um_futures import UMFutures

from .base import ExchangeClient, ExchangeRegistry
from ..accounts.models import AccountConfig, ExchangeCredential, ExchangeEnvironment
from ..orders.models import CancelRequest, CancelResponse, OrderRequest, OrderResponse
from ..utils.logging import get_logger

logger = get_logger("trading.exchanges.binance")


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


BINANCE_RECV_WINDOW_MS = _int_env("BINANCE_RECV_WINDOW_MS", 20000)
BINANCE_TIMESTAMP_RETRY_COUNT = _int_env("BINANCE_TIMESTAMP_RETRY_COUNT", 2)
BINANCE_TIMESTAMP_RETRY_DELAY = _float_env("BINANCE_TIMESTAMP_RETRY_DELAY", 0.25)


def _binance_signed_kwargs() -> Dict[str, int]:
    return {"recvWindow": BINANCE_RECV_WINDOW_MS}


def _is_binance_timestamp_error(exc: Exception) -> bool:
    msg = str(exc)
    return "-1021" in msg or "outside of the recvWindow" in msg or "recvWindow" in msg


def _call_with_timestamp_retry(func, action: str):
    retries = max(BINANCE_TIMESTAMP_RETRY_COUNT, 0)
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as exc:
            if not _is_binance_timestamp_error(exc):
                raise
            if attempt < retries:
                logger.warning(
                    "[EXCHANGE][TIME][BINANCE][RETRY] action=%s attempt=%s/%s recvWindow=%s err=%s",
                    action,
                    attempt + 1,
                    retries + 1,
                    BINANCE_RECV_WINDOW_MS,
                    exc,
                )
                time.sleep(max(BINANCE_TIMESTAMP_RETRY_DELAY, 0.0))
                continue
            logger.error(
                "[EXCHANGE][TIME][BINANCE][FAIL] action=%s recvWindow=%s err=%s",
                action,
                BINANCE_RECV_WINDOW_MS,
                exc,
            )
            raise
    return func()


class BinanceClient(ExchangeClient):
    name = "binance"

    def _build_client(self, credential: ExchangeCredential) -> UMFutures:
        api_key, api_secret = credential.resolve_keys(os.environ)
        base_url = "https://testnet.binancefuture.com" if credential.environment == ExchangeEnvironment.TESTNET else None
        timeout = _float_env("BINANCE_HTTP_TIMEOUT", 10.0)
        if base_url:
            return UMFutures(key=api_key, secret=api_secret, base_url=base_url, timeout=timeout)
        return UMFutures(key=api_key, secret=api_secret, timeout=timeout)

    @staticmethod
    def _format_order_params(order: OrderRequest) -> Dict[str, Optional[str]]:
        # Binance UM ETHUSDT: qty step 0.001, tick size 0.1 (ajusta si usas otro símbolo).
        def _quantize(value: float, step: str) -> str:
            dv = Decimal(str(value)).quantize(Decimal(step), rounding=ROUND_DOWN)
            if dv <= 0:
                dv = Decimal(step)
            # Normaliza sin notación científica
            return format(dv, "f")

        params: Dict[str, Optional[str]] = {
            "symbol": order.symbol,
            "side": order.side.value,
            "type": order.type.value,
            "quantity": _quantize(order.quantity, "0.001"),
            "reduceOnly": "true" if order.reduce_only else "false",
        }
        if order.client_order_id:
            params["newClientOrderId"] = str(order.client_order_id)
        if order.type.value == "MARKET":
            # Binance rechaza timeInForce/isPostOnly en órdenes MARKET
            return params

        # LIMIT / STOP_LIMIT conservan post-only + timeInForce/price
        if not order.reduce_only:
            params["isPostOnly"] = "true"
        if order.time_in_force:
            params["timeInForce"] = order.time_in_force.value
        if order.price:
            params["price"] = _quantize(order.price, "0.1")
        return params

    def _place_bracket(
        self,
        client: UMFutures,
        symbol: str,
        side: str,
        quantity: float,
        tp: float | None,
        sl: float | None,
    ) -> Dict[str, Any]:
        """
        Envía TP/SL como órdenes condicionadas de mercado con closePosition=true
        (equivalente a reduceOnly) disparadas por MARK_PRICE.
        """
        results: Dict[str, Any] = {}

        def _quant(v: float, step: str) -> str:
            dv = Decimal(str(v)).quantize(Decimal(step), rounding=ROUND_DOWN)
            if dv <= 0:
                dv = Decimal(step)
            return format(dv, "f")

        qty_str = _quant(quantity, "0.001")
        if tp and tp > 0:
            try:
                resp_tp = client.new_order(
                    symbol=symbol,
                    side="SELL" if side == "BUY" else "BUY",
                    type="TAKE_PROFIT_MARKET",
                    stopPrice=_quant(tp, "0.1"),
                    workingType="MARK_PRICE",
                    # closePosition hace que no abra posición nueva y cierre todo
                    closePosition="true",
                    timeInForce="GTC",
                    **_binance_signed_kwargs(),
                )
                results["tp"] = resp_tp
            except Exception as exc:  # pragma: no cover - externo
                logger.error("Error enviando TP reduceOnly: %s", exc)
                results["tp_error"] = str(exc)

        if sl and sl > 0:
            try:
                resp_sl = client.new_order(
                    symbol=symbol,
                    side="SELL" if side == "BUY" else "BUY",
                    type="STOP_MARKET",
                    stopPrice=_quant(sl, "0.1"),
                    workingType="MARK_PRICE",
                    closePosition="true",
                    timeInForce="GTC",
                    **_binance_signed_kwargs(),
                )
                results["sl"] = resp_sl
            except Exception as exc:  # pragma: no cover - externo
                logger.error("Error enviando SL reduceOnly: %s", exc)
                results["sl_error"] = str(exc)

        return results

    def _current_position_qty(self, client: UMFutures, symbol: str) -> float:
        """
        Devuelve el tamaño de posición actual (signed: >0 long, <0 short) para el símbolo.
        Usa get_position_risk, que está soportado en la lib actual.
        """
        try:
            positions = _call_with_timestamp_retry(
                lambda: client.get_position_risk(symbol=symbol, **_binance_signed_kwargs()),
                action="get_position_risk",
            )
            if positions:
                pos_amt = positions[0].get("positionAmt")
                return float(pos_amt or 0.0)
        except Exception as exc:  # pragma: no cover - externo
            if _is_binance_timestamp_error(exc):
                logger.error(
                    "[EXCHANGE][TIME][BINANCE][FAIL] action=get_position_risk symbol=%s recvWindow=%s err=%s",
                    symbol,
                    BINANCE_RECV_WINDOW_MS,
                    exc,
                )
            else:
                logger.error("No se pudo obtener posición actual para %s: %s", symbol, exc)
        return 0.0

    def _cancel_open_reduce_only(self, client: UMFutures, symbol: str) -> List[Dict[str, Any]]:
        """
        Cancela órdenes abiertas reduceOnly del símbolo (TP/SL previos) antes de colocar nuevos.
        """
        canceled: List[Dict[str, Any]] = []
        try:
            open_orders = _call_with_timestamp_retry(
                lambda: client.get_open_orders(symbol=symbol, **_binance_signed_kwargs()),
                action="get_open_orders",
            )
        except Exception as exc:  # pragma: no cover - externo
            logger.error("Error listando órdenes abiertas para cancelar reduceOnly: %s", exc)
            return canceled

        for order in open_orders:
            try:
                if not bool(order.get("reduceOnly")):
                    continue
                oid = order.get("orderId")
                resp = _call_with_timestamp_retry(
                    lambda: client.cancel_order(symbol=symbol, orderId=oid, **_binance_signed_kwargs()),
                    action="cancel_order",
                )
                canceled.append(resp)
            except Exception as exc:  # pragma: no cover - externo
                logger.error("Error cancelando orden reduceOnly %s: %s", order.get("orderId"), exc)
        return canceled

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
                raw={
                    "dry_run": True,
                    "symbol": order.symbol,
                    "side": order.side.value,
                    "quantity": order.quantity,
                    "type": order.type.value,
                    "price": order.price,
                    "reduce_only": order.reduce_only,
                    "time_in_force": order.time_in_force.value,
                },
            )

        try:
            client = self._build_client(credential)
            params = self._format_order_params(order)
            response = _call_with_timestamp_retry(
                lambda: client.new_order(**params, **_binance_signed_kwargs()),
                action="new_order",
            )
            status = response.get("status") or "NEW"
            order_id = str(response.get("orderId") or "")
            filled_qty = float(response.get("executedQty") or 0.0)
            avg_price = float(response.get("avgPrice") or order.price or 0.0)
            tp = order.extra_params.get("tp") if order.extra_params else None
            sl = order.extra_params.get("sl") if order.extra_params else None
            # Binance rechaza TP/SL con el endpoint estándar (error -4120). Deshabilitamos brackets
            # y delegamos los cierres al watcher (cierres MARKET por ±5%/±9%).
            bracket_raw: Dict[str, Any] = {}
            logger.info(
                "Orden enviada (entry + bracket) symbol=%s side=%s qty=%s tp=%s sl=%s bracket=%s",
                order.symbol,
                order.side.value,
                order.quantity,
                tp,
                sl,
                bracket_raw,
            )
            return OrderResponse(
                success=True,
                status=status,
                exchange_order_id=order_id,
                filled_quantity=filled_qty,
                avg_price=avg_price,
                raw={"entry": response, "bracket": bracket_raw},
            )
        except Exception as exc:
            logger.exception("Error enviando orden a Binance: %s", exc)
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
        logger.info(
            "Cancelación (dry_run=%s) usuario=%s exchange=%s symbol=%s order_id=%s client_id=%s",
            dry_run,
            account.user_id,
            credential.exchange,
            request.symbol,
            request.exchange_order_id,
            request.client_order_id,
        )
        return CancelResponse(
            success=True,
            raw={
                "dry_run": dry_run or credential.environment == ExchangeEnvironment.TESTNET,
                "symbol": request.symbol,
                "order_id": request.exchange_order_id,
                "client_order_id": request.client_order_id,
            },
        )

    def fetch_account_balance(
        self,
        account: AccountConfig,
        credential: ExchangeCredential,
    ) -> Dict[str, float]:
        logger.info(
            "Consulta de balance (simulado) usuario=%s exchange=%s",
            account.user_id,
            credential.exchange,
        )
        return {"USDT": 0.0}


ExchangeRegistry.register(BinanceClient)
