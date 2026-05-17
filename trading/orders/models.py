from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    STOP_LIMIT = "STOP_LIMIT"


class TimeInForce(str, Enum):
    GTC = "GTC"  # Good 'til cancelled
    IOC = "IOC"  # Immediate or cancel
    FOK = "FOK"  # Fill or kill
    GTE_GTC = "GTE_GTC"  # Good till expiry (futuros Binance)


@dataclass(slots=True)
class OrderRequest:
    symbol: str
    side: OrderSide
    type: OrderType
    quantity: float
    price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("OrderRequest.symbol vacío.")
        if self.quantity <= 0:
            raise ValueError("OrderRequest.quantity debe ser mayor a cero.")
        if self.type in {OrderType.LIMIT, OrderType.STOP_LIMIT} and (self.price is None or self.price <= 0):
            raise ValueError("Las órdenes LIMIT requieren price > 0.")


@dataclass(slots=True)
class OrderResponse:
    success: bool
    status: str
    exchange_order_id: Optional[str] = None
    filled_quantity: float = 0.0
    avg_price: Optional[float] = None
    raw: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.success and not self.error:
            self.error = "Unknown error"


@dataclass(slots=True)
class CancelRequest:
    symbol: str
    exchange_order_id: Optional[str] = None
    client_order_id: Optional[str] = None

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("CancelRequest.symbol vacío.")
        if not (self.exchange_order_id or self.client_order_id):
            raise ValueError("CancelRequest requiere exchange_order_id o client_order_id.")


@dataclass(slots=True)
class CancelResponse:
    success: bool
    raw: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
