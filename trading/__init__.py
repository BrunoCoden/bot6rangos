"""
Paquete de soporte para ejecución real y gestión de cuentas.

Se divide en submódulos:
- trading.exchanges: abstracciones y clientes específicos de cada exchange.
- trading.accounts: modelos y utilitarios para manejar credenciales multiusuario.
- trading.orders: estructuras comunes para órdenes, posiciones y resultados.
- trading.utils: utilitarios compartidos (logging, tiempo, etc.).
"""

from .orders.models import OrderRequest, OrderResponse, OrderSide, OrderType, TimeInForce
from .accounts.models import AccountConfig, ExchangeCredential, ExchangeEnvironment

__all__ = [
    "OrderRequest",
    "OrderResponse",
    "OrderSide",
    "OrderType",
    "TimeInForce",
    "AccountConfig",
    "ExchangeCredential",
    "ExchangeEnvironment",
]
