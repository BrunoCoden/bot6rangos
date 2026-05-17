from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

from ..accounts.models import AccountConfig, ExchangeCredential
from ..orders.models import CancelRequest, CancelResponse, OrderRequest, OrderResponse


class ExchangeClient(ABC):
    """
    Interface que deben implementar todos los exchanges soportados.
    """

    name: str

    @abstractmethod
    def place_order(
        self,
        account: AccountConfig,
        credential: ExchangeCredential,
        order: OrderRequest,
        *,
        dry_run: bool = False,
    ) -> OrderResponse:
        ...

    @abstractmethod
    def cancel_order(
        self,
        account: AccountConfig,
        credential: ExchangeCredential,
        request: CancelRequest,
        *,
        dry_run: bool = False,
    ) -> CancelResponse:
        ...

    @abstractmethod
    def fetch_account_balance(self, account: AccountConfig, credential: ExchangeCredential) -> Dict[str, float]:
        ...


class ExchangeRegistry:
    """
    Registro global para mapear nombres de exchanges a implementaciones.
    """

    _clients: Dict[str, Type[ExchangeClient]] = {}

    @classmethod
    def register(cls, client_cls: Type[ExchangeClient]) -> None:
        key = client_cls.name.lower()
        cls._clients[key] = client_cls

    @classmethod
    def get(cls, name: str) -> Type[ExchangeClient]:
        key = name.lower()
        if key not in cls._clients:
            raise KeyError(f"Exchange '{name}' no registrado.")
        return cls._clients[key]

    @classmethod
    def list_names(cls) -> list[str]:
        return sorted(cls._clients.keys())
