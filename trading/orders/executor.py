from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Type

from ..accounts.manager import AccountManager
from ..orders.models import OrderRequest, OrderResponse
from ..exchanges.base import ExchangeClient, ExchangeRegistry
from ..accounts.models import AccountConfig, ExchangeCredential


@dataclass(slots=True)
class ExecutionContext:
    account: AccountConfig
    credential: ExchangeCredential
    exchange_client: ExchangeClient


class OrderExecutor:
    """
    Orquestador que toma una señal genérica y la envía al exchange correspondiente.
    """

    def __init__(self, account_manager: AccountManager):
        self._accounts = account_manager

    def _resolve_context(self, user_id: str, exchange_name: str) -> ExecutionContext:
        account = self._accounts.get_account(user_id)
        credential = account.get_exchange(exchange_name)
        client_cls: Type[ExchangeClient] = ExchangeRegistry.get(exchange_name)
        client = client_cls()
        return ExecutionContext(account=account, credential=credential, exchange_client=client)

    def execute(self, user_id: str, exchange_name: str, order: OrderRequest, *, dry_run: bool = True) -> OrderResponse:
        order.validate()
        ctx = self._resolve_context(user_id, exchange_name)
        return ctx.exchange_client.place_order(ctx.account, ctx.credential, order, dry_run=dry_run)
