from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping


class ExchangeEnvironment(str, Enum):
    TESTNET = "testnet"
    LIVE = "live"


@dataclass(slots=True)
class ExchangeCredential:
    exchange: str
    api_key_env: str
    api_secret_env: str
    environment: ExchangeEnvironment = ExchangeEnvironment.TESTNET
    notional_usdt: float | None = None
    leverage: int | None = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def _env_candidates(self, key: str) -> list[str]:
        """
        Genera variantes de nombres de variables para tolerar:
        - IDs con espacios (DashCRUD histórico): "DIEGO BYBIT_..." -> "DIEGO_BYBIT_..."
        - Convención LIVE_API_KEY vs API_KEY_LIVE
        """
        key = (key or "").strip()
        if not key:
            return []

        candidates: list[str] = []

        def add(v: str) -> None:
            if v and v not in candidates:
                candidates.append(v)

        add(key)
        add(key.replace(" ", "_"))

        swapped = key
        swapped = swapped.replace("_API_KEY_LIVE", "_LIVE_API_KEY").replace("_LIVE_API_KEY", "_API_KEY_LIVE")
        swapped = swapped.replace("_API_SECRET_LIVE", "_LIVE_API_SECRET").replace("_LIVE_API_SECRET", "_API_SECRET_LIVE")
        swapped = swapped.replace("_API_KEY_TESTNET", "_TESTNET_API_KEY").replace("_TESTNET_API_KEY", "_API_KEY_TESTNET")
        swapped = swapped.replace("_API_SECRET_TESTNET", "_TESTNET_API_SECRET").replace("_TESTNET_API_SECRET", "_API_SECRET_TESTNET")
        add(swapped)
        add(swapped.replace(" ", "_"))

        # normaliza dobles underscores por si venía con espacios/concat raras
        for v in list(candidates):
            add(v.replace("__", "_"))

        return candidates

    def _resolve_env_value(self, env: Mapping[str, str], key: str) -> str | None:
        for cand in self._env_candidates(key):
            val = env.get(cand)
            if val is None:
                continue
            # también consideramos inválido el caso "VAR=VAR" (placeholder accidental)
            if val.strip() and val.strip() != cand:
                return val
        return None

    def resolve_keys(self, env: Mapping[str, str]) -> tuple[str, str]:
        api_key = self._resolve_env_value(env, self.api_key_env)
        api_secret = self._resolve_env_value(env, self.api_secret_env)
        if not api_key or not api_secret:
            raise RuntimeError(
                f"Credenciales faltantes para {self.exchange}: "
                f"{self.api_key_env}/{self.api_secret_env}"
            )
        return api_key, api_secret

    def resolve_optional(self, env: Mapping[str, str], key: str | None) -> str | None:
        if key is None:
            return None
        return env.get(key) or None


@dataclass(slots=True)
class AccountConfig:
    user_id: str
    label: str
    enabled: bool = True
    exchanges: Dict[str, ExchangeCredential] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_exchange(self, exchange_name: str) -> ExchangeCredential:
        key = exchange_name.lower()
        if key not in self.exchanges:
            raise KeyError(f"La cuenta {self.user_id} no tiene credenciales para {exchange_name}.")
        return self.exchanges[key]
