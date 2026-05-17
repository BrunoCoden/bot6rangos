from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from .models import AccountConfig, ExchangeCredential, ExchangeEnvironment

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None


class AccountManager:
    """
    Administra múltiples cuentas/usuarios y sus credenciales por exchange.
    """

    def __init__(self, accounts: Iterable[AccountConfig]):
        self._accounts: Dict[str, AccountConfig] = {}
        for account in accounts:
            self._accounts[account.user_id] = account

    @classmethod
    def empty(cls) -> "AccountManager":
        return cls(accounts=[])

    @classmethod
    def from_dict(cls, data: Mapping) -> "AccountManager":
        def _flatten_extra(value: Any) -> dict:
            if not isinstance(value, dict):
                return {}
            merged: dict = dict(value)
            while isinstance(merged.get("extra"), dict):
                nested = merged.pop("extra")
                merged = {**nested, **merged}
            return merged

        users = data.get("users") or data.get("accounts") or []
        accounts: list[AccountConfig] = []
        for entry in users:
            user_id = entry.get("id") or entry.get("user_id")
            if not user_id:
                raise ValueError("Cada cuenta debe especificar 'id'.")
            label = entry.get("label") or user_id
            exchanges_data = entry.get("exchanges") or {}
            exchanges: Dict[str, ExchangeCredential] = {}
            for ex_name, ex_conf in exchanges_data.items():
                exchange = ex_conf.get("exchange", ex_name).lower()
                env_value = (ex_conf.get("environment") or ExchangeEnvironment.TESTNET.value).lower()
                environment = ExchangeEnvironment(env_value)
                notional_val = ex_conf.get("notional_usdt")
                notional = float(notional_val) if notional_val not in (None, "") else None
                leverage_val = ex_conf.get("leverage")
                leverage = int(leverage_val) if leverage_val not in (None, "") else None
                cred = ExchangeCredential(
                    exchange=exchange,
                    api_key_env=ex_conf["api_key_env"],
                    api_secret_env=ex_conf["api_secret_env"],
                    environment=environment,
                    notional_usdt=notional,
                    leverage=leverage,
                    extra={
                        **_flatten_extra(ex_conf.get("extra")),
                        **{
                            k: v
                            for k, v in ex_conf.items()
                            if k
                            not in {
                                "api_key_env",
                                "api_secret_env",
                                "environment",
                                "notional_usdt",
                                "leverage",
                                "extra",
                            }
                        },
                    },
                )
                exchanges[exchange] = cred
            metadata = entry.get("metadata") or {}
            accounts.append(
                AccountConfig(
                    user_id=user_id,
                    label=label,
                    enabled=bool(entry.get("enabled", True)),
                    exchanges=exchanges,
                    metadata=metadata,
                )
            )
        return cls(accounts)

    @classmethod
    def from_file(cls, path: Path) -> "AccountManager":
        if not path.exists():
            raise FileNotFoundError(f"No se encontró archivo de cuentas: {path}")

        if path.suffix in {".yaml", ".yml"}:
            if yaml is None:
                raise RuntimeError("PyYAML no está instalado. `pip install pyyaml` para leer archivos YAML.")
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
        else:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        return cls.from_dict(data)

    def list_accounts(self) -> list[AccountConfig]:
        return list(self._accounts.values())

    def get_account(self, user_id: str) -> AccountConfig:
        try:
            return self._accounts[user_id]
        except KeyError as exc:
            raise KeyError(f"No existe la cuenta '{user_id}'.") from exc

    def get_exchange_credential(self, user_id: str, exchange: str) -> ExchangeCredential:
        account = self.get_account(user_id)
        return account.get_exchange(exchange)

    def resolve_keys(self, user_id: str, exchange: str, env: Optional[Mapping[str, str]] = None) -> tuple[str, str]:
        credential = self.get_exchange_credential(user_id, exchange)
        env_mapping = env or os.environ
        return credential.resolve_keys(env_mapping)

    def to_dict(self) -> dict:
        """
        Devuelve una representación serializable a JSON/YAML.

        Se normaliza `environment` a su valor string para evitar que se
        serialice como Enum en el archivo de cuentas.
        """

        def _serialize_credential(cred: ExchangeCredential) -> dict:
            data = asdict(cred)
            data["environment"] = cred.environment.value
            return data

        def _serialize_account(acc: AccountConfig) -> dict[str, Any]:
            return {
                "id": acc.user_id,
                "label": acc.label,
                "enabled": acc.enabled,
                "metadata": acc.metadata or {},
                "exchanges": {name: _serialize_credential(cred) for name, cred in acc.exchanges.items()},
            }

        return {"users": [_serialize_account(acc) for acc in self._accounts.values()]}

    # --- Mutadores -----------------------------------------------------

    def upsert_account(
        self,
        user_id: str,
        *,
        label: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        enabled: Optional[bool] = None,
    ) -> AccountConfig:
        """
        Crea o actualiza una cuenta. No cambia el user_id existente.
        """
        if not user_id:
            raise ValueError("user_id no puede ser vacío.")
        account = self._accounts.get(user_id)
        if account:
            if label:
                account.label = label
            if metadata is not None:
                account.metadata = dict(metadata)
            if enabled is not None:
                account.enabled = bool(enabled)
            return account

        new_account = AccountConfig(
            user_id=user_id,
            label=label or user_id,
            enabled=True if enabled is None else bool(enabled),
            exchanges={},
            metadata=dict(metadata or {}),
        )
        self._accounts[user_id] = new_account
        return new_account

    def remove_account(self, user_id: str) -> None:
        try:
            del self._accounts[user_id]
        except KeyError as exc:
            raise KeyError(f"No existe la cuenta '{user_id}'.") from exc

    def rename_account(self, old_id: str, new_id: str) -> AccountConfig:
        if not new_id:
            raise ValueError("new_id no puede ser vacío.")
        if new_id == old_id:
            return self.get_account(old_id)
        if new_id in self._accounts:
            raise ValueError(f"Ya existe la cuenta '{new_id}'.")
        account = self.get_account(old_id)
        del self._accounts[old_id]
        account.user_id = new_id
        self._accounts[new_id] = account
        return account

    def upsert_exchange(self, user_id: str, credential: ExchangeCredential) -> None:
        account = self.get_account(user_id)
        credential.exchange = credential.exchange.lower()
        account.exchanges[credential.exchange] = credential

    def remove_exchange(self, user_id: str, exchange: str) -> None:
        account = self.get_account(user_id)
        key = exchange.lower()
        if key not in account.exchanges:
            raise KeyError(f"La cuenta {user_id} no tiene credenciales para {exchange}.")
        del account.exchanges[key]

    def save_to_file(self, path: Path) -> None:
        """
        Persiste las cuentas al archivo indicado (YAML o JSON).
        """
        data = self.to_dict()
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix in {".yaml", ".yml"}:
            if yaml is None:
                raise RuntimeError("PyYAML no está instalado. `pip install pyyaml` para escribir archivos YAML.")
            with path.open("w", encoding="utf-8") as fh:
                yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=False)
            return

        with path.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
