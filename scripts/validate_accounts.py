#!/usr/bin/env python3
"""
Valida que las cuentas configuradas tengan sus credenciales en variables de entorno.

Uso:
    python scripts/validate_accounts.py --accounts trading/accounts/sample_accounts.yaml
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from trading.accounts.manager import AccountManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Valida credenciales definidas en accounts.yaml.")
    parser.add_argument(
        "--accounts",
        type=str,
        default=os.getenv("TRADING_ACCOUNTS_FILE", "trading/accounts/sample_accounts.yaml"),
        help="Ruta al archivo YAML/JSON con la configuración de cuentas.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Imprime el detalle completo de cada cuenta.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.accounts)
    try:
        manager = AccountManager.from_file(path)
    except Exception as exc:
        print(f"[ERROR] No se pudo leer {path}: {exc}", file=sys.stderr)
        return 1

    missing = []
    for account in manager.list_accounts():
        if args.verbose:
            print(f"- Cuenta: {account.user_id} ({account.label})")
        for name, credential in account.exchanges.items():
            try:
                credential.resolve_keys(os.environ)
                if args.verbose:
                    envs = (credential.api_key_env, credential.api_secret_env)
                    print(f"  • {name}: OK ({envs[0]}, {envs[1]})")
            except RuntimeError as exc:
                missing.append(str(exc))
                if args.verbose:
                    print(f"  • {name}: ERROR -> {exc}")

    if missing:
        print("\n[WARN] Credenciales faltantes:")
        for msg in missing:
            print(f"  - {msg}")
        print("\nExportá las variables correspondientes (ej: export VAR=valor) o configuralas en tu gestor de secretos.")
        return 2

    print(f"[OK] Todas las cuentas de {path} tienen credenciales disponibles en las variables de entorno.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
