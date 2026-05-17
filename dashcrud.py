#!/usr/bin/env python3
"""
DashCRUD: dashboard mínimo para CRUD de cuentas/exchanges.

- Usa YAML/JSON en trading/accounts/* para persistir.
- No expone secretos; solo nombres de variables de entorno.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse, unquote
import shutil
import subprocess

import requests
import yaml

# Asegura imports relativos al repo
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trading.accounts.manager import AccountManager
from trading.accounts.models import AccountConfig, ExchangeCredential, ExchangeEnvironment

DEFAULT_ACCOUNTS_PATH = Path("trading/accounts/oci_accounts.yaml")
DEFAULT_HTML = REPO_ROOT / "trading/accounts/dashcrud.html"
DEFAULT_ENV_PATH = Path(os.getenv("DASHCRUD_ENV_PATH", "/home/ubuntu/bot/.env"))
# Path secundario opcional para compatibilidad (systemd env file)
SECONDARY_ENV_PATH = Path("/etc/systemd/system/bot.env")
FALLBACK_SYMBOLS = {"binance": {"ETHUSDT", "BTCUSDT"}, "bybit": {"ETHUSDT", "BTCUSDT"}}
PENDING_APPLY_PATH = Path(os.getenv("DASHCRUD_PENDING_PATH", "trading/accounts/.dashcrud_pending.json"))
APPLY_SERVICES = [
    s.strip()
    for s in os.getenv("DASHCRUD_APPLY_SERVICES", "bot-watcher.service").split(",")
    if s.strip()
]


def _load_pending() -> dict:
    try:
        if not PENDING_APPLY_PATH.exists():
            return {"pending": False, "changes": []}
        data = json.loads(PENDING_APPLY_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"pending": True, "changes": []}
        changes = data.get("changes") if isinstance(data.get("changes"), list) else []
        return {
            "pending": bool(data.get("pending", True)),
            "updated_at": data.get("updated_at"),
            "changes": changes,
        }
    except Exception:
        return {"pending": True, "changes": []}


def _mark_pending(change: dict) -> None:
    try:
        PENDING_APPLY_PATH.parent.mkdir(parents=True, exist_ok=True)
        state = _load_pending()
        changes = state.get("changes") if isinstance(state.get("changes"), list) else []
        changes.append(change)
        out = {
            "pending": True,
            "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "changes": changes[-200:],
        }
        PENDING_APPLY_PATH.write_text(json.dumps(out, indent=2), encoding="utf-8")
    except Exception:
        pass


def _clear_pending() -> None:
    try:
        if PENDING_APPLY_PATH.exists():
            PENDING_APPLY_PATH.unlink()
    except Exception:
        pass


def _normalize_identifier(value: str) -> str:
    """Normaliza IDs para usarlos en nombres de variables (sin espacios)."""
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    # Compacta guiones/underscores múltiples
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("_-")


def _load_manager(path: Path) -> AccountManager:
    try:
        return AccountManager.from_file(path)
    except FileNotFoundError:
        print(f"[WARN] {path} no existe; se inicializa vacío.")
        return AccountManager.empty()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] No se pudo leer {path}: {exc}", file=sys.stderr)
        sys.exit(1)


def _serialize(manager: AccountManager, accounts_path: Path) -> dict:
    data = manager.to_dict()
    out = []
    for acc in data.get("users", []):
        exchanges = []
        for name, cred in (acc.get("exchanges") or {}).items():
            exchanges.append(
                {
                    "name": name,
                    "exchange": cred.get("exchange", name),
                    "environment": cred.get("environment"),
                    "api_key_env": cred.get("api_key_env"),
                    "api_secret_env": cred.get("api_secret_env"),
                    "notional_usdt": cred.get("notional_usdt"),
                    "leverage": cred.get("leverage"),
                    "symbol": (cred.get("extra") or {}).get("symbol"),
                    "extra": cred.get("extra") or {},
                }
            )
        out.append(
            {
                "id": acc["id"],
                "label": acc.get("label", acc["id"]),
                "enabled": bool(acc.get("enabled", True)),
                "metadata": acc.get("metadata") or {},
                "exchanges": exchanges,
            }
        )
    pending = _load_pending()
    return {"accounts_path": str(accounts_path), "pending": pending, "users": out}


def _validate_symbol(exchange: str, environment: ExchangeEnvironment, symbol: str) -> None:
    """
    Valida el símbolo contra el exchange.
    - Binance: consulta exchangeInfo.
    - Bybit: se acepta sin validar contra la API (asumimos símbolo válido), con fallback si se configuró.
    - Otros: usa fallback si está configurado.
    """
    ex = exchange.lower()
    sym = symbol.upper()
    if ex == "binance":
        base_url = "https://testnet.binancefuture.com" if environment == ExchangeEnvironment.TESTNET else "https://fapi.binance.com"
        try:
            resp = requests.get(f"{base_url}/fapi/v1/exchangeInfo", timeout=8)
            resp.raise_for_status()
            data = resp.json()
            symbols = {
                s["symbol"]
                for s in data.get("symbols", [])
                if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL"
            }
            if sym not in symbols:
                raise ValueError(f"El símbolo {sym} no está disponible en {exchange} ({environment.value}).")
            return
        except requests.RequestException as exc:
            if sym in FALLBACK_SYMBOLS.get(ex, set()):
                return
            raise ValueError(f"No se pudo validar el símbolo en {exchange}: {exc}")
    if ex == "bybit":
        return
    if sym in FALLBACK_SYMBOLS.get(ex, set()):
        return
    raise ValueError(f"No se reconoce el exchange '{exchange}' o el símbolo {sym} no está permitido.")


def _generate_env_names(user_id: str, exchange: str, environment: ExchangeEnvironment) -> tuple[str, str]:
    normalized_user = _normalize_identifier(user_id)
    base = f"{normalized_user}_{exchange}_{environment.value}".upper().replace("-", "_")
    return f"{base}_API_KEY", f"{base}_API_SECRET"


def _load_env_file(env_path: Path) -> list[str]:
    if not env_path.exists():
        return []
    try:
        return env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []


def _save_env_file(env_path: Path, lines: list[str]) -> None:
    env_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines) + "\n"
    env_path.write_text(content, encoding="utf-8")


def _set_env_vars(env_path: Path, mapping: Dict[str, str]) -> None:
    """Actualiza/crea variables en el env file, con backup previo."""
    try:
        lines = _load_env_file(env_path)
        out = []
        seen = set()
        for line in lines:
            if not line or line.strip().startswith("#") or "=" not in line:
                out.append(line)
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            if key in mapping:
                out.append(f"{key}={mapping[key]}")
                seen.add(key)
            else:
                out.append(line)
        # add missing keys
        for k, v in mapping.items():
            if k not in seen:
                out.append(f"{k}={v}")
        _save_env_file(env_path, out)
    except PermissionError as exc:
        print(f"[ENV][WARN] No se pudo escribir {env_path}: {exc}; las variables deben cargarse a mano.")
        return


def _build_credential(payload: Dict[str, Any], default_name: str | None = None, *, user_id: str | None = None, env_path: Path = DEFAULT_ENV_PATH) -> ExchangeCredential:
    name = (payload.get("exchange") or payload.get("name") or default_name or "").lower()
    if not name:
        raise ValueError("exchange es obligatorio.")
    env_raw = (payload.get("environment") or ExchangeEnvironment.TESTNET.value).lower()
    try:
        environment = ExchangeEnvironment(env_raw)
    except ValueError:
        valid = [e.value for e in ExchangeEnvironment]
        raise ValueError(f"environment debe ser uno de {valid}.")

    api_key_env = str(payload.get("api_key_env") or "").strip()
    api_secret_env = str(payload.get("api_secret_env") or "").strip()
    # Valores en texto plano (acepta *_plain o *_text)
    api_key_plain = str(
        payload.get("api_key_plain")
        or payload.get("api_key_text")
        or payload.get("api_key")
        or ""
    ).strip()
    api_secret_plain = str(
        payload.get("api_secret_plain")
        or payload.get("api_secret_text")
        or payload.get("api_secret")
        or ""
    ).strip()
    def _looks_like_secret(val: str) -> bool:
        return bool(val) and len(val) >= 20 and " " not in val and "=" not in val

    # Heurística: si el usuario pegó las claves en los campos *_env (confusión común), las tratamos como valores.
    if not api_key_plain and not api_secret_plain and _looks_like_secret(api_key_env) and _looks_like_secret(api_secret_env):
        api_key_plain, api_secret_plain = api_key_env, api_secret_env
        api_key_env, api_secret_env = "", ""

    if api_key_plain and api_secret_plain:
        if not user_id:
            raise ValueError("user_id es obligatorio para generar variables de entorno.")
        # Si no especificaron nombres de env, se generan; si los pasaron, se usan esos
        if not api_key_env or not api_secret_env:
            gen_key, gen_secret = _generate_env_names(user_id, name, environment)
            api_key_env, api_secret_env = gen_key, gen_secret
        _set_env_vars(env_path, {api_key_env: api_key_plain, api_secret_env: api_secret_plain})
        # Compatibilidad: también intentamos escribir en /etc/systemd/system/bot.env si existe o se puede
        try:
            _set_env_vars(SECONDARY_ENV_PATH, {api_key_env: api_key_plain, api_secret_env: api_secret_plain})
        except Exception:
            pass

    if not api_key_env or not api_secret_env:
        raise ValueError("api_key_env y api_secret_env son obligatorios (o provée keys en texto para generarlas).")

    symbol = str(payload.get("symbol") or payload.get("pair") or "").strip().upper()
    if not symbol:
        raise ValueError("symbol es obligatorio.")

    _validate_symbol(name, environment, symbol)

    notional_val = payload.get("notional_usdt")
    leverage_val = payload.get("leverage")
    notional = float(notional_val) if notional_val not in (None, "", False) else None
    leverage = int(leverage_val) if leverage_val not in (None, "", False) else None
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    extra = {**extra, "symbol": symbol}
    return ExchangeCredential(
        exchange=name,
        api_key_env=api_key_env,
        api_secret_env=api_secret_env,
        environment=environment,
        notional_usdt=notional,
        leverage=leverage,
        extra=extra,
    )


def _save_with_backup(manager: AccountManager, path: Path) -> None:
    if path.exists():
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        backup = path.with_suffix(path.suffix + f".bak.{ts}")
        shutil.copy2(path, backup)
    manager.save_to_file(path)


def _restart_services(services: list[str]) -> tuple[bool, str | None]:
    """
    Reinicia servicios systemd. Devuelve (ok, error_msg).
    """
    cmds = [
        ["sudo", "systemctl", "restart", *services],
        ["systemctl", "restart", *services],
    ]
    errors = []
    for cmd in cmds:
        try:
            subprocess.run(cmd, check=True)
            return True, None
        except subprocess.CalledProcessError as exc:  # pragma: no cover - externo
            errors.append(str(exc))
    return False, "; ".join(errors)


class DashCRUDHandler(BaseHTTPRequestHandler):
    manager: AccountManager
    accounts_path: Path
    html_path: Path

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: D401
        """Log mínimo a stdout."""
        msg = fmt % args
        print(f"[HTTP] {self.address_string()} {msg}")

    # --- Helpers -------------------------------------------------- #
    def _read_json(self) -> dict | None:
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length > 0 else b""
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            try:
                decoded = raw.decode("utf-8", errors="replace")
                print(f"[HTTP][WARN] JSON inválido (json.loads): {decoded}")
                # Intento con YAML para tolerar pequeños desvíos de sintaxis
                try:
                    alt = yaml.safe_load(decoded)
                    if isinstance(alt, dict):
                        print("[HTTP][INFO] JSON parseado vía YAML fallback")
                        return alt
                except Exception as exc:
                    print(f"[HTTP][WARN] YAML fallback falló: {exc}")
            except Exception:
                pass
            self._send_json(400, {"error": "JSON inválido"})
            return None

    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_html(self) -> None:
        try:
            html = self.html_path.read_bytes()
        except FileNotFoundError:
            self.send_error(404, "Dashboard HTML no encontrado")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.end_headers()
        self.wfile.write(html)

    def _snapshot(self) -> dict:
        return _serialize(self.manager, self.accounts_path)

    # --- Routing -------------------------------------------------- #
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        parts = [unquote(p) for p in path.split("/") if p]

        if path == "/":
            self._serve_html()
            return
        if parts[:2] == ["api", "accounts"]:
            if len(parts) == 2:
                self._send_json(200, self._snapshot())
                return
            if len(parts) == 3:
                user_id = parts[2]
                try:
                    acc = self.manager.get_account(user_id)
                except KeyError:
                    self._send_json(404, {"error": f"No existe la cuenta '{user_id}'."})
                    return
                tmp_manager = AccountManager([acc])
                self._send_json(200, _serialize(tmp_manager, self.accounts_path))
                return
        self.send_error(404, "Ruta no encontrada")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(p) for p in parsed.path.split("/") if p]
        if parts == ["api", "apply"]:
            ok, err = _restart_services(APPLY_SERVICES)
            resp = self._snapshot()
            if ok:
                _clear_pending()
                resp["applied"] = True
                resp["message"] = f"Servicios reiniciados: {', '.join(APPLY_SERVICES)}"
                print(f"[DASHCRUD][APPLY] ok services={APPLY_SERVICES}")
            else:
                resp["applied"] = False
                resp["error"] = f"No se pudieron reiniciar servicios: {err or 'unknown error'}"
                print(f"[DASHCRUD][APPLY] error services={APPLY_SERVICES} err={err}")
            self._send_json(200 if ok else 500, resp)
            return
        if parts == ["api", "accounts"]:
            payload = self._read_json()
            if payload is None:
                return
            user_id = (payload.get("id") or payload.get("user_id") or "").strip()
            label = (payload.get("label") or "").strip()
            enabled = bool(payload.get("enabled", True))
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            if not user_id:
                self._send_json(400, {"error": "id es obligatorio."})
                return
            if user_id in {a.user_id for a in self.manager.list_accounts()}:
                self._send_json(409, {"error": f"La cuenta '{user_id}' ya existe."})
                return
            account = self.manager.upsert_account(user_id, label=label or None, metadata=metadata, enabled=enabled)
            exchange_payload = payload.get("exchange") or {}
            if exchange_payload:
                try:
                    cred = _build_credential(exchange_payload, user_id=user_id, env_path=DEFAULT_ENV_PATH)
                    self.manager.upsert_exchange(user_id, cred)
                except ValueError as exc:
                    self._send_json(400, {"error": str(exc)})
                    return
            _save_with_backup(self.manager, self.accounts_path)
            _mark_pending({"type": "account_create", "user_id": user_id, "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z"})
            tmp_manager = AccountManager([account])
            self._send_json(201, _serialize(tmp_manager, self.accounts_path))
            return

        self.send_error(404, "Ruta no encontrada")

    def do_PUT(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(p) for p in parsed.path.split("/") if p]
        if parts[:2] != ["api", "accounts"]:
            self.send_error(404, "Ruta no encontrada")
            return
        payload = self._read_json()
        if payload is None:
            return

        if len(parts) == 3:
            # Actualiza cuenta (label/enabled/metadata)
            user_id = parts[2]
            try:
                self.manager.get_account(user_id)
            except KeyError:
                self._send_json(404, {"error": f"No existe la cuenta '{user_id}'."})
                return
            new_id = (payload.get("id") or payload.get("user_id") or user_id).strip()
            if not new_id:
                self._send_json(400, {"error": "id no puede ser vacío."})
                return
            if new_id != user_id and new_id in {a.user_id for a in self.manager.list_accounts()}:
                self._send_json(409, {"error": f"La cuenta '{new_id}' ya existe."})
                return

            label = payload.get("label")
            enabled = payload.get("enabled")
            metadata = payload.get("metadata")
            if new_id != user_id:
                self.manager.rename_account(user_id, new_id)
                user_id = new_id
            self.manager.upsert_account(
                user_id,
                label=label if label is not None else None,
                metadata=metadata if isinstance(metadata, dict) else None,
                enabled=enabled if enabled is not None else None,
            )
            _save_with_backup(self.manager, self.accounts_path)
            _mark_pending({"type": "account_update", "user_id": user_id, "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z"})
            self._send_json(200, self._snapshot())
            return

        if len(parts) == 4 and parts[3] == "exchange":
            user_id = parts[2]
            try:
                account = self.manager.get_account(user_id)
            except KeyError:
                self._send_json(404, {"error": f"No existe la cuenta '{user_id}'."})
                return
            try:
                cred = _build_credential(payload, default_name=payload.get("exchange") or payload.get("name"), user_id=user_id, env_path=DEFAULT_ENV_PATH)
            except Exception as exc:  # noqa: BLE001
                self._send_json(400, {"error": str(exc)})
                return
            # Mantener un único exchange por usuario: se limpia y se inserta el nuevo.
            account.exchanges = {}
            self.manager.upsert_exchange(user_id, cred)
            _save_with_backup(self.manager, self.accounts_path)
            _mark_pending(
                {
                    "type": "exchange_save",
                    "user_id": user_id,
                    "exchange": cred.exchange,
                    "environment": cred.environment.value,
                    "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                }
            )
            print(f"[DASHCRUD][SAVE] exchange user={user_id} ex={cred.exchange} env={cred.environment.value}")
            self._send_json(200, self._snapshot())
            return

        self.send_error(404, "Ruta no encontrada")

    def do_DELETE(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(p) for p in parsed.path.split("/") if p]
        if parts[:2] != ["api", "accounts"] or len(parts) != 3:
            self.send_error(404, "Ruta no encontrada")
            return
        user_id = parts[2]
        try:
            account = self.manager.get_account(user_id)
        except KeyError:
            self._send_json(404, {"error": f"No existe la cuenta '{user_id}'."})
            return
        # Borrado lógico: enabled = False
        account.enabled = False
        _save_with_backup(self.manager, self.accounts_path)
        self._send_json(200, self._snapshot())


def _build_handler(manager: AccountManager, accounts_path: Path, html_path: Path):
    class _Handler(DashCRUDHandler):
        pass

    _Handler.manager = manager
    _Handler.accounts_path = accounts_path
    _Handler.html_path = html_path
    return _Handler


def main() -> int:
    parser = argparse.ArgumentParser(description="DashCRUD: server HTTP para cuentas/exchanges.")
    parser.add_argument("--accounts", type=str, default=DEFAULT_ACCOUNTS_PATH, help="Archivo YAML/JSON de cuentas.")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host de escucha (default 0.0.0.0).")
    parser.add_argument("--port", type=int, default=8050, help="Puerto de escucha (default 8050).")
    parser.add_argument("--html", type=str, default=None, help="Ruta del HTML del dashboard.")
    args = parser.parse_args()

    accounts_path = Path(args.accounts)
    html_path = Path(args.html) if args.html else DEFAULT_HTML
    manager = _load_manager(accounts_path)
    handler = _build_handler(manager, accounts_path, html_path)

    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"[INFO] DashCRUD en http://{args.host}:{args.port}")
    print(f"[INFO] Archivo de cuentas: {accounts_path}")
    print(f"[INFO] HTML: {html_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Detenido por el usuario.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
