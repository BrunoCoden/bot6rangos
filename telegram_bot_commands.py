# telegram_bot_commands.py
"""
Bot sencillo que atiende comandos de Telegram relacionados con la estrategia.
Actualmente soporta:
    /estavivo  -> devuelve el mismo estado que produce el heartbeat.
    /usuarios  -> lista usuarios activos y sus exchanges.
    /notional  -> actualiza el notional por usuario/exchange.
"""
from __future__ import annotations

import os
import json
import time
import sys
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, Optional
import tempfile

import requests
from dotenv import load_dotenv

from balance_ledger import parse_month_from_tokens
from trades_table_ledger import (
    LOCAL_TZ,
    consolidate_overlapping_rows,
    filter_trades_rows,
    read_trades_rows,
    summarize_trades_rows,
    write_trades_csv,
)
from heartbeat_monitor import generate_systemd_heartbeat_message, required_services_from_env
from trading.accounts.manager import AccountManager
from trading.accounts.models import ExchangeCredential, ExchangeEnvironment

_PENDING_ENV_UPDATES: dict[str, dict[str, object]] = {}


def _load_thresholds(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _binance_position_details(cred: ExchangeCredential, symbol: str) -> tuple[float | None, float | None]:
    try:
        from binance.um_futures import UMFutures
    except Exception as exc:
        return None, None
    try:
        api_key, api_secret = cred.resolve_keys(os.environ)
        client = UMFutures(key=api_key, secret=api_secret)
        positions = client.get_position_risk(symbol=symbol)
        if not positions:
            return 0.0, None
        pos = positions[0]
        amt = float(pos.get("positionAmt") or 0.0)
        entry = float(pos.get("entryPrice") or 0.0)
        return amt, (entry if entry > 0 else None)
    except Exception:
        return None, None


def _bybit_position_details(cred: ExchangeCredential, symbol: str) -> tuple[float | None, float | None]:
    try:
        from pybit.unified_trading import HTTP
    except Exception:
        return None, None
    try:
        api_key, api_secret = cred.resolve_keys(os.environ)
        is_testnet = cred.environment != ExchangeEnvironment.LIVE
        session = HTTP(testnet=is_testnet, api_key=api_key, api_secret=api_secret)
        resp = session.get_positions(category="linear", symbol=symbol)
        data = resp.get("result", {}).get("list", []) if isinstance(resp, dict) else []
        if not data:
            return 0.0, None
        pos = data[0]
        size = float(pos.get("size") or 0.0)
        side = (pos.get("side") or "").lower()
        if side == "sell":
            size = -abs(size)
        elif side == "buy":
            size = abs(size)
        entry = float(pos.get("avgPrice") or 0.0)
        return size, (entry if entry > 0 else None)
    except Exception:
        return None, None

def _parse_chat_ids(chat_ids_env: str | None) -> list[str]:
    if not chat_ids_env:
        return []
    parts = [part.strip() for part in chat_ids_env.replace(";", ",").split(",")]
    return [part for part in parts if part]


def _send_message(token: str, chat_id: int | str, text: str, reply_to: Optional[int] = None) -> None:
    payload = {
        "chat_id": chat_id,
        "text": text,
    }
    if reply_to is not None:
        payload["reply_to_message_id"] = reply_to

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload,
            timeout=10,
        ).raise_for_status()
    except Exception as exc:
        print(f"[BOT][WARN] No se pudo enviar respuesta a Telegram ({chat_id}): {exc}")


def _send_document(
    token: str,
    chat_id: int | str,
    file_path: Path,
    *,
    caption: str | None = None,
    reply_to: Optional[int] = None,
) -> None:
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
    if reply_to is not None:
        data["reply_to_message_id"] = str(reply_to)
    try:
        with file_path.open("rb") as fh:
            files = {"document": (file_path.name, fh, "text/csv")}
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data=data,
                files=files,
                timeout=30,
            )
            resp.raise_for_status()
    except Exception as exc:
        print(f"[BOT][WARN] No se pudo enviar CSV a Telegram ({chat_id}): {exc}")


def _fetch_updates(token: str, offset: Optional[int]) -> dict:
    params = {
        "timeout": 30,
    }
    if offset is not None:
        params["offset"] = offset
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            params=params,
            timeout=35,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[BOT][WARN] Error consultando getUpdates: {exc}")
        return {"ok": False, "result": []}


def _is_authorized(chat_id: int | str, allowed: Iterable[str]) -> bool:
    if not allowed:
        return True
    return str(chat_id) in allowed


def _normalize_command(text: str) -> str:
    if not text:
        return ""
    return text.strip().lower()

def _extract_command_and_arg(raw_text: str | None) -> tuple[str, str]:
    if not raw_text:
        return "", ""
    text = raw_text.strip()
    if not text.startswith("/"):
        return "", ""
    parts = text.split(maxsplit=1)
    cmd = parts[0].strip().lower()
    arg = parts[1].strip() if len(parts) == 2 else ""
    return cmd, arg


def _save_accounts_with_backup(manager: AccountManager, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        ts = int(time.time())
        backup = path.with_suffix(path.suffix + f".bak.{ts}")
        try:
            backup.write_bytes(path.read_bytes())
        except Exception:
            pass
    manager.save_to_file(path)

def _load_env_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def _write_env_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(lines).rstrip() + "\n"
    path.write_text(text, encoding="utf-8")


def _update_env_vars(path: Path, updates: dict[str, str]) -> None:
    """
    Actualiza/crea variables en el archivo .env sin tocar otras entradas.
    """
    existing = _load_env_lines(path)
    new_lines: list[str] = []
    remaining = dict(updates)
    for line in existing:
        if not line or line.lstrip().startswith("#") or "=" not in line:
            new_lines.append(line)
            continue
        key, _ = line.split("=", 1)
        key = key.strip()
        if key in remaining:
            new_lines.append(f"{key}={remaining.pop(key)}")
        else:
            new_lines.append(line)
    for key, value in remaining.items():
        new_lines.append(f"{key}={value}")
    _write_env_lines(path, new_lines)


def _parse_kv_pairs(raw: str) -> tuple[list[tuple[str, str]], list[str]]:
    pairs: list[tuple[str, str]] = []
    errors: list[str] = []
    if not raw:
        return pairs, errors
    tokens = raw.split()
    for token in tokens:
        if "=" not in token:
            errors.append(token)
            continue
        key, value = token.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            errors.append(token)
            continue
        if " " in key or " " in value:
            errors.append(token)
            continue
        pairs.append((key, value))
    return pairs, errors


def _parse_args_map(raw: str) -> tuple[dict[str, str], list[str]]:
    pairs, errors = _parse_kv_pairs(raw)
    data: dict[str, str] = {}
    for key, value in pairs:
        data[key] = value
    return data, errors


def _parse_notional_args(raw: str) -> tuple[str, str, float, str | None]:
    if not raw or not raw.strip():
        return "", "", 0.0, "Uso: /notional <user_id> <exchange> <monto_usdt>"
    if "=" in raw:
        args_map, errors = _parse_args_map(raw)
        if errors:
            return "", "", 0.0, "Formato inválido. Usá KEY=VALUE sin espacios."
        user_id = args_map.get("user_id") or args_map.get("user")
        exchange = args_map.get("exchange") or args_map.get("ex")
        amount = (
            args_map.get("notional_usdt")
            or args_map.get("notional")
            or args_map.get("amount")
        )
    else:
        parts = raw.split()
        if len(parts) != 3:
            return "", "", 0.0, "Uso: /notional <user_id> <exchange> <monto_usdt>"
        user_id, exchange, amount = parts

    if not user_id or not exchange or not amount:
        return "", "", 0.0, "Uso: /notional <user_id> <exchange> <monto_usdt>"
    try:
        notional = float(amount)
    except Exception:
        return "", "", 0.0, "Monto inválido. Ejemplo: /notional diego binance 30"
    if notional <= 0:
        return "", "", 0.0, "Monto inválido. Debe ser mayor a 0."
    return str(user_id), str(exchange).lower(), notional, None


def _parse_single_kv(raw: str) -> tuple[str | None, str | None]:
    text = (raw or "").strip()
    if not text or "=" not in text:
        return None, None
    key, value = text.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key or not value or " " in key or " " in value:
        return None, None
    return key, value


def _collect_enabled_pairs(manager: AccountManager) -> set[tuple[str, str]]:
    enabled: set[tuple[str, str]] = set()
    for account in manager.list_accounts():
        if not account.enabled:
            continue
        for ex_name, cred in (account.exchanges or {}).items():
            if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                continue
            enabled.add((account.user_id, ex_name.lower()))
    return enabled


def _format_balance_reply(*, summary: dict, user_filter: str | None, month_label: str | None) -> str:
    pnl_pct = float(summary.get("pnl_sum") or 0.0) * 100.0
    trades = int(summary.get("trades") or 0)
    wins = int(summary.get("wins") or 0)
    losses = int(summary.get("losses") or 0)
    title = "Balance"
    if user_filter:
        title += f" {user_filter}"
    if month_label:
        title += f" ({month_label})"
    lines = [
        f"{title}:",
        f"- trades={trades} wins={wins} losses={losses}",
        f"- pnl_acumulado={pnl_pct:+.2f}%",
    ]
    if not user_filter:
        breakdown = summary.get("breakdown") or {}
        if isinstance(breakdown, dict) and breakdown:
            lines.append("Desglose:")
            for key in sorted(breakdown.keys(), key=lambda x: (str(x[0]).lower(), str(x[1]).lower())):
                data = breakdown.get(key) or {}
                user_id, exchange = key
                row_trades = int(data.get("trades") or 0)
                row_pct = float(data.get("pnl_sum") or 0.0) * 100.0
                lines.append(f"- {user_id}/{exchange}: {row_pct:+.2f}% ({row_trades} trades)")
    return "\n".join(lines)


def _resolve_exchange_token(token: str) -> str | None:
    v = str(token or "").strip().lower()
    if v in {"binance", "bybit"}:
        return v
    return None


def _slug_token(value: str) -> str:
    txt = str(value or "").strip().lower()
    if not txt:
        return "all"
    out = []
    for ch in txt:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    slug = "".join(out)
    while "__" in slug:
        slug = slug.replace("__", "_")
    slug = slug.strip("_")
    return slug or "all"


def _default_trades_start_utc() -> datetime:
    return datetime(2026, 4, 1, 3, 0, 0, tzinfo=timezone.utc)


def _parse_trades_args(raw: str) -> tuple[str | None, str | None, tuple[datetime, datetime] | None, str | None, str | None]:
    tokens = [tok for tok in (raw or "").split() if tok.strip()]
    month_period, used_idx = parse_month_from_tokens(tokens)
    month_label = None
    if month_period and used_idx:
        month_label = " ".join(tokens[i] for i in sorted(used_idx))
    rest = [tok for idx, tok in enumerate(tokens) if idx not in used_idx]

    exchange_filter = None
    user_filter = None
    for tok in rest:
        ex = _resolve_exchange_token(tok)
        if ex and exchange_filter is None:
            exchange_filter = ex
            continue
        if user_filter is None:
            user_filter = tok
            continue
        return None, None, None, None, (
            "Uso: /trades [user_id] [exchange] [mes]\n"
            "Ejemplos: /trades | /trades diego | /trades binance | /trades diego binance abril2026"
        )
    return user_filter, exchange_filter, month_period, month_label, None


def _refresh_trades_ledger_from_exchange(
    *,
    accounts_path: Path,
    ledger_path: Path,
    user_filter: str | None,
    exchange_filter: str | None,
) -> tuple[bool, str]:
    enabled = str(os.getenv("TRADES_AUTO_REFRESH_ON_COMMAND", "true")).strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return True, "disabled"

    base_dir = Path(__file__).resolve().parent
    script_path = base_dir / "scripts" / "backfill_trades_table.py"
    if not script_path.exists():
        return False, f"script no encontrado: {script_path}"

    python_bin = (
        str(os.getenv("TRADES_REFRESH_PYTHON_BIN") or "").strip()
        or str(os.getenv("PYTHON_BIN") or "").strip()
        or sys.executable
        or "python3"
    )
    timeout_s = int(float(os.getenv("TRADES_REFRESH_TIMEOUT_SECONDS", "180")))
    from_local = str(os.getenv("TRADES_REFRESH_FROM_LOCAL", "2026-04-01T00:00:00-03:00")).strip()
    prefix_raw = os.getenv("TRADES_REFRESH_PREFIX")
    if prefix_raw is None:
        prefix = str(os.getenv("WATCHER_ORDER_ID_PREFIX", "BOT1")).strip()
    else:
        prefix = str(prefix_raw).strip()

    cmd = [
        python_bin,
        str(script_path),
        "--target",
        str(ledger_path),
        "--accounts",
        str(accounts_path),
        "--from-local",
        from_local,
        "--prefix",
        prefix,
    ]
    if user_filter:
        cmd.extend(["--user", str(user_filter).strip()])
    if exchange_filter:
        cmd.extend(["--exchange", str(exchange_filter).strip()])

    try:
        env = os.environ.copy()
        repo_path = str(base_dir)
        prev_pp = str(env.get("PYTHONPATH") or "").strip()
        env["PYTHONPATH"] = f"{repo_path}:{prev_pp}" if prev_pp else repo_path
        result = subprocess.run(
            cmd,
            cwd=str(base_dir),
            env=env,
            capture_output=True,
            text=True,
            timeout=max(timeout_s, 30),
        )
        if result.returncode == 0:
            out = (result.stdout or "").strip()
            last = out.splitlines()[-1] if out else "ok"
            return True, last
        detail = ((result.stderr or "") + "\n" + (result.stdout or "")).strip()
        return False, detail[-800:] if detail else f"returncode={result.returncode}"
    except subprocess.TimeoutExpired:
        return False, f"timeout refresh ({timeout_s}s)"
    except Exception as exc:
        return False, str(exc)


def _begin_interactive_env(chat_id: int | str, user_id: str) -> None:
    _PENDING_ENV_UPDATES[str(chat_id)] = {"mode": "env", "user_id": user_id, "vars": {}}


def _begin_interactive_user(chat_id: int | str) -> None:
    _PENDING_ENV_UPDATES[str(chat_id)] = {"mode": "user", "fields": {}}


def _begin_interactive(chat_id: int | str, user_id: str) -> None:
    _begin_interactive_env(chat_id, user_id)


def _pending_state(chat_id: int | str) -> dict[str, object] | None:
    return _PENDING_ENV_UPDATES.get(str(chat_id))


def _clear_pending(chat_id: int | str) -> None:
    _PENDING_ENV_UPDATES.pop(str(chat_id), None)


def _apply_pending_env_updates(
    *,
    token: str,
    chat_id: int,
    message_id: Optional[int],
    env_path: Path,
) -> None:
    state = _pending_state(chat_id)
    if not state or state.get("mode") != "env":
        _send_message(token, chat_id, "No hay cambios pendientes para aplicar.", reply_to=message_id)
        return
    vars_map = state.get("vars") or {}
    if not isinstance(vars_map, dict) or not vars_map:
        _send_message(token, chat_id, "No hay variables pendientes para aplicar.", reply_to=message_id)
        return
    updates = {str(k): str(v) for k, v in vars_map.items()}
    try:
        _update_env_vars(env_path, updates)
    except Exception as exc:
        _send_message(token, chat_id, f"No pude actualizar {env_path}: {exc}", reply_to=message_id)
        return
    applied = ", ".join(sorted(updates.keys()))
    _clear_pending(chat_id)
    _send_message(
        token,
        chat_id,
        f"Variables aplicadas: {applied}.\nReiniciando watcher...",
        reply_to=message_id,
    )
    service = os.getenv("WATCHER_SERVICE_NAME", "bot-watcher.service")
    ok, detail = _restart_service(service)
    if ok:
        _send_message(token, chat_id, f"Watcher reiniciado: {service}", reply_to=message_id)
    else:
        _send_message(token, chat_id, f"No pude reiniciar {service}: {detail}", reply_to=message_id)


def _apply_pending_user_create(
    *,
    token: str,
    chat_id: int,
    message_id: Optional[int],
    accounts_path: Path,
) -> None:
    state = _pending_state(chat_id)
    if not state or state.get("mode") != "user":
        _send_message(token, chat_id, "No hay alta de usuario pendiente.", reply_to=message_id)
        return
    fields = state.get("fields") or {}
    if not isinstance(fields, dict):
        _send_message(token, chat_id, "No hay datos para el alta de usuario.", reply_to=message_id)
        return
    required = ["user_id", "exchange", "api_key_env", "api_secret_env", "environment", "notional_usdt", "symbol"]
    missing = [key for key in required if not fields.get(key)]
    if missing:
        _send_message(
            token,
            chat_id,
            f"Faltan campos requeridos: {', '.join(missing)}",
            reply_to=message_id,
        )
        return
    user_id = str(fields["user_id"])
    exchange = str(fields["exchange"]).lower()
    try:
        environment = ExchangeEnvironment(str(fields["environment"]).lower())
    except Exception:
        _send_message(token, chat_id, "environment inválido (usar live/testnet).", reply_to=message_id)
        return
    try:
        notional = float(fields["notional_usdt"])
    except Exception:
        _send_message(token, chat_id, "notional_usdt inválido.", reply_to=message_id)
        return
    leverage = None
    if fields.get("leverage"):
        try:
            leverage = int(fields["leverage"])
        except Exception:
            _send_message(token, chat_id, "leverage inválido.", reply_to=message_id)
            return
    enabled = str(fields.get("enabled", "true")).lower() != "false"
    label = str(fields.get("label", user_id))
    extra: dict[str, str] = {}
    for key, value in fields.items():
        if str(key).startswith("extra_"):
            extra[str(key).removeprefix("extra_")] = str(value)
    if "symbol" not in extra:
        extra["symbol"] = str(fields["symbol"])
    if "margin_mode" not in extra:
        extra["margin_mode"] = "isolated"

    try:
        if accounts_path.exists():
            manager = AccountManager.from_file(accounts_path)
        else:
            manager = AccountManager.empty()
        account = manager.upsert_account(user_id, label=label, enabled=enabled)
        credential = ExchangeCredential(
            exchange=exchange,
            api_key_env=str(fields["api_key_env"]),
            api_secret_env=str(fields["api_secret_env"]),
            environment=environment,
            notional_usdt=notional,
            leverage=leverage if leverage is not None else 5,
            extra=extra,
        )
        manager.upsert_exchange(account.user_id, credential)
        _save_accounts_with_backup(manager, accounts_path)
    except Exception as exc:
        _send_message(token, chat_id, f"No pude guardar usuario ({accounts_path}): {exc}", reply_to=message_id)
        return
    _clear_pending(chat_id)
    ok, detail = _restart_service(os.getenv("WATCHER_SERVICE_NAME", "bot-watcher.service"))
    if ok:
        _send_message(token, chat_id, f"Usuario {user_id} creado/actualizado ✅", reply_to=message_id)
    else:
        _send_message(token, chat_id, f"Usuario {user_id} creado, pero no pude reiniciar watcher: {detail}", reply_to=message_id)


def _restart_service(service: str) -> tuple[bool, str]:
    try:
        subprocess.run(
            ["sudo", "-n", "systemctl", "restart", service],
            check=True,
            capture_output=True,
            text=True,
        )
        return True, ""
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.strip() if exc.stderr else exc.stdout.strip()
        return False, detail


def _ensure_chat_allowed(
    *,
    token: str,
    chat_id: int | str,
    allowed: list[str],
    env_path: Path,
) -> bool:
    chat_id_str = str(chat_id)
    if chat_id_str in allowed:
        return True

    # Auto-agrega el chat al allowlist y al .env para que reciba alertas.
    updated = [cid for cid in allowed if cid]
    if chat_id_str not in updated:
        updated.append(chat_id_str)
    try:
        _update_env_vars(env_path, {"TELEGRAM_CHAT_IDS": ",".join(updated)})
        allowed[:] = updated
        _send_message(
            token,
            chat_id,
            "Chat habilitado automaticamente. Aplicando cambios...",
        )
    except Exception as exc:
        _send_message(token, chat_id, f"No pude habilitar el chat: {exc}")
        return False

    service = os.getenv("WATCHER_SERVICE_NAME", "bot-watcher.service")
    ok, detail = _restart_service(service)
    if ok:
        _send_message(token, chat_id, f"Watcher reiniciado: {service}")
    else:
        _send_message(token, chat_id, f"No pude reiniciar {service}: {detail}")
    return True


def _handle_command(
    *,
    token: str,
    chat_id: int,
    message_id: Optional[int],
    command: str,
    arg: str,
    required_services: list[str],
) -> None:
    if command.startswith("/estavivo"):
        report = generate_systemd_heartbeat_message(required_services)
        _send_message(token, chat_id, report, reply_to=message_id)
        return

    if command.startswith("/posicion"):
        accounts_path = Path(os.getenv("WATCHER_ACCOUNTS_FILE", "trading/accounts/oci_accounts.yaml"))
        thresholds_path = Path(os.getenv("WATCHER_THRESHOLDS_FILE", "backtest/backtestTR/pending_thresholds.json"))
        try:
            manager = AccountManager.from_file(accounts_path)
        except Exception as exc:
            _send_message(token, chat_id, f"No pude cargar cuentas: {exc}", reply_to=message_id)
            return

        arg_map, errors = _parse_args_map(arg)
        user_filter = ""
        exchange_filter = ""
        if not errors and arg_map:
            user_filter = str(arg_map.get("user_id", "")).strip()
            exchange_filter = str(arg_map.get("exchange", "")).strip().lower()
        else:
            tokens = arg.split()
            if tokens:
                if len(tokens) >= 2 and tokens[-1].lower() in {"binance", "bybit"}:
                    exchange_filter = tokens[-1].lower()
                    user_filter = " ".join(tokens[:-1]).strip()
                else:
                    user_filter = " ".join(tokens).strip()

        thresholds = _load_thresholds(thresholds_path)
        rows = []
        for account in manager.list_accounts():
            if not account.enabled:
                continue
            if user_filter and account.user_id != user_filter:
                continue
            for ex_name, cred in (account.exchanges or {}).items():
                if exchange_filter and ex_name.lower() != exchange_filter:
                    continue
                if isinstance(cred.extra, dict) and cred.extra.get("enabled") is False:
                    continue
                symbol = (cred.extra or {}).get("symbol") or "ETHUSDT"
                pos_amt = None
                entry_price = None
                if ex_name.lower() == "binance":
                    pos_amt, entry_price = _binance_position_details(cred, symbol)
                elif ex_name.lower() == "bybit":
                    pos_amt, entry_price = _bybit_position_details(cred, symbol)
                else:
                    continue
                if pos_amt is None:
                    status = "pos=ERROR"
                else:
                    status = f"pos={pos_amt:.4f}"
                if entry_price:
                    status += f" entry={entry_price:.4f}"
                th = None
                for t in thresholds:
                    if t.get("user_id") == account.user_id and t.get("exchange") == ex_name and t.get("symbol") == symbol:
                        th = t
                        break
                if th:
                    status += f" SL={float(th.get('loss_price') or 0):.4f}"
                    gain = th.get('gain_price')
                    if gain not in (None, ""):
                        status += f" TP={float(gain):.4f}"
                rows.append(f"- {account.user_id}/{ex_name} {symbol} → {status}")

        if not rows:
            _send_message(token, chat_id, "No hay posiciones abiertas (o no se pudo consultar).", reply_to=message_id)
        else:
            _send_message(token, chat_id, "Posiciones:\n" + "\n".join(rows), reply_to=message_id)
        return

    if command.startswith("/usuarios"):
        accounts_path = os.getenv("WATCHER_ACCOUNTS_FILE", "trading/accounts/oci_accounts.yaml")
        try:
            manager = AccountManager.from_file(Path(accounts_path))
        except Exception as exc:
            _send_message(token, chat_id, f"No pude leer cuentas ({accounts_path}): {exc}", reply_to=message_id)
            return
        lines = ["Usuarios:"]
        for account in manager.list_accounts():
            exchanges_data = account.exchanges or {}
            if not exchanges_data:
                continue
            state = "ON" if account.enabled else "OFF"
            details = []
            for ex_name in sorted(exchanges_data.keys()):
                cred = exchanges_data.get(ex_name)
                if not cred:
                    continue
                notional = cred.notional_usdt
                notional_label = f"notional={notional:.2f}" if isinstance(notional, (int, float)) else "notional=NA"
                leverage = cred.leverage
                leverage_label = f"lev={leverage}x" if isinstance(leverage, int) and leverage > 0 else "lev=NA"
                margin_mode = None
                if isinstance(cred.extra, dict):
                    margin_mode = cred.extra.get("margin_mode")
                margin_label = f"margin={margin_mode}" if margin_mode else "margin=NA"
                details.append(f"{ex_name} ({notional_label} {leverage_label} {margin_label})")
            if not details:
                continue
            lines.append(f"- {account.user_id} [{state}]: {', '.join(details)}")
        if len(lines) == 1:
            lines.append("(ninguno)")
        _send_message(token, chat_id, "\n".join(lines), reply_to=message_id)
        return

    if command.startswith("/trades"):
        accounts_path = Path(os.getenv("WATCHER_ACCOUNTS_FILE", "trading/accounts/oci_accounts.yaml"))
        ledger_path = Path(os.getenv("TRADES_TABLE_LEDGER_PATH", "backtest/backtestTR/trades_table_ledger.jsonl"))
        try:
            manager = AccountManager.from_file(accounts_path)
        except Exception as exc:
            _send_message(token, chat_id, f"No pude leer cuentas ({accounts_path}): {exc}", reply_to=message_id)
            return

        user_filter, exchange_filter, month_period, month_label, parse_error = _parse_trades_args(arg)
        if parse_error:
            _send_message(token, chat_id, parse_error, reply_to=message_id)
            return

        refreshed_ok, refreshed_detail = _refresh_trades_ledger_from_exchange(
            accounts_path=accounts_path,
            ledger_path=ledger_path,
            user_filter=user_filter,
            exchange_filter=exchange_filter,
        )
        if not refreshed_ok:
            _send_message(
                token,
                chat_id,
                f"Aviso /trades: no pude refrescar desde exchange ({refreshed_detail}). Exporto con datos disponibles.",
                reply_to=message_id,
            )

        rows = read_trades_rows(ledger_path)

        default_source = str(os.getenv("TRADES_EXPORT_DEFAULT_SOURCE", "live") or "").strip().lower()
        now_local = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")
        out_dir = Path(tempfile.gettempdir()) / "bot_trades_exports"
        enabled_pairs_all = _collect_enabled_pairs(manager)

        # Sin parámetros: exporta un CSV por usuario activo del repo.
        if not (arg or "").strip():
            active_users = sorted({uid for uid, _ in enabled_pairs_all}, key=lambda x: x.lower())
            if not active_users:
                _send_message(token, chat_id, "No hay usuarios activos para exportar.", reply_to=message_id)
                return

            sent = 0
            for user_active in active_users:
                filtered = filter_trades_rows(
                    rows,
                    user_id=user_active,
                    exchange=None,
                    month_period=None,
                    enabled_pairs=enabled_pairs_all,
                    start_from_utc=_default_trades_start_utc(),
                )
                pre_source_count = len(filtered)
                if default_source:
                    filtered = [row for row in filtered if str(row.get("source") or "").strip().lower() == default_source]
                if str(os.getenv("TRADES_EXPORT_CONSOLIDATE_OVERLAPS", "false")).strip().lower() in {"1", "true", "yes", "on"}:
                    filtered = consolidate_overlapping_rows(filtered)
                omitted_strict = max(pre_source_count - len(filtered), 0)
                summary = summarize_trades_rows(filtered)
                out_path = out_dir / f"trades_bot_{_slug_token(user_active)}_all_since_2026_04_01_{now_local}.csv"
                write_trades_csv(filtered, out_path, tz=LOCAL_TZ)
                msg = (
                    f"Trades exportados: user={user_active} filas={int(summary.get('trades') or 0)} "
                    f"wins={int(summary.get('wins') or 0)} losses={int(summary.get('losses') or 0)}\n"
                    f"pnl%={float(summary.get('pnl_sum_pct') or 0.0) * 100:.4f}\n"
                    f"pnl_usdt={float(summary.get('pnl_sum_usdt') or 0.0):.6f}"
                )
                _send_message(token, chat_id, msg, reply_to=message_id if sent == 0 else None)
                _send_document(
                    token,
                    chat_id,
                    out_path,
                    caption=f"CSV /trades ({user_active})",
                    reply_to=message_id if sent == 0 else None,
                )
                sent += 1

            return

        # Con parámetros: mantiene comportamiento actual.
        if not rows:
            _send_message(token, chat_id, "No hay operaciones en trades_table_ledger.", reply_to=message_id)
            return

        enabled_pairs = None if user_filter else enabled_pairs_all
        start_default = None if month_period else _default_trades_start_utc()
        filtered = filter_trades_rows(
            rows,
            user_id=user_filter,
            exchange=exchange_filter,
            month_period=month_period,
            enabled_pairs=enabled_pairs,
            start_from_utc=start_default,
        )
        pre_source_count = len(filtered)
        if default_source:
            filtered = [row for row in filtered if str(row.get("source") or "").strip().lower() == default_source]
        if str(os.getenv("TRADES_EXPORT_CONSOLIDATE_OVERLAPS", "false")).strip().lower() in {"1", "true", "yes", "on"}:
            filtered = consolidate_overlapping_rows(filtered)
        omitted_strict = max(pre_source_count - len(filtered), 0)
        if not filtered:
            _send_message(token, chat_id, "Sin operaciones para el filtro solicitado.", reply_to=message_id)
            return

        summary = summarize_trades_rows(filtered)
        filters_slug = "_".join(
            [
                _slug_token(user_filter or "all"),
                _slug_token(exchange_filter or "all"),
                _slug_token((month_label or "since_2026_04_01").replace("/", "-").replace(" ", "_")),
            ]
        )
        out_path = out_dir / f"trades_bot_{filters_slug}_{now_local}.csv"
        write_trades_csv(filtered, out_path, tz=LOCAL_TZ)

        msg = (
            f"Trades exportados: filas={int(summary.get('trades') or 0)} "
            f"wins={int(summary.get('wins') or 0)} losses={int(summary.get('losses') or 0)}\n"
            f"pnl%={float(summary.get('pnl_sum_pct') or 0.0) * 100:.4f}\n"
            f"pnl_usdt={float(summary.get('pnl_sum_usdt') or 0.0):.6f}"
        )
        _send_message(token, chat_id, msg, reply_to=message_id)
        _send_document(token, chat_id, out_path, caption="CSV /trades", reply_to=message_id)
        return

    if command.startswith("/notional"):
        user_id, exchange, notional, error = _parse_notional_args(arg)
        if error:
            _send_message(token, chat_id, error, reply_to=message_id)
            return
        accounts_path = Path(os.getenv("WATCHER_ACCOUNTS_FILE", "trading/accounts/oci_accounts.yaml"))
        try:
            manager = AccountManager.from_file(accounts_path)
            cred = manager.get_exchange_credential(user_id, exchange)
        except Exception as exc:
            _send_message(token, chat_id, f"No pude cargar {user_id}/{exchange}: {exc}", reply_to=message_id)
            return
        cred.notional_usdt = notional
        try:
            manager.upsert_exchange(user_id, cred)
            _save_accounts_with_backup(manager, accounts_path)
        except Exception as exc:
            _send_message(token, chat_id, f"No pude guardar notional en {accounts_path}: {exc}", reply_to=message_id)
            return
        ok, detail = _restart_service(os.getenv("WATCHER_SERVICE_NAME", "bot-watcher.service"))
        if ok:
            _send_message(
                token,
                chat_id,
                f"Notional actualizado: {user_id}/{exchange} -> {notional:.2f}.\nWatcher reiniciado ✅",
                reply_to=message_id,
            )
        else:
            _send_message(
                token,
                chat_id,
                f"Notional actualizado: {user_id}/{exchange} -> {notional:.2f}.\n"
                f"No pude reiniciar watcher: {detail}",
                reply_to=message_id,
            )
        return

    if command in {"/reiniciar_watcher", "/restart_watcher"}:
        service = os.getenv("WATCHER_SERVICE_NAME", "bot-watcher.service")
        ok, detail = _restart_service(service)
        if ok:
            _send_message(token, chat_id, f"Watcher reiniciado: {service}", reply_to=message_id)
        else:
            _send_message(token, chat_id, f"No pude reiniciar {service}: {detail}", reply_to=message_id)
        return

    if command in {"/start", "/help"}:
        help_text = (
            "Comandos disponibles:\n"
            "• /estavivo — chequea los procesos criticos y devuelve el estado actual.\n"
            "• /usuarios — lista usuarios activos y sus exchanges.\n"
            "• /trades — exporta CSV de trades cerrados (datos estrictos del ledger real).\n"
            "    Uso: /trades [user_id] [exchange] [mes]\n"
            "    Ejemplos: /trades | /trades diego | /trades binance | /trades diego binance abril2026\n"
            "• /notional — actualiza el notional de un usuario/exchange.\n"
            "    Uso: /notional <user_id> <exchange> <monto_usdt>\n"
            "    Tambien acepta: /notional user_id=... exchange=... notional_usdt=...\n"
            "• /reiniciar_watcher — reinicia el servicio del watcher.\n"
            "Los mensajes siguen el formato del heartbeat automatico."
        )
        _send_message(token, chat_id, help_text, reply_to=message_id)
        return


def main() -> None:
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit("TELEGRAM_BOT_TOKEN no configurado.")

    allowed_chat_ids = _parse_chat_ids(os.getenv("TELEGRAM_CHAT_IDS"))
    env_path = Path(os.getenv("WATCHER_ENV_FILE", ".env"))
    required_services = required_services_from_env(None)
    if not required_services:
        raise SystemExit("HEARTBEAT_SERVICES vacio; defini servicios a monitorear.")

    print("[BOT] Telegram command listener iniciado.")
    offset: Optional[int] = None

    while True:
        data = _fetch_updates(token, offset)
        if not data.get("ok"):
            time.sleep(5)
            continue

        for update in data.get("result", []):
            offset = update["update_id"] + 1

            message = update.get("message") or update.get("channel_post")
            if not message:
                continue
            chat = message.get("chat") or {}
            chat_id = chat.get("id")
            if chat_id is None:
                continue
            if not _is_authorized(chat_id, allowed_chat_ids):
                if not _ensure_chat_allowed(
                    token=token,
                    chat_id=chat_id,
                    allowed=allowed_chat_ids,
                    env_path=env_path,
                ):
                    continue

            text = message.get("text")
            command, arg = _extract_command_and_arg(text)
            if not command:
                pending = _pending_state(chat_id)
                if pending and text:
                    key, value = _parse_single_kv(text)
                    if not key:
                        _send_message(
                            token,
                            chat_id,
                            "Formato invalido. Envia KEY=VALUE sin espacios.",
                            reply_to=message.get("message_id"),
                        )
                        continue
                    if pending.get("mode") == "user":
                        fields = pending.get("fields")
                        if not isinstance(fields, dict):
                            fields = {}
                            pending["fields"] = fields
                        fields[key] = value
                    else:
                        vars_map = pending.get("vars")
                        if not isinstance(vars_map, dict):
                            vars_map = {}
                            pending["vars"] = vars_map
                        vars_map[key] = value
                    if pending.get("mode") == "user":
                        required = {"user_id", "exchange", "api_key_env", "api_secret_env", "environment", "notional_usdt", "symbol"}
                        fields = pending.get("fields") if isinstance(pending.get("fields"), dict) else {}
                        missing = sorted([r for r in required if r not in fields or not fields.get(r)])
                        status = f"Faltan: {', '.join(missing)}" if missing else "Campos requeridos completos."
                        msg = f"Campo registrado: {key}. {status} Envia otro."
                    else:
                        msg = f"Variable registrada: {key}. Envia otra."
                    _send_message(
                        token,
                        chat_id,
                        msg,
                        reply_to=message.get("message_id"),
                    )
                continue

            _handle_command(
                token=token,
                chat_id=chat_id,
                message_id=message.get("message_id"),
                command=command,
                arg=arg,
                required_services=required_services,
            )

        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[BOT] Finalizado por el usuario.")
