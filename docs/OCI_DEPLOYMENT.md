# OCI Deployment Guide

This guide documents how to host the Bollinger strategy stack on Oracle Cloud Infrastructure (OCI) so the watchers, alerting bot, and optional trading hooks keep running 24/7.

---

## 1. Architecture Overview

| Component | Purpose | Runtime |
| --- | --- | --- |
| `watcher_alertas.py` | Streams Bollinger signals, sends Telegram alerts, optionally triggers orders via `trading/` | long–running service |
| `backtest/order_fill_listener.py` | Matches pending stop/limit orders against 1 m data (only needed when the real‑time state file is used) | optional daemon |
| `heartbeat_monitor.py` | Sends a periodic “healthy / not healthy” summary to Telegram | timer/daemon |
| `scripts/validate_accounts.py` | Verifies that OCI environment variables contain every API key referenced in `trading/accounts/*.yaml` | manual check |

Services share the same `.env`, `trading/accounts/*.yaml` and Python virtual environment (`.venv`).

---

## 2. Prepare an Always Free Compute Instance

1. **Create an A1.Flex VM**  
   - Shape: `VM.Standard.A1.Flex` (Arm). Always Free lets you allocate up to 4 OCPUs + 24 GB RAM; for this stack 2 OCPUs / 8 GB RAM is enough.  
   - Image: Ubuntu 22.04 ARM64 (preferred) or Oracle Linux 9 ARM.  
   - **Public IP options:**  
     - *Ephemeral (default)* – assigned at launch and released if you stop/terminate the VM. Zero cost, perfect if you only need outbound HTTPS (Binance/Telegram) or will use a jump host/VPN to reach the box.  
     - *Reserved* – allocate a Reserved Public IP (still free for one address in Always Free) and attach it to the instance; the IP survives stops/restarts, which is handy if you want to pin firewall rules, use a fixed SSH hostname, or whitelist the bot in third-party services. Choose this if you expect to stop/start the VM frequently or need the same IP for reverse tunnels/webhooks.  
   - Upload your SSH key when provisioning.

2. **Networking tips (Always Free limits)**  
   - Outbound HTTPS is available; no extra security list rules required unless you hardened the VCN.  
   - Keep boot volume ≤ 200 GB (limit for Always Free). Using the default 50 GB SSD is fine.  
   - If you need to expose Grafana/Prometheus later, remember Free tier only includes one load balancer—consider using WireGuard instead.

3. **Install base packages (ARM)**  
   ```bash
   sudo apt update && sudo apt install -y \
     git python3.11 python3.11-venv python3-pip \
     build-essential pkg-config libffi-dev libssl-dev \
     tmux unzip jq
   ```
   > Ubuntu 22.04 ships Python 3.10 by default; the `python3.11` apt package installs the newer interpreter compatible with the repo. On Oracle Linux use `dnf install python3.11 python3.11-devel`.

---

## 3. Clone the Repository and Create the Runtime User

```bash
sudo useradd --system --create-home --shell /bin/bash stratbot
sudo su - stratbot
git clone <tu_repo_privado>.git bot
cd bot
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Keep the repository under `/home/stratbot/bot` for the examples below.

---

## 4. Configure Secrets and Environment

1. **`.env` base file**  
   Copy the local `.env` into `/home/stratbot/bot/.env` and adjust:
   - `SYMBOL`, `STREAM_INTERVAL`, `CHANNEL_INTERVAL`, Bollinger params.
   - Telegram bot token (`TELEGRAM_BOT_TOKEN`) and chat IDs.
   - Backtest output paths (default values already point inside `backtest/`).
   - `WATCHER_ENABLE_TRADING`, `WATCHER_TRADING_*` flags (set to `false` until live trading is ready).

2. **Trading accounts**  
   Create `trading/accounts/oci_accounts.yaml` describing each user/exchange and export the referenced API keys as environment variables (recommended to keep them in `/etc/systemd/system/bot.env` so systemd can load them).

3. **Quick validation**
   ```bash
   source .venv/bin/activate
   set -a && source .env && set +a
   python scripts/validate_accounts.py --accounts trading/accounts/oci_accounts.yaml --verbose
   ```

---

## 5. Manual Smoke Tests

1. **Watcher dry run**
   ```bash
   source .venv/bin/activate
   set -a && source .env && set +a
   python watcher_alertas.py
   ```
   Confirm alerts reach Telegram. Stop with `Ctrl+C`.

2. **Backtest sanity check**
   ```bash
   CHANNEL_INTERVAL=1h STREAM_INTERVAL=1h python backtest/run_backtest.py --profile tr --weeks 1
   ```
   Ensures Binance connectivity works from OCI.

---

## 6. Systemd Services

Create `/etc/systemd/system/bot.env` (owned by `root:root`, mode `0600`) with secrets that must not live inside git:

```ini
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
TELEGRAM_BOT_TOKEN=...
WATCHER_ENABLE_TRADING=true
WATCHER_TRADING_USER=diego
WATCHER_TRADING_EXCHANGE=binance
WATCHER_TRADING_DEFAULT_QTY=0.05
```

Assuming the repo lives at `/home/stratbot/bot`, create the services below.

### 6.1 Watcher

`/etc/systemd/system/bot-watcher.service`

```ini
[Unit]
Description=Bollinger watcher + Telegram alerts
After=network.target

[Service]
User=stratbot
WorkingDirectory=/home/stratbot/bot
EnvironmentFile=/etc/systemd/system/bot.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/stratbot/bot/.venv/bin/python watcher_alertas.py
Restart=always
RestartSec=5
StandardOutput=append:/var/log/stratbot/watcher.log
StandardError=append:/var/log/stratbot/watcher.err

[Install]
WantedBy=multi-user.target
```

### 6.2 Heartbeat

`/etc/systemd/system/bot-heartbeat.service`

```ini
[Unit]
Description=Telegram heartbeat for StratBot
After=bot-watcher.service

[Service]
User=stratbot
WorkingDirectory=/home/stratbot/bot
EnvironmentFile=/etc/systemd/system/bot.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/stratbot/bot/.venv/bin/python heartbeat_monitor.py
Restart=on-failure
RestartSec=60
StandardOutput=append:/var/log/stratbot/heartbeat.log
StandardError=append:/var/log/stratbot/heartbeat.err

[Install]
WantedBy=multi-user.target
```

> Adjust `HEARTBEAT_PROCESSES` in `.env` if you add/remove daemons.

### 6.3 Optional order listener

Use the same pattern if you rely on `backtest/realtime_state.json` fills:

`/etc/systemd/system/bot-order-listener.service`

```ini
[Unit]
Description=Backtest order fill listener
After=bot-watcher.service

[Service]
User=stratbot
WorkingDirectory=/home/stratbot/bot
EnvironmentFile=/etc/systemd/system/bot.env
ExecStart=/home/stratbot/bot/.venv/bin/python backtest/order_fill_listener.py --profile tr
Restart=always
RestartSec=10
StandardOutput=append:/var/log/stratbot/order_listener.log
StandardError=append:/var/log/stratbot/order_listener.err

[Install]
WantedBy=multi-user.target
```

### Enable everything

```bash
sudo mkdir -p /var/log/stratbot
sudo chown stratbot:stratbot /var/log/stratbot
sudo systemctl daemon-reload
sudo systemctl enable --now bot-watcher.service
sudo systemctl enable --now bot-heartbeat.service
# optional
sudo systemctl enable --now bot-order-listener.service
```

Check status/logs:

```bash
sudo systemctl status bot-watcher.service
journalctl -u bot-watcher.service -f
```

---

## 7. Updates and Maintenance

1. **Pull latest code**
   ```bash
   sudo systemctl stop bot-watcher.service bot-heartbeat.service
   cd /home/stratbot/bot
   git pull
   source .venv/bin/activate && pip install -r requirements.txt
   sudo systemctl start bot-watcher.service bot-heartbeat.service
   ```

2. **Rotate logs**  
   Add `/etc/logrotate.d/stratbot`:
   ```
   /var/log/stratbot/*.log /var/log/stratbot/*.err {
       weekly
       rotate 6
       compress
       missingok
       notifempty
       copytruncate
   }
   ```

3. **Backups**  
   - Snapshot the boot volume weekly via OCI scheduled policy.  
   - Copy `/home/stratbot/bot/backtest/backtestTR/*.csv` and dashboards to Object Storage if you need historical archives.

---

## 8. Disaster Recovery Checklist

1. Provision a new VM (Section 2).  
2. Restore repo (`git clone` + `git checkout <tag/commit>`).  
3. Copy `.env`, `trading/accounts/*.yaml`, and `/etc/systemd/system/bot.env` secrets from your password manager.  
4. Re-create `/var/log/stratbot` and re-enable services.  
5. Run `scripts/validate_accounts.py` and a 1‑week backtest to confirm data access before enabling trading.

With this setup the repo stays self-contained and the only OCI-specific pieces are the systemd units and the environment file with secrets.
