from dotenv import load_dotenv
import os
import requests
import json

load_dotenv(".env")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise SystemExit("TELEGRAM_BOT_TOKEN vacío; configurá .env y reintentá.")

resp = requests.get(f"https://api.telegram.org/bot{TOKEN}/getMe", timeout=10)
print(json.dumps(resp.json(), indent=2, ensure_ascii=False))
