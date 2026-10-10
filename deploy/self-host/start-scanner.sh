#!/bin/sh
set -eu

# Streamlit reads secrets.toml; it does not automatically map environment
# variables into st.secrets. Generate the file at container startup so secrets
# are supplied at runtime and never baked into the image.
python - <<'PY'
import json
import os
from pathlib import Path

keys = (
    "ALPACA_API_KEY",
    "ALPACA_SECRET_KEY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "FMP_API_KEY",
    "FINNHUB_API_KEY",
    "SUPABASE_URL",
    "SUPABASE_ANON_KEY",
    "ADMIN_TOKEN",
    "ADMIN_TOKENS",
)
target = Path("/app/.streamlit/secrets.toml")
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(
    "\n".join(f"{key} = {json.dumps(os.environ.get(key, ''))}" for key in keys) + "\n",
    encoding="utf-8",
)
target.chmod(0o600)
PY

exec streamlit run app.py --server.address=0.0.0.0 --server.port=8501 --server.headless=true --browser.gatherUsageStats=false
