#!/bin/zsh
set -euo pipefail

cd /Users/evgenymakarov/oil_bot

mkdir -p /Users/evgenymakarov/oil_bot/logs

export OIL_DRY_RUN=false
export OIL_ALLOW_ORDERS=true
export OIL_LOCAL_LIVE_CONFIRM=I_UNDERSTAND_LIVE_TRADING

exec /Users/evgenymakarov/oil_bot/.venv/bin/python /Users/evgenymakarov/oil_bot/bot_oil_main.py
