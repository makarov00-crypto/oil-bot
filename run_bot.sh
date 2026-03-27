#!/bin/zsh
set -euo pipefail

cd /Users/evgenymakarov/oil_bot

mkdir -p /Users/evgenymakarov/oil_bot/logs

exec /Users/evgenymakarov/oil_bot/.venv/bin/python /Users/evgenymakarov/oil_bot/bot_oil_main.py
