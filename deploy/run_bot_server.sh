#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/oil-bot}"
VENV_PYTHON="${VENV_PYTHON:-$APP_DIR/.venv/bin/python}"
LOG_DIR="${LOG_DIR:-$APP_DIR/logs}"

cd "$APP_DIR"
mkdir -p "$LOG_DIR"

exec "$VENV_PYTHON" "$APP_DIR/bot_oil_main.py"
