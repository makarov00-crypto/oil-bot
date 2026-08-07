#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/oil-bot}"
VENV_PYTHON="${VENV_PYTHON:-$APP_DIR/.venv/bin/python}"
LOG_DIR="${LOG_DIR:-$APP_DIR/logs}"

cd "$APP_DIR"
mkdir -p "$LOG_DIR"

# Requests otherwise uses certifi's static bundle, which may miss Russian root CAs.
if [[ -f /etc/ssl/certs/ca-certificates.crt ]]; then
  export REQUESTS_CA_BUNDLE="${REQUESTS_CA_BUNDLE:-/etc/ssl/certs/ca-certificates.crt}"
fi

exec "$VENV_PYTHON" "$APP_DIR/bot_oil_main.py"
