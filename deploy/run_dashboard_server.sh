#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/oil-bot}"
VENV_BIN="${VENV_BIN:-$APP_DIR/.venv/bin}"
DASHBOARD_HOST="${DASHBOARD_HOST:-0.0.0.0}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8000}"

cd "$APP_DIR"

# Keep dashboard REST requests on the host trust store as well.
if [[ -f /etc/ssl/certs/ca-certificates.crt ]]; then
  export REQUESTS_CA_BUNDLE="${REQUESTS_CA_BUNDLE:-/etc/ssl/certs/ca-certificates.crt}"
fi

exec "$VENV_BIN/uvicorn" web_dashboard:app --host "$DASHBOARD_HOST" --port "$DASHBOARD_PORT"
