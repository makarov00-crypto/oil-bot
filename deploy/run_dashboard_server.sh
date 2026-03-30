#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/oil-bot}"
VENV_BIN="${VENV_BIN:-$APP_DIR/.venv/bin}"
DASHBOARD_HOST="${DASHBOARD_HOST:-0.0.0.0}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8000}"

cd "$APP_DIR"

exec "$VENV_BIN/uvicorn" web_dashboard:app --host "$DASHBOARD_HOST" --port "$DASHBOARD_PORT"
