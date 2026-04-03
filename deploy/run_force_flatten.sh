#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/oil-bot}"
VENV_BIN="${VENV_BIN:-$APP_DIR/.venv/bin}"
REASON="${REASON:-Принудительное закрытие позиций в 23:55 МСК}"

systemctl stop oil-bot || true

cd "$APP_DIR"
exec runuser -u oilbot -- "$VENV_BIN/python" "$APP_DIR/flatten_positions.py" "$REASON"
