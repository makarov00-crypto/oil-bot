#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/oil-bot}"
VENV_PYTHON="${VENV_PYTHON:-$APP_DIR/.venv/bin/python}"
SCRIPT_PATH="${SCRIPT_PATH:-$APP_DIR/remote_ai_review.py}"
LOG_DIR="${LOG_DIR:-$APP_DIR/logs/automation}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/remote_ai_review.log}"
LOCK_DIR="${LOCK_DIR:-$APP_DIR/.locks}"
LOCK_PATH="${LOCK_PATH:-$LOCK_DIR/remote_ai_review.lock}"

mkdir -p "$LOG_DIR" "$LOCK_DIR"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$1" >> "$LOG_FILE"
}

if ! mkdir "$LOCK_PATH" 2>/dev/null; then
  log "Пропуск: предыдущий запуск еще идет."
  exit 0
fi

cleanup() {
  rmdir "$LOCK_PATH" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if [ ! -x "$VENV_PYTHON" ]; then
  log "Ошибка: не найден Python в $VENV_PYTHON"
  exit 1
fi

if [ ! -f "$SCRIPT_PATH" ]; then
  log "Ошибка: не найден скрипт $SCRIPT_PATH"
  exit 1
fi

if ! /usr/bin/curl -Is --max-time 15 https://api.openai.com/v1/models >/dev/null 2>&1; then
  log "Пропуск: нет доступа к OpenAI API"
  exit 0
fi

log "Старт AI-разбора на сервере."
attempt=1
while [ "$attempt" -le 2 ]; do
  if "$VENV_PYTHON" "$SCRIPT_PATH" --publish-to-server >> "$LOG_FILE" 2>&1; then
    log "AI-разбор завершен успешно."
    exit 0
  fi
  log "AI-разбор: попытка $attempt завершилась ошибкой."
  attempt=$((attempt + 1))
  if [ "$attempt" -le 2 ]; then
    /bin/sleep 20
  fi
done

log "AI-разбор завершился с ошибкой."
exit 1
