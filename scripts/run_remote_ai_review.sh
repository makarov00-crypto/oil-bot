#!/bin/zsh
set -u

ROOT_DIR="/Users/evgenymakarov/oil_bot"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
SCRIPT_PATH="$ROOT_DIR/remote_ai_review.py"
LOG_DIR="$ROOT_DIR/logs/automation"
LOG_FILE="$LOG_DIR/remote_ai_review.log"
LOCK_DIR="$ROOT_DIR/.locks"
LOCK_PATH="$LOCK_DIR/remote_ai_review.lock"

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

if [ ! -x "$PYTHON_BIN" ]; then
  log "Ошибка: не найден Python в $PYTHON_BIN"
  exit 1
fi

if [ ! -f "$SCRIPT_PATH" ]; then
  log "Ошибка: не найден скрипт $SCRIPT_PATH"
  exit 1
fi

if ! /usr/bin/curl -Is --max-time 10 https://jwizzbot.ru/api/health >/dev/null 2>&1; then
  log "Пропуск: нет доступа к jwizzbot.ru"
  exit 0
fi

if ! /usr/bin/curl -Is --max-time 10 https://api.openai.com/v1/models >/dev/null 2>&1; then
  log "Пропуск: нет доступа к OpenAI API"
  exit 0
fi

log "Старт AI-разбора."
if "$PYTHON_BIN" "$SCRIPT_PATH" --publish-to-server >> "$LOG_FILE" 2>&1; then
  log "AI-разбор завершен успешно."
  exit 0
fi

log "AI-разбор завершился с ошибкой."
exit 1
