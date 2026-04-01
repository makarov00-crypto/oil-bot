# Быстрый деплой на сервер

## 1. Подготовка сервера

```bash
sudo adduser --disabled-password --gecos "" oilbot
sudo mkdir -p /opt/oil-bot
sudo chown -R oilbot:oilbot /opt/oil-bot
```

## 2. Клонирование проекта

```bash
sudo -u oilbot git clone git@github.com:makarov00-crypto/oil-bot.git /opt/oil-bot
cd /opt/oil-bot
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

После этого нужно заполнить `/opt/oil-bot/.env` боевыми значениями.

## 3. Установка systemd-сервиса

```bash
sudo cp /opt/oil-bot/deploy/oil-bot.service /etc/systemd/system/oil-bot.service
sudo cp /opt/oil-bot/deploy/oil-bot-dashboard.service /etc/systemd/system/oil-bot-dashboard.service
sudo systemctl daemon-reload
sudo systemctl enable oil-bot
sudo systemctl enable oil-bot-dashboard
sudo systemctl start oil-bot
sudo systemctl start oil-bot-dashboard
```

## 4. Проверка

```bash
sudo systemctl status oil-bot
sudo systemctl status oil-bot-dashboard
journalctl -u oil-bot -f
journalctl -u oil-bot-dashboard -f
```

## 5. Обновление

```bash
cd /opt/oil-bot
git pull
.venv/bin/pip install -r requirements.txt
sudo systemctl restart oil-bot
sudo systemctl restart oil-bot-dashboard
```

## 6. Что важно

- `.env` в git не хранить
- на сервере использовать SSH-ключ для GitHub
- если будет веб-панель, лучше поднимать её отдельным сервисом
- панель по умолчанию слушает порт `8000`

## 7. Локальная автоматизация AI-разбора на Mac

Для локального запуска AI-аналитики по расписанию используются:

- [run_remote_ai_review.sh](/Users/evgenymakarov/oil_bot/scripts/run_remote_ai_review.sh)
- [com.jwizzbot.ai-review.plist](/Users/evgenymakarov/oil_bot/deploy/mac/com.jwizzbot.ai-review.plist)

Окна запуска по Москве:

- `09:00`
- `11:00`
- `13:00`
- `15:00`
- `17:00`
- `19:00`
- `21:00`
- `22:00`

Логика:

- перед запуском проверяется доступ к `https://jwizzbot.ru/api/health`
- затем проверяется доступ к `https://api.openai.com`
- если Mac без интернета, слот просто пропускается
- повторный параллельный запуск блокируется lock-папкой
- готовый review автоматически публикуется обратно на сервер

Установка в `launchd`:

```bash
mkdir -p ~/Library/LaunchAgents
cp /Users/evgenymakarov/oil_bot/deploy/mac/com.jwizzbot.ai-review.plist ~/Library/LaunchAgents/
launchctl unload ~/Library/LaunchAgents/com.jwizzbot.ai-review.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.jwizzbot.ai-review.plist
```

Ручной запуск:

```bash
/bin/zsh /Users/evgenymakarov/oil_bot/scripts/run_remote_ai_review.sh
```

Логи:

- `/Users/evgenymakarov/oil_bot/logs/automation/remote_ai_review.log`
- `/Users/evgenymakarov/oil_bot/logs/automation/launchd.out.log`
- `/Users/evgenymakarov/oil_bot/logs/automation/launchd.err.log`
