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
