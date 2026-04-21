from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from dotenv import load_dotenv
from custom_instruments import (
    list_custom_instruments,
    merge_with_custom_symbols,
    upsert_custom_instrument,
    validate_custom_symbol,
)
from instrument_groups import DEFAULT_SYMBOLS, GROUP_BY_SYMBOL, get_instrument_group
from strategy_registry import get_primary_strategies, get_secondary_strategies
from daily_ai_review import (
    FOLLOWUP_SYSTEM_INSTRUCTIONS,
    DEFAULT_MODEL as DEFAULT_AI_MODEL,
    build_review_prompt,
    request_openai_text,
)
from trade_storage import load_trade_rows as load_trade_rows_from_storage


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

STATE_DIR = BASE_DIR / "bot_state"
LOG_DIR = BASE_DIR / "logs"
TRADE_JOURNAL_PATH = LOG_DIR / "trade_journal.jsonl"
TRADE_DB_PATH = STATE_DIR / "trade_analytics.sqlite3"
PORTFOLIO_SNAPSHOT_PATH = STATE_DIR / "_portfolio_snapshot.json"
ACCOUNTING_HISTORY_PATH = STATE_DIR / "_accounting_history.json"
RUNTIME_STATUS_PATH = STATE_DIR / "_runtime_status.json"
NEWS_SNAPSHOT_PATH = STATE_DIR / "_news_snapshot.json"
AI_REVIEW_DIR = LOG_DIR / "ai_reviews"
AI_REVIEW_SCRIPT_PATH = BASE_DIR / "deploy" / "run_remote_ai_review_server.sh"
AI_REVIEW_LOG_PATH = LOG_DIR / "automation" / "remote_ai_review.log"
AI_REVIEW_LOCK_PATH = BASE_DIR / ".locks" / "remote_ai_review.lock"
TRADE_RECOVERY_SCRIPT_PATH = BASE_DIR / "scripts" / "recover_trade_operations.py"
TRADE_RECOVERY_LOCK_PATH = BASE_DIR / ".locks" / "trade_operations_recovery.lock"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
STATE_STALE_MINUTES = 20
RUNTIME_STALE_MINUTES = 10


app = FastAPI(title="Oil Bot Dashboard", docs_url=None, redoc_url=None)

INSTRUMENT_DISPLAY_NAMES: dict[str, str] = {
    "BRK6": "BR-5.26 Нефть Brent",
    "USDRUBF": "USDRUBF Доллар - Рубль",
    "CNYRUBF": "CNYRUBF Юань - Рубль",
    "IMOEXF": "IMOEXF Индекс МосБиржи",
    "SRM6": "SBRF-6.26 Сбер Банк",
    "GNM6": "GOLDM-6.26 Золото (мини)",
    "NGJ6": "NG-4.26 Природный газ",
    "RBM6": "RGBI-6.26 Индекс гос. облигаций",
    "UCM6": "UCNY-6.26 Доллар США - Юань",
    "VBM6": "VTBR-6.26 Банк ВТБ",
}


STRATEGY_DOCS: dict[str, dict[str, str]] = {
    "momentum_breakout": {
        "title": "Импульсный пробой",
        "summary": "Вход по импульсному продолжению уже начавшегося движения, когда цена уверенно выталкивается из диапазона вверх или вниз.",
        "when": "Лучше всего работает в сильном трендовом дне с подтверждением по старшему таймфрейму, импульсу и объёму.",
    },
    "trend_pullback": {
        "title": "Откат по тренду",
        "summary": "Вход по откату к тренду: бот ждёт возврат цены к зоне EMA/баланса и пытается зайти по направлению основного движения.",
        "when": "Подходит для спокойного направленного тренда, когда рынок делает технические откаты, а не полноценный разворот.",
    },
    "trend_rollover": {
        "title": "Перезапуск тренда",
        "summary": "Ловит перезапуск тренда после локальной паузы, когда рынок подтверждает rollover и снова пытается развить движение.",
        "when": "Используется там, где инструмент любит сначала притормозить, а потом ещё раз ускориться по тренду.",
    },
    "range_break_continuation": {
        "title": "Продолжение пробоя диапазона",
        "summary": "Вход после подтверждённого пробоя диапазона с расчётом на продолжение движения за пределами локального коридора.",
        "when": "Полезна для индексов и акций, когда рынок долго стоит в диапазоне, а потом начинает направленный выход.",
    },
    "failed_breakout": {
        "title": "Ложный пробой",
        "summary": "Контртрендовая идея на ложном пробое: рынок не удержал выход из диапазона и быстро вернулся обратно.",
        "when": "Актуальна только там, где инструмент часто даёт ложные выносы и быстрые возвраты в коридор.",
    },
    "opening_range_breakout": {
        "title": "Пробой утреннего диапазона",
        "summary": "Вход по пробою утреннего диапазона, чаще всего по валютным фьючерсам, когда рынок выбирает направление сессии.",
        "when": "Лучше всего работает в начале дня, пока импульс открытия ещё не выдохся.",
    },
    "williams": {
        "title": "Подтверждение по Williams %R",
        "summary": "Вторичный фильтр для валютных инструментов на базе Williams %R, который уточняет качество входа и степень перегретости движения.",
        "when": "Используется как дополнительное подтверждение, а не как самостоятельная основная стратегия.",
    },
}


def build_site_nav(active: str) -> str:
    links = [
        ("/", "Дашборд", "dashboard"),
        ("/docs", "Документация", "docs"),
        ("/contracts", "Параметры контрактов", "contracts"),
    ]
    items: list[str] = []
    for href, label, key in links:
        cls = "site-nav__link is-active" if key == active else "site-nav__link"
        items.append(f'<a href="{href}" class="{cls}">{label}</a>')
    return f"""
  <header class="site-header">
    <div class="site-header__inner">
      <div class="site-brand">
        <div class="site-brand__eyebrow">JWizzBot</div>
        <div class="site-brand__title">Центр управления Oil Bot</div>
      </div>
      <nav class="site-nav">
        {''.join(items)}
      </nav>
    </div>
  </header>
"""


def load_base_symbols_from_env() -> list[str]:
    raw = os.getenv("T_INVEST_SYMBOLS", DEFAULT_SYMBOLS)
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def build_manual_instruments_payload() -> dict:
    base_symbols = load_base_symbols_from_env()
    configured_symbols = merge_with_custom_symbols(base_symbols)
    templates: list[dict[str, str]] = []
    seen: set[str] = set()
    for symbol in configured_symbols:
        normalized = str(symbol or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        templates.append(
            {
                "symbol": normalized,
                "display_name": INSTRUMENT_DISPLAY_NAMES.get(normalized, normalized),
                "primary_strategies": get_primary_strategies(normalized),
                "secondary_strategies": get_secondary_strategies(normalized),
            }
        )
    return {
        "templates": templates,
        "custom_instruments": list_custom_instruments(),
        "watchlist_refresh_seconds": 300,
    }


def build_instrument_catalog(portfolio: dict | None = None, trades: list[dict] | None = None) -> dict[str, str]:
    catalog = dict(INSTRUMENT_DISPLAY_NAMES)
    for item in (portfolio or {}).get("broker_open_positions", []) or []:
        symbol = str(item.get("symbol") or "").strip().upper()
        display_name = str(item.get("display_name") or "").strip()
        if symbol and display_name:
            catalog[symbol] = display_name
    for row in trades or []:
        symbol = str(row.get("symbol") or "").strip().upper()
        display_name = str(row.get("display_name") or "").strip()
        if symbol and display_name:
            catalog[symbol] = display_name
    return catalog


def validate_futures_ticker_exists(symbol: str) -> dict[str, str]:
    from tinkoff.invest import Client
    from bot_oil_main import load_config

    config = load_config()
    if not config.token:
        raise RuntimeError("Не задан T_INVEST_TOKEN для проверки тикера у брокера.")
    with Client(config.token, target=config.target) as client:
        futures = client.instruments.futures().instruments
    for item in futures:
        ticker = str(getattr(item, "ticker", "") or "").strip().upper()
        if ticker != symbol:
            continue
        return {
            "symbol": symbol,
            "display_name": str(getattr(item, "name", "") or symbol).strip() or symbol,
        }
    raise RuntimeError("Брокер не знает такой фьючерсный тикер.")


def build_strategy_docs_rows() -> tuple[str, str]:
    cards: list[str] = []
    rows: list[str] = []
    for key, payload in STRATEGY_DOCS.items():
        cards.append(
            f"""
            <article class="doc-card">
              <div class="doc-card__eyebrow mono">{key}</div>
              <h3>{payload['title']}</h3>
              <p>{payload['summary']}</p>
              <p class="muted"><strong>Когда используется:</strong> {payload['when']}</p>
            </article>
            """
        )

    for symbol in sorted(GROUP_BY_SYMBOL):
        group = get_instrument_group(symbol)
        primary = ", ".join(get_primary_strategies(symbol))
        secondary = ", ".join(get_secondary_strategies(symbol)) or "—"
        rows.append(
            f"""
            <tr>
              <td class="mono">{symbol}</td>
              <td>{group.name}</td>
              <td>{group.description}</td>
              <td>{primary}</td>
              <td>{secondary}</td>
            </tr>
            """
        )
    return "".join(cards), "".join(rows)


def build_docs_html() -> str:
    strategy_cards, strategy_rows = build_strategy_docs_rows()
    return f"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex, nofollow, noarchive, nosnippet" />
  <title>Документация Oil Bot</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #030711;
      --bg2: #091120;
      --panel: rgba(8, 14, 28, 0.88);
      --panel-strong: rgba(10, 18, 34, 0.98);
      --ink: #ebf4ff;
      --muted: #7f95b3;
      --line: rgba(102, 174, 255, 0.18);
      --accent: #43c5ff;
      --accent2: #7d8cff;
      --accent3: #14f1ff;
      --glow: rgba(67, 197, 255, 0.22);
      --shadow: rgba(0, 0, 0, 0.45);
    }}
    body {{
      margin: 0;
      font-family: "Manrope", "Segoe UI", Arial, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(67, 197, 255, 0.18), transparent 24%),
        radial-gradient(circle at top right, rgba(125, 140, 255, 0.16), transparent 20%),
        radial-gradient(circle at 50% 0%, rgba(20, 241, 255, 0.08), transparent 28%),
        linear-gradient(180deg, var(--bg2) 0%, var(--bg) 100%);
      color: var(--ink);
      min-height: 100vh;
    }}
    .site-header {{
      position: sticky;
      top: 0;
      z-index: 20;
      backdrop-filter: blur(18px);
      background: rgba(4, 9, 18, 0.78);
      border-bottom: 1px solid rgba(102, 174, 255, 0.12);
    }}
    .site-header__inner {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 18px 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
    }}
    .site-brand__eyebrow {{
      color: var(--accent3);
      font: 700 12px/1 "JetBrains Mono", monospace;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      margin-bottom: 6px;
    }}
    .site-brand__title {{
      font: 700 18px/1.1 "Sora", sans-serif;
      text-shadow: 0 0 22px var(--glow);
    }}
    .site-nav {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .site-nav__link {{
      color: #b8cae3;
      text-decoration: none;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(102, 174, 255, 0.18);
      background: rgba(67, 197, 255, 0.05);
      font-weight: 600;
    }}
    .site-nav__link.is-active {{
      color: white;
      background: linear-gradient(135deg, rgba(67, 197, 255, 0.22), rgba(125, 140, 255, 0.24));
      border-color: rgba(102, 174, 255, 0.32);
      box-shadow: 0 0 18px rgba(67, 197, 255, 0.12);
    }}
    .wrap {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 28px;
    }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h1 {{
      font-family: "Sora", sans-serif;
      font-size: 32px;
      line-height: 1.08;
      text-shadow: 0 0 28px var(--glow);
    }}
    h2 {{
      font-family: "Sora", sans-serif;
      font-size: 24px;
      line-height: 1.15;
    }}
    .muted {{ color: var(--muted); }}
    .panel {{
      background: linear-gradient(180deg, var(--panel-strong) 0%, var(--panel) 100%);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 22px 24px;
      box-shadow:
        0 18px 50px var(--shadow),
        inset 0 1px 0 rgba(255, 255, 255, 0.03),
        0 0 0 1px rgba(67, 197, 255, 0.03);
      margin-bottom: 18px;
    }}
    .hero p {{
      max-width: 880px;
      line-height: 1.6;
      color: #d5e1f0;
    }}
    .overview-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
    }}
    .overview-card {{
      background: rgba(7, 13, 26, 0.72);
      border: 1px solid rgba(102, 174, 255, 0.12);
      border-radius: 18px;
      padding: 18px;
    }}
    .overview-card h3 {{
      margin-bottom: 10px;
    }}
    .overview-card p {{
      margin: 0;
      line-height: 1.6;
      color: #d5e1f0;
    }}
    .steps {{
      display: grid;
      gap: 12px;
    }}
    .step {{
      display: grid;
      grid-template-columns: 42px 1fr;
      gap: 14px;
      align-items: start;
      background: rgba(7, 13, 26, 0.72);
      border: 1px solid rgba(102, 174, 255, 0.12);
      border-radius: 18px;
      padding: 16px 18px;
    }}
    .step__num {{
      width: 42px;
      height: 42px;
      border-radius: 999px;
      display: grid;
      place-items: center;
      color: white;
      font: 700 14px/1 "JetBrains Mono", monospace;
      background: linear-gradient(135deg, rgba(67, 197, 255, 0.24), rgba(125, 140, 255, 0.24));
      border: 1px solid rgba(102, 174, 255, 0.2);
      box-shadow: 0 0 18px rgba(67, 197, 255, 0.12);
    }}
    .step h3 {{
      margin-bottom: 8px;
    }}
    .step p {{
      margin: 0;
      line-height: 1.6;
      color: #d5e1f0;
    }}
    .doc-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
    }}
    .doc-card {{
      background: rgba(7, 13, 26, 0.72);
      border: 1px solid rgba(102, 174, 255, 0.12);
      border-radius: 18px;
      padding: 18px;
    }}
    .doc-card__eyebrow {{
      color: var(--accent);
      margin-bottom: 8px;
    }}
    .mono {{
      font-family: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      color: #b8cae3;
      font-weight: 600;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      font-size: 12px;
    }}
    .table-scroll {{
      overflow: auto;
      border-radius: 14px;
    }}
    @media (max-width: 860px) {{
      .site-header__inner {{
        align-items: flex-start;
        flex-direction: column;
      }}
      .wrap {{
        padding: 20px 16px 28px;
      }}
    }}
  </style>
</head>
<body>
  {build_site_nav("docs")}
  <main class="wrap">
    <section class="panel hero">
      <h1>Документация стратегий</h1>
      <p>
        Здесь собрана живая карта торговых стратегий бота: что делает каждая логика входа,
        в каких рыночных условиях она полезна и где именно она используется в текущем реестре инструментов.
        Это отражение реальной конфигурации, из которой бот сейчас принимает решения.
      </p>
    </section>
    <section class="panel">
      <h2>Как бот принимает решение</h2>
      <div class="overview-grid">
        <article class="overview-card">
          <h3>Контекст рынка</h3>
          <p>
            Сначала бот оценивает старший таймфрейм, новости, текущую торговую сессию и жив ли вообще поток данных по инструменту.
            Если рынок закрыт или данные устарели, новый вход не рассматривается.
          </p>
        </article>
        <article class="overview-card">
          <h3>Поиск подходящей логики</h3>
          <p>
            Для каждого инструмента есть свой набор основных и вторичных стратегий. Бот идёт по ним по порядку и ищет первую
            логику, которая реально подтверждается рынком, а не просто выглядит красиво на одном индикаторе.
          </p>
        </article>
        <article class="overview-card">
          <h3>Проверка качества входа</h3>
          <p>
            Перед входом дополнительно проверяются импульс, объём, положение цены относительно средних, риск, доступное ГО
            и ограничения по размеру позиции. Если один из этих блоков не проходит, бот остаётся в ожидании.
          </p>
        </article>
      </div>
    </section>
    <section class="panel">
      <h2>Жизненный цикл сделки</h2>
      <div class="steps">
        <article class="step">
          <div class="step__num">01</div>
          <div>
            <h3>Сигнал найден</h3>
            <p>
              Бот видит совпадение рыночного контекста и условий конкретной стратегии. На этом этапе в таблице сигналов
              появляется обоснование, почему именно сейчас инструмент интересен.
            </p>
          </div>
        </article>
        <article class="step">
          <div class="step__num">02</div>
          <div>
            <h3>Вход подтверждён</h3>
            <p>
              После отправки заявки бот подтверждает позицию по брокерскому портфелю и операциям. Это защищает систему
              от случаев, когда статус заявки у брокера приходит неидеально или с задержкой.
            </p>
          </div>
        </article>
        <article class="step">
          <div class="step__num">03</div>
          <div>
            <h3>Сделка живёт под контролем</h3>
            <p>
              Пока позиция открыта, бот следит за текущей вариационной маржой, сменой сигнала, подтверждением по MACD,
              EMA, RSI и за тем, не появился ли повод защищать прибыль или ограничить убыток.
            </p>
          </div>
        </article>
        <article class="step">
          <div class="step__num">04</div>
          <div>
            <h3>Выход и итог</h3>
            <p>
              После закрытия в журнале фиксируется торговая причина выхода, а в портфеле отдельно видны чистый результат,
              комиссии и общая вариационная маржа. Это позволяет честно сравнивать дашборд с брокерским терминалом.
            </p>
          </div>
        </article>
      </div>
    </section>
    <section class="panel">
      <h2>Как бот ограничивает риск</h2>
      <div class="overview-grid">
        <article class="overview-card">
          <h3>Размер позиции</h3>
          <p>
            Бот не открывает позицию просто потому, что увидел красивый сигнал. Сначала он проверяет доступный капитал,
            гарантийное обеспечение, внутренние лимиты на инструмент и допустимый размер риска на одну сделку.
          </p>
        </article>
        <article class="overview-card">
          <h3>Защита от переторговки</h3>
          <p>
            После убыточных или слишком быстрых выходов бот может включать паузу перед повторным входом. Это нужно,
            чтобы не открываться снова в ту же сторону на шумном или рваном рынке.
          </p>
        </article>
        <article class="overview-card">
          <h3>Проверка перед входом</h3>
          <p>
            Даже если стратегия формально разрешает сделку, бот не будет входить, если не хватает средств, объёма,
            подтверждения по старшему таймфрейму или есть жёсткое ограничение по состоянию счёта.
          </p>
        </article>
      </div>
    </section>
    <section class="panel">
      <h2>Как бот выходит из сделки</h2>
      <div class="overview-grid">
        <article class="overview-card">
          <h3>Технический разворот</h3>
          <p>
            Чаще всего выход происходит тогда, когда рынок перестаёт подтверждать исходную идею: ослабевает импульс,
            MACD меняет направление, цена теряет ключевую среднюю или структура движения ломается.
          </p>
        </article>
        <article class="overview-card">
          <h3>Защита прибыли</h3>
          <p>
            Если сделка уже в плюсе, бот может закрыть её не только по жёсткому стопу, но и по признакам выдыхания
            движения. Это позволяет не ждать полного разворота там, где рынок явно теряет силу.
          </p>
        </article>
        <article class="overview-card">
          <h3>Выход глазами пользователя</h3>
          <p>
            В ленте событий и в обзоре сделок сохраняется именно торговая причина выхода, чтобы было понятно не только
            то, что позиция закрылась, но и почему система сочла это правильным решением.
          </p>
        </article>
      </div>
    </section>
    <section class="panel">
      <h2>Суть стратегий</h2>
      <div class="doc-grid">
        {strategy_cards}
      </div>
    </section>
    <section class="panel">
      <h2>Где какие стратегии используются</h2>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Инструмент</th>
              <th>Группа</th>
              <th>Описание группы</th>
              <th>Основные стратегии</th>
              <th>Вторичные стратегии</th>
            </tr>
          </thead>
          <tbody>
            {strategy_rows}
          </tbody>
        </table>
      </div>
    </section>
    <section class="panel">
      <h2>Как читать дашборд</h2>
      <div class="steps">
        <article class="step">
          <div class="step__num">01</div>
          <div>
            <h3>Портфель</h3>
            <p>
              Здесь видно общее состояние счёта и результат самого бота. Особенно важны чистый результат по закрытым
              сделкам, комиссии по счёту, общая вариационная маржа и итог по боту за выбранный день.
            </p>
          </div>
        </article>
        <article class="step">
          <div class="step__num">02</div>
          <div>
            <h3>Позиции и сигналы</h3>
            <p>
              Блок позиций показывает только то, что реально живёт у брокера сейчас. Блок сигналов отвечает на другой
              вопрос: какие идеи бот видит по инструментам и почему он либо ждёт, либо готов действовать.
            </p>
          </div>
        </article>
        <article class="step">
          <div class="step__num">03</div>
          <div>
            <h3>Лента событий и обзор сделок</h3>
            <p>
              Лента событий нужна для живого наблюдения за входами и выходами по мере их появления. Обзор сделок — это
              уже собранная картина дня: сколько закрыто, какие инструменты были лучшими и худшими и каков общий результат.
            </p>
          </div>
        </article>
        <article class="step">
          <div class="step__num">04</div>
          <div>
            <h3>Новости и AI-разбор</h3>
            <p>
              Новости показывают внешний контекст по инструментам, а AI-разбор собирает сводку по рыночной картине дня.
              Это не замена журналу сделок, а дополнительный слой анализа и обзора рынка.
            </p>
          </div>
        </article>
      </div>
    </section>
  </main>
</body>
</html>
    """


def quotation_like_to_float(value: object) -> float | None:
    if value is None:
        return None
    units = getattr(value, "units", None)
    nano = getattr(value, "nano", None)
    if units is None or nano is None:
        return None
    return float(units) + float(nano) / 1_000_000_000


def load_contracts_payload() -> dict:
    generated_at = datetime.now(timezone.utc)
    payload = {
        "margin": {},
        "contracts": [],
        "generated_at": generated_at.isoformat(),
        "generated_at_moscow": generated_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
        "error": "",
    }
    try:
        from tinkoff.invest import Client
        from tinkoff.invest.schemas import GetMaxLotsRequest
        from bot_oil_main import load_config, quotation_to_float, resolve_instruments

        config = load_config()
        with Client(config.token, target=config.target) as client:
            margin = client.users.get_margin_attributes(account_id=config.account_id)
            liquid_portfolio = quotation_like_to_float(getattr(margin, "liquid_portfolio", None)) or 0.0
            starting_margin = quotation_like_to_float(getattr(margin, "starting_margin", None)) or 0.0
            minimal_margin = quotation_like_to_float(getattr(margin, "minimal_margin", None)) or 0.0
            funds_sufficiency_level = quotation_like_to_float(getattr(margin, "funds_sufficiency_level", None))
            amount_of_missing_funds = quotation_like_to_float(getattr(margin, "amount_of_missing_funds", None)) or 0.0
            margin_headroom = max(0.0, -amount_of_missing_funds)
            payload["margin"] = {
                "liquid_portfolio_rub": round(liquid_portfolio, 2),
                "starting_margin_rub": round(starting_margin, 2),
                "minimal_margin_rub": round(minimal_margin, 2),
                "funds_sufficiency_level": round(funds_sufficiency_level, 2) if funds_sufficiency_level is not None else None,
                "amount_of_missing_funds_rub": round(amount_of_missing_funds, 2),
                "margin_headroom_rub": round(margin_headroom, 2),
            }

            instruments = resolve_instruments(client, config)
            last_prices = client.market_data.get_last_prices(figi=[item.figi for item in instruments]).last_prices
            price_map = {
                item.figi: quotation_to_float(getattr(item, "price", None))
                for item in last_prices
            }

            rows: list[dict] = []
            for instrument in instruments:
                current_price = price_map.get(instrument.figi) or 0.0
                step_price = instrument.min_price_increment or 0.0
                step_money = instrument.min_price_increment_amount or 0.0
                multiplier = (step_money / step_price) if step_price > 0 else 0.0
                notional_per_lot = current_price * multiplier if current_price > 0 and multiplier > 0 else 0.0
                long_margin = instrument.initial_margin_on_buy or 0.0
                short_margin = instrument.initial_margin_on_sell or 0.0
                leverage_long = (notional_per_lot / long_margin) if long_margin > 0 else None
                leverage_short = (notional_per_lot / short_margin) if short_margin > 0 else None
                approx_long_lots = int(margin_headroom // long_margin) if long_margin > 0 else 0
                approx_short_lots = int(margin_headroom // short_margin) if short_margin > 0 else 0
                broker_buy_max = 0
                broker_sell_max = 0
                try:
                    limits = client.orders.get_max_lots(
                        GetMaxLotsRequest(account_id=config.account_id, instrument_id=instrument.figi)
                    )
                    broker_buy_max = int(getattr(getattr(limits, "buy_limits", None), "buy_max_market_lots", 0) or 0)
                    broker_sell_max = int(getattr(getattr(limits, "sell_limits", None), "sell_max_lots", 0) or 0)
                except Exception:
                    broker_buy_max = 0
                    broker_sell_max = 0

                rows.append(
                    {
                        "symbol": instrument.symbol,
                        "display_name": instrument.display_name,
                        "lot": instrument.lot,
                        "current_price": round(current_price, 4) if current_price else 0.0,
                        "multiplier": round(multiplier, 4) if multiplier else 0.0,
                        "notional_per_lot_rub": round(notional_per_lot, 2) if notional_per_lot else 0.0,
                        "initial_margin_on_buy_rub": round(long_margin, 2) if long_margin else 0.0,
                        "initial_margin_on_sell_rub": round(short_margin, 2) if short_margin else 0.0,
                        "leverage_long": round(leverage_long, 2) if leverage_long is not None else None,
                        "leverage_short": round(leverage_short, 2) if leverage_short is not None else None,
                        "approx_long_lots": approx_long_lots,
                        "approx_short_lots": approx_short_lots,
                        "broker_buy_max_lots": broker_buy_max,
                        "broker_sell_max_lots": broker_sell_max,
                    }
                )
            payload["contracts"] = rows
    except Exception as error:
        payload["error"] = str(error)
    return payload


def build_contracts_html() -> str:
    return f"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex, nofollow, noarchive, nosnippet" />
  <title>Параметры контрактов Oil Bot</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #030711;
      --bg2: #091120;
      --panel: rgba(8, 14, 28, 0.88);
      --panel-strong: rgba(10, 18, 34, 0.98);
      --ink: #ebf4ff;
      --muted: #7f95b3;
      --line: rgba(102, 174, 255, 0.18);
      --accent: #43c5ff;
      --accent2: #7d8cff;
      --accent3: #14f1ff;
      --glow: rgba(67, 197, 255, 0.22);
      --shadow: rgba(0, 0, 0, 0.45);
    }}
    body {{
      margin: 0;
      font-family: "Manrope", "Segoe UI", Arial, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(67, 197, 255, 0.18), transparent 24%),
        radial-gradient(circle at top right, rgba(125, 140, 255, 0.16), transparent 20%),
        radial-gradient(circle at 50% 0%, rgba(20, 241, 255, 0.08), transparent 28%),
        linear-gradient(180deg, var(--bg2) 0%, var(--bg) 100%);
      color: var(--ink);
      min-height: 100vh;
    }}
    .site-header {{
      position: sticky;
      top: 0;
      z-index: 20;
      backdrop-filter: blur(18px);
      background: rgba(4, 9, 18, 0.78);
      border-bottom: 1px solid rgba(102, 174, 255, 0.12);
    }}
    .site-header__inner {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 18px 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
    }}
    .site-brand__eyebrow {{
      color: var(--accent3);
      font: 700 12px/1 "JetBrains Mono", monospace;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      margin-bottom: 6px;
    }}
    .site-brand__title {{
      font: 700 18px/1.1 "Sora", sans-serif;
      text-shadow: 0 0 22px var(--glow);
    }}
    .site-nav {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .site-nav__link {{
      color: #b8cae3;
      text-decoration: none;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(102, 174, 255, 0.18);
      background: rgba(67, 197, 255, 0.05);
      font-weight: 600;
    }}
    .site-nav__link.is-active {{
      color: white;
      background: linear-gradient(135deg, rgba(67, 197, 255, 0.22), rgba(125, 140, 255, 0.24));
      border-color: rgba(102, 174, 255, 0.32);
      box-shadow: 0 0 18px rgba(67, 197, 255, 0.12);
    }}
    .wrap {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 28px;
    }}
    .panel {{
      background: linear-gradient(180deg, var(--panel-strong) 0%, var(--panel) 100%);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 20px 22px;
      box-shadow: 0 18px 50px var(--shadow);
      margin-bottom: 18px;
    }}
    h1, h2 {{ margin: 0 0 12px; }}
    h1 {{
      font-family: "Sora", sans-serif;
      font-size: 32px;
      line-height: 1.08;
      text-shadow: 0 0 28px var(--glow);
    }}
    h2 {{
      font-family: "Sora", sans-serif;
      font-size: 24px;
      line-height: 1.15;
    }}
    .muted {{ color: var(--muted); }}
    .mono {{ font-family: "JetBrains Mono", "SFMono-Regular", Consolas, monospace; }}
    .grid {{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }}
    .metric-card {{
      background: rgba(7, 13, 26, 0.72);
      border: 1px solid rgba(102, 174, 255, 0.12);
      border-radius: 18px;
      padding: 18px;
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .metric-value {{
      font: 700 28px/1.15 "Sora", sans-serif;
      overflow-wrap: anywhere;
      text-shadow: 0 0 20px var(--glow);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      color: #b8cae3;
      font-weight: 600;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      font-size: 12px;
    }}
    .table-scroll {{
      overflow: auto;
      border-radius: 14px;
    }}
    .hint {{
      line-height: 1.6;
      color: #d5e1f0;
      max-width: 980px;
    }}
    .error {{
      color: #ff8ea1;
      white-space: pre-wrap;
    }}
    @media (max-width: 860px) {{
      .site-header__inner {{
        align-items: flex-start;
        flex-direction: column;
      }}
      .wrap {{
        padding: 20px 16px 28px;
      }}
    }}
  </style>
</head>
<body>
  {build_site_nav("contracts")}
  <main class="wrap">
    <section class="panel">
      <h1>Параметры контрактов</h1>
      <p class="hint">
        Это отдельная справочная страница по маржинальным параметрам счёта и контрактов. Здесь видно,
        сколько стоит один лот, какое по нему гарантийное обеспечение, какое фактическое плечо получается
        по текущей цене и сколько лотов примерно помещается в текущий маржинальный запас.
      </p>
      <p class="muted" id="contractsGeneratedAt">Загрузка данных…</p>
    </section>
    <section class="panel">
      <h2>Маржинальные параметры счёта</h2>
      <div class="grid" id="marginGrid"></div>
      <div id="contractsError" class="error" style="display:none; margin-top:14px;"></div>
    </section>
    <section class="panel">
      <h2>Текущие параметры по инструментам</h2>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Инструмент</th>
              <th>Текущая цена</th>
              <th>Стоимость 1 лота</th>
              <th>ГО LONG</th>
              <th>ГО SHORT</th>
              <th>Плечо LONG</th>
              <th>Плечо SHORT</th>
              <th>Влезает LONG</th>
              <th>Влезает SHORT</th>
              <th>Лимит брокера LONG</th>
              <th>Лимит брокера SHORT</th>
            </tr>
          </thead>
          <tbody id="contractsBody">
            <tr><td colspan="11" class="muted">Загрузка…</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const formatRub = (value) => {{
      const num = Number(value || 0);
      return new Intl.NumberFormat('ru-RU', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }}).format(num) + ' RUB';
    }};
    const formatNum = (value, digits = 2) => {{
      if (value === null || value === undefined || value === '') return '—';
      return new Intl.NumberFormat('ru-RU', {{ minimumFractionDigits: digits, maximumFractionDigits: digits }}).format(Number(value));
    }};
    const metricCard = (label, value) => `
      <article class="metric-card">
        <div class="metric-label">${{label}}</div>
        <div class="metric-value">${{value}}</div>
      </article>
    `;

    async function loadContracts() {{
      const resp = await fetch('/api/contracts', {{ cache: 'no-store' }});
      if (!resp.ok) throw new Error('Не удалось загрузить параметры контрактов');
      return await resp.json();
    }}

    function renderContracts(data) {{
      document.getElementById('contractsGeneratedAt').textContent =
        `Срез построен: ${{data.generated_at_moscow || '-'}}`;
      const margin = data.margin || {{}};
      document.getElementById('marginGrid').innerHTML = [
        metricCard('Ликвидный портфель', formatRub(margin.liquid_portfolio_rub || 0)),
        metricCard('Начальная маржа', formatRub(margin.starting_margin_rub || 0)),
        metricCard('Минимальная маржа', formatRub(margin.minimal_margin_rub || 0)),
        metricCard('Уровень достаточности', formatNum(margin.funds_sufficiency_level, 2)),
        metricCard('Недостающие средства', formatRub(margin.amount_of_missing_funds_rub || 0)),
        metricCard('Свободный маржинальный запас', formatRub(margin.margin_headroom_rub || 0)),
      ].join('');

      const body = document.getElementById('contractsBody');
      const rows = (data.contracts || []).map((row) => `
        <tr>
          <td><div class="mono">${{row.symbol}}</div><div class="muted">${{row.display_name || ''}}</div></td>
          <td class="mono">${{formatNum(row.current_price, 4)}}</td>
          <td class="mono">${{formatRub(row.notional_per_lot_rub || 0)}}</td>
          <td class="mono">${{formatRub(row.initial_margin_on_buy_rub || 0)}}</td>
          <td class="mono">${{formatRub(row.initial_margin_on_sell_rub || 0)}}</td>
          <td class="mono">${{row.leverage_long ? 'x' + formatNum(row.leverage_long, 2) : '—'}}</td>
          <td class="mono">${{row.leverage_short ? 'x' + formatNum(row.leverage_short, 2) : '—'}}</td>
          <td class="mono">${{row.approx_long_lots ?? '—'}}</td>
          <td class="mono">${{row.approx_short_lots ?? '—'}}</td>
          <td class="mono">${{row.broker_buy_max_lots || '0'}}</td>
          <td class="mono">${{row.broker_sell_max_lots || '0'}}</td>
        </tr>
      `);
      body.innerHTML = rows.length ? rows.join('') : '<tr><td colspan="11" class="muted">Данные недоступны.</td></tr>';

      const errorNode = document.getElementById('contractsError');
      if (data.error) {{
        errorNode.style.display = 'block';
        errorNode.textContent = `Техническая ошибка получения данных: ${{data.error}}`;
      }} else {{
        errorNode.style.display = 'none';
        errorNode.textContent = '';
      }}
    }}

    loadContracts().then(renderContracts).catch((error) => {{
      document.getElementById('contractsGeneratedAt').textContent = 'Не удалось загрузить данные.';
      document.getElementById('contractsError').style.display = 'block';
      document.getElementById('contractsError').textContent = String(error);
      document.getElementById('contractsBody').innerHTML = '<tr><td colspan="11" class="muted">Загрузка не удалась.</td></tr>';
    }});
  </script>
</body>
</html>
"""


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_states() -> dict[str, dict]:
    states: dict[str, dict] = {}
    if not STATE_DIR.exists():
        return states
    now = datetime.now(timezone.utc)
    for path in sorted(STATE_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            payload = load_json(path)
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            payload["_state_updated_at"] = mtime.isoformat()
            payload["_state_updated_at_moscow"] = mtime.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК")
            payload["_state_stale"] = (now - mtime).total_seconds() > STATE_STALE_MINUTES * 60
            states[path.stem] = payload
        except Exception:
            continue
    return states


def load_meta() -> dict:
    path = STATE_DIR / "_bot_meta.json"
    if not path.exists():
        return {}
    try:
        return load_json(path)
    except Exception:
        return {}


def load_portfolio_snapshot() -> dict:
    if not PORTFOLIO_SNAPSHOT_PATH.exists():
        return {}
    try:
        return load_json(PORTFOLIO_SNAPSHOT_PATH)
    except Exception:
        return {}


def load_accounting_history() -> dict:
    if not ACCOUNTING_HISTORY_PATH.exists():
        return {}
    try:
        return json.loads(ACCOUNTING_HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_runtime_status() -> dict:
    if not RUNTIME_STATUS_PATH.exists():
        return {}
    try:
        return load_json(RUNTIME_STATUS_PATH)
    except Exception:
        return {}


def runtime_heartbeat_age_seconds(runtime: dict) -> float | None:
    raw_value = runtime.get("last_cycle_at") or runtime.get("updated_at")
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()


def load_news_snapshot() -> dict:
    if not NEWS_SNAPSHOT_PATH.exists():
        return {}
    try:
        return load_json(NEWS_SNAPSHOT_PATH)
    except Exception:
        return {}


CAPITAL_ALERT_PATTERNS = (
    "не хватает средств/го",
    "ограничений по го/марже",
    "внутренний лимит го",
    "доступный лимит по заявке сейчас 0 лотов",
    "риск-бюджет слишком мал",
)


def build_capital_alert(states: dict[str, dict]) -> dict:
    affected: list[dict] = []
    for symbol, state in states.items():
        if state.get("_state_stale"):
            continue
        candidates: list[str] = []
        last_error = str(state.get("last_error") or "").strip()
        if last_error:
            candidates.append(last_error)
        for item in state.get("last_signal_summary") or []:
            text = str(item or "").strip()
            if text:
                candidates.append(text)

        matched_reason = ""
        for text in candidates:
            lowered = text.lower()
            if any(pattern in lowered for pattern in CAPITAL_ALERT_PATTERNS):
                matched_reason = text
                break
        if matched_reason:
            affected.append({"symbol": symbol, "reason": matched_reason})

    if not affected:
        return {"active": False, "title": "", "message": "", "symbols": [], "count": 0}

    symbols = [item["symbol"] for item in affected]
    first_reason = affected[0]["reason"]
    if len(affected) == 1:
        message = f"{symbols[0]} не открыл сделку: {first_reason}"
    else:
        joined = ", ".join(symbols)
        message = (
            f"Части сигналов не хватило капитала/ГО: {joined}. "
            f"Последняя причина: {first_reason}"
        )
    return {
        "active": True,
        "title": "Не хватает капитала для части сделок",
        "message": message,
        "symbols": symbols,
        "count": len(symbols),
    }


def load_trade_rows(limit: int = 50) -> list[dict]:
    try:
        rows = load_trade_rows_from_storage(TRADE_JOURNAL_PATH, TRADE_DB_PATH, limit=limit)
    except Exception:
        return []
    normalized: list[dict] = []
    for row in rows[-limit:]:
        item = dict(row)
        raw_time = item.get("time")
        if raw_time:
            try:
                dt = datetime.fromisoformat(raw_time)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                item["time"] = dt.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S")
            except Exception:
                pass
        if item.get("price") is not None:
            try:
                item["price"] = f"{float(item['price']):.4f}"
            except Exception:
                pass
        if item.get("pnl_rub") is not None:
            try:
                item["pnl_rub"] = f"{float(item['pnl_rub']):.2f}"
            except Exception:
                pass
        if item.get("gross_pnl_rub") is not None:
            try:
                item["gross_pnl_rub"] = f"{float(item['gross_pnl_rub']):.2f}"
            except Exception:
                pass
        if item.get("commission_rub") is not None:
            try:
                item["commission_rub"] = f"{float(item['commission_rub']):.2f}"
            except Exception:
                pass
        if item.get("net_pnl_rub") is not None:
            try:
                item["net_pnl_rub"] = f"{float(item['net_pnl_rub']):.2f}"
            except Exception:
                pass
        item["reason_display"] = humanize_trade_reason(
            item.get("reason"),
            item.get("source"),
            item.get("event"),
            item.get("strategy"),
        )
        item["context_display"] = trade_context_display(item)
        normalized.append(item)
    return normalized


def stringify_money(value: Any, default: str = "-") -> str:
    if value in (None, ""):
        return default
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value)


def trade_context_display(row: dict[str, Any]) -> str:
    context = row.get("context")
    if not isinstance(context, dict):
        return "-"
    parts: list[str] = []
    regime = str(context.get("market_regime") or "").strip()
    if regime:
        parts.append(f"режим {regime}")
    quality_label = str(context.get("setup_quality_label") or "").strip()
    quality_score = context.get("setup_quality_score")
    if quality_label:
        score_text = ""
        try:
            if quality_score not in (None, ""):
                score_text = f" {int(quality_score)}/6"
        except Exception:
            score_text = ""
        parts.append(f"сетап {quality_label}{score_text}")
    atr_pct = context.get("atr_pct")
    if atr_pct not in (None, ""):
        try:
            parts.append(f"ATR {float(atr_pct) * 100:.2f}%")
        except Exception:
            pass
    volume_ratio = context.get("volume_ratio")
    if volume_ratio not in (None, ""):
        try:
            parts.append(f"объём x{float(volume_ratio):.2f}")
        except Exception:
            pass
    return " | ".join(parts) if parts else "-"


def humanize_strategy_name(strategy: str | None) -> str:
    value = str(strategy or "").strip()
    mapping = {
        "opening_range_breakout": "пробой стартового диапазона",
        "range_break_continuation": "продолжение пробоя диапазона",
        "trend_pullback": "откат по тренду",
        "trend_rollover": "разворот тренда",
        "momentum_breakout": "импульсный пробой",
        "failed_breakout": "ложный пробой",
        "breakdown_continuation": "продолжение слома диапазона",
        "recovered_position": "восстановленная позиция",
    }
    return mapping.get(value, value or "неизвестная стратегия")


def is_service_trade_reason(reason: str | None) -> bool:
    text = str(reason or "").strip().lower()
    if not text:
        return True
    service_prefixes = (
        "позиция подтверждена",
        "восстановлено после рестарта",
        "закрытие подтверждено",
        "закрытие восстановлено",
        "live fill",
        "dry_run",
        "тестовая запись",
        "заявка на закрытие исполнена",
        "восстановлено из broker operations",
    )
    return text.startswith(service_prefixes)


def fallback_trade_reason(
    event: str | None,
    strategy: str | None,
    source: str | None,
) -> str:
    event_name = str(event or "").strip().upper()
    strategy_text = humanize_strategy_name(strategy)
    source_text = str(source or "").strip().lower()
    if event_name == "OPEN":
        return f"Вход по торговой логике стратегии «{strategy_text}»."
    if source_text in {"delayed_broker_ops_recovery", "pending_order_recovery"}:
        return "Торговая причина выхода не сохранилась, закрытие подтверждено брокерскими операциями."
    if source_text in {"portfolio_confirmation", "portfolio_recovery", "order_fill"}:
        return "Торговая причина выхода не сохранилась, закрытие подтверждено брокером."
    return f"Выход по сопровождению позиции стратегии «{strategy_text}»."


def summarize_open_trade_reason(reason: str, strategy: str | None) -> str:
    text = str(reason or "").strip()
    if not text:
        return fallback_trade_reason("OPEN", strategy, "")

    compact = " ".join(text.split())
    signal_match = re.search(r"Сигнал\s+(LONG|SHORT)\s+\(([^)]+)\):", compact)
    direction = signal_match.group(1) if signal_match else ""
    strategy_code = signal_match.group(2) if signal_match else (strategy or "")
    strategy_text = humanize_strategy_name(strategy_code)

    extracted: list[str] = []
    patterns = (
        r"старший ТФ=([^;:.]+)",
        r"(цена выше EMA20 и EMA50: да|цена ниже EMA20 и EMA50: да)",
        r"(пробой вверх диапазона [^;:.]+: да|пробой вниз диапазона [^;:.]+: да)",
        r"(мягкий пробой вверх: да|мягкий пробой вниз: да|мягкий breakout вниз: да|мягкий breakdown вниз: да|мягкий breakout вверх: да)",
        r"(продолжение вверх после пробоя: да|продолжение вниз после слома: да)",
        r"(rollover вверх: да|rollover вниз: да)",
        r"(MACD поддерживает рост|MACD поддерживает снижение)",
        r"(объём выше базового|объём подтверждает вход|объём сильный)",
    )
    for pattern in patterns:
        match = re.search(pattern, compact, re.IGNORECASE)
        if match:
            value = match.group(1).strip().rstrip(".")
            if value not in extracted:
                extracted.append(value)
        if len(extracted) >= 3:
            break

    prefix = "Вход"
    if direction:
        prefix = f"Вход {direction}"

    details = "; ".join(extracted[:3])
    if details:
        return f"{prefix} по стратегии «{strategy_text}»: {details}."
    return f"{prefix} по стратегии «{strategy_text}»."


def humanize_trade_reason(
    reason: str | None,
    source: str | None,
    event: str | None,
    strategy: str | None = None,
) -> str:
    reason_text = str(reason or "").strip()
    source_text = str(source or "").strip().lower()
    event_name = str(event or "").strip().upper()
    strategy_text = humanize_strategy_name(strategy)
    if reason_text and not is_service_trade_reason(reason_text):
        if event_name == "OPEN":
            return summarize_open_trade_reason(reason_text, strategy_text)
        return reason_text
    if source_text == "dry_run":
        return "Тестовая запись DRY_RUN."
    return fallback_trade_reason(event_name, strategy_text, source_text)


def parse_trade_time(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    try:
        dt = datetime.fromisoformat(str(raw_value))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MOSCOW_TZ)


def load_all_trade_rows() -> list[dict]:
    try:
        rows = load_trade_rows_from_storage(TRADE_JOURNAL_PATH, TRADE_DB_PATH)
    except Exception:
        return []
    normalized_rows: list[dict] = []
    for row in rows:
        dt = parse_trade_time(row.get("time"))
        if not dt:
            continue
        item = dict(row)
        item["_dt"] = dt
        item["_date"] = dt.date().isoformat()
        normalized_rows.append(item)
    return normalized_rows


def load_trade_rows_for_day(target_day: date, limit: int = 200) -> list[dict]:
    rows = [row for row in load_all_trade_rows() if row.get("_date") == target_day.isoformat()]
    rows.sort(key=lambda row: row.get("_dt") or datetime.min.replace(tzinfo=MOSCOW_TZ))
    normalized: list[dict] = []
    for row in rows[-limit:]:
        item = dict(row)
        dt = item.pop("_dt", None)
        item.pop("_date", None)
        if dt:
            item["time"] = dt.strftime("%d.%m %H:%M:%S")
        if item.get("price") is not None:
            try:
                item["price"] = f"{float(item['price']):.4f}"
            except Exception:
                pass
        if item.get("pnl_rub") is not None:
            try:
                item["pnl_rub"] = f"{float(item['pnl_rub']):.2f}"
            except Exception:
                pass
        if item.get("gross_pnl_rub") is not None:
            try:
                item["gross_pnl_rub"] = f"{float(item['gross_pnl_rub']):.2f}"
            except Exception:
                pass
        if item.get("commission_rub") is not None:
            try:
                item["commission_rub"] = f"{float(item['commission_rub']):.2f}"
            except Exception:
                pass
        if item.get("net_pnl_rub") is not None:
            try:
                item["net_pnl_rub"] = f"{float(item['net_pnl_rub']):.2f}"
            except Exception:
                pass
        item["reason_display"] = humanize_trade_reason(
            item.get("reason"),
            item.get("source"),
            item.get("event"),
            item.get("strategy"),
        )
        item["context_display"] = trade_context_display(item)
        normalized.append(item)
    return normalized


def annotate_trade_rows(
    rows: list[dict],
    states: dict[str, dict],
    live_positions: dict[str, dict] | None = None,
) -> list[dict]:
    annotated: list[dict] = []
    open_by_key: dict[tuple[str, str], list[dict]] = {}

    for idx, row in enumerate(rows):
        item = dict(row)
        item["_row_id"] = idx
        item["event_status"] = "history"
        annotated.append(item)

    for item in annotated:
        symbol = str(item.get("symbol", ""))
        event = str(item.get("event", "")).upper()
        side = str(item.get("side", "")).upper()
        key = (symbol, side)

        if event == "OPEN":
            open_by_key.setdefault(key, []).append(item)
        elif event == "CLOSE":
            item["event_status"] = "closed"
            if open_by_key.get(key):
                open_item = open_by_key[key].pop(0)
                open_item["event_status"] = "closed"

    active_open_ids: set[int] = set()
    for key, items in open_by_key.items():
        if not items:
            continue
        symbol, side = key
        broker_position = (live_positions or {}).get(symbol)
        if broker_position:
            state_side = str(broker_position.get("side") or "FLAT").upper()
            state_qty = int(broker_position.get("qty") or 0)
        else:
            state = states.get(symbol, {})
            state_side = str(state.get("position_side", "FLAT")).upper()
            state_qty = int(state.get("position_qty") or 0)
        if state_side == side and state_side != "FLAT" and state_qty > 0:
            active_open_ids.add(int(items[-1].get("_row_id")))

    for item in annotated:
        if str(item.get("event", "")).upper() != "OPEN":
            continue
        if item.get("event_status") == "closed":
            continue
        if int(item.get("_row_id")) in active_open_ids:
            item["event_status"] = "active"
        else:
            item["event_status"] = "history"

    for item in annotated:
        item.pop("_row_id", None)

    return annotated


def filter_current_open_rows(
    rows: list[dict],
    states: dict[str, dict] | None = None,
    live_positions: dict[str, dict] | None = None,
) -> list[dict]:
    if not states and not live_positions:
        return rows
    filtered: list[dict] = []
    for row in rows:
        symbol = str(row.get("symbol", ""))
        if not symbol:
            continue
        side = str(row.get("side", "")).upper()
        if live_positions is not None:
            live = live_positions.get(symbol)
            if live is None:
                continue
            live_side = str(live.get("side", "FLAT")).upper()
            live_qty = int(live.get("qty") or 0)
        else:
            state = (states or {}).get(symbol, {})
            live_side = str(state.get("position_side", "FLAT")).upper()
            live_qty = int(state.get("position_qty") or 0)
        if live_side == "FLAT" or live_qty <= 0:
            continue
        if side and live_side and side != live_side:
            continue
        filtered.append(row)
    return filtered


def format_trade_review_row(
    close_row: dict[str, Any],
    open_row: dict[str, Any] | None,
    pnl_numeric: float,
    verdict: str,
) -> dict[str, Any]:
    entry_dt = open_row.get("_dt") if open_row else None
    exit_dt = close_row.get("_dt")
    exit_time = exit_dt.strftime("%d.%m %H:%M:%S") if exit_dt else (close_row.get("time") or "-")
    return {
        "symbol": str(close_row.get("symbol", "")),
        "side": close_row.get("side") or (open_row.get("side") if open_row else ""),
        "strategy": close_row.get("strategy") or (open_row.get("strategy") if open_row else ""),
        "session": close_row.get("session") or (open_row.get("session") if open_row else ""),
        "entry_time": entry_dt.strftime("%d.%m %H:%M:%S") if entry_dt else "-",
        "exit_time": exit_time,
        "close_time": exit_time,
        "entry_price": f"{float(open_row['price']):.4f}" if open_row and open_row.get("price") is not None else "-",
        "exit_price": f"{float(close_row['price']):.4f}" if close_row.get("price") is not None else "-",
        "qty_lots": close_row.get("qty_lots") or (open_row.get("qty_lots") if open_row else 0),
        "pnl_rub": f"{pnl_numeric:.2f}",
        "gross_pnl_rub": stringify_money(close_row.get("gross_pnl_rub")),
        "commission_rub": stringify_money(close_row.get("commission_rub")),
        "net_pnl_rub": stringify_money(close_row.get("net_pnl_rub"), stringify_money(close_row.get("pnl_rub"))),
        "entry_reason": open_row.get("reason") if open_row else "-",
        "exit_reason": close_row.get("reason") or "-",
        "entry_context_display": trade_context_display(open_row or {}),
        "exit_context_display": trade_context_display(close_row),
        "verdict": verdict,
        "_exit_dt": exit_dt,
    }


def build_trade_review(
    rows: list[dict],
    states: dict[str, dict] | None = None,
    live_positions: dict[str, dict] | None = None,
) -> dict:
    open_by_key: dict[tuple[str, str], list[dict]] = {}
    closed_reviews: list[dict] = []
    last_orphan_close_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    last_kept_close_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    def classify_verdict(pnl_numeric: float, exit_reason: str) -> str:
        text = str(exit_reason or "").lower()
        if pnl_numeric > 0:
            return "хорошая сделка"
        if "стоп" in text or "трейлинг" in text:
            return "нормальная убыточная"
        if "macd" in text or "rsi" in text:
            return "возможно ранний выход"
        if "противоположный сигнал" in text:
            return "закрыта по смене режима"
        return "требует разбора"

    def is_probable_duplicate_orphan(close_row: dict[str, Any], key: tuple[str, str]) -> bool:
        previous = last_orphan_close_by_key.get(key)
        if not previous:
            return False
        current_dt = close_row.get("_dt")
        previous_dt = previous.get("_dt")
        if not current_dt or not previous_dt:
            return False
        if abs((current_dt - previous_dt).total_seconds()) > 90:
            return False
        if int(close_row.get("qty_lots") or 0) != int(previous.get("qty_lots") or 0):
            return False
        try:
            current_price = round(float(close_row.get("price") or 0.0), 4)
            previous_price = round(float(previous.get("price") or 0.0), 4)
        except Exception:
            return False
        return current_price == previous_price

    def is_probable_duplicate_close(close_row: dict[str, Any], key: tuple[str, str]) -> bool:
        previous = last_kept_close_by_key.get(key)
        if not previous:
            return False
        current_dt = close_row.get("_dt")
        previous_dt = previous.get("_dt")
        if not current_dt or not previous_dt:
            return False
        if abs((current_dt - previous_dt).total_seconds()) > 90:
            return False
        if int(close_row.get("qty_lots") or 0) != int(previous.get("qty_lots") or 0):
            return False
        try:
            current_price = round(float(close_row.get("price") or 0.0), 4)
            previous_price = round(float(previous.get("price") or 0.0), 4)
        except Exception:
            return False
        return current_price == previous_price

    ordered_rows = sorted(rows, key=lambda row: row.get("_dt") or datetime.min.replace(tzinfo=MOSCOW_TZ))
    for row in ordered_rows:
        symbol = str(row.get("symbol", ""))
        event = str(row.get("event", "")).upper()
        side = str(row.get("side", "")).upper()
        if not symbol or not side:
            continue
        key = (symbol, side)
        if event == "OPEN":
            try:
                open_qty = max(1, int(row.get("qty_lots") or 1))
            except Exception:
                open_qty = 1
            for _ in range(open_qty):
                unit_row = dict(row)
                unit_row["qty_lots"] = 1
                open_by_key.setdefault(key, []).append(unit_row)
            continue
        if event != "CLOSE":
            continue

        open_row = None
        if open_by_key.get(key):
            open_row = open_by_key[key].pop(0)
        elif is_probable_duplicate_orphan(row, key) or is_probable_duplicate_close(row, key):
            continue

        pnl_value = row.get("pnl_rub")
        try:
            pnl_numeric = float(pnl_value) if pnl_value not in (None, "", "-") else 0.0
        except Exception:
            pnl_numeric = 0.0

        closed_reviews.append(
            format_trade_review_row(
                row,
                open_row,
                pnl_numeric,
                classify_verdict(pnl_numeric, row.get("reason") or ""),
            )
        )
        if open_row is None:
            last_orphan_close_by_key[key] = row
        last_kept_close_by_key[key] = row

    current_open = []
    for (symbol, _), items in open_by_key.items():
        if not items:
            continue
        row = dict(items[-1])
        remaining_qty = len(items)
        live = (live_positions or {}).get(symbol)
        if live is not None:
            try:
                live_qty = int(live.get("qty") or 0)
            except Exception:
                live_qty = 0
            if live_qty > 0:
                remaining_qty = live_qty
        row["qty_lots"] = remaining_qty
        current_open.append(row)
    current_open = filter_current_open_rows(current_open, states, live_positions)
    current_open.sort(key=lambda row: row.get("_dt") or datetime.min.replace(tzinfo=MOSCOW_TZ), reverse=True)

    closed_reviews.sort(key=lambda item: item.get("_exit_dt") or datetime.min.replace(tzinfo=MOSCOW_TZ), reverse=True)
    for item in closed_reviews:
        item.pop("_exit_dt", None)

    wins = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) > 0)
    losses = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) < 0)
    total = sum(float(item.get("pnl_rub") or 0.0) for item in closed_reviews)
    win_rate = round((wins / len(closed_reviews)) * 100, 1) if closed_reviews else 0.0

    by_symbol: dict[str, float] = {}
    by_strategy: dict[str, float] = {}
    by_regime: dict[str, float] = {}
    by_strategy_regime: dict[str, float] = {}
    for item in closed_reviews:
        pnl = float(item.get("pnl_rub") or 0.0)
        by_symbol[item["symbol"]] = by_symbol.get(item["symbol"], 0.0) + pnl
        strategy = item.get("strategy") or "-"
        by_strategy[strategy] = by_strategy.get(strategy, 0.0) + pnl
        regime = item.get("exit_context_display") or "-"
        by_regime[regime] = by_regime.get(regime, 0.0) + pnl
        strategy_regime = f"{strategy} @ {regime}"
        by_strategy_regime[strategy_regime] = by_strategy_regime.get(strategy_regime, 0.0) + pnl

    best_symbol = max(by_symbol.items(), key=lambda x: x[1]) if by_symbol else None
    worst_symbol = min(by_symbol.items(), key=lambda x: x[1]) if by_symbol else None
    best_strategy = max(by_strategy.items(), key=lambda x: x[1]) if by_strategy else None
    worst_strategy = min(by_strategy.items(), key=lambda x: x[1]) if by_strategy else None
    best_regime = max(by_regime.items(), key=lambda x: x[1]) if by_regime else None
    worst_regime = min(by_regime.items(), key=lambda x: x[1]) if by_regime else None
    best_strategy_regime = max(by_strategy_regime.items(), key=lambda x: x[1]) if by_strategy_regime else None
    worst_strategy_regime = min(by_strategy_regime.items(), key=lambda x: x[1]) if by_strategy_regime else None

    return {
        "closed_count": len(closed_reviews),
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "closed_total_pnl_rub": round(total, 2),
        "best_symbol": {"symbol": best_symbol[0], "pnl_rub": round(best_symbol[1], 2)} if best_symbol else None,
        "worst_symbol": {"symbol": worst_symbol[0], "pnl_rub": round(worst_symbol[1], 2)} if worst_symbol else None,
        "best_strategy": {"strategy": best_strategy[0], "pnl_rub": round(best_strategy[1], 2)} if best_strategy else None,
        "worst_strategy": {"strategy": worst_strategy[0], "pnl_rub": round(worst_strategy[1], 2)} if worst_strategy else None,
        "best_regime": {"regime": best_regime[0], "pnl_rub": round(best_regime[1], 2)} if best_regime else None,
        "worst_regime": {"regime": worst_regime[0], "pnl_rub": round(worst_regime[1], 2)} if worst_regime else None,
        "best_strategy_regime": {"label": best_strategy_regime[0], "pnl_rub": round(best_strategy_regime[1], 2)} if best_strategy_regime else None,
        "worst_strategy_regime": {"label": worst_strategy_regime[0], "pnl_rub": round(worst_strategy_regime[1], 2)} if worst_strategy_regime else None,
        "closed_reviews": closed_reviews[:20],
        "current_open": current_open[:20],
    }


def load_trade_review(limit: int = 80, states: dict[str, dict] | None = None) -> dict:
    rows = load_all_trade_rows()[-limit:]
    return build_trade_review(rows, states)


def load_trade_review_for_day(
    target_day: date,
    limit: int = 200,
    states: dict[str, dict] | None = None,
    live_positions: dict[str, dict] | None = None,
) -> dict:
    rows = [row for row in load_all_trade_rows() if row.get("_date") == target_day.isoformat()][-limit:]
    return build_trade_review(rows, states, live_positions)


def build_daily_performance(portfolio: dict, target_day: date, accounting_history: dict[str, Any] | None = None) -> dict:
    current_portfolio = float(portfolio.get("total_portfolio_rub") or 0.0)
    rows = load_all_trade_rows()
    by_day: dict[str, dict[str, float]] = {}
    cumulative = 0.0

    days = sorted({row["_date"] for row in rows} | set((accounting_history or {}).keys()))
    for day_key in days:
        day_rows = []
        for row in rows:
            if row.get("_date") == day_key and str(row.get("event", "")).upper() == "CLOSE":
                day_rows.append(row)
        pnl = 0.0
        wins = 0
        losses = 0
        for row in day_rows:
            try:
                trade_pnl = float(row.get("pnl_rub") or 0.0)
            except Exception:
                trade_pnl = 0.0
            pnl += trade_pnl
            if trade_pnl > 0:
                wins += 1
            elif trade_pnl < 0:
                losses += 1
        cumulative += pnl
        pct = (pnl / current_portfolio * 100.0) if current_portfolio else 0.0
        cumulative_pct = (cumulative / current_portfolio * 100.0) if current_portfolio else 0.0
        by_day[day_key] = {
            "date": day_key,
            "closed_count": len(day_rows),
            "wins": wins,
            "losses": losses,
            "pnl_rub": round(pnl, 2),
            "pnl_pct": round(pct, 2),
            "cumulative_pnl_rub": round(cumulative, 2),
            "cumulative_pnl_pct": round(cumulative_pct, 2),
        }

    selected_key = target_day.isoformat()
    return {
        "selected_date": selected_key,
        "available_dates": days,
        "selected": by_day.get(
            selected_key,
            {
                "date": selected_key,
                "closed_count": 0,
                "wins": 0,
                "losses": 0,
                "pnl_rub": 0.0,
                "pnl_pct": 0.0,
                "cumulative_pnl_rub": 0.0,
                "cumulative_pnl_pct": 0.0,
            },
        ),
        "series": [by_day[day_key] for day_key in days],
    }


def build_portfolio_view_for_day(
    portfolio: dict,
    target_day: date,
    accounting_history: dict[str, Any],
) -> dict[str, Any]:
    view = dict(portfolio or {})
    day_key = target_day.isoformat()
    history_entry = dict((accounting_history or {}).get(day_key) or {})
    rows = load_trade_rows_for_day(target_day, 300)
    closed_totals = {
        "gross_pnl_rub": 0.0,
        "commission_rub": 0.0,
        "net_pnl_rub": 0.0,
    }
    for row in rows:
        if str(row.get("event", "")).upper() != "CLOSE":
            continue
        try:
            closed_totals["gross_pnl_rub"] += float(row.get("gross_pnl_rub") or 0.0)
        except Exception:
            pass
        try:
            closed_totals["commission_rub"] += float(row.get("commission_rub") or 0.0)
        except Exception:
            pass
        try:
            closed_totals["net_pnl_rub"] += float(row.get("net_pnl_rub") or row.get("pnl_rub") or 0.0)
        except Exception:
            pass

    selected_is_today = target_day == datetime.now(MOSCOW_TZ).date()
    selected_actual_vm = float(
        history_entry.get(
            "actual_varmargin_rub",
            portfolio.get("bot_actual_varmargin_rub") if selected_is_today else 0.0,
        )
        or 0.0
    )
    selected_actual_fee = float(
        history_entry.get(
            "actual_fee_expense_rub",
            portfolio.get("bot_actual_fee_rub") if selected_is_today else 0.0,
        )
        or 0.0
    )
    selected_cash_effect = float(
        history_entry.get(
            "actual_account_cash_effect_rub",
            portfolio.get("bot_actual_cash_effect_rub") if selected_is_today else (selected_actual_vm - selected_actual_fee),
        )
        or 0.0
    )
    broker_open_positions_pnl = 0.0
    for item in (portfolio.get("broker_open_positions") or []):
        try:
            broker_open_positions_pnl += float(item.get("expected_yield_rub") or 0.0)
        except Exception:
            pass

    view["selected_date"] = day_key
    view["report_date"] = day_key
    view["selected_date_moscow"] = target_day.strftime("%d.%m.%Y")
    view["selected_is_today"] = selected_is_today
    view["free_cash_rub"] = view.get("free_rub")
    view["bot_realized_gross_pnl_rub"] = round(closed_totals["gross_pnl_rub"], 2)
    view["bot_realized_commission_rub"] = round(closed_totals["commission_rub"], 2)
    view["bot_realized_pnl_rub"] = round(closed_totals["net_pnl_rub"], 2)
    view["bot_closed_net_pnl_rub"] = round(closed_totals["net_pnl_rub"], 2)
    view["bot_closed_gross_pnl_rub"] = round(closed_totals["gross_pnl_rub"], 2)
    view["bot_closed_fee_rub"] = round(closed_totals["commission_rub"], 2)
    view["bot_actual_varmargin_rub"] = round(selected_actual_vm, 2)
    view["bot_actual_fee_rub"] = round(selected_actual_fee, 2)
    view["bot_actual_cash_effect_rub"] = round(selected_cash_effect, 2)
    if selected_is_today:
        estimated_variation = float(portfolio.get("bot_estimated_variation_margin_rub") or 0.0)
        open_positions_count = portfolio.get("open_positions_count")
        live_varmargin_by_symbol: dict[str, float] = {}
        for item in (portfolio.get("broker_open_positions") or []):
            symbol = str(item.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            try:
                live_value = float(item.get("variation_margin_rub") or 0.0)
            except Exception:
                continue
            live_varmargin_by_symbol[symbol] = round(live_varmargin_by_symbol.get(symbol, 0.0) + live_value, 2)
    else:
        estimated_variation = 0.0
        open_positions_count = 0
        live_varmargin_by_symbol = {}
    view["bot_estimated_variation_margin_rub"] = round(estimated_variation, 2)
    view["open_positions_count"] = open_positions_count
    view["bot_broker_day_pnl_rub"] = round(broker_open_positions_pnl, 2)
    view["bot_open_positions_live_pnl_rub"] = round(broker_open_positions_pnl, 2)
    total_varmargin = float(
        history_entry.get(
            "total_varmargin_rub",
            closed_totals["gross_pnl_rub"] + broker_open_positions_pnl,
        )
        or 0.0
    )
    total_pnl = float(
        history_entry.get(
            "total_pnl_rub",
            total_varmargin - selected_actual_fee,
        )
        or 0.0
    )
    view["bot_total_varmargin_rub"] = round(total_varmargin, 2)
    view["bot_total_variation_margin_rub"] = round(total_varmargin, 2)
    view["bot_total_pnl_rub"] = round(total_pnl, 2)
    view["bot_analytical_total_pnl_rub"] = round(total_pnl, 2)
    view["bot_operations_cash_effect_rub"] = round(selected_cash_effect, 2)
    actual_varmargin_by_symbol = history_entry.get(
        "varmargin_by_symbol",
        portfolio.get("bot_actual_varmargin_by_symbol") if selected_is_today else {},
    ) or {}
    actual_varmargin_source = "broker_operations"
    if not actual_varmargin_by_symbol and live_varmargin_by_symbol:
        actual_varmargin_by_symbol = live_varmargin_by_symbol
        actual_varmargin_source = "live_positions_fallback"
    view["bot_actual_varmargin_by_symbol"] = actual_varmargin_by_symbol
    view["bot_actual_varmargin_by_symbol_source"] = actual_varmargin_source
    view["generated_at_moscow"] = history_entry.get("generated_at_moscow") or portfolio.get("generated_at_moscow")
    return view


def load_ai_review(target_day: date) -> dict:
    dated_path = AI_REVIEW_DIR / f"{target_day.isoformat()}_review.md"
    latest_path = AI_REVIEW_DIR / "latest_review.md"
    source_path = dated_path if dated_path.exists() else latest_path
    if not source_path.exists():
        return {
            "available": False,
            "date": target_day.isoformat(),
            "content": "",
            "updated_at_moscow": None,
            "status": "missing",
            "followups": [],
        }
    try:
        content = source_path.read_text(encoding="utf-8").strip()
    except Exception:
        return {
            "available": False,
            "date": target_day.isoformat(),
            "content": "",
            "updated_at_moscow": None,
            "status": "error",
            "followups": [],
        }
    try:
        modified = datetime.fromtimestamp(source_path.stat().st_mtime, tz=timezone.utc).astimezone(MOSCOW_TZ)
        modified_text = modified.strftime("%d.%m %H:%M:%S МСК")
    except Exception:
        modified_text = None
    return {
        "available": bool(content),
        "date": target_day.isoformat(),
        "source": source_path.name,
        "content": content,
        "updated_at_moscow": modified_text,
        "status": "ready" if content else "empty",
        "followups": load_ai_review_followups(target_day),
    }


def get_ai_review_followup_path(target_day: date) -> Path:
    return AI_REVIEW_DIR / f"{target_day.isoformat()}_followups.json"


def load_ai_review_followups(target_day: date) -> list[dict[str, object]]:
    path = get_ai_review_followup_path(target_day)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    items: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        items.append(
            {
                "id": str(item.get("id") or ""),
                "question": str(item.get("question") or ""),
                "answer": str(item.get("answer") or ""),
                "model": str(item.get("model") or ""),
                "created_at_moscow": str(item.get("created_at_moscow") or ""),
            }
        )
    return items


def save_ai_review_followups(target_day: date, items: list[dict[str, object]]) -> None:
    path = get_ai_review_followup_path(target_day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def build_ai_review_followup_prompt(target_day: date, review_content: str, question: str) -> str:
    context_prompt = build_review_prompt(BASE_DIR, target_day)
    return (
        f"Дата разбора: {target_day.isoformat()}\n\n"
        "Основной AI-разбор дня:\n"
        f"{review_content.strip()}\n\n"
        "Текущий контекст дня:\n"
        f"{context_prompt.strip()}\n\n"
        "Дополнительный вопрос пользователя:\n"
        f"{question.strip()}\n"
    )


def run_ai_review_followup(target_day: date, question: str) -> dict[str, object]:
    clean_question = str(question or "").strip()
    if not clean_question:
        raise HTTPException(status_code=400, detail="Нужно ввести вопрос к AI-разбору.")
    ai_review = load_ai_review(target_day)
    if not ai_review.get("available") or not str(ai_review.get("content") or "").strip():
        raise HTTPException(status_code=400, detail="Сначала нужен основной AI-разбор за выбранную дату.")

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="На сервере не задан OPENAI_API_KEY.")

    model = os.getenv("OIL_AI_FOLLOWUP_MODEL", os.getenv("OIL_AI_MODEL", DEFAULT_AI_MODEL)).strip() or DEFAULT_AI_MODEL
    prompt = build_ai_review_followup_prompt(target_day, str(ai_review.get("content") or ""), clean_question)
    answer = request_openai_text(api_key, model, FOLLOWUP_SYSTEM_INSTRUCTIONS, prompt)
    created_at = datetime.now(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК")
    item = {
        "id": uuid4().hex,
        "question": clean_question,
        "answer": answer.strip(),
        "model": model,
        "created_at_moscow": created_at,
    }
    followups = load_ai_review_followups(target_day)
    followups.append(item)
    save_ai_review_followups(target_day, followups[-12:])
    return {
        "ok": True,
        "date": target_day.isoformat(),
        "item": item,
        "message": "Дополнительный AI-разбор готов.",
    }


def start_ai_review_refresh(target_day: date | None = None) -> dict[str, object]:
    if not AI_REVIEW_SCRIPT_PATH.exists():
        raise HTTPException(status_code=500, detail="Скрипт AI-разбора не найден на сервере.")
    if AI_REVIEW_LOCK_PATH.exists():
        return {
            "started": False,
            "status": "already_running",
            "message": "AI-разбор уже выполняется.",
            "date": target_day.isoformat() if target_day else None,
        }

    env = os.environ.copy()
    env["APP_DIR"] = str(BASE_DIR)
    if target_day is not None:
        env["AI_TARGET_DATE"] = target_day.isoformat()

    AI_REVIEW_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with AI_REVIEW_LOG_PATH.open("ab") as log_handle:
        subprocess.Popen(
            [str(AI_REVIEW_SCRIPT_PATH)],
            cwd=str(BASE_DIR),
            env=env,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
        )

    return {
        "started": True,
        "status": "started",
        "message": (
            f"AI-разбор для {target_day.isoformat()} запущен."
            if target_day is not None
            else "AI-разбор запущен."
        ),
        "date": target_day.isoformat() if target_day else None,
    }


def run_trade_operations_recovery(target_day: date | None = None) -> dict[str, object]:
    if not TRADE_RECOVERY_SCRIPT_PATH.exists():
        raise HTTPException(status_code=500, detail="Скрипт восстановления операций не найден на сервере.")
    if TRADE_RECOVERY_LOCK_PATH.exists():
        return {
            "started": False,
            "status": "already_running",
            "message": "Восстановление операций уже выполняется.",
            "date": target_day.isoformat() if target_day else None,
        }

    target = target_day or datetime.now(MOSCOW_TZ).date()
    TRADE_RECOVERY_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    TRADE_RECOVERY_LOCK_PATH.write_text(target.isoformat(), encoding="utf-8")
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(TRADE_RECOVERY_SCRIPT_PATH),
                "--date",
                target.isoformat(),
                "--write",
                "--json",
            ],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    finally:
        TRADE_RECOVERY_LOCK_PATH.unlink(missing_ok=True)

    output = (result.stdout or "").strip()
    error_text = (result.stderr or "").strip()
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=error_text or output or "Не удалось выполнить восстановление операций.",
        )
    try:
        payload = json.loads(output or "{}")
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Скрипт восстановления вернул непонятный ответ: {error}") from error

    payload.setdefault("started", True)
    payload.setdefault("status", "completed")
    payload.setdefault("date", target.isoformat())
    return payload


def get_service_status(service_name: str) -> dict:
    try:
        active = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        enabled = subprocess.run(
            ["systemctl", "is-enabled", service_name],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        return {
            "service": service_name,
            "active": active.stdout.strip() or "unknown",
            "enabled": enabled.stdout.strip() or "unknown",
        }
    except Exception as error:
        return {
            "service": service_name,
            "active": "unknown",
            "enabled": "unknown",
            "error": str(error),
        }


def get_bot_service_status() -> dict:
    service_name = os.getenv("OIL_SERVICE_NAME", "oil-bot")
    return get_service_status(service_name)


def get_dashboard_service_status() -> dict:
    service_name = os.getenv("OIL_DASHBOARD_SERVICE_NAME", "oil-bot-dashboard")
    return get_service_status(service_name)


def summarize_states(states: dict[str, dict], portfolio: dict | None = None) -> dict:
    realized = float((portfolio or {}).get("bot_realized_pnl_rub") or 0.0)
    open_positions = []
    signals = {"LONG": 0, "SHORT": 0, "HOLD": 0}

    for symbol, state in states.items():
        if state.get("_state_stale"):
            continue
        signal = (state.get("last_signal") or "HOLD").upper()
        if signal in signals:
            signals[signal] += 1

    broker_positions = (portfolio or {}).get("broker_open_positions") or []
    if broker_positions:
        for pos in broker_positions:
            symbol = str(pos.get("symbol", ""))
            state = states.get(symbol, {})
            open_positions.append(
                {
                    "symbol": symbol,
                    "side": str(pos.get("side") or state.get("position_side") or "FLAT").upper(),
                    "qty": int(pos.get("qty") or state.get("position_qty") or 0),
                    "entry_price": pos.get("entry_price", state.get("entry_price")),
                    "current_price": pos.get("current_price", state.get("last_market_price")),
                    "notional_rub": pos.get("notional_rub") or state.get("position_notional_rub") or 0.0,
                    "variation_margin_rub": pos.get("variation_margin_rub") or 0.0,
                    "pnl_pct": state.get("position_pnl_pct") or 0.0,
                    "strategy": state.get("entry_strategy") or "-",
                    "last_signal": (state.get("last_signal") or "HOLD").upper(),
                }
            )
    else:
        for symbol, state in states.items():
            side = (state.get("position_side") or "FLAT").upper()
            qty = int(state.get("position_qty") or 0)
            if side != "FLAT" and qty > 0:
                open_positions.append(
                    {
                        "symbol": symbol,
                        "side": side,
                        "qty": qty,
                        "entry_price": state.get("entry_price"),
                        "current_price": state.get("last_market_price"),
                        "notional_rub": state.get("position_notional_rub") or 0.0,
                        "variation_margin_rub": state.get("position_variation_margin_rub") or 0.0,
                        "pnl_pct": state.get("position_pnl_pct") or 0.0,
                        "strategy": state.get("entry_strategy") or "-",
                        "last_signal": (state.get("last_signal") or "HOLD").upper(),
                    }
                )

    return {
        "realized_pnl_rub": round(realized, 2),
        "open_positions": open_positions,
        "signal_counts": signals,
        "symbols_total": len(states),
    }


def build_health_payload(states: dict[str, dict]) -> dict:
    bot_service = get_bot_service_status()
    dashboard_service = get_dashboard_service_status()
    runtime = load_runtime_status()
    heartbeat_age = runtime_heartbeat_age_seconds(runtime)
    runtime_stale = heartbeat_age is None or heartbeat_age > RUNTIME_STALE_MINUTES * 60
    services_ok = bot_service.get("active") == "active" and dashboard_service.get("active") == "active"
    return {
        "ok": services_ok and not runtime_stale,
        "bot_service": bot_service,
        "dashboard_service": dashboard_service,
        "runtime_stale": runtime_stale,
        "runtime_heartbeat_age_seconds": round(heartbeat_age, 1) if heartbeat_age is not None else None,
        "runtime_stale_threshold_seconds": RUNTIME_STALE_MINUTES * 60,
        "symbols": sorted(states.keys()),
        "symbols_count": len(states),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_moscow": datetime.now(timezone.utc).astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
    }


def build_dashboard_html() -> str:
    html = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex, nofollow, noarchive, nosnippet" />
  <title>Панель Oil Bot</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #030711;
      --bg2: #091120;
      --panel: rgba(8, 14, 28, 0.88);
      --panel-strong: rgba(10, 18, 34, 0.98);
      --ink: #ebf4ff;
      --muted: #7f95b3;
      --line: rgba(102, 174, 255, 0.18);
      --good: #37e6a4;
      --bad: #ff6b87;
      --warn: #ffca62;
      --accent: #43c5ff;
      --accent2: #7d8cff;
      --accent3: #14f1ff;
      --glow: rgba(67, 197, 255, 0.22);
      --shadow: rgba(0, 0, 0, 0.45);
    }
    body {
      margin: 0;
      font-family: "Manrope", "Segoe UI", Arial, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(67, 197, 255, 0.18), transparent 24%),
        radial-gradient(circle at top right, rgba(125, 140, 255, 0.16), transparent 20%),
        radial-gradient(circle at 50% 0%, rgba(20, 241, 255, 0.08), transparent 28%),
        linear-gradient(180deg, var(--bg2) 0%, var(--bg) 100%);
      color: var(--ink);
      min-height: 100vh;
    }
    .site-header {
      position: sticky;
      top: 0;
      z-index: 20;
      backdrop-filter: blur(18px);
      background: rgba(4, 9, 18, 0.78);
      border-bottom: 1px solid rgba(102, 174, 255, 0.12);
    }
    .site-header__inner {
      max-width: 1380px;
      margin: 0 auto;
      padding: 18px 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
    }
    .site-brand__eyebrow {
      color: var(--accent3);
      font: 700 12px/1 "JetBrains Mono", monospace;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      margin-bottom: 6px;
    }
    .site-brand__title {
      font: 700 18px/1.1 "Sora", sans-serif;
      text-shadow: 0 0 22px var(--glow);
    }
    .site-nav {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .site-nav__link {
      color: #b8cae3;
      text-decoration: none;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(102, 174, 255, 0.18);
      background: rgba(67, 197, 255, 0.05);
      font-weight: 600;
    }
    .site-nav__link.is-active {
      color: white;
      background: linear-gradient(135deg, rgba(67, 197, 255, 0.22), rgba(125, 140, 255, 0.24));
      border-color: rgba(102, 174, 255, 0.32);
      box-shadow: 0 0 18px rgba(67, 197, 255, 0.12);
    }
    .wrap {
      max-width: 1380px;
      margin: 0 auto;
      padding: 28px;
    }
    h1, h2 { margin: 0 0 12px; }
    h1 {
      letter-spacing: 0.01em;
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 30px;
      line-height: 1.1;
      text-shadow: 0 0 28px var(--glow);
    }
    h2 {
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 24px;
      line-height: 1.15;
      letter-spacing: 0.01em;
    }
    .grid {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    }
    .panel {
      background: linear-gradient(180deg, var(--panel-strong) 0%, var(--panel) 100%);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px 20px;
      box-shadow:
        0 18px 50px var(--shadow),
        inset 0 1px 0 rgba(255, 255, 255, 0.03),
        0 0 0 1px rgba(67, 197, 255, 0.03),
        0 0 22px rgba(67, 197, 255, 0.05);
      backdrop-filter: blur(14px);
    }
    .hero {
      margin-bottom: 16px;
    }
    .metric {
      font-size: clamp(22px, 2.2vw, 34px);
      font-weight: 700;
      font-family: "Sora", "Manrope", sans-serif;
      letter-spacing: -0.02em;
      line-height: 1.12;
      overflow-wrap: anywhere;
      word-break: break-word;
      text-wrap: balance;
      text-shadow: 0 0 12px var(--glow);
    }
    .metric-wide {
      font-size: clamp(17px, 1.5vw, 24px);
      line-height: 1.2;
      letter-spacing: -0.01em;
    }
    .metric-compact {
      font-size: clamp(16px, 1.35vw, 24px);
      line-height: 1.18;
      letter-spacing: -0.01em;
    }
    .muted { color: var(--muted); }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    .table-scroll {
      max-height: 460px;
      overflow: auto;
      border-radius: 14px;
      -webkit-overflow-scrolling: touch;
    }
    th, td {
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    th {
      color: #b8cae3;
      font-weight: 600;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      font-size: 12px;
    }
    tr:hover td {
      background: rgba(68, 184, 255, 0.04);
    }
    #positionsTable,
    #signalsTable,
    #newsTable,
    #reviewTable {
      min-width: 880px;
    }
    #tradesTable {
      min-width: 1100px;
    }
    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      background: rgba(68, 184, 255, 0.12);
      color: #9fdcff;
      font-size: 12px;
      border: 1px solid rgba(68, 184, 255, 0.20);
    }
    .badge.long {
      background: rgba(55, 230, 164, 0.12);
      border-color: rgba(55, 230, 164, 0.22);
      color: var(--good);
    }
    .badge.short {
      background: rgba(255, 107, 135, 0.12);
      border-color: rgba(255, 107, 135, 0.22);
      color: var(--bad);
    }
    .badge.hold {
      background: rgba(255, 202, 98, 0.12);
      border-color: rgba(255, 202, 98, 0.18);
      color: var(--warn);
    }
    .mono {
      font-family: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
    }
    .right {
      text-align: right;
    }
    .reason {
      max-width: 420px;
      color: #c8d7ea;
      line-height: 1.35;
    }
    .news-reason {
      display: flex;
      align-items: flex-start;
      gap: 10px;
    }
    .hint-button {
      flex: 0 0 auto;
      appearance: none;
      border: 1px solid rgba(102, 174, 255, 0.22);
      background: rgba(67, 197, 255, 0.10);
      color: #bfe8ff;
      border-radius: 999px;
      padding: 4px 10px;
      font: 600 12px/1 "Manrope", sans-serif;
      cursor: pointer;
      transition: background 0.15s ease, border-color 0.15s ease;
    }
    .hint-button:hover {
      background: rgba(67, 197, 255, 0.16);
      border-color: rgba(102, 174, 255, 0.32);
    }
    .btn-secondary {
      appearance: none;
      border: 1px solid rgba(102, 174, 255, 0.22);
      background: rgba(67, 197, 255, 0.10);
      color: #d9f3ff;
      border-radius: 12px;
      padding: 8px 14px;
      font: 700 13px/1 "Manrope", sans-serif;
      cursor: pointer;
      transition: background 0.15s ease, border-color 0.15s ease, opacity 0.15s ease;
    }
    .btn-secondary:hover {
      background: rgba(67, 197, 255, 0.16);
      border-color: rgba(102, 174, 255, 0.32);
    }
    .btn-secondary:disabled {
      opacity: 0.55;
      cursor: default;
    }
    .news-popover {
      position: fixed;
      z-index: 1000;
      width: min(420px, calc(100vw - 32px));
      background: linear-gradient(180deg, rgba(9, 16, 31, 0.98) 0%, rgba(6, 12, 25, 0.98) 100%);
      border: 1px solid rgba(102, 174, 255, 0.22);
      border-radius: 16px;
      box-shadow: 0 18px 50px rgba(0, 0, 0, 0.45), 0 0 24px rgba(67, 197, 255, 0.10);
      padding: 14px 16px;
      display: none;
    }
    .news-popover.open {
      display: block;
    }
    .news-popover-title {
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 13px;
      color: #d8ecff;
      margin-bottom: 8px;
    }
    .news-popover-text {
      white-space: pre-wrap;
      line-height: 1.45;
      color: #c8d7ea;
      font-size: 13px;
    }
    .section-title {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
    }
    .mobile-cards {
      display: none;
      gap: 12px;
    }
    .mobile-card {
      background: rgba(10, 18, 34, 0.72);
      border: 1px solid rgba(102, 174, 255, 0.14);
      border-radius: 16px;
      padding: 14px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
    }
    .mobile-card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 10px;
    }
    .mobile-card-title {
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 16px;
      font-weight: 600;
      letter-spacing: 0.01em;
    }
    .mobile-card-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px 14px;
      margin-bottom: 10px;
    }
    .mobile-card-item .muted {
      display: block;
      font-size: 11px;
      margin-bottom: 3px;
    }
    .mobile-card-value {
      font-size: 13px;
      line-height: 1.35;
      color: #dbe9f8;
    }
    .mobile-card-footer {
      display: grid;
      gap: 8px;
      border-top: 1px solid rgba(102, 174, 255, 0.10);
      padding-top: 10px;
    }
    .mobile-card-text {
      font-size: 13px;
      line-height: 1.4;
      color: #c8d7ea;
    }
    .generated {
      font-size: 12px;
      color: var(--muted);
      font-family: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
    }
    .alert-panel {
      border-color: rgba(255, 202, 98, 0.28);
      box-shadow:
        0 18px 50px rgba(0, 0, 0, 0.22),
        inset 0 1px 0 rgba(255, 255, 255, 0.03),
        0 0 0 1px rgba(255, 202, 98, 0.05),
        0 0 28px rgba(255, 202, 98, 0.08);
    }
    .alert-row {
      display: flex;
      gap: 14px;
      align-items: flex-start;
    }
    .alert-icon {
      flex: 0 0 auto;
      width: 40px;
      height: 40px;
      border-radius: 14px;
      display: grid;
      place-items: center;
      background: rgba(255, 202, 98, 0.12);
      border: 1px solid rgba(255, 202, 98, 0.18);
      color: var(--warn);
      font-size: 18px;
    }
    .alert-title {
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 16px;
      font-weight: 600;
      margin-bottom: 4px;
      color: #f4fbff;
    }
    .alert-message {
      color: #d8e3ef;
      line-height: 1.5;
      max-width: 90ch;
    }
    .alert-meta {
      margin-top: 10px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      font-size: 12px;
      color: var(--muted);
    }
    .is-hidden {
      display: none !important;
    }
    .toolbar-inline {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }
    .toolbar-inline input,
    .toolbar-inline select {
      background: #0b1324;
      color: #ebf4ff;
      border: 1px solid rgba(102,174,255,0.18);
      border-radius: 10px;
      padding: 8px 10px;
      font: 500 13px/1 "Manrope", sans-serif;
    }
    .chart-wrap {
      margin-top: 18px;
      border: 1px solid rgba(102, 174, 255, 0.14);
      border-radius: 16px;
      background: rgba(7, 13, 24, 0.72);
      padding: 14px;
    }
    .chart-legend {
      display: flex;
      gap: 18px;
      margin-top: 10px;
      font-size: 12px;
      color: var(--muted);
      flex-wrap: wrap;
    }
    .legend-dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
      margin-right: 6px;
    }
    #pnlChart {
      width: 100%;
      height: 280px;
      display: block;
    }
    .prose-review {
      font-size: 14px;
      line-height: 1.6;
      color: #dbe9f8;
      white-space: normal;
    }
    .prose-review h1,
    .prose-review h2,
    .prose-review h3,
    .prose-review h4 {
      font-family: "Sora", "Manrope", sans-serif;
      margin: 18px 0 8px;
      font-size: 18px;
    }
    .prose-review ul,
    .prose-review ol {
      margin: 8px 0 12px 18px;
      padding: 0;
    }
    .prose-review li {
      margin: 4px 0;
    }
    .prose-review p {
      margin: 8px 0 12px;
    }
    .prose-review strong {
      color: #f4fbff;
      font-weight: 700;
    }
    .prose-review code {
      font-family: "JetBrains Mono", monospace;
      background: rgba(67, 197, 255, 0.10);
      border: 1px solid rgba(67, 197, 255, 0.12);
      border-radius: 8px;
      padding: 1px 6px;
      color: #bfe8ff;
    }
    .hero p {
      max-width: 62ch;
      line-height: 1.5;
    }
    a {
      color: var(--accent);
    }
    @media (max-width: 860px) {
      .site-header__inner {
        align-items: flex-start;
        flex-direction: column;
      }
      .wrap {
        padding: 14px;
      }
      h1 {
        font-size: 24px;
      }
      h2 {
        font-size: 20px;
      }
      .panel {
        padding: 14px 14px;
        border-radius: 16px;
      }
      .grid {
        grid-template-columns: 1fr;
        gap: 12px;
      }
      .metric {
        font-size: clamp(20px, 7vw, 28px);
      }
      .metric-wide,
      .metric-compact {
        font-size: clamp(15px, 4.6vw, 20px);
      }
      .section-title {
        align-items: flex-start;
        flex-direction: column;
        gap: 10px;
      }
      .generated {
        font-size: 11px;
      }
      .toolbar-inline {
        width: 100%;
      }
      .toolbar-inline input,
      .toolbar-inline select {
        width: 100%;
      }
      table {
        font-size: 13px;
      }
      th, td {
        padding: 8px 6px;
      }
      .badge {
        font-size: 11px;
        padding: 3px 7px;
      }
      .reason {
        max-width: 220px;
      }
      .table-scroll {
        margin: 0 -4px;
        padding-bottom: 2px;
      }
      .desktop-table {
        display: none;
      }
      .mobile-cards {
        display: grid;
      }
      .mobile-card-grid {
        grid-template-columns: 1fr 1fr;
      }
    }
  </style>
</head>
<body>
  __SITE_NAV__
  <div class="wrap">
    <div class="hero">
      <section class="panel">
        <div class="section-title">
          <h1>Панель Oil Bot</h1>
          <div class="generated" id="generatedAt">Обновление: -</div>
        </div>
        <p class="muted">Живой обзор бота, позиций, новостей и состояния сервиса. Обновление каждые 15 секунд.</p>
        <div class="grid">
          <div>
            <div class="muted">Реализовано</div>
            <div class="metric" id="realized">-</div>
          </div>
          <div>
            <div class="muted">Открытые позиции</div>
            <div class="metric" id="openCount">-</div>
          </div>
          <div>
            <div class="muted">Сервис</div>
            <div class="metric" id="serviceState">-</div>
          </div>
          <div>
            <div class="muted">Инструментов</div>
            <div class="metric" id="symbolsTotal">-</div>
          </div>
        </div>
      </section>
    </div>

    <section class="panel" style="margin-bottom:16px;">
      <div class="section-title">
        <h2>Портфель</h2>
        <div class="generated" id="portfolioGeneratedAt">Срез портфеля: -</div>
      </div>
      <div class="muted" style="margin-bottom:12px;" id="portfolioMeaning">
        Блок разделён на три смысла: свободные средства и ГО брокера, денежный эффект операций счёта, и аналитический итог бота по закрытым и открытым позициям.
      </div>
      <div class="grid">
        <div>
          <div class="muted">Режим</div>
            <div class="metric metric-wide" id="portfolioMode">-</div>
        </div>
        <div>
          <div class="muted">Портфель</div>
          <div class="metric" id="portfolioTotal">-</div>
        </div>
        <div>
          <div class="muted">Свободные RUB</div>
          <div class="metric" id="portfolioFree">-</div>
        </div>
        <div>
          <div class="muted">ГО</div>
          <div class="metric" id="portfolioBlocked">-</div>
        </div>
        <div>
          <div class="muted">Закрытые сделки, NET</div>
          <div class="metric" id="portfolioRealized">-</div>
        </div>
        <div>
          <div class="muted">Комиссия по счёту</div>
          <div class="metric" id="portfolioActualFee">-</div>
        </div>
        <div>
          <div class="muted">Клиринговая ВМ</div>
          <div class="metric" id="portfolioActualVm">-</div>
        </div>
        <div>
          <div class="muted">Денежный эффект операций</div>
          <div class="metric" id="portfolioCashEffect">-</div>
        </div>
        <div>
          <div class="muted">Текущая вар. маржа позиций</div>
          <div class="metric" id="portfolioVariation">-</div>
        </div>
        <div>
          <div class="muted">Открытые позиции, live</div>
          <div class="metric" id="portfolioOpenLive">-</div>
        </div>
        <div>
          <div class="muted">Gross закрытых + live открытых</div>
          <div class="metric" id="portfolioTotalVm">-</div>
        </div>
        <div>
          <div class="muted">Аналитический итог бота</div>
          <div class="metric" id="portfolioTotalPnl">-</div>
        </div>
        <div>
          <div class="muted">Открытых позиций</div>
          <div class="metric" id="portfolioOpenCount">-</div>
        </div>
      </div>
    </section>

    <section class="panel alert-panel is-hidden" id="capitalAlertPanel" style="margin-bottom:16px;">
      <div class="alert-row">
        <div class="alert-icon">!</div>
        <div>
          <div class="alert-title" id="capitalAlertTitle">Не хватает капитала для части сделок</div>
          <div class="alert-message" id="capitalAlertMessage">-</div>
          <div class="alert-meta">
            <span id="capitalAlertCount">-</span>
            <span id="capitalAlertSymbols">-</span>
          </div>
        </div>
      </div>
    </section>

    <section class="panel" style="margin-bottom:16px;">
      <div class="section-title">
        <h2>Дневная аналитика</h2>
        <div class="toolbar-inline">
          <label class="muted" for="selectedDate">Дата:</label>
          <input id="selectedDate" type="date" />
        </div>
      </div>
      <div class="grid">
        <div>
          <div class="muted">Итог за день</div>
          <div class="metric" id="dayPnlRub">-</div>
        </div>
        <div>
          <div class="muted">Итог за день, %</div>
          <div class="metric" id="dayPnlPct">-</div>
        </div>
        <div>
          <div class="muted">Сделок закрыто</div>
          <div class="metric" id="dayClosedCount">-</div>
        </div>
        <div>
          <div class="muted">Накопленный итог</div>
          <div class="metric" id="cumPnlRub">-</div>
        </div>
        <div>
          <div class="muted">Накопленный итог, %</div>
          <div class="metric" id="cumPnlPct">-</div>
        </div>
      </div>
      <div class="chart-wrap">
        <canvas id="pnlChart" width="1200" height="280"></canvas>
        <div class="chart-legend">
          <span><span class="legend-dot" style="background:#43c5ff;"></span>Итог за день, RUB</span>
          <span><span class="legend-dot" style="background:#37e6a4;"></span>Накопленный итог, RUB</span>
        </div>
      </div>
    </section>

    <div class="grid">
      <section class="panel">
        <h2>Позиции</h2>
        <div id="positionsCards" class="mobile-cards"></div>
        <div class="table-scroll desktop-table">
          <table id="positionsTable">
            <thead><tr><th>Инструмент</th><th>Сторона</th><th>Лоты</th><th>Вход</th><th>Текущая</th><th>Стоимость</th><th>Вар. маржа</th><th>Изм. %</th><th>Стратегия</th><th>Сигнал</th></tr></thead>
            <tbody></tbody>
          </table>
        </div>
      </section>

    </div>

    <section class="panel" style="margin-top:16px;">
      <div class="section-title">
        <h2>Сигналы по инструментам</h2>
        <div class="toolbar-inline">
          <input id="manualInstrumentTicker" type="text" placeholder="Новый тикер, например VBM6" />
          <select id="manualInstrumentTemplate"></select>
          <button id="manualInstrumentAddBtn" class="btn-secondary" type="button">Добавить инструмент</button>
        </div>
      </div>
      <div class="muted" id="manualInstrumentStatus" style="margin-bottom:12px;">Можно добавить новый тикер и скопировать для него стратегии уже существующего инструмента.</div>
      <div class="muted" id="manualInstrumentList" style="margin-bottom:12px;">Ручных инструментов пока нет.</div>
      <div id="signalsCards" class="mobile-cards"></div>
      <div class="table-scroll desktop-table">
        <table id="signalsTable">
          <thead>
            <tr>
              <th>Инструмент</th><th>Сигнал</th><th>Стратегия</th><th>Старший ТФ</th><th>News bias</th><th>Влияние</th><th>Аллокатор</th><th>Ключевая причина</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class="panel" style="margin-top:16px;">
      <div class="section-title">
        <h2>Новости</h2>
        <div class="generated" id="newsUpdatedAt">Новости: -</div>
      </div>
      <div class="grid">
        <div>
          <div class="muted">Активных bias</div>
          <div class="metric" id="newsCount">-</div>
        </div>
        <div>
          <div class="muted">LONG</div>
          <div class="metric" id="newsLongCount">-</div>
        </div>
        <div>
          <div class="muted">SHORT</div>
          <div class="metric" id="newsShortCount">-</div>
        </div>
        <div>
          <div class="muted">BLOCK</div>
          <div class="metric" id="newsBlockCount">-</div>
        </div>
      </div>
      <div id="newsCards" class="mobile-cards" style="margin-top:16px;"></div>
      <div class="table-scroll desktop-table">
        <table id="newsTable" style="margin-top:16px;">
          <thead>
            <tr>
              <th>Инструмент</th><th>Bias</th><th>Сила</th><th>Источник</th><th>Актуально до</th><th>Причина</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class="panel" style="margin-top:16px;">
      <div class="section-title">
        <h2>Лента событий</h2>
        <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
          <label class="muted" for="eventStatusFilter">Статус:
            <select id="eventStatusFilter" style="margin-left:8px; background:#0b1324; color:#ebf4ff; border:1px solid rgba(102,174,255,0.18); border-radius:10px; padding:6px 10px;">
              <option value="all">Все</option>
              <option value="active">Активные</option>
              <option value="closed">Закрытые</option>
              <option value="history">История</option>
            </select>
          </label>
          <button id="tradeRecoveryBtn" class="btn-secondary" type="button">Восстановить операции</button>
        </div>
      </div>
      <div class="muted" id="tradeRecoveryStatus" style="margin-bottom:12px;">Ручное восстановление не запускалось.</div>
      <div id="tradesCards" class="mobile-cards" style="margin-top:16px;"></div>
      <div class="table-scroll desktop-table">
        <table id="tradesTable">
          <thead>
            <tr>
              <th>Время</th><th>Инструмент</th><th>Событие</th><th>Статус</th><th>Сторона</th><th>Лоты</th><th class="right">Цена</th><th class="right">Gross</th><th class="right">Комиссия</th><th class="right">Net</th><th>Стратегия</th><th>Причина</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class="panel" style="margin-top:16px;">
      <h2>Обзор сделок</h2>
      <div class="grid">
        <div>
          <div class="muted">Закрыто</div>
          <div class="metric metric-compact" id="reviewClosed">-</div>
        </div>
        <div>
          <div class="muted">Плюсовых</div>
          <div class="metric metric-compact" id="reviewWins">-</div>
        </div>
        <div>
          <div class="muted">Минусовых</div>
          <div class="metric metric-compact" id="reviewLosses">-</div>
        </div>
        <div>
          <div class="muted">Итог по закрытым</div>
          <div class="metric metric-compact" id="reviewPnl">-</div>
        </div>
        <div>
          <div class="muted">Win rate</div>
          <div class="metric metric-compact" id="reviewWinRate">-</div>
        </div>
        <div>
          <div class="muted">Лучший инструмент</div>
          <div class="metric metric-wide metric-compact" id="reviewBestSymbol">-</div>
        </div>
        <div>
          <div class="muted">Худший инструмент</div>
          <div class="metric metric-wide metric-compact" id="reviewWorstSymbol">-</div>
        </div>
        <div>
          <div class="muted">Лучшая стратегия</div>
          <div class="metric metric-wide metric-compact" id="reviewBestStrategy">-</div>
        </div>
        <div>
          <div class="muted">Худшая стратегия</div>
          <div class="metric metric-wide metric-compact" id="reviewWorstStrategy">-</div>
        </div>
        <div>
          <div class="muted">Лучший режим</div>
          <div class="metric metric-wide metric-compact" id="reviewBestRegime">-</div>
        </div>
        <div>
          <div class="muted">Худший режим</div>
          <div class="metric metric-wide metric-compact" id="reviewWorstRegime">-</div>
        </div>
        <div>
          <div class="muted">Лучшая связка</div>
          <div class="metric metric-wide metric-compact" id="reviewBestStrategyRegime">-</div>
        </div>
        <div>
          <div class="muted">Худшая связка</div>
          <div class="metric metric-wide metric-compact" id="reviewWorstStrategyRegime">-</div>
        </div>
      </div>
      <div id="reviewCards" class="mobile-cards" style="margin-top:16px;"></div>
      <div class="table-scroll desktop-table">
        <table id="reviewTable" style="margin-top:16px;">
          <thead>
            <tr>
              <th>Инструмент</th><th>Сторона</th><th>Стратегия</th><th>Вход</th><th>Выход</th><th class="right">Gross</th><th class="right">Комиссия</th><th class="right">Net</th><th>Выход</th><th>Вердикт</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <section class="panel" style="margin-top:16px;">
      <div class="section-title">
        <h2>Мониторинг сервиса</h2>
        <div class="generated" id="runtimeUpdatedAt">Runtime: -</div>
      </div>
      <div class="grid">
        <div>
          <div class="muted">Состояние runtime</div>
          <div class="metric metric-wide" id="runtimeState">-</div>
        </div>
        <div>
          <div class="muted">Сессия</div>
          <div class="metric metric-wide" id="runtimeSession">-</div>
        </div>
        <div>
          <div class="muted">Циклов</div>
          <div class="metric" id="runtimeCycles">-</div>
        </div>
        <div>
          <div class="muted">Ошибок подряд</div>
          <div class="metric" id="runtimeErrors">-</div>
        </div>
      </div>
      <table id="runtimeTable" style="margin-top:16px;">
        <tbody></tbody>
      </table>
    </section>

    <section class="panel" style="margin-top:16px;">
      <div class="section-title">
        <h2>AI-разбор дня</h2>
        <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
          <button id="aiReviewRefreshBtn" class="btn-secondary" type="button">Обновить AI-разбор</button>
          <div class="generated" id="aiReviewMeta">AI review: -</div>
        </div>
      </div>
      <div class="muted" id="aiReviewStatus" style="margin-bottom:12px;">Ручной запуск не выполнялся.</div>
      <div id="aiReviewContent" class="prose-review muted">AI-review пока не загружен.</div>
      <div style="margin-top:16px; display:grid; gap:10px;">
        <label for="aiReviewFollowupInput" class="muted">Дополнительный вопрос к AI-разбору</label>
        <textarea id="aiReviewFollowupInput" rows="4" placeholder="Например: почему бот слабо использовал движение по нефти после 18:00?" style="width:100%; resize:vertical; background:rgba(8,16,32,.75); color:#e8f0ff; border:1px solid rgba(138,163,255,.16); border-radius:14px; padding:12px;"></textarea>
        <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
          <button id="aiReviewFollowupBtn" class="btn-secondary" type="button">Задать доп. вопрос</button>
          <div class="generated" id="aiReviewFollowupStatus">Дополнительный разбор не запускался.</div>
        </div>
      </div>
      <div id="aiReviewFollowups" style="margin-top:16px; display:grid; gap:14px;"></div>
    </section>
  </div>
  <div id="newsPopover" class="news-popover" role="dialog" aria-hidden="true">
    <div class="news-popover-title" id="newsPopoverTitle">Текст новости</div>
    <div class="news-popover-text" id="newsPopoverText"></div>
  </div>
  <script>
    function escapeHtml(value) {
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    let instrumentNames = {
      BRK6: 'BR-5.26 Нефть Brent',
      USDRUBF: 'USDRUBF Доллар - Рубль',
      CNYRUBF: 'CNYRUBF Юань - Рубль',
      IMOEXF: 'IMOEXF Индекс МосБиржи',
      SRM6: 'SBRF-6.26 Сбер Банк',
      GNM6: 'GOLDM-6.26 Золото (мини)',
      NGJ6: 'NG-4.26 Природный газ',
      RBM6: 'RGBI-6.26 Индекс гос. облигаций',
      UCM6: 'UCNY-6.26 Доллар США - Юань',
      VBM6: 'VTBR-6.26 Банк ВТБ',
    };

    function renderInstrumentLabel(symbol, explicitName = '') {
      const ticker = String(symbol || '-');
      const displayName = String(explicitName || instrumentNames[ticker] || '').trim();
      if (!displayName) {
        return `<div class="mono">${escapeHtml(ticker)}</div>`;
      }
      return `<div class="mono">${escapeHtml(ticker)}</div><div class="muted">${escapeHtml(displayName)}</div>`;
    }

    function signalBadge(value) {
      const raw = String(value || '-').toUpperCase();
      const css = raw === 'LONG' || raw === 'ACTIVE' ? 'long' : raw === 'SHORT' || raw === 'FAILED' ? 'short' : 'hold';
      const labelMap = {
        LONG: 'ЛОНГ',
        SHORT: 'ШОРТ',
        HOLD: 'ОЖИДАНИЕ',
        FLAT: 'ВНЕ ПОЗИЦИИ',
        ACTIVE: 'АКТИВЕН',
        FAILED: 'ОШИБКА',
        CLOSED: 'ЗАКРЫТ',
        BLOCK: 'БЛОК',
      };
      return `<span class="badge ${css}">${escapeHtml(labelMap[raw] || raw)}</span>`;
    }

    function eventStatusBadge(value) {
      const raw = String(value || '-').toUpperCase();
      const css = raw === 'ACTIVE' ? 'long' : raw === 'CLOSED' ? 'short' : 'hold';
      const label = raw === 'ACTIVE' ? 'АКТИВНА' : raw === 'CLOSED' ? 'ЗАКРЫТА' : 'ИСТОРИЯ';
      return `<span class="badge ${css}">${label}</span>`;
    }

    function formatRub(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return '-';
      }
      return `${num.toFixed(2)} RUB`;
    }

    function formatPrice(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return '-';
      }
      return num.toFixed(4);
    }

    function formatPct(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return '-';
      }
      const sign = num > 0 ? '+' : '';
      return `${sign}${num.toFixed(2)}%`;
    }

    function formatStrength(value) {
      const raw = String(value || '').toUpperCase();
      const map = { HIGH: 'СИЛЬНЫЙ', MEDIUM: 'СРЕДНИЙ', LOW: 'СЛАБЫЙ' };
      return map[raw] || raw || '-';
    }

    function formatRuntimeState(value) {
      const raw = String(value || '').toLowerCase();
      const map = {
        starting: 'СТАРТ',
        running: 'РАБОТАЕТ',
        api_error: 'СБОЙ API',
        internal_error: 'ВНУТРЕННЯЯ ОШИБКА',
        stopped_after_errors: 'ОСТАНОВЛЕН',
        startup_api_retry: 'ПОВТОР API',
        startup_internal_retry: 'ПОВТОР СТАРТА',
      };
      return map[raw] || (value || '-');
    }

    function formatEventLabel(value) {
      const raw = String(value || '').toUpperCase();
      const map = { OPEN: 'ОТКРЫТИЕ', CLOSE: 'ЗАКРЫТИЕ' };
      return map[raw] || raw || '-';
    }

    function formatBiasLabel(value) {
      const raw = String(value || '').toUpperCase();
      if (!raw || raw === 'NEUTRAL') return 'НЕЙТРАЛЬНО';
      const [bias, strength] = raw.split('/');
      const biasMap = { LONG: 'ЛОНГ', SHORT: 'ШОРТ', BLOCK: 'БЛОК' };
      if (strength) {
        return `${biasMap[bias] || bias} / ${formatStrength(strength)}`;
      }
      return biasMap[raw] || raw;
    }

    function formatSessionLabel(value) {
      const raw = String(value || '').toUpperCase();
      const map = {
        MORNING: 'УТРО',
        DAY: 'ДЕНЬ',
        EVENING: 'ВЕЧЕР',
        CLOSED: 'ЗАКРЫТО',
        WEEKEND: 'ВЫХОДНОЙ',
      };
      return map[raw] || raw || '-';
    }

    function humanizeNewsReason(value) {
      const raw = String(value || '').trim();
      if (!raw) return '-';
      if (raw.startsWith('keywords=')) {
        const parts = raw.slice(9).split(',').map((item) => item.trim()).filter(Boolean);
        return parts.length ? `Ключевые темы: ${parts.join(', ')}` : '-';
      }
      return raw;
    }

    function closeNewsPopover() {
      const popover = document.getElementById('newsPopover');
      if (!popover) return;
      popover.classList.remove('open');
      popover.setAttribute('aria-hidden', 'true');
    }

    function openNewsPopover(trigger) {
      const popover = document.getElementById('newsPopover');
      const title = document.getElementById('newsPopoverTitle');
      const text = document.getElementById('newsPopoverText');
      if (!popover || !title || !text) return;

      const source = trigger.dataset.source || 'Новость';
      const newsText = trigger.dataset.newsText || 'Текст новости недоступен.';
      title.textContent = source;
      text.textContent = newsText;

      const rect = trigger.getBoundingClientRect();
      const top = Math.min(rect.bottom + 10, window.innerHeight - 220);
      const left = Math.min(rect.left, window.innerWidth - Math.min(420, window.innerWidth - 32) - 16);
      popover.style.top = `${Math.max(16, top)}px`;
      popover.style.left = `${Math.max(16, left)}px`;
      popover.classList.add('open');
      popover.setAttribute('aria-hidden', 'false');
    }

    function filterTradeRows(rows) {
      const select = document.getElementById('eventStatusFilter');
      if (!select) return rows;
      const value = select.value || 'all';
      if (value === 'all') return rows;
      return rows.filter((row) => String(row.event_status || '').toLowerCase() === value);
    }

    function markdownToHtml(value) {
      const text = String(value || '')
        .replace(/\\r\\n/g, '\\n')
        .replace(/^```(?:markdown|md)?\\s*$/gim, '')
        .replace(/^```\\s*$/gm, '')
        .trim();
      if (!text) return '<span class="muted">AI-review для выбранной даты пока не найден.</span>';
      const inlineMarkdown = (raw) => escapeHtml(raw)
        .replace(/\\*\\*([^*\\n]+?)\\*\\*/g, '<strong>$1</strong>')
        .replace(/`([^`\\n]+?)`/g, '<code>$1</code>');
      const blocks = [];
      let paragraph = [];
      let listItems = [];

      const flushParagraph = () => {
        if (!paragraph.length) return;
        blocks.push(`<p>${inlineMarkdown(paragraph.join(' '))}</p>`);
        paragraph = [];
      };
      const flushList = () => {
        if (!listItems.length) return;
        blocks.push(`<ul>${listItems.map((item) => `<li>${inlineMarkdown(item)}</li>`).join('')}</ul>`);
        listItems = [];
      };

      text.split('\\n').forEach((line) => {
        const trimmed = line.trim();
        if (!trimmed || trimmed === '---') {
          flushParagraph();
          flushList();
          return;
        }
        const heading = trimmed.match(/^(#{1,3})\\s+(.+)$/);
        if (heading) {
          flushParagraph();
          flushList();
          const level = heading[1].length;
          blocks.push(`<h${level}>${inlineMarkdown(heading[2])}</h${level}>`);
          return;
        }
        const bullet = trimmed.match(/^[-*]\\s+(.+)$/);
        if (bullet) {
          flushParagraph();
          listItems.push(bullet[1]);
          return;
        }
        flushList();
        paragraph.push(trimmed);
      });
      flushParagraph();
      flushList();
      return blocks.join('');
    }

    function renderPnlChart(series, selectedDate) {
      const canvas = document.getElementById('pnlChart');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const width = canvas.width;
      const height = canvas.height;
      ctx.clearRect(0, 0, width, height);

      if (!Array.isArray(series) || !series.length) {
        ctx.fillStyle = '#7f95b3';
        ctx.font = '14px Manrope';
        ctx.fillText('История по дням пока пуста.', 24, 40);
        return;
      }

      const values = [];
      for (const item of series) {
        values.push(Number(item.pnl_rub || 0));
        values.push(Number(item.cumulative_pnl_rub || 0));
      }
      const min = Math.min(...values, 0);
      const max = Math.max(...values, 0);
      const range = Math.max(1, max - min);
      const left = 56;
      const right = width - 24;
      const top = 18;
      const bottom = height - 44;
      const plotWidth = right - left;
      const plotHeight = bottom - top;

      const yFor = (value) => bottom - ((value - min) / range) * plotHeight;
      const xFor = (index) => left + (plotWidth / Math.max(1, series.length - 1)) * index;
      const zeroY = yFor(0);

      ctx.strokeStyle = 'rgba(102, 174, 255, 0.14)';
      ctx.lineWidth = 1;
      for (let i = 0; i < 4; i += 1) {
        const y = top + (plotHeight / 3) * i;
        ctx.beginPath();
        ctx.moveTo(left, y);
        ctx.lineTo(right, y);
        ctx.stroke();
      }

      ctx.strokeStyle = 'rgba(255,255,255,0.10)';
      ctx.beginPath();
      ctx.moveTo(left, zeroY);
      ctx.lineTo(right, zeroY);
      ctx.stroke();

      ctx.fillStyle = '#7f95b3';
      ctx.font = '12px JetBrains Mono';
      ctx.fillText(`${max.toFixed(0)} RUB`, 4, top + 6);
      ctx.fillText(`${min.toFixed(0)} RUB`, 4, bottom);

      const drawLine = (key, color) => {
        ctx.strokeStyle = color;
        ctx.lineWidth = 3;
        ctx.beginPath();
        series.forEach((item, idx) => {
          const x = xFor(idx);
          const y = yFor(Number(item[key] || 0));
          if (idx === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();

        series.forEach((item, idx) => {
          const x = xFor(idx);
          const y = yFor(Number(item[key] || 0));
          ctx.fillStyle = item.date === selectedDate ? '#ffffff' : color;
          ctx.beginPath();
          ctx.arc(x, y, item.date === selectedDate ? 5 : 3.5, 0, Math.PI * 2);
          ctx.fill();
        });
      };

      drawLine('pnl_rub', '#43c5ff');
      drawLine('cumulative_pnl_rub', '#37e6a4');

      ctx.fillStyle = '#9db1cb';
      ctx.font = '11px JetBrains Mono';
      series.forEach((item, idx) => {
        const x = xFor(idx);
        ctx.fillText(String(item.date || '').slice(5), x - 18, height - 16);
      });
    }

    async function loadData() {
      const dateInput = document.getElementById('selectedDate');
      const selectedDate = dateInput && dateInput.value ? dateInput.value : '';
      const response = await fetch(`/api/dashboard${selectedDate ? `?date=${encodeURIComponent(selectedDate)}` : ''}`);
      const data = await response.json();
      instrumentNames = { ...instrumentNames, ...(data.instrument_catalog || {}) };

      if (dateInput && data.daily && data.daily.selected_date) {
        dateInput.value = data.daily.selected_date;
      }

      document.getElementById('realized').textContent = `${data.summary.realized_pnl_rub.toFixed(2)} RUB`;
      document.getElementById('openCount').textContent = data.summary.open_positions.length;
      document.getElementById('serviceState').textContent = formatRuntimeState(data.runtime?.state || data.service.active);
      document.getElementById('symbolsTotal').textContent = data.summary.symbols_total;
      document.getElementById('generatedAt').textContent = `Обновление: ${data.generated_at_moscow || '-'}`;

      const portfolio = data.portfolio || {};
      const selectedDateLabel = portfolio.selected_date_moscow ? ` | Дата отчёта: ${portfolio.selected_date_moscow}` : '';
      document.getElementById('portfolioGeneratedAt').textContent = `Срез портфеля: ${portfolio.generated_at_moscow || '-'}${selectedDateLabel}`;
      document.getElementById('portfolioMode').textContent = portfolio.mode === 'DRY_RUN' ? 'ТЕСТ' : (portfolio.mode || '-');
      document.getElementById('portfolioTotal').textContent = formatRub(portfolio.total_portfolio_rub);
      document.getElementById('portfolioFree').textContent = formatRub(portfolio.free_cash_rub ?? portfolio.free_rub);
      document.getElementById('portfolioBlocked').textContent = formatRub(portfolio.blocked_guarantee_rub);
      document.getElementById('portfolioRealized').textContent = formatRub(portfolio.bot_closed_net_pnl_rub ?? portfolio.bot_realized_pnl_rub);
      document.getElementById('portfolioActualFee').textContent = formatRub(portfolio.bot_actual_fee_rub);
      document.getElementById('portfolioActualVm').textContent = formatRub(portfolio.bot_actual_varmargin_rub);
      document.getElementById('portfolioCashEffect').textContent = formatRub(portfolio.bot_operations_cash_effect_rub ?? portfolio.bot_actual_cash_effect_rub);
      document.getElementById('portfolioVariation').textContent = formatRub(portfolio.bot_estimated_variation_margin_rub);
      document.getElementById('portfolioOpenLive').textContent = formatRub(portfolio.bot_open_positions_live_pnl_rub ?? portfolio.bot_broker_day_pnl_rub);
      document.getElementById('portfolioTotalVm').textContent = formatRub(portfolio.bot_total_variation_margin_rub ?? portfolio.bot_total_varmargin_rub);
      document.getElementById('portfolioTotalPnl').textContent = formatRub(portfolio.bot_analytical_total_pnl_rub ?? portfolio.bot_total_pnl_rub);
      document.getElementById('portfolioOpenCount').textContent = portfolio.open_positions_count ?? '-';
      const capitalAlert = data.capital_alert || {};
      const capitalPanel = document.getElementById('capitalAlertPanel');
      if (capitalPanel && capitalAlert.active) {
        capitalPanel.classList.remove('is-hidden');
        document.getElementById('capitalAlertTitle').textContent = capitalAlert.title || 'Не хватает капитала для части сделок';
        document.getElementById('capitalAlertMessage').textContent = capitalAlert.message || '-';
        document.getElementById('capitalAlertCount').textContent = `Задето инструментов: ${capitalAlert.count ?? 0}`;
        const symbolsText = Array.isArray(capitalAlert.symbols) && capitalAlert.symbols.length
          ? `Инструменты: ${capitalAlert.symbols.join(', ')}`
          : 'Инструменты: -';
        document.getElementById('capitalAlertSymbols').textContent = symbolsText;
      } else if (capitalPanel) {
        capitalPanel.classList.add('is-hidden');
      }

      const daily = data.daily || {};
      const daySelected = daily.selected || {};
      document.getElementById('dayPnlRub').textContent = formatRub(daySelected.pnl_rub);
      document.getElementById('dayPnlPct').textContent = formatPct(daySelected.pnl_pct);
      document.getElementById('dayClosedCount').textContent = daySelected.closed_count ?? 0;
      document.getElementById('cumPnlRub').textContent = formatRub(daySelected.cumulative_pnl_rub);
      document.getElementById('cumPnlPct').textContent = formatPct(daySelected.cumulative_pnl_pct);
      renderPnlChart(daily.series || [], daily.selected_date || '');

      const runtime = data.runtime || {};
      document.getElementById('runtimeUpdatedAt').textContent = `Runtime: ${runtime.updated_at_moscow || '-'}`;
      document.getElementById('runtimeState').textContent = formatRuntimeState(runtime.state || '-');
      document.getElementById('runtimeSession').textContent = formatSessionLabel(runtime.session || '-');
      document.getElementById('runtimeCycles').textContent = runtime.cycle_count ?? '-';
      document.getElementById('runtimeErrors').textContent = runtime.consecutive_errors ?? '-';

      const runtimeBody = document.querySelector('#runtimeTable tbody');
      runtimeBody.innerHTML = `
        <tr><td>Бот</td><td>${signalBadge(data.health.bot_service.active || '-')}</td></tr>
        <tr><td>Панель</td><td>${signalBadge(data.health.dashboard_service.active || '-')}</td></tr>
        <tr><td>Health</td><td>${data.health.ok ? '<span class="good mono">OK</span>' : '<span class="bad mono">FAIL</span>'}</td></tr>
        <tr><td>Runtime stale</td><td>${data.health.runtime_stale ? '<span class="bad mono">ДА</span>' : '<span class="good mono">НЕТ</span>'}</td></tr>
        <tr><td>Возраст цикла</td><td class="mono">${data.health.runtime_heartbeat_age_seconds ?? '-'} сек</td></tr>
        <tr><td>Инструментов</td><td class="mono">${data.health.symbols_count}</td></tr>
        <tr><td>Срез health</td><td class="mono">${escapeHtml(data.health.generated_at_moscow || '-')}</td></tr>
        <tr><td>Режим</td><td>${escapeHtml(runtime.mode === 'DRY_RUN' ? 'ТЕСТ' : (runtime.mode || '-'))}</td></tr>
        <tr><td>Старт</td><td class="mono">${escapeHtml(runtime.started_at_moscow || '-')}</td></tr>
        <tr><td>Последний цикл</td><td class="mono">${escapeHtml(runtime.last_cycle_at_moscow || '-')}</td></tr>
        <tr><td>Последняя ошибка</td><td class="reason">${escapeHtml(runtime.last_error || '-')}</td></tr>
      `;

      const news = data.news || {};
      const activeBiases = Array.isArray(news.active_biases) ? news.active_biases : [];
      document.getElementById('newsUpdatedAt').textContent = `Новости: ${news.fetched_at_moscow || '-'}`;
      document.getElementById('newsCount').textContent = activeBiases.length;
      document.getElementById('newsLongCount').textContent = activeBiases.filter((item) => item.bias === 'LONG').length;
      document.getElementById('newsShortCount').textContent = activeBiases.filter((item) => item.bias === 'SHORT').length;
      document.getElementById('newsBlockCount').textContent = activeBiases.filter((item) => item.bias === 'BLOCK').length;

      const newsBody = document.querySelector('#newsTable tbody');
      const newsCards = document.getElementById('newsCards');
      newsBody.innerHTML = '';
      newsCards.innerHTML = '';
      for (const item of activeBiases) {
        const hasMessage = String(item.message_text || '').trim().length > 0;
        const reasonText = humanizeNewsReason(item.reason || '-');
        const sourceLabel = String(item.source || '-').replaceAll('_', ' ');
        const detailsButton = hasMessage
          ? `<button type="button" class="hint-button js-news-popover" data-source="${escapeHtml(sourceLabel)}" data-news-text="${escapeHtml(item.message_text)}">текст</button>`
          : '';
        newsBody.insertAdjacentHTML('beforeend', `<tr>
          <td>${renderInstrumentLabel(item.symbol || '-', item.display_name || '')}</td>
          <td>${signalBadge(item.bias || '-')}</td>
          <td>${escapeHtml(formatStrength(item.strength || '-'))}</td>
          <td>${escapeHtml(item.source || '-')}</td>
          <td class="mono">${escapeHtml(item.expires_at_moscow || '-')}</td>
          <td><div class="news-reason"><span class="reason">${escapeHtml(reasonText)}</span>${detailsButton}</div></td>
        </tr>`);
        newsCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title">${renderInstrumentLabel(item.symbol || '-', item.display_name || '')}</div>
            ${signalBadge(item.bias || '-')}
          </div>
          <div class="mobile-card-grid">
            <div class="mobile-card-item"><span class="muted">Сила</span><div class="mobile-card-value">${escapeHtml(formatStrength(item.strength || '-'))}</div></div>
            <div class="mobile-card-item"><span class="muted">Источник</span><div class="mobile-card-value">${escapeHtml(item.source || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Актуально до</span><div class="mobile-card-value mono">${escapeHtml(item.expires_at_moscow || '-')}</div></div>
          </div>
          <div class="mobile-card-footer">
            <div class="mobile-card-text"><span class="muted">Причина</span><br>${escapeHtml(reasonText)}</div>
            ${hasMessage ? `<div class="mobile-card-text">${detailsButton}</div>` : ''}
          </div>
        </article>`);
      }
      if (!activeBiases.length) {
        newsBody.insertAdjacentHTML('beforeend', '<tr><td colspan="6" class="muted">Активных news bias сейчас нет.</td></tr>');
        newsCards.insertAdjacentHTML('beforeend', '<div class="muted">Активных news bias сейчас нет.</div>');
      }

      const posBody = document.querySelector('#positionsTable tbody');
      const posCards = document.getElementById('positionsCards');
      posBody.innerHTML = '';
      posCards.innerHTML = '';
      for (const pos of data.summary.open_positions) {
        const vm = Number(pos.variation_margin_rub || 0);
        const pct = Number(pos.pnl_pct || 0);
        const vmClass = vm > 0 ? 'good' : vm < 0 ? 'bad' : 'muted';
        const pctClass = pct > 0 ? 'good' : pct < 0 ? 'bad' : 'muted';
        posBody.insertAdjacentHTML('beforeend', `<tr>
          <td>${renderInstrumentLabel(pos.symbol, pos.display_name || '')}</td>
          <td>${signalBadge(pos.side)}</td>
          <td class="mono">${escapeHtml(pos.qty)}</td>
          <td class="mono">${escapeHtml(formatPrice(pos.entry_price))}</td>
          <td class="mono">${escapeHtml(formatPrice(pos.current_price))}</td>
          <td class="mono right">${escapeHtml(formatRub(pos.notional_rub))}</td>
          <td class="mono right ${vmClass}">${escapeHtml(formatRub(pos.variation_margin_rub))}</td>
          <td class="mono right ${pctClass}">${escapeHtml(formatPct(pos.pnl_pct))}</td>
          <td>${escapeHtml(pos.strategy)}</td>
          <td>${signalBadge(pos.last_signal)}</td>
        </tr>`);
        posCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title">${renderInstrumentLabel(pos.symbol, pos.display_name || '')}</div>
            ${signalBadge(pos.side)}
          </div>
          <div class="mobile-card-grid">
            <div class="mobile-card-item"><span class="muted">Лоты</span><div class="mobile-card-value mono">${escapeHtml(pos.qty)}</div></div>
            <div class="mobile-card-item"><span class="muted">Сигнал</span><div class="mobile-card-value">${signalBadge(pos.last_signal)}</div></div>
            <div class="mobile-card-item"><span class="muted">Вход</span><div class="mobile-card-value mono">${escapeHtml(formatPrice(pos.entry_price))}</div></div>
            <div class="mobile-card-item"><span class="muted">Текущая</span><div class="mobile-card-value mono">${escapeHtml(formatPrice(pos.current_price))}</div></div>
            <div class="mobile-card-item"><span class="muted">Стоимость</span><div class="mobile-card-value mono">${escapeHtml(formatRub(pos.notional_rub))}</div></div>
            <div class="mobile-card-item"><span class="muted">Изм. %</span><div class="mobile-card-value mono ${pctClass}">${escapeHtml(formatPct(pos.pnl_pct))}</div></div>
            <div class="mobile-card-item"><span class="muted">Вар. маржа</span><div class="mobile-card-value mono ${vmClass}">${escapeHtml(formatRub(pos.variation_margin_rub))}</div></div>
            <div class="mobile-card-item"><span class="muted">Стратегия</span><div class="mobile-card-value">${escapeHtml(pos.strategy)}</div></div>
          </div>
        </article>`);
      }
      if (!data.summary.open_positions.length) {
        posBody.insertAdjacentHTML('beforeend', '<tr><td colspan="10" class="muted">Открытых позиций нет.</td></tr>');
        posCards.insertAdjacentHTML('beforeend', '<div class="muted">Открытых позиций нет.</div>');
      }

      const signalBody = document.querySelector('#signalsTable tbody');
      const signalCards = document.getElementById('signalsCards');
      signalBody.innerHTML = '';
      signalCards.innerHTML = '';
      for (const [symbol, state] of Object.entries(data.states)) {
        const summary = Array.isArray(state.last_signal_summary) && state.last_signal_summary.length
          ? state.last_signal_summary[0]
          : (state.last_error || '-');
        const allocatorSummary = state.last_allocator_summary || 'Нет активного расчёта размера позиции.';
        signalBody.insertAdjacentHTML('beforeend', `<tr>
          <td>${renderInstrumentLabel(symbol, state.display_name || '')}</td>
          <td>${signalBadge(state.last_signal || '-')}</td>
          <td>${escapeHtml(state.last_strategy_name || state.entry_strategy || '-')}</td>
          <td>${signalBadge(state.last_higher_tf_bias || '-')}</td>
          <td>${escapeHtml(formatBiasLabel(state.last_news_bias || 'NEUTRAL'))}</td>
          <td class="reason">${escapeHtml(state.last_news_impact || '-')}</td>
          <td class="reason">${escapeHtml(allocatorSummary)}</td>
          <td class="reason">${escapeHtml(summary)}</td>
        </tr>`);
        signalCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title">${renderInstrumentLabel(symbol, state.display_name || '')}</div>
            ${signalBadge(state.last_signal || '-')}
          </div>
          <div class="mobile-card-grid">
            <div class="mobile-card-item"><span class="muted">Стратегия</span><div class="mobile-card-value">${escapeHtml(state.last_strategy_name || state.entry_strategy || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Старший ТФ</span><div class="mobile-card-value">${signalBadge(state.last_higher_tf_bias || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Новости</span><div class="mobile-card-value">${escapeHtml(formatBiasLabel(state.last_news_bias || 'NEUTRAL'))}</div></div>
            <div class="mobile-card-item"><span class="muted">Влияние</span><div class="mobile-card-value">${escapeHtml(state.last_news_impact || '-')}</div></div>
          </div>
          <div class="mobile-card-footer">
            <div class="mobile-card-text"><span class="muted">Аллокатор</span><br>${escapeHtml(allocatorSummary)}</div>
          </div>
          <div class="mobile-card-footer">
            <div class="mobile-card-text"><span class="muted">Ключевая причина</span><br>${escapeHtml(summary)}</div>
          </div>
        </article>`);
      }

      const manual = data.manual_instruments || {};
      const templateSelect = document.getElementById('manualInstrumentTemplate');
      if (templateSelect) {
        const currentValue = templateSelect.value;
        const options = Array.isArray(manual.templates) ? manual.templates : [];
        templateSelect.innerHTML = options.length
          ? options.map((item) => {
              const primary = Array.isArray(item.primary_strategies) && item.primary_strategies.length
                ? item.primary_strategies.join(', ')
                : 'без стратегий';
              return `<option value="${escapeHtml(item.symbol || '')}">${escapeHtml(item.symbol || '')} → ${escapeHtml(primary)}</option>`;
            }).join('')
          : '<option value="">Нет доступных шаблонов</option>';
        if (currentValue && options.some((item) => item.symbol === currentValue)) {
          templateSelect.value = currentValue;
        }
      }
      const manualList = document.getElementById('manualInstrumentList');
      if (manualList) {
        const items = Array.isArray(manual.custom_instruments) ? manual.custom_instruments : [];
        manualList.textContent = items.length
          ? `Ручные инструменты: ${items.map((item) => `${item.symbol} как ${item.clone_from}`).join(' | ')}`
          : 'Ручных инструментов пока нет.';
      }

      const tradeBody = document.querySelector('#tradesTable tbody');
      const tradeCards = document.getElementById('tradesCards');
      tradeBody.innerHTML = '';
      tradeCards.innerHTML = '';
      const filteredTrades = filterTradeRows((data.trades || []).slice().reverse());
      for (const row of filteredTrades) {
        const isOpenEvent = String(row.event || '').toUpperCase() === 'OPEN';
        const pnl = row.pnl_rub ?? '-';
        const pnlNum = Number(pnl);
        const pnlClass = Number.isFinite(pnlNum) ? (pnlNum >= 0 ? 'good' : 'bad') : 'muted';
        const grossText = isOpenEvent ? 'не применяется' : (row.gross_pnl_rub ?? '-');
        const commissionText = isOpenEvent
          ? (row.commission_rub ?? 'уточняется')
          : (row.commission_rub ?? '-');
        const netText = isOpenEvent ? 'не применяется' : (row.net_pnl_rub ?? pnl);
        tradeBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(row.time || '-')}</td>
          <td>${renderInstrumentLabel(row.symbol || '-', row.display_name || '')}</td>
          <td>${escapeHtml(formatEventLabel(row.event || '-'))}</td>
          <td>${eventStatusBadge(row.event_status || 'history')}</td>
          <td>${signalBadge(row.side || '-')}</td>
          <td class="mono">${escapeHtml(row.qty_lots || '-')}</td>
          <td class="mono right">${escapeHtml(row.price ?? '-')}</td>
          <td class="mono right">${escapeHtml(grossText)}</td>
          <td class="mono right">${escapeHtml(commissionText)}</td>
          <td class="mono right ${pnlClass}">${escapeHtml(netText)}</td>
          <td>${escapeHtml(row.strategy || '-')}</td>
          <td class="reason">${escapeHtml(row.reason_display || row.reason || '-')}<br><span class="muted">${escapeHtml(row.context_display || '-')}</span></td>
        </tr>`);
        tradeCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title">${renderInstrumentLabel(row.symbol || '-', row.display_name || '')}</div>
            ${eventStatusBadge(row.event_status || 'history')}
          </div>
          <div class="mobile-card-grid">
            <div class="mobile-card-item"><span class="muted">Время</span><div class="mobile-card-value mono">${escapeHtml(row.time || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Событие</span><div class="mobile-card-value">${escapeHtml(formatEventLabel(row.event || '-'))}</div></div>
            <div class="mobile-card-item"><span class="muted">Сторона</span><div class="mobile-card-value">${signalBadge(row.side || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Лоты</span><div class="mobile-card-value mono">${escapeHtml(row.qty_lots || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Цена</span><div class="mobile-card-value mono">${escapeHtml(row.price ?? '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Gross</span><div class="mobile-card-value mono">${escapeHtml(grossText)}</div></div>
            <div class="mobile-card-item"><span class="muted">Комиссия</span><div class="mobile-card-value mono">${escapeHtml(commissionText)}</div></div>
            <div class="mobile-card-item"><span class="muted">Net</span><div class="mobile-card-value mono ${pnlClass}">${escapeHtml(netText)}</div></div>
            <div class="mobile-card-item"><span class="muted">Стратегия</span><div class="mobile-card-value">${escapeHtml(row.strategy || '-')}</div></div>
          </div>
          <div class="mobile-card-footer">
            <div class="mobile-card-text"><span class="muted">Причина</span><br>${escapeHtml(row.reason_display || row.reason || '-')}</div>
            <div class="mobile-card-text"><span class="muted">Контекст</span><br>${escapeHtml(row.context_display || '-')}</div>
          </div>
        </article>`);
      }
      if (!filteredTrades.length) {
        tradeBody.insertAdjacentHTML('beforeend', '<tr><td colspan="12" class="muted">Журнал сделок пока пуст.</td></tr>');
        tradeCards.insertAdjacentHTML('beforeend', '<div class="muted">Журнал сделок пока пуст.</div>');
      }

      const review = data.trade_review || {};
      document.getElementById('reviewClosed').textContent = review.closed_count ?? 0;
      document.getElementById('reviewWins').textContent = review.wins ?? 0;
      document.getElementById('reviewLosses').textContent = review.losses ?? 0;
      document.getElementById('reviewPnl').textContent = formatRub(review.closed_total_pnl_rub);
      document.getElementById('reviewWinRate').textContent = `${Number(review.win_rate || 0).toFixed(1)}%`;
      document.getElementById('reviewBestSymbol').textContent = review.best_symbol ? `${review.best_symbol.symbol} (${Number(review.best_symbol.pnl_rub).toFixed(2)})` : '-';
      document.getElementById('reviewWorstSymbol').textContent = review.worst_symbol ? `${review.worst_symbol.symbol} (${Number(review.worst_symbol.pnl_rub).toFixed(2)})` : '-';
      document.getElementById('reviewBestStrategy').textContent = review.best_strategy ? `${review.best_strategy.strategy} (${Number(review.best_strategy.pnl_rub).toFixed(2)})` : '-';
      document.getElementById('reviewWorstStrategy').textContent = review.worst_strategy ? `${review.worst_strategy.strategy} (${Number(review.worst_strategy.pnl_rub).toFixed(2)})` : '-';
      document.getElementById('reviewBestRegime').textContent = review.best_regime ? `${review.best_regime.regime} (${Number(review.best_regime.pnl_rub).toFixed(2)})` : '-';
      document.getElementById('reviewWorstRegime').textContent = review.worst_regime ? `${review.worst_regime.regime} (${Number(review.worst_regime.pnl_rub).toFixed(2)})` : '-';
      document.getElementById('reviewBestStrategyRegime').textContent = review.best_strategy_regime ? `${review.best_strategy_regime.label} (${Number(review.best_strategy_regime.pnl_rub).toFixed(2)})` : '-';
      document.getElementById('reviewWorstStrategyRegime').textContent = review.worst_strategy_regime ? `${review.worst_strategy_regime.label} (${Number(review.worst_strategy_regime.pnl_rub).toFixed(2)})` : '-';

      const reviewBody = document.querySelector('#reviewTable tbody');
      const reviewCards = document.getElementById('reviewCards');
      reviewBody.innerHTML = '';
      reviewCards.innerHTML = '';
      for (const row of (review.closed_reviews || [])) {
        const pnlNum = Number(row.pnl_rub);
        const pnlClass = Number.isFinite(pnlNum) ? (pnlNum >= 0 ? 'good' : 'bad') : 'muted';
        reviewBody.insertAdjacentHTML('beforeend', `<tr>
          <td>${renderInstrumentLabel(row.symbol || '-', row.display_name || '')}</td>
          <td>${signalBadge(row.side || '-')}</td>
          <td>${escapeHtml(row.strategy || '-')}</td>
          <td class="mono">${escapeHtml(row.entry_time || '-')}</td>
          <td class="mono">${escapeHtml(row.exit_time || '-')}</td>
          <td class="mono right">${escapeHtml(row.gross_pnl_rub ?? '-')}</td>
          <td class="mono right">${escapeHtml(row.commission_rub ?? '-')}</td>
          <td class="mono right ${pnlClass}">${escapeHtml(row.net_pnl_rub ?? row.pnl_rub ?? '-')}</td>
          <td class="reason">${escapeHtml(row.exit_reason || '-')}<br><span class="muted">${escapeHtml(row.exit_context_display || '-')}</span></td>
          <td>${escapeHtml(row.verdict || '-')}</td>
        </tr>`);
        reviewCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title">${renderInstrumentLabel(row.symbol || '-', row.display_name || '')}</div>
            ${signalBadge(row.side || '-')}
          </div>
          <div class="mobile-card-grid">
            <div class="mobile-card-item"><span class="muted">Стратегия</span><div class="mobile-card-value">${escapeHtml(row.strategy || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Gross</span><div class="mobile-card-value mono">${escapeHtml(row.gross_pnl_rub ?? '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Комиссия</span><div class="mobile-card-value mono">${escapeHtml(row.commission_rub ?? '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Net</span><div class="mobile-card-value mono ${pnlClass}">${escapeHtml(row.net_pnl_rub ?? row.pnl_rub ?? '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Вход</span><div class="mobile-card-value mono">${escapeHtml(row.entry_time || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Выход</span><div class="mobile-card-value mono">${escapeHtml(row.exit_time || '-')}</div></div>
          </div>
          <div class="mobile-card-footer">
            <div class="mobile-card-text"><span class="muted">Причина выхода</span><br>${escapeHtml(row.exit_reason || '-')}</div>
            <div class="mobile-card-text"><span class="muted">Контекст</span><br>${escapeHtml(row.exit_context_display || '-')}</div>
            <div class="mobile-card-text"><span class="muted">Вердикт</span><br>${escapeHtml(row.verdict || '-')}</div>
          </div>
        </article>`);
      }
      if (!(review.closed_reviews || []).length) {
        const currentOpen = Array.isArray(review.current_open) ? review.current_open : [];
        const hint = currentOpen.length
          ? `Закрытых сделок пока нет. Сейчас открыто позиций: ${currentOpen.length}.`
          : 'Закрытых сделок пока нет.';
        reviewBody.insertAdjacentHTML('beforeend', `<tr><td colspan="10" class="muted">${escapeHtml(hint)}</td></tr>`);
        reviewCards.insertAdjacentHTML('beforeend', `<div class="muted">${escapeHtml(hint)}</div>`);
        for (const row of currentOpen) {
          const openCommissionText = row.commission_rub ?? '-';
          reviewBody.insertAdjacentHTML('beforeend', `<tr>
            <td>${renderInstrumentLabel(row.symbol || '-', row.display_name || '')}</td>
            <td>${signalBadge(row.side || '-')}</td>
            <td>${escapeHtml(row.strategy || '-')}</td>
            <td class="mono">${escapeHtml(row.time || '-')}</td>
            <td class="mono">в позиции</td>
            <td class="mono right">-</td>
            <td class="mono right">${escapeHtml(openCommissionText)}</td>
            <td class="mono right">-</td>
            <td class="reason">${escapeHtml(row.reason_display || row.reason || 'позиция открыта')}<br><span class="muted">${escapeHtml(row.context_display || '-')}</span></td>
            <td>открыта</td>
          </tr>`);
          reviewCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
            <div class="mobile-card-head">
              <div class="mobile-card-title">${renderInstrumentLabel(row.symbol || '-', row.display_name || '')}</div>
              ${signalBadge(row.side || '-')}
            </div>
            <div class="mobile-card-grid">
              <div class="mobile-card-item"><span class="muted">Стратегия</span><div class="mobile-card-value">${escapeHtml(row.strategy || '-')}</div></div>
              <div class="mobile-card-item"><span class="muted">Статус</span><div class="mobile-card-value">открыта</div></div>
              <div class="mobile-card-item"><span class="muted">Время входа</span><div class="mobile-card-value mono">${escapeHtml(row.time || '-')}</div></div>
              <div class="mobile-card-item"><span class="muted">Цена входа</span><div class="mobile-card-value mono">${escapeHtml(row.price || '-')}</div></div>
              <div class="mobile-card-item"><span class="muted">Комиссия входа</span><div class="mobile-card-value mono">${escapeHtml(openCommissionText)}</div></div>
            </div>
            <div class="mobile-card-footer">
              <div class="mobile-card-text"><span class="muted">Причина</span><br>${escapeHtml(row.reason_display || row.reason || 'позиция открыта')}</div>
              <div class="mobile-card-text"><span class="muted">Контекст</span><br>${escapeHtml(row.context_display || '-')}</div>
            </div>
          </article>`);
        }
      }

const aiReview = data.ai_review || {};
      document.getElementById('aiReviewMeta').textContent = aiReview.available
        ? `AI review: ${aiReview.source || '-'} • обновлено ${aiReview.updated_at_moscow || '-'}`
        : `AI review: пока нет${aiReview.updated_at_moscow ? ` • последняя попытка ${aiReview.updated_at_moscow}` : ''}`;
      document.getElementById('aiReviewContent').innerHTML = markdownToHtml(aiReview.content || '');
      const aiFollowupsEl = document.getElementById('aiReviewFollowups');
      if (aiFollowupsEl) {
        const followups = Array.isArray(aiReview.followups) ? aiReview.followups : [];
        aiFollowupsEl.innerHTML = '';
        for (const item of followups.slice().reverse()) {
          aiFollowupsEl.insertAdjacentHTML('beforeend', `
            <div class="glass-card">
              <div class="muted" style="margin-bottom:8px;">Доп. вопрос • ${escapeHtml(item.created_at_moscow || '-')} • ${escapeHtml(item.model || '-')}</div>
              <div style="font-weight:600; margin-bottom:10px;">${escapeHtml(item.question || '-')}</div>
              <div class="prose-review">${markdownToHtml(item.answer || '')}</div>
            </div>
          `);
        }
      }
    }

    async function refreshAIReview() {
      const dateInput = document.getElementById('selectedDate');
      const selectedDate = dateInput && dateInput.value ? dateInput.value : '';
      const btn = document.getElementById('aiReviewRefreshBtn');
      const status = document.getElementById('aiReviewStatus');
      if (!btn || !status) return;
      btn.disabled = true;
      status.textContent = 'Запускаю AI-разбор...';
      try {
        const response = await fetch(`/api/ai-review/refresh${selectedDate ? `?date=${encodeURIComponent(selectedDate)}` : ''}`, {
          method: 'POST',
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || payload.message || 'Не удалось запустить AI-разбор.');
        }
        status.textContent = payload.message || 'AI-разбор запущен.';
        window.setTimeout(loadData, 4000);
      } catch (error) {
        status.textContent = error?.message || 'Не удалось запустить AI-разбор.';
      } finally {
        btn.disabled = false;
      }
    }

    async function askAIReviewFollowup() {
      const dateInput = document.getElementById('selectedDate');
      const selectedDate = dateInput && dateInput.value ? dateInput.value : '';
      const input = document.getElementById('aiReviewFollowupInput');
      const btn = document.getElementById('aiReviewFollowupBtn');
      const status = document.getElementById('aiReviewFollowupStatus');
      const question = input && input.value ? input.value.trim() : '';
      if (!input || !btn || !status) return;
      if (!question) {
        status.textContent = 'Сначала введи вопрос к AI-разбору.';
        return;
      }
      btn.disabled = true;
      status.textContent = 'Запрашиваю дополнительный AI-разбор...';
      try {
        const response = await fetch(`/api/ai-review/followup${selectedDate ? `?date=${encodeURIComponent(selectedDate)}` : ''}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question }),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || payload.message || 'Не удалось получить дополнительный AI-разбор.');
        }
        status.textContent = payload.message || 'Дополнительный AI-разбор готов.';
        input.value = '';
        await loadData();
      } catch (error) {
        status.textContent = error?.message || 'Не удалось получить дополнительный AI-разбор.';
      } finally {
        btn.disabled = false;
      }
    }

    async function recoverTradeOperations() {
      const dateInput = document.getElementById('selectedDate');
      const selectedDate = dateInput && dateInput.value ? dateInput.value : '';
      const btn = document.getElementById('tradeRecoveryBtn');
      const status = document.getElementById('tradeRecoveryStatus');
      if (!btn || !status) return;
      btn.disabled = true;
      status.textContent = 'Восстанавливаю пропавшие операции...';
      try {
        const response = await fetch(`/api/trades/recover${selectedDate ? `?date=${encodeURIComponent(selectedDate)}` : ''}`, {
          method: 'POST',
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || payload.message || 'Не удалось восстановить операции.');
        }
        status.textContent = payload.message || 'Восстановление завершено.';
        window.setTimeout(loadData, 1500);
      } catch (error) {
        status.textContent = error?.message || 'Не удалось восстановить операции.';
      } finally {
        btn.disabled = false;
      }
    }

    async function addManualInstrument() {
      const tickerInput = document.getElementById('manualInstrumentTicker');
      const templateSelect = document.getElementById('manualInstrumentTemplate');
      const status = document.getElementById('manualInstrumentStatus');
      const btn = document.getElementById('manualInstrumentAddBtn');
      if (!tickerInput || !templateSelect || !status || !btn) return;
      const symbol = String(tickerInput.value || '').trim().toUpperCase();
      const cloneFrom = String(templateSelect.value || '').trim().toUpperCase();
      if (!symbol) {
        status.textContent = 'Сначала введи тикер нового инструмента.';
        return;
      }
      if (!cloneFrom) {
        status.textContent = 'Сначала выбери похожий инструмент, от которого копировать стратегии.';
        return;
      }
      btn.disabled = true;
      status.textContent = 'Добавляю инструмент в конфигурацию бота...';
      try {
        const response = await fetch('/api/instruments/add', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ symbol, clone_from: cloneFrom }),
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || payload.message || 'Не удалось добавить инструмент.');
        }
        status.textContent = payload.message || 'Инструмент добавлен.';
        tickerInput.value = '';
        await loadData();
      } catch (error) {
        status.textContent = error?.message || 'Не удалось добавить инструмент.';
      } finally {
        btn.disabled = false;
      }
    }

    document.addEventListener('DOMContentLoaded', () => {
      const filter = document.getElementById('eventStatusFilter');
      const dateInput = document.getElementById('selectedDate');
      if (filter) {
        filter.addEventListener('change', loadData);
      }
      if (dateInput) {
        dateInput.addEventListener('change', loadData);
      }
      const aiRefreshBtn = document.getElementById('aiReviewRefreshBtn');
      if (aiRefreshBtn) {
        aiRefreshBtn.addEventListener('click', refreshAIReview);
      }
      const aiFollowupBtn = document.getElementById('aiReviewFollowupBtn');
      if (aiFollowupBtn) {
        aiFollowupBtn.addEventListener('click', askAIReviewFollowup);
      }
      const tradeRecoveryBtn = document.getElementById('tradeRecoveryBtn');
      if (tradeRecoveryBtn) {
        tradeRecoveryBtn.addEventListener('click', recoverTradeOperations);
      }
      const manualInstrumentAddBtn = document.getElementById('manualInstrumentAddBtn');
      if (manualInstrumentAddBtn) {
        manualInstrumentAddBtn.addEventListener('click', addManualInstrument);
      }
      document.addEventListener('click', (event) => {
        const trigger = event.target.closest('.js-news-popover');
        if (trigger) {
          event.stopPropagation();
          openNewsPopover(trigger);
          return;
        }
        if (!event.target.closest('#newsPopover')) {
          closeNewsPopover();
        }
      });
      loadData();
      setInterval(loadData, 15000);
    });
  </script>
</body>
</html>
"""
    return html.replace("__SITE_NAV__", build_site_nav("dashboard"))


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return build_dashboard_html()


@app.get("/docs", response_class=HTMLResponse)
def docs() -> str:
    return build_docs_html()


@app.get("/contracts", response_class=HTMLResponse)
def contracts() -> str:
    return build_contracts_html()


@app.get("/api/dashboard", response_class=JSONResponse)
def api_dashboard(date: str | None = None) -> dict:
    states = load_states()
    generated_at = datetime.now(timezone.utc)
    portfolio = load_portfolio_snapshot()
    accounting_history = load_accounting_history()
    runtime = load_runtime_status()
    session_name = str((runtime or {}).get("session") or "").upper()
    session_closed = session_name in {"CLOSED", "WEEKEND"}
    broker_positions = {
        str(item.get("symbol", "")): item
        for item in ((portfolio or {}).get("broker_open_positions") or [])
        if str(item.get("symbol", ""))
    }
    display_states: dict[str, dict] = {}
    for symbol, state in states.items():
        item = dict(state)
        if item.get("_state_stale") and symbol not in broker_positions:
            item["last_signal"] = "HOLD"
            item["last_strategy_name"] = item.get("last_strategy_name") or item.get("entry_strategy") or "-"
            if session_closed:
                closed_message = "Вне торговой сессии срочного рынка Мосбиржи."
                item["last_news_impact"] = "торговая сессия закрыта"
                item["last_error"] = closed_message
                existing_summary = [str(part).strip() for part in (item.get("last_signal_summary") or []) if str(part).strip()]
                if not existing_summary:
                    item["last_signal_summary"] = ["Нет актуального расчёта сигнала."]
            else:
                stale_at = item.get("_state_updated_at_moscow") or "-"
                item["last_signal_summary"] = [f"Данные по инструменту устарели: последнее обновление {stale_at}."]
                item["last_news_impact"] = "стейт не обновляется"
                item["last_error"] = f"State stale с {stale_at}"
        display_states[symbol] = item
    target_day = datetime.now(MOSCOW_TZ).date()
    if date:
        try:
            target_day = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            pass
    portfolio_view = build_portfolio_view_for_day(portfolio, target_day, accounting_history)
    trades = annotate_trade_rows(load_trade_rows_for_day(target_day, 200), display_states, broker_positions)
    instrument_catalog = build_instrument_catalog(portfolio, trades)
    return {
        "service": get_bot_service_status(),
        "health": build_health_payload(display_states),
        "capital_alert": build_capital_alert(display_states),
        "portfolio": portfolio_view,
        "runtime": runtime,
        "news": load_news_snapshot(),
        "trade_review": load_trade_review_for_day(target_day, 200, display_states, broker_positions),
        "summary": summarize_states(display_states, portfolio_view),
        "meta": load_meta(),
        "states": display_states,
        "trades": trades,
        "manual_instruments": build_manual_instruments_payload(),
        "instrument_catalog": instrument_catalog,
        "daily": build_daily_performance(portfolio, target_day, accounting_history),
        "ai_review": load_ai_review(target_day),
        "generated_at": generated_at.isoformat(),
        "generated_at_moscow": generated_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
    }


@app.post("/api/ai-review/refresh", response_class=JSONResponse)
def api_ai_review_refresh(date: str | None = None) -> dict:
    target_day: date | None = None
    if date:
        try:
            target_day = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Дата должна быть в формате YYYY-MM-DD.") from error
    return start_ai_review_refresh(target_day)


@app.post("/api/ai-review/followup", response_class=JSONResponse)
def api_ai_review_followup(payload: dict = Body(default={}), date: str | None = None) -> dict:
    target_day = datetime.now(MOSCOW_TZ).date()
    if date:
        try:
            target_day = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Дата должна быть в формате YYYY-MM-DD.") from error
    question = str((payload or {}).get("question") or "").strip()
    return run_ai_review_followup(target_day, question)


@app.post("/api/trades/recover", response_class=JSONResponse)
def api_trades_recover(date: str | None = None) -> dict:
    target_day: date | None = None
    if date:
        try:
            target_day = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Дата должна быть в формате YYYY-MM-DD.") from error
    return run_trade_operations_recovery(target_day)


@app.post("/api/instruments/add", response_class=JSONResponse)
def api_instruments_add(payload: dict = Body(default={})) -> dict:
    symbol = str((payload or {}).get("symbol") or "").strip().upper()
    clone_from = str((payload or {}).get("clone_from") or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="Нужно указать тикер нового инструмента.")
    if not clone_from:
        raise HTTPException(status_code=400, detail="Нужно выбрать существующий инструмент-шаблон.")
    try:
        normalized_symbol = validate_custom_symbol(symbol)
        normalized_clone = validate_custom_symbol(clone_from)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    if normalized_symbol in set(load_base_symbols_from_env()):
        raise HTTPException(status_code=400, detail="Этот тикер уже есть в базовой конфигурации бота.")
    available_templates = set(merge_with_custom_symbols(load_base_symbols_from_env()))
    if normalized_clone not in available_templates:
        raise HTTPException(status_code=400, detail="Выбранный шаблон не найден среди доступных инструментов.")
    try:
        instrument_info = validate_futures_ticker_exists(normalized_symbol)
    except RuntimeError as error:
        raise HTTPException(status_code=400, detail=f"Тикер {normalized_symbol} не прошёл проверку у брокера: {error}") from error
    entry = upsert_custom_instrument(normalized_symbol, normalized_clone, template_symbol=normalized_clone)
    refresh_minutes = max(1, build_manual_instruments_payload().get("watchlist_refresh_seconds", 300) // 60)
    action = "обновлён" if entry.get("status") == "updated" else "добавлен"
    return {
        "ok": True,
        "entry": entry,
        "message": f"Инструмент {normalized_symbol} ({instrument_info['display_name']}) {action}. Бот подхватит его автоматически при ближайшем обновлении watchlist, обычно в течение {refresh_minutes} мин.",
    }


@app.get("/api/contracts", response_class=JSONResponse)
def api_contracts() -> dict:
    return load_contracts_payload()


@app.get("/api/health", response_class=JSONResponse)
def api_health() -> dict:
    states = load_states()
    return build_health_payload(states)


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt() -> str:
    return "User-agent: *\nDisallow: /\n"
