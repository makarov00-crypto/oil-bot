from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "bot_state"
LOG_DIR = BASE_DIR / "logs"
TRADE_JOURNAL_PATH = LOG_DIR / "trade_journal.jsonl"
PORTFOLIO_SNAPSHOT_PATH = STATE_DIR / "_portfolio_snapshot.json"
RUNTIME_STATUS_PATH = STATE_DIR / "_runtime_status.json"
NEWS_SNAPSHOT_PATH = STATE_DIR / "_news_snapshot.json"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


app = FastAPI(title="Oil Bot Dashboard")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_states() -> dict[str, dict]:
    states: dict[str, dict] = {}
    if not STATE_DIR.exists():
        return states
    for path in sorted(STATE_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            states[path.stem] = load_json(path)
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


def load_runtime_status() -> dict:
    if not RUNTIME_STATUS_PATH.exists():
        return {}
    try:
        return load_json(RUNTIME_STATUS_PATH)
    except Exception:
        return {}


def load_news_snapshot() -> dict:
    if not NEWS_SNAPSHOT_PATH.exists():
        return {}
    try:
        return load_json(NEWS_SNAPSHOT_PATH)
    except Exception:
        return {}


def load_trade_rows(limit: int = 50) -> list[dict]:
    if not TRADE_JOURNAL_PATH.exists():
        return []
    rows: list[dict] = []
    try:
        with TRADE_JOURNAL_PATH.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
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
        normalized.append(item)
    return normalized


def annotate_trade_rows(rows: list[dict], states: dict[str, dict]) -> list[dict]:
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

    for item in annotated:
        if str(item.get("event", "")).upper() != "OPEN":
            continue
        if item.get("event_status") == "closed":
            continue
        symbol = str(item.get("symbol", ""))
        side = str(item.get("side", "")).upper()
        state = states.get(symbol, {})
        state_side = str(state.get("position_side", "FLAT")).upper()
        state_qty = int(state.get("position_qty") or 0)
        if state_side == side and state_side != "FLAT" and state_qty > 0:
            item["event_status"] = "active"
        else:
            item["event_status"] = "history"

    for item in annotated:
        item.pop("_row_id", None)

    return annotated


def load_trade_review(limit: int = 80) -> dict:
    rows = load_trade_rows(limit)
    open_by_symbol: dict[str, list[dict]] = {}
    closed_reviews: list[dict] = []

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

    for row in rows:
        symbol = str(row.get("symbol", ""))
        event = str(row.get("event", "")).upper()
        if not symbol:
            continue
        if event == "OPEN":
            open_by_symbol.setdefault(symbol, []).append(row)
            continue
        if event != "CLOSE":
            continue
        open_row = None
        if open_by_symbol.get(symbol):
            open_row = open_by_symbol[symbol].pop(0)
        pnl_value = row.get("pnl_rub")
        try:
            pnl_numeric = float(pnl_value) if pnl_value not in (None, "", "-") else 0.0
        except Exception:
            pnl_numeric = 0.0
        closed_reviews.append(
            {
                "symbol": symbol,
                "side": row.get("side") or (open_row.get("side") if open_row else ""),
                "strategy": row.get("strategy") or (open_row.get("strategy") if open_row else ""),
                "session": row.get("session") or (open_row.get("session") if open_row else ""),
                "entry_time": open_row.get("time") if open_row else "-",
                "exit_time": row.get("time") or "-",
                "entry_price": open_row.get("price") if open_row else "-",
                "exit_price": row.get("price") or "-",
                "qty_lots": row.get("qty_lots") or (open_row.get("qty_lots") if open_row else 0),
                "pnl_rub": f"{pnl_numeric:.2f}",
                "entry_reason": open_row.get("reason") if open_row else "-",
                "exit_reason": row.get("reason") or "-",
                "verdict": classify_verdict(pnl_numeric, row.get("reason") or ""),
            }
        )

    current_open = []
    for symbol, items in open_by_symbol.items():
        if not items:
            continue
        current_open.append(items[-1])

    wins = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) > 0)
    losses = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) < 0)
    total = sum(float(item.get("pnl_rub") or 0.0) for item in closed_reviews)
    win_rate = round((wins / len(closed_reviews)) * 100, 1) if closed_reviews else 0.0

    by_symbol: dict[str, float] = {}
    by_strategy: dict[str, float] = {}
    for item in closed_reviews:
        pnl = float(item.get("pnl_rub") or 0.0)
        by_symbol[item["symbol"]] = by_symbol.get(item["symbol"], 0.0) + pnl
        strategy = item.get("strategy") or "-"
        by_strategy[strategy] = by_strategy.get(strategy, 0.0) + pnl

    best_symbol = max(by_symbol.items(), key=lambda x: x[1]) if by_symbol else None
    worst_symbol = min(by_symbol.items(), key=lambda x: x[1]) if by_symbol else None
    best_strategy = max(by_strategy.items(), key=lambda x: x[1]) if by_strategy else None
    worst_strategy = min(by_strategy.items(), key=lambda x: x[1]) if by_strategy else None

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
        "closed_reviews": closed_reviews[-20:],
        "current_open": current_open[-20:],
    }


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


def summarize_states(states: dict[str, dict]) -> dict:
    realized = 0.0
    open_positions = []
    signals = {"LONG": 0, "SHORT": 0, "HOLD": 0}

    for symbol, state in states.items():
        realized += float(state.get("realized_pnl") or 0.0)
        signal = (state.get("last_signal") or "HOLD").upper()
        if signal in signals:
            signals[signal] += 1
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
                    "last_signal": signal,
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
    return {
        "ok": bot_service.get("active") == "active" and dashboard_service.get("active") == "active",
        "bot_service": bot_service,
        "dashboard_service": dashboard_service,
        "symbols": sorted(states.keys()),
        "symbols_count": len(states),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_moscow": datetime.now(timezone.utc).astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
    }


def build_dashboard_html() -> str:
    return """
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
      display: grid;
      gap: 16px;
      grid-template-columns: 2fr 1fr;
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
    .section-title {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
    }
    .generated {
      font-size: 12px;
      color: var(--muted);
      font-family: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
    }
    .hero p {
      max-width: 62ch;
      line-height: 1.5;
    }
    a {
      color: var(--accent);
    }
    @media (max-width: 860px) {
      .hero { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
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
          <div class="muted">Свободно</div>
          <div class="metric" id="portfolioFree">-</div>
        </div>
        <div>
          <div class="muted">ГО</div>
          <div class="metric" id="portfolioBlocked">-</div>
        </div>
        <div>
          <div class="muted">Реализовано ботом</div>
          <div class="metric" id="portfolioRealized">-</div>
        </div>
        <div>
          <div class="muted">Вариационная маржа</div>
          <div class="metric" id="portfolioVariation">-</div>
        </div>
        <div>
          <div class="muted">Итог по боту</div>
          <div class="metric" id="portfolioTotalPnl">-</div>
        </div>
        <div>
          <div class="muted">Открытых позиций</div>
          <div class="metric" id="portfolioOpenCount">-</div>
        </div>
      </div>
    </section>

    <div class="grid">
      <section class="panel">
        <h2>Позиции</h2>
        <table id="positionsTable">
          <thead><tr><th>Инструмент</th><th>Сторона</th><th>Лоты</th><th>Вход</th><th>Текущая</th><th>В позиции</th><th>Вар. маржа</th><th>Изм. %</th><th>Стратегия</th><th>Сигнал</th></tr></thead>
          <tbody></tbody>
        </table>
      </section>

    </div>

    <section class="panel" style="margin-top:16px;">
      <h2>Сигналы по инструментам</h2>
      <table id="signalsTable">
        <thead>
          <tr>
            <th>Инструмент</th><th>Сигнал</th><th>Стратегия</th><th>Старший ТФ</th><th>News bias</th><th>Влияние</th><th>Ключевая причина</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
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
      <table id="newsTable" style="margin-top:16px;">
        <thead>
          <tr>
            <th>Инструмент</th><th>Bias</th><th>Сила</th><th>Источник</th><th>Актуально до</th><th>Причина</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </section>

    <section class="panel" style="margin-top:16px;">
      <div class="section-title">
        <h2>Лента событий</h2>
        <label class="muted" for="eventStatusFilter">Статус:
          <select id="eventStatusFilter" style="margin-left:8px; background:#0b1324; color:#ebf4ff; border:1px solid rgba(102,174,255,0.18); border-radius:10px; padding:6px 10px;">
            <option value="all">Все</option>
            <option value="active">Активные</option>
            <option value="closed">Закрытые</option>
            <option value="history">История</option>
          </select>
        </label>
      </div>
      <div class="table-scroll">
        <table id="tradesTable">
          <thead>
            <tr>
              <th>Время</th><th>Инструмент</th><th>Событие</th><th>Статус</th><th>Сторона</th><th>Лоты</th><th class="right">Цена</th><th class="right">PnL RUB</th><th>Стратегия</th><th>Причина</th>
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
      </div>
      <table id="reviewTable" style="margin-top:16px;">
        <thead>
          <tr>
            <th>Инструмент</th><th>Сторона</th><th>Стратегия</th><th>Вход</th><th>Выход</th><th class="right">PnL RUB</th><th>Выход</th><th>Вердикт</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </section>
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

    function filterTradeRows(rows) {
      const select = document.getElementById('eventStatusFilter');
      if (!select) return rows;
      const value = select.value || 'all';
      if (value === 'all') return rows;
      return rows.filter((row) => String(row.event_status || '').toLowerCase() === value);
    }

    async function loadData() {
      const response = await fetch('/api/dashboard');
      const data = await response.json();

      document.getElementById('realized').textContent = `${data.summary.realized_pnl_rub.toFixed(2)} RUB`;
      document.getElementById('openCount').textContent = data.summary.open_positions.length;
      document.getElementById('serviceState').textContent = formatRuntimeState(data.runtime?.state || data.service.active);
      document.getElementById('symbolsTotal').textContent = data.summary.symbols_total;
      document.getElementById('generatedAt').textContent = `Обновление: ${data.generated_at_moscow || '-'}`;

      const portfolio = data.portfolio || {};
      document.getElementById('portfolioGeneratedAt').textContent = `Срез портфеля: ${portfolio.generated_at_moscow || '-'}`;
      document.getElementById('portfolioMode').textContent = portfolio.mode === 'DRY_RUN' ? 'ТЕСТ' : (portfolio.mode || '-');
      document.getElementById('portfolioTotal').textContent = formatRub(portfolio.total_portfolio_rub);
      document.getElementById('portfolioFree').textContent = formatRub(portfolio.free_rub);
      document.getElementById('portfolioBlocked').textContent = formatRub(portfolio.blocked_guarantee_rub);
      document.getElementById('portfolioRealized').textContent = formatRub(portfolio.bot_realized_pnl_rub);
      document.getElementById('portfolioVariation').textContent = formatRub(portfolio.bot_estimated_variation_margin_rub);
      document.getElementById('portfolioTotalPnl').textContent = formatRub(portfolio.bot_total_pnl_rub);
      document.getElementById('portfolioOpenCount').textContent = portfolio.open_positions_count ?? '-';

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
      newsBody.innerHTML = '';
      for (const item of activeBiases) {
        newsBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(item.symbol || '-')}</td>
          <td>${signalBadge(item.bias || '-')}</td>
          <td>${escapeHtml(formatStrength(item.strength || '-'))}</td>
          <td>${escapeHtml(item.source || '-')}</td>
          <td class="mono">${escapeHtml(item.expires_at_moscow || '-')}</td>
          <td class="reason">${escapeHtml(item.reason || '-')}</td>
        </tr>`);
      }
      if (!activeBiases.length) {
        newsBody.insertAdjacentHTML('beforeend', '<tr><td colspan="6" class="muted">Активных news bias сейчас нет.</td></tr>');
      }

      const posBody = document.querySelector('#positionsTable tbody');
      posBody.innerHTML = '';
      for (const pos of data.summary.open_positions) {
        const vm = Number(pos.variation_margin_rub || 0);
        const pct = Number(pos.pnl_pct || 0);
        const vmClass = vm > 0 ? 'good' : vm < 0 ? 'bad' : 'muted';
        const pctClass = pct > 0 ? 'good' : pct < 0 ? 'bad' : 'muted';
        posBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(pos.symbol)}</td>
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
      }
      if (!data.summary.open_positions.length) {
        posBody.insertAdjacentHTML('beforeend', '<tr><td colspan="10" class="muted">Открытых позиций нет.</td></tr>');
      }

      const signalBody = document.querySelector('#signalsTable tbody');
      signalBody.innerHTML = '';
      for (const [symbol, state] of Object.entries(data.states)) {
        const summary = Array.isArray(state.last_signal_summary) && state.last_signal_summary.length
          ? state.last_signal_summary[0]
          : (state.last_error || '-');
        signalBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(symbol)}</td>
          <td>${signalBadge(state.last_signal || '-')}</td>
          <td>${escapeHtml(state.last_strategy_name || state.entry_strategy || '-')}</td>
          <td>${signalBadge(state.last_higher_tf_bias || '-')}</td>
          <td>${escapeHtml(formatBiasLabel(state.last_news_bias || 'NEUTRAL'))}</td>
          <td class="reason">${escapeHtml(state.last_news_impact || '-')}</td>
          <td class="reason">${escapeHtml(summary)}</td>
        </tr>`);
      }

      const tradeBody = document.querySelector('#tradesTable tbody');
      tradeBody.innerHTML = '';
      const filteredTrades = filterTradeRows(data.trades.slice().reverse());
      for (const row of filteredTrades) {
        const pnl = row.pnl_rub ?? '-';
        const pnlNum = Number(pnl);
        const pnlClass = Number.isFinite(pnlNum) ? (pnlNum >= 0 ? 'good' : 'bad') : 'muted';
        tradeBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(row.time || '-')}</td>
          <td class="mono">${escapeHtml(row.symbol || '-')}</td>
          <td>${escapeHtml(formatEventLabel(row.event || '-'))}</td>
          <td>${eventStatusBadge(row.event_status || 'history')}</td>
          <td>${signalBadge(row.side || '-')}</td>
          <td class="mono">${escapeHtml(row.qty_lots || '-')}</td>
          <td class="mono right">${escapeHtml(row.price ?? '-')}</td>
          <td class="mono right ${pnlClass}">${escapeHtml(pnl)}</td>
          <td>${escapeHtml(row.strategy || '-')}</td>
          <td class="reason">${escapeHtml(row.reason || '-')}</td>
        </tr>`);
      }
      if (!filteredTrades.length) {
        tradeBody.insertAdjacentHTML('beforeend', '<tr><td colspan="10" class="muted">Журнал сделок пока пуст.</td></tr>');
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

      const reviewBody = document.querySelector('#reviewTable tbody');
      reviewBody.innerHTML = '';
      for (const row of (review.closed_reviews || []).slice().reverse()) {
        const pnlNum = Number(row.pnl_rub);
        const pnlClass = Number.isFinite(pnlNum) ? (pnlNum >= 0 ? 'good' : 'bad') : 'muted';
        reviewBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(row.symbol || '-')}</td>
          <td>${signalBadge(row.side || '-')}</td>
          <td>${escapeHtml(row.strategy || '-')}</td>
          <td class="mono">${escapeHtml(row.entry_time || '-')}</td>
          <td class="mono">${escapeHtml(row.exit_time || '-')}</td>
          <td class="mono right ${pnlClass}">${escapeHtml(row.pnl_rub || '-')}</td>
          <td class="reason">${escapeHtml(row.exit_reason || '-')}</td>
          <td>${escapeHtml(row.verdict || '-')}</td>
        </tr>`);
      }
      if (!(review.closed_reviews || []).length) {
        reviewBody.insertAdjacentHTML('beforeend', '<tr><td colspan="8" class="muted">Закрытых сделок пока нет.</td></tr>');
      }
    }

    document.addEventListener('DOMContentLoaded', () => {
      const filter = document.getElementById('eventStatusFilter');
      if (filter) {
        filter.addEventListener('change', loadData);
      }
      loadData();
      setInterval(loadData, 15000);
    });
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return build_dashboard_html()


@app.get("/api/dashboard", response_class=JSONResponse)
def api_dashboard() -> dict:
    states = load_states()
    generated_at = datetime.now(timezone.utc)
    trades = annotate_trade_rows(load_trade_rows(80), states)
    return {
        "service": get_bot_service_status(),
        "health": build_health_payload(states),
        "portfolio": load_portfolio_snapshot(),
        "runtime": load_runtime_status(),
        "news": load_news_snapshot(),
        "trade_review": load_trade_review(120),
        "summary": summarize_states(states),
        "meta": load_meta(),
        "states": states,
        "trades": trades,
        "generated_at": generated_at.isoformat(),
        "generated_at_moscow": generated_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
    }


@app.get("/api/health", response_class=JSONResponse)
def api_health() -> dict:
    states = load_states()
    return build_health_payload(states)


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt() -> str:
    return "User-agent: *\nDisallow: /\n"
