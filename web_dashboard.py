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
  <title>Oil Bot Dashboard</title>
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
      font-family: "Inter", "Segoe UI", Arial, sans-serif;
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
      letter-spacing: 0.04em;
      text-transform: uppercase;
      font-size: 30px;
      text-shadow: 0 0 28px var(--glow);
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
      font-size: 31px;
      font-weight: 700;
      font-family: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
      letter-spacing: -0.03em;
      text-shadow: 0 0 18px var(--glow);
    }
    .muted { color: var(--muted); }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
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
          <h1>Oil Bot Dashboard</h1>
          <div class="generated" id="generatedAt">Обновление: -</div>
        </div>
        <p class="muted">Мониторинг бота, сделок и состояния сервиса без графиков. Обновление каждые 15 секунд.</p>
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
      <section class="panel">
        <h2>Сигналы</h2>
        <div class="grid">
          <div><div class="muted">LONG</div><div class="metric" id="longCount">-</div></div>
          <div><div class="muted">SHORT</div><div class="metric" id="shortCount">-</div></div>
          <div><div class="muted">HOLD</div><div class="metric" id="holdCount">-</div></div>
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
          <div class="metric" id="portfolioMode">-</div>
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
          <thead><tr><th>Инструмент</th><th>Сторона</th><th>Лоты</th><th>Стратегия</th><th>Сигнал</th></tr></thead>
          <tbody></tbody>
        </table>
      </section>

      <section class="panel">
        <h2>Сервис</h2>
        <table id="serviceTable">
          <tbody></tbody>
        </table>
      </section>
    </div>

    <section class="panel" style="margin-top:16px;">
      <h2>Состояние инструментов</h2>
      <table id="statesTable">
        <thead>
          <tr>
            <th>Инструмент</th><th>Сигнал</th><th>Позиция</th><th>Лоты</th><th>Стратегия</th><th>Свеча</th><th>PnL RUB</th><th>Ошибка</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </section>

    <section class="panel" style="margin-top:16px;">
      <h2>Последние сделки</h2>
      <table id="tradesTable">
        <thead>
          <tr>
            <th>Время</th><th>Инструмент</th><th>Событие</th><th>Сторона</th><th>Лоты</th><th class="right">Цена</th><th class="right">PnL RUB</th><th>Стратегия</th><th>Причина</th>
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
      return `<span class="badge ${css}">${escapeHtml(raw)}</span>`;
    }

    function formatRub(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return '-';
      }
      return `${num.toFixed(2)} RUB`;
    }

    async function loadData() {
      const response = await fetch('/api/dashboard');
      const data = await response.json();

      document.getElementById('realized').textContent = `${data.summary.realized_pnl_rub.toFixed(2)} RUB`;
      document.getElementById('openCount').textContent = data.summary.open_positions.length;
      document.getElementById('serviceState').textContent = data.service.active;
      document.getElementById('symbolsTotal').textContent = data.summary.symbols_total;
      document.getElementById('longCount').textContent = data.summary.signal_counts.LONG;
      document.getElementById('shortCount').textContent = data.summary.signal_counts.SHORT;
      document.getElementById('holdCount').textContent = data.summary.signal_counts.HOLD;
      document.getElementById('generatedAt').textContent = `Обновление: ${data.generated_at_moscow || '-'}`;

      const portfolio = data.portfolio || {};
      document.getElementById('portfolioGeneratedAt').textContent = `Срез портфеля: ${portfolio.generated_at_moscow || '-'}`;
      document.getElementById('portfolioMode').textContent = portfolio.mode || '-';
      document.getElementById('portfolioTotal').textContent = formatRub(portfolio.total_portfolio_rub);
      document.getElementById('portfolioFree').textContent = formatRub(portfolio.free_rub);
      document.getElementById('portfolioBlocked').textContent = formatRub(portfolio.blocked_guarantee_rub);
      document.getElementById('portfolioRealized').textContent = formatRub(portfolio.bot_realized_pnl_rub);
      document.getElementById('portfolioVariation').textContent = formatRub(portfolio.bot_estimated_variation_margin_rub);
      document.getElementById('portfolioTotalPnl').textContent = formatRub(portfolio.bot_total_pnl_rub);
      document.getElementById('portfolioOpenCount').textContent = portfolio.open_positions_count ?? '-';

      const posBody = document.querySelector('#positionsTable tbody');
      posBody.innerHTML = '';
      for (const pos of data.summary.open_positions) {
        posBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(pos.symbol)}</td>
          <td>${signalBadge(pos.side)}</td>
          <td class="mono">${escapeHtml(pos.qty)}</td>
          <td>${escapeHtml(pos.strategy)}</td>
          <td>${signalBadge(pos.last_signal)}</td>
        </tr>`);
      }
      if (!data.summary.open_positions.length) {
        posBody.insertAdjacentHTML('beforeend', '<tr><td colspan="5" class="muted">Открытых позиций нет.</td></tr>');
      }

      const svcBody = document.querySelector('#serviceTable tbody');
      svcBody.innerHTML = `
        <tr><td>Бот</td><td>${signalBadge(data.health.bot_service.active || '-')}</td></tr>
        <tr><td>Dashboard</td><td>${signalBadge(data.health.dashboard_service.active || '-')}</td></tr>
        <tr><td>Health</td><td>${data.health.ok ? '<span class="good mono">OK</span>' : '<span class="bad mono">FAIL</span>'}</td></tr>
        <tr><td>Инструментов</td><td class="mono">${data.health.symbols_count}</td></tr>
        <tr><td>Срез</td><td class="mono">${escapeHtml(data.health.generated_at_moscow || '-')}</td></tr>
      `;

      const stateBody = document.querySelector('#statesTable tbody');
      stateBody.innerHTML = '';
      for (const [symbol, state] of Object.entries(data.states)) {
        stateBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(symbol)}</td>
          <td>${signalBadge(state.last_signal || '-')}</td>
          <td>${signalBadge(state.position_side || 'FLAT')}</td>
          <td class="mono">${escapeHtml(state.position_qty || 0)}</td>
          <td>${escapeHtml(state.entry_strategy || '-')}</td>
          <td class="mono">${escapeHtml(state.last_status_candle || '-')}</td>
          <td class="mono right">${Number(state.realized_pnl || 0).toFixed(2)}</td>
          <td class="reason">${escapeHtml(state.last_error || '-')}</td>
        </tr>`);
      }

      const tradeBody = document.querySelector('#tradesTable tbody');
      tradeBody.innerHTML = '';
      for (const row of data.trades.slice().reverse()) {
        const pnl = row.pnl_rub ?? '-';
        const pnlNum = Number(pnl);
        const pnlClass = Number.isFinite(pnlNum) ? (pnlNum >= 0 ? 'good' : 'bad') : 'muted';
        tradeBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(row.time || '-')}</td>
          <td class="mono">${escapeHtml(row.symbol || '-')}</td>
          <td>${escapeHtml(row.event || '-')}</td>
          <td>${signalBadge(row.side || '-')}</td>
          <td class="mono">${escapeHtml(row.qty_lots || '-')}</td>
          <td class="mono right">${escapeHtml(row.price ?? '-')}</td>
          <td class="mono right ${pnlClass}">${escapeHtml(pnl)}</td>
          <td>${escapeHtml(row.strategy || '-')}</td>
          <td class="reason">${escapeHtml(row.reason || '-')}</td>
        </tr>`);
      }
      if (!data.trades.length) {
        tradeBody.insertAdjacentHTML('beforeend', '<tr><td colspan="9" class="muted">Журнал сделок пока пуст.</td></tr>');
      }
    }

    loadData();
    setInterval(loadData, 15000);
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
    return {
        "service": get_bot_service_status(),
        "health": build_health_payload(states),
        "portfolio": load_portfolio_snapshot(),
        "summary": summarize_states(states),
        "meta": load_meta(),
        "states": states,
        "trades": load_trade_rows(80),
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
