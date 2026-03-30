from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "bot_state"
LOG_DIR = BASE_DIR / "logs"
TRADE_JOURNAL_PATH = LOG_DIR / "trade_journal.jsonl"
MOSCOW_TZ = timezone.utc


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
    return rows[-limit:]


def get_bot_service_status() -> dict:
    service_name = os.getenv("OIL_SERVICE_NAME", "oil-bot")
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


def build_dashboard_html() -> str:
    return """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Oil Bot Dashboard</title>
  <style>
    :root {
      --bg: #f4efe6;
      --panel: #fffaf2;
      --ink: #1e1a17;
      --muted: #756b61;
      --line: #d9cbb8;
      --good: #2f6d4f;
      --bad: #9a3b2e;
      --accent: #c26b2b;
    }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: linear-gradient(180deg, #efe5d5 0%, var(--bg) 100%);
      color: var(--ink);
    }
    .wrap {
      max-width: 1240px;
      margin: 0 auto;
      padding: 24px;
    }
    h1, h2 { margin: 0 0 12px; }
    .grid {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 8px 28px rgba(35, 24, 12, 0.08);
    }
    .hero {
      display: grid;
      gap: 16px;
      grid-template-columns: 2fr 1fr;
      margin-bottom: 16px;
    }
    .metric {
      font-size: 28px;
      font-weight: bold;
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
      padding: 8px 6px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      background: #efe0cf;
      color: #5a3820;
      font-size: 12px;
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
        <h1>Oil Bot Dashboard</h1>
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
            <th>Время</th><th>Инструмент</th><th>Событие</th><th>Сторона</th><th>Лоты</th><th>Цена</th><th>PnL RUB</th><th>Стратегия</th><th>Причина</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </section>
  </div>
  <script>
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

      const posBody = document.querySelector('#positionsTable tbody');
      posBody.innerHTML = '';
      for (const pos of data.summary.open_positions) {
        posBody.insertAdjacentHTML('beforeend', `<tr>
          <td>${pos.symbol}</td>
          <td>${pos.side}</td>
          <td>${pos.qty}</td>
          <td>${pos.strategy}</td>
          <td>${pos.last_signal}</td>
        </tr>`);
      }
      if (!data.summary.open_positions.length) {
        posBody.insertAdjacentHTML('beforeend', '<tr><td colspan="5" class="muted">Открытых позиций нет.</td></tr>');
      }

      const svcBody = document.querySelector('#serviceTable tbody');
      svcBody.innerHTML = `
        <tr><td>Имя</td><td>${data.service.service}</td></tr>
        <tr><td>Active</td><td>${data.service.active}</td></tr>
        <tr><td>Enabled</td><td>${data.service.enabled}</td></tr>
      `;

      const stateBody = document.querySelector('#statesTable tbody');
      stateBody.innerHTML = '';
      for (const [symbol, state] of Object.entries(data.states)) {
        stateBody.insertAdjacentHTML('beforeend', `<tr>
          <td>${symbol}</td>
          <td><span class="badge">${state.last_signal || '-'}</span></td>
          <td>${state.position_side || 'FLAT'}</td>
          <td>${state.position_qty || 0}</td>
          <td>${state.entry_strategy || '-'}</td>
          <td>${state.last_status_candle || '-'}</td>
          <td>${Number(state.realized_pnl || 0).toFixed(2)}</td>
          <td>${state.last_error || '-'}</td>
        </tr>`);
      }

      const tradeBody = document.querySelector('#tradesTable tbody');
      tradeBody.innerHTML = '';
      for (const row of data.trades.slice().reverse()) {
        tradeBody.insertAdjacentHTML('beforeend', `<tr>
          <td>${row.time || '-'}</td>
          <td>${row.symbol || '-'}</td>
          <td>${row.event || '-'}</td>
          <td>${row.side || '-'}</td>
          <td>${row.qty_lots || '-'}</td>
          <td>${row.price ?? '-'}</td>
          <td>${row.pnl_rub ?? '-'}</td>
          <td>${row.strategy || '-'}</td>
          <td>${row.reason || '-'}</td>
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
    return {
        "service": get_bot_service_status(),
        "summary": summarize_states(states),
        "meta": load_meta(),
        "states": states,
        "trades": load_trade_rows(80),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/health", response_class=JSONResponse)
def api_health() -> dict:
    states = load_states()
    service = get_bot_service_status()
    return {
        "ok": service.get("active") == "active",
        "service": service,
        "symbols": sorted(states.keys()),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

