from __future__ import annotations

import json
import os
import subprocess
from datetime import date, datetime, timezone
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
AI_REVIEW_DIR = LOG_DIR / "ai_reviews"
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
        normalized.append(item)
    return normalized


def stringify_money(value: Any, default: str = "-") -> str:
    if value in (None, ""):
        return default
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value)


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
                    row = json.loads(line)
                except Exception:
                    continue
                dt = parse_trade_time(row.get("time"))
                if not dt:
                    continue
                row["_dt"] = dt
                row["_date"] = dt.date().isoformat()
                rows.append(row)
    except Exception:
        return []
    return rows


def load_trade_rows_for_day(target_day: date, limit: int = 200) -> list[dict]:
    rows = [row for row in load_all_trade_rows() if row.get("_date") == target_day.isoformat()]
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


def filter_current_open_rows(rows: list[dict], states: dict[str, dict] | None = None) -> list[dict]:
    if not states:
        return rows
    filtered: list[dict] = []
    for row in rows:
        symbol = str(row.get("symbol", ""))
        if not symbol:
            continue
        state = states.get(symbol, {})
        side = str(row.get("side", "")).upper()
        live_side = str(state.get("position_side", "FLAT")).upper()
        live_qty = int(state.get("position_qty") or 0)
        if live_side == "FLAT" or live_qty <= 0:
            continue
        if side and live_side and side != live_side:
            continue
        filtered.append(row)
    return filtered


def load_trade_review(limit: int = 80, states: dict[str, dict] | None = None) -> dict:
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
                "gross_pnl_rub": stringify_money(row.get("gross_pnl_rub")),
                "commission_rub": stringify_money(row.get("commission_rub")),
                "net_pnl_rub": stringify_money(row.get("net_pnl_rub"), stringify_money(row.get("pnl_rub"))),
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
    current_open = filter_current_open_rows(current_open, states)

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


def load_trade_review_for_day(target_day: date, limit: int = 200, states: dict[str, dict] | None = None) -> dict:
    rows = load_trade_rows_for_day(target_day, limit)
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
                "gross_pnl_rub": stringify_money(row.get("gross_pnl_rub")),
                "commission_rub": stringify_money(row.get("commission_rub")),
                "net_pnl_rub": stringify_money(row.get("net_pnl_rub"), stringify_money(row.get("pnl_rub"))),
                "entry_reason": open_row.get("reason") if open_row else "-",
                "exit_reason": row.get("reason") or "-",
                "verdict": classify_verdict(pnl_numeric, row.get("reason") or ""),
            }
        )

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
    current_open = []
    for symbol, items in open_by_symbol.items():
        if not items:
            continue
        current_open.append(items[-1])
    current_open = filter_current_open_rows(current_open, states)

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


def build_daily_performance(portfolio: dict, target_day: date) -> dict:
    current_portfolio = float(portfolio.get("total_portfolio_rub") or 0.0)
    rows = load_all_trade_rows()
    by_day: dict[str, dict[str, float]] = {}
    cumulative = 0.0

    days = sorted({row["_date"] for row in rows})
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
      white-space: pre-wrap;
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
          <div class="muted">Реализовано</div>
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
          <div class="muted">Текущая вар. маржа позиций</div>
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
      <h2>Сигналы по инструментам</h2>
      <div id="signalsCards" class="mobile-cards"></div>
      <div class="table-scroll desktop-table">
        <table id="signalsTable">
          <thead>
            <tr>
              <th>Инструмент</th><th>Сигнал</th><th>Стратегия</th><th>Старший ТФ</th><th>News bias</th><th>Влияние</th><th>Ключевая причина</th>
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
        <label class="muted" for="eventStatusFilter">Статус:
          <select id="eventStatusFilter" style="margin-left:8px; background:#0b1324; color:#ebf4ff; border:1px solid rgba(102,174,255,0.18); border-radius:10px; padding:6px 10px;">
            <option value="all">Все</option>
            <option value="active">Активные</option>
            <option value="closed">Закрытые</option>
            <option value="history">История</option>
          </select>
        </label>
      </div>
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
        <div class="generated" id="aiReviewMeta">AI review: -</div>
      </div>
      <div id="aiReviewContent" class="prose-review muted">AI-review пока не загружен.</div>
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
      const text = String(value || '').trim();
      if (!text) return '<span class="muted">AI-review для выбранной даты пока не найден.</span>';
      let html = escapeHtml(text);
      html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');
      html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
      html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
      html = html.replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>');
      html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
      html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
      html = html.replace(/(<li>.*<\\/li>)/gs, '<ul>$1</ul>');
      html = html.replace(/<\\/ul>\\s*<ul>/g, '');
      html = html.replace(/\\n{2,}/g, '</p><p>');
      html = `<p>${html}</p>`;
      html = html.replace(/<p>\\s*(<h[1-3]>)/g, '$1');
      html = html.replace(/(<\\/h[1-3]>)\\s*<\\/p>/g, '$1');
      html = html.replace(/<p>\\s*(<ul>)/g, '$1');
      html = html.replace(/(<\\/ul>)\\s*<\\/p>/g, '$1');
      return html;
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

      if (dateInput && data.daily && data.daily.selected_date) {
        dateInput.value = data.daily.selected_date;
      }

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
      document.getElementById('portfolioActualFee').textContent = formatRub(portfolio.bot_actual_fee_rub);
      document.getElementById('portfolioActualVm').textContent = formatRub(portfolio.bot_actual_varmargin_rub);
      document.getElementById('portfolioVariation').textContent = formatRub(portfolio.bot_estimated_variation_margin_rub);
      document.getElementById('portfolioTotalPnl').textContent = formatRub(portfolio.bot_total_pnl_rub);
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
          <td class="mono">${escapeHtml(item.symbol || '-')}</td>
          <td>${signalBadge(item.bias || '-')}</td>
          <td>${escapeHtml(formatStrength(item.strength || '-'))}</td>
          <td>${escapeHtml(item.source || '-')}</td>
          <td class="mono">${escapeHtml(item.expires_at_moscow || '-')}</td>
          <td><div class="news-reason"><span class="reason">${escapeHtml(reasonText)}</span>${detailsButton}</div></td>
        </tr>`);
        newsCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title mono">${escapeHtml(item.symbol || '-')}</div>
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
        posCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title mono">${escapeHtml(pos.symbol)}</div>
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
        signalBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(symbol)}</td>
          <td>${signalBadge(state.last_signal || '-')}</td>
          <td>${escapeHtml(state.last_strategy_name || state.entry_strategy || '-')}</td>
          <td>${signalBadge(state.last_higher_tf_bias || '-')}</td>
          <td>${escapeHtml(formatBiasLabel(state.last_news_bias || 'NEUTRAL'))}</td>
          <td class="reason">${escapeHtml(state.last_news_impact || '-')}</td>
          <td class="reason">${escapeHtml(summary)}</td>
        </tr>`);
        signalCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title mono">${escapeHtml(symbol)}</div>
            ${signalBadge(state.last_signal || '-')}
          </div>
          <div class="mobile-card-grid">
            <div class="mobile-card-item"><span class="muted">Стратегия</span><div class="mobile-card-value">${escapeHtml(state.last_strategy_name || state.entry_strategy || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Старший ТФ</span><div class="mobile-card-value">${signalBadge(state.last_higher_tf_bias || '-')}</div></div>
            <div class="mobile-card-item"><span class="muted">Новости</span><div class="mobile-card-value">${escapeHtml(formatBiasLabel(state.last_news_bias || 'NEUTRAL'))}</div></div>
            <div class="mobile-card-item"><span class="muted">Влияние</span><div class="mobile-card-value">${escapeHtml(state.last_news_impact || '-')}</div></div>
          </div>
          <div class="mobile-card-footer">
            <div class="mobile-card-text"><span class="muted">Ключевая причина</span><br>${escapeHtml(summary)}</div>
          </div>
        </article>`);
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
        const grossText = isOpenEvent ? '-' : (row.gross_pnl_rub ?? '-');
        const commissionText = row.commission_rub ?? '-';
        const netText = isOpenEvent ? '-' : (row.net_pnl_rub ?? pnl);
        tradeBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(row.time || '-')}</td>
          <td class="mono">${escapeHtml(row.symbol || '-')}</td>
          <td>${escapeHtml(formatEventLabel(row.event || '-'))}</td>
          <td>${eventStatusBadge(row.event_status || 'history')}</td>
          <td>${signalBadge(row.side || '-')}</td>
          <td class="mono">${escapeHtml(row.qty_lots || '-')}</td>
          <td class="mono right">${escapeHtml(row.price ?? '-')}</td>
          <td class="mono right">${escapeHtml(grossText)}</td>
          <td class="mono right">${escapeHtml(commissionText)}</td>
          <td class="mono right ${pnlClass}">${escapeHtml(netText)}</td>
          <td>${escapeHtml(row.strategy || '-')}</td>
          <td class="reason">${escapeHtml(row.reason || '-')}</td>
        </tr>`);
        tradeCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title mono">${escapeHtml(row.symbol || '-')}</div>
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
            <div class="mobile-card-text"><span class="muted">Причина</span><br>${escapeHtml(row.reason || '-')}</div>
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

      const reviewBody = document.querySelector('#reviewTable tbody');
      const reviewCards = document.getElementById('reviewCards');
      reviewBody.innerHTML = '';
      reviewCards.innerHTML = '';
      for (const row of (review.closed_reviews || []).slice().reverse()) {
        const pnlNum = Number(row.pnl_rub);
        const pnlClass = Number.isFinite(pnlNum) ? (pnlNum >= 0 ? 'good' : 'bad') : 'muted';
        reviewBody.insertAdjacentHTML('beforeend', `<tr>
          <td class="mono">${escapeHtml(row.symbol || '-')}</td>
          <td>${signalBadge(row.side || '-')}</td>
          <td>${escapeHtml(row.strategy || '-')}</td>
          <td class="mono">${escapeHtml(row.entry_time || '-')}</td>
          <td class="mono">${escapeHtml(row.exit_time || '-')}</td>
          <td class="mono right">${escapeHtml(row.gross_pnl_rub ?? '-')}</td>
          <td class="mono right">${escapeHtml(row.commission_rub ?? '-')}</td>
          <td class="mono right ${pnlClass}">${escapeHtml(row.net_pnl_rub ?? row.pnl_rub ?? '-')}</td>
          <td class="reason">${escapeHtml(row.exit_reason || '-')}</td>
          <td>${escapeHtml(row.verdict || '-')}</td>
        </tr>`);
        reviewCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title mono">${escapeHtml(row.symbol || '-')}</div>
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
        for (const row of currentOpen.slice().reverse()) {
          const openCommissionText = row.commission_rub ?? '-';
          reviewBody.insertAdjacentHTML('beforeend', `<tr>
            <td class="mono">${escapeHtml(row.symbol || '-')}</td>
            <td>${signalBadge(row.side || '-')}</td>
            <td>${escapeHtml(row.strategy || '-')}</td>
            <td class="mono">${escapeHtml(row.time || '-')}</td>
            <td class="mono">в позиции</td>
            <td class="mono right">-</td>
            <td class="mono right">${escapeHtml(openCommissionText)}</td>
            <td class="mono right">-</td>
            <td class="reason">${escapeHtml(row.reason || 'позиция открыта')}</td>
            <td>открыта</td>
          </tr>`);
          reviewCards.insertAdjacentHTML('beforeend', `<article class="mobile-card">
            <div class="mobile-card-head">
              <div class="mobile-card-title mono">${escapeHtml(row.symbol || '-')}</div>
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
              <div class="mobile-card-text"><span class="muted">Причина</span><br>${escapeHtml(row.reason || 'позиция открыта')}</div>
            </div>
          </article>`);
        }
      }

      const aiReview = data.ai_review || {};
      document.getElementById('aiReviewMeta').textContent = aiReview.available
        ? `AI review: ${aiReview.source || '-'} • обновлено ${aiReview.updated_at_moscow || '-'}`
        : `AI review: пока нет${aiReview.updated_at_moscow ? ` • последняя попытка ${aiReview.updated_at_moscow}` : ''}`;
      document.getElementById('aiReviewContent').innerHTML = markdownToHtml(aiReview.content || '');
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


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return build_dashboard_html()


@app.get("/api/dashboard", response_class=JSONResponse)
def api_dashboard(date: str | None = None) -> dict:
    states = load_states()
    generated_at = datetime.now(timezone.utc)
    portfolio = load_portfolio_snapshot()
    target_day = datetime.now(MOSCOW_TZ).date()
    if date:
        try:
            target_day = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            pass
    trades = annotate_trade_rows(load_trade_rows_for_day(target_day, 200), states)
    return {
        "service": get_bot_service_status(),
        "health": build_health_payload(states),
        "capital_alert": build_capital_alert(states),
        "portfolio": portfolio,
        "runtime": load_runtime_status(),
        "news": load_news_snapshot(),
        "trade_review": load_trade_review_for_day(target_day, 200, states),
        "summary": summarize_states(states),
        "meta": load_meta(),
        "states": states,
        "trades": trades,
        "daily": build_daily_performance(portfolio, target_day),
        "ai_review": load_ai_review(target_day),
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
