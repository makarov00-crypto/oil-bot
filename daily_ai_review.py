from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from trade_storage import load_trade_rows as load_trade_rows_from_storage
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

DEFAULT_MODEL = "gpt-5-mini"
DEFAULT_OUTPUT_PATH = BASE_DIR / "logs" / "ai_reviews" / "latest_review.md"

SYSTEM_INSTRUCTIONS = """Ты рыночный аналитик для фьючерсного бота на Мосбирже.

Твоя задача:
дать сводку по рынку и по тому, как бот воспользовался рыночными движениями в течение дня.
Не оценивай качество разработки стратегии и не предлагай изменения в коде. Не выступай как ревьюер бота.
Не придумывай данные. Опирайся только на переданный контекст.

Что нужно вернуть:
1. Короткий итог дня в 3-5 предложениях:
   - какая была рыночная картина
   - где были направленные движения
   - как бот в целом участвовал в них
2. Картина по инструментам:
   - по каждому ключевому инструменту кратко:
     - что делал рынок
     - где был хороший вход
     - где был хороший выход
     - как этим воспользовался бот
3. Хорошо реализованные моменты:
   - где бот вошёл/вышел уместно
   - где сигнал был использован по делу
4. Упущенные или спорные моменты:
   - где движение было, но бот использовал его слабо
   - где был лишний вход/выход
   - где бот вышел слишком рано или вошёл поздно
5. Текущая картина на конец среза:
   - какие инструменты ещё выглядят направленно
   - где бот уже в позиции
   - где сигнал есть, но участия нет

Правила:
- Пиши кратко, по делу, без воды.
- Не делай разделы "лучшие/худшие инструменты", "ошибки", "что менять завтра", "уровень риска".
- Не оценивай работу бота как правильную или неправильную в общем виде.
- Анализируй связку: рынок -> сигнал/движение -> действие бота.
- Если данных недостаточно, скажи это явно.
- Не делай общих выводов о всей системе по одному инструменту.
- Ответ дай на русском языке, в Markdown.
"""

FOLLOWUP_SYSTEM_INSTRUCTIONS = """Ты продолжаешь уже готовый дневной AI-разбор торгового бота.

Твоя задача:
- ответить на дополнительный вопрос пользователя по конкретному моменту дня, инструменту или поведению бота;
- опираться только на переданный основной AI-разбор и контекст дня;
- не придумывать факты, которых нет в контексте;
- отвечать коротко, предметно и на русском языке, в Markdown.

Правила:
- не переписывай весь основной разбор заново;
- отвечай именно на уточняющий вопрос;
- если данных недостаточно, скажи это прямо;
- если вопрос про конкретный инструмент, держи фокус на нём.
"""


@dataclass
class ClosedTrade:
    symbol: str
    side: str
    strategy: str
    entry_time: str
    exit_time: str
    entry_price: float | None
    exit_price: float | None
    pnl_rub: float
    entry_reason: str
    exit_reason: str
    market_regime: str
    setup_quality_label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily AI review for the trading bot")
    parser.add_argument("--date", dest="target_date", help="Дата в формате YYYY-MM-DD, по умолчанию сегодня по Москве")
    parser.add_argument("--preview", action="store_true", help="Только показать подготовленный prompt без вызова OpenAI")
    parser.add_argument("--model", default=os.getenv("OIL_AI_MODEL", DEFAULT_MODEL), help="Модель OpenAI")
    parser.add_argument("--output", default=os.getenv("OIL_AI_REVIEW_OUTPUT", str((BASE_DIR / "logs" / "ai_reviews" / "latest_review.md"))), help="Куда сохранить итоговый review")
    parser.add_argument("--base-dir", default=str(BASE_DIR), help="Каталог с bot_state и logs")
    return parser.parse_args()


def parse_target_date(raw_value: str | None) -> date:
    if raw_value:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    return datetime.now(MOSCOW_TZ).date()


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_state_dir(base_dir: Path) -> Path:
    return base_dir / "bot_state"


def get_log_dir(base_dir: Path) -> Path:
    return base_dir / "logs"


def get_trade_journal_path(base_dir: Path) -> Path:
    return get_log_dir(base_dir) / "trade_journal.jsonl"


def get_trade_db_path(base_dir: Path) -> Path:
    return get_state_dir(base_dir) / "trade_analytics.sqlite3"


def get_portfolio_snapshot_path(base_dir: Path) -> Path:
    return get_state_dir(base_dir) / "_portfolio_snapshot.json"


def get_news_snapshot_path(base_dir: Path) -> Path:
    return get_state_dir(base_dir) / "_news_snapshot.json"


def load_states(base_dir: Path) -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    state_dir = get_state_dir(base_dir)
    if not state_dir.exists():
        return states
    for path in sorted(state_dir.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            states[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
    return states


def load_trade_rows(base_dir: Path, target_day: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    trade_journal_path = get_trade_journal_path(base_dir)
    trade_db_path = get_trade_db_path(base_dir)
    try:
        source_rows = load_trade_rows_from_storage(
            trade_journal_path,
            trade_db_path,
            target_day=target_day,
        )
    except Exception:
        source_rows = []
    for row in source_rows:
        raw_time = str(row.get("time") or "")
        if not raw_time:
            continue
        try:
            dt = datetime.fromisoformat(raw_time)
        except ValueError:
            continue
        if dt.astimezone(MOSCOW_TZ).date() == target_day:
            item = dict(row)
            item["_dt"] = dt
            rows.append(item)
    return rows


def pair_closed_trades(rows: list[dict[str, Any]]) -> list[ClosedTrade]:
    open_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    closed: list[ClosedTrade] = []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        side = str(row.get("side") or "")
        event = str(row.get("event") or "").upper()
        if not symbol or not side:
            continue
        key = (symbol, side)
        if event == "OPEN":
            open_rows[key].append(row)
            continue
        if event != "CLOSE":
            continue
        open_row = open_rows[key].pop(0) if open_rows.get(key) else None
        try:
            pnl_rub = float(row.get("pnl_rub") or 0.0)
        except Exception:
            pnl_rub = 0.0
        closed.append(
            ClosedTrade(
                symbol=symbol,
                side=side,
                strategy=str(row.get("strategy") or (open_row or {}).get("strategy") or "-"),
                entry_time=format_time((open_row or {}).get("_dt")),
                exit_time=format_time(row.get("_dt")),
                entry_price=safe_float((open_row or {}).get("price")),
                exit_price=safe_float(row.get("price")),
                pnl_rub=pnl_rub,
                entry_reason=str((open_row or {}).get("reason") or "-"),
                exit_reason=str(row.get("reason") or "-"),
                market_regime=str(((row.get("context") or {}) if isinstance(row.get("context"), dict) else {}).get("market_regime") or "-"),
                setup_quality_label=str(((row.get("context") or {}) if isinstance(row.get("context"), dict) else {}).get("setup_quality_label") or "-"),
            )
        )
    return closed


def safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def format_time(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S")
    return "-"


def format_price(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.4f}"


def format_rub(value: Any) -> str:
    try:
        return f"{float(value):.2f} RUB"
    except Exception:
        return "-"


def summarize_closed_trades(trades: list[ClosedTrade]) -> dict[str, Any]:
    wins = sum(1 for trade in trades if trade.pnl_rub > 0)
    losses = sum(1 for trade in trades if trade.pnl_rub < 0)
    total = sum(trade.pnl_rub for trade in trades)
    win_rate = (wins / len(trades) * 100.0) if trades else 0.0

    by_symbol: dict[str, float] = defaultdict(float)
    by_strategy: dict[str, float] = defaultdict(float)
    by_regime: dict[str, float] = defaultdict(float)
    by_setup_quality: dict[str, float] = defaultdict(float)
    by_strategy_regime: dict[str, float] = defaultdict(float)
    by_opens: dict[str, int] = defaultdict(int)
    for trade in trades:
        by_symbol[trade.symbol] += trade.pnl_rub
        by_strategy[trade.strategy] += trade.pnl_rub
        by_regime[trade.market_regime] += trade.pnl_rub
        by_setup_quality[trade.setup_quality_label] += trade.pnl_rub
        strategy_regime = f"{trade.strategy} @ {trade.market_regime}"
        by_strategy_regime[strategy_regime] += trade.pnl_rub
        by_opens[trade.symbol] += 1

    best_regime = max(by_regime.items(), key=lambda item: item[1]) if by_regime else None
    worst_regime = min(by_regime.items(), key=lambda item: item[1]) if by_regime else None
    best_setup_quality = max(by_setup_quality.items(), key=lambda item: item[1]) if by_setup_quality else None
    worst_setup_quality = min(by_setup_quality.items(), key=lambda item: item[1]) if by_setup_quality else None
    best_strategy_regime = max(by_strategy_regime.items(), key=lambda item: item[1]) if by_strategy_regime else None
    worst_strategy_regime = min(by_strategy_regime.items(), key=lambda item: item[1]) if by_strategy_regime else None
    top_positive_strategy_regimes = sorted(
        ((name, pnl) for name, pnl in by_strategy_regime.items() if pnl > 0),
        key=lambda item: item[1],
        reverse=True,
    )[:3]
    top_negative_strategy_regimes = sorted(
        ((name, pnl) for name, pnl in by_strategy_regime.items() if pnl < 0),
        key=lambda item: item[1],
    )[:3]

    return {
        "closed_count": len(trades),
        "wins": wins,
        "losses": losses,
        "total_pnl_rub": total,
        "win_rate": win_rate,
        "by_symbol": dict(sorted(by_symbol.items())),
        "by_strategy": dict(sorted(by_strategy.items())),
        "by_regime": dict(sorted(by_regime.items())),
        "by_setup_quality": dict(sorted(by_setup_quality.items())),
        "by_strategy_regime": dict(sorted(by_strategy_regime.items())),
        "trade_count_by_symbol": dict(sorted(by_opens.items())),
        "best_regime": {"name": best_regime[0], "pnl_rub": best_regime[1]} if best_regime else None,
        "worst_regime": {"name": worst_regime[0], "pnl_rub": worst_regime[1]} if worst_regime else None,
        "best_setup_quality": {"name": best_setup_quality[0], "pnl_rub": best_setup_quality[1]} if best_setup_quality else None,
        "worst_setup_quality": {"name": worst_setup_quality[0], "pnl_rub": worst_setup_quality[1]} if worst_setup_quality else None,
        "best_strategy_regime": {"name": best_strategy_regime[0], "pnl_rub": best_strategy_regime[1]} if best_strategy_regime else None,
        "worst_strategy_regime": {"name": worst_strategy_regime[0], "pnl_rub": worst_strategy_regime[1]} if worst_strategy_regime else None,
        "top_positive_strategy_regimes": [{"name": name, "pnl_rub": pnl} for name, pnl in top_positive_strategy_regimes],
        "top_negative_strategy_regimes": [{"name": name, "pnl_rub": pnl} for name, pnl in top_negative_strategy_regimes],
    }


def build_market_observations(trades: list[ClosedTrade], states: dict[str, dict[str, Any]]) -> list[str]:
    notes: list[str] = []
    by_symbol: dict[str, list[ClosedTrade]] = defaultdict(list)
    for trade in trades:
        by_symbol[trade.symbol].append(trade)

    for symbol in sorted(set(by_symbol.keys()) | set(states.keys())):
        symbol_trades = by_symbol.get(symbol, [])
        state = states.get(symbol, {})
        if symbol_trades:
            total_pnl = sum(item.pnl_rub for item in symbol_trades)
            notes.append(
                f"{symbol}: закрытых сделок {len(symbol_trades)}, итог {total_pnl:.2f} RUB, "
                f"последняя стратегия {state.get('last_strategy_name') or state.get('entry_strategy') or '-'}, "
                f"текущий сигнал {state.get('last_signal','-')}, позиция {state.get('position_side','FLAT')}."
            )
        else:
            notes.append(
                f"{symbol}: закрытых сделок нет, текущий сигнал {state.get('last_signal','-')}, "
                f"позиция {state.get('position_side','FLAT')}."
            )
    return notes[:12]


def build_prompt(
    target_day: date,
    portfolio: dict[str, Any],
    news: dict[str, Any],
    states: dict[str, dict[str, Any]],
    closed_trades: list[ClosedTrade],
    recent_closed_trades: list[ClosedTrade] | None = None,
) -> str:
    summary = summarize_closed_trades(closed_trades)
    recent_summary = summarize_closed_trades(recent_closed_trades or closed_trades)
    active_news = list(news.get("active_biases") or [])
    market_notes = build_market_observations(closed_trades, states)

    portfolio_lines = [
        f"- реализовано: {format_rub(portfolio.get('bot_realized_pnl_rub'))}",
        f"- вариационная маржа: {format_rub(portfolio.get('bot_estimated_variation_margin_rub'))}",
        f"- итог: {format_rub(portfolio.get('bot_total_pnl_rub'))}",
        f"- открытых позиций: {portfolio.get('open_positions_count', 0)}",
        f"- портфель: {format_rub(portfolio.get('total_portfolio_rub'))}",
    ]

    trades_lines = []
    for trade in closed_trades[-20:]:
        trades_lines.append(
            f"- {trade.symbol} {trade.side} | {trade.strategy} | вход {trade.entry_time} @{format_price(trade.entry_price)} | "
            f"выход {trade.exit_time} @{format_price(trade.exit_price)} | {trade.pnl_rub:.2f} RUB | "
            f"режим {trade.market_regime} | сетап {trade.setup_quality_label} | "
            f"вход: {trade.entry_reason} | выход: {trade.exit_reason}"
        )
    if not trades_lines:
        trades_lines.append("- Закрытых сделок за день нет.")

    open_positions_lines = []
    for symbol, state in sorted(states.items()):
        if state.get("position_side") and state.get("position_side") != "FLAT":
            open_positions_lines.append(
                f"- {symbol}: {state.get('position_side')} {state.get('position_qty', 0)} | "
                f"вход {format_price(safe_float(state.get('entry_price')))} | "
                f"текущая {format_price(safe_float(state.get('last_market_price')))} | "
                f"вар. маржа {format_rub(state.get('position_variation_margin_rub'))} | "
                f"стратегия {state.get('entry_strategy') or state.get('last_strategy_name') or '-'}"
            )
    if not open_positions_lines:
        open_positions_lines.append("- Открытых позиций нет.")

    signal_lines = []
    for symbol, state in sorted(states.items()):
        signal_lines.append(
            f"- {symbol}: сигнал={state.get('last_signal','-')}, стратегия={state.get('last_strategy_name') or state.get('entry_strategy') or '-'}, "
            f"старший_тф={state.get('last_higher_tf_bias','-')}, новости={state.get('last_news_bias','NEUTRAL')}, "
            f"блокер={first_summary_line(state)}"
        )

    news_lines = []
    for item in active_news[:10]:
        news_lines.append(
            f"- {item.get('symbol','-')}: {item.get('bias','-')}/{item.get('strength','-')} | "
            f"{item.get('source','-')} | {item.get('reason','-')}"
        )
    if not news_lines:
        news_lines.append("- Активных news bias сейчас нет.")

    by_symbol_lines = [f"- {symbol}: {pnl:.2f} RUB" for symbol, pnl in summary["by_symbol"].items()]
    if not by_symbol_lines:
        by_symbol_lines = ["- Нет закрытых сделок."]

    by_strategy_lines = [f"- {name}: {pnl:.2f} RUB" for name, pnl in summary["by_strategy"].items()]
    if not by_strategy_lines:
        by_strategy_lines = ["- Нет данных по стратегиям."]
    by_regime_lines = [f"- {name}: {pnl:.2f} RUB" for name, pnl in summary["by_regime"].items()]
    if not by_regime_lines:
        by_regime_lines = ["- Нет данных по режимам рынка."]
    by_setup_quality_lines = [f"- {name}: {pnl:.2f} RUB" for name, pnl in summary["by_setup_quality"].items()]
    if not by_setup_quality_lines:
        by_setup_quality_lines = ["- Нет данных по качеству сетапов."]
    by_strategy_regime_lines = [f"- {name}: {pnl:.2f} RUB" for name, pnl in summary["by_strategy_regime"].items()]
    if not by_strategy_regime_lines:
        by_strategy_regime_lines = ["- Нет данных по сочетаниям стратегия/режим."]
    recent_positive_lines = [
        f"- {item['name']}: {item['pnl_rub']:.2f} RUB" for item in recent_summary["top_positive_strategy_regimes"]
    ]
    if not recent_positive_lines:
        recent_positive_lines = ["- Нет устойчиво сильных сочетаний за период."]
    recent_negative_lines = [
        f"- {item['name']}: {item['pnl_rub']:.2f} RUB" for item in recent_summary["top_negative_strategy_regimes"]
    ]
    if not recent_negative_lines:
        recent_negative_lines = ["- Нет устойчиво токсичных сочетаний за период."]

    focal_points_lines = []
    best_regime = summary.get("best_regime")
    worst_regime = summary.get("worst_regime")
    best_setup_quality = summary.get("best_setup_quality")
    worst_setup_quality = summary.get("worst_setup_quality")
    best_strategy_regime = summary.get("best_strategy_regime")
    worst_strategy_regime = summary.get("worst_strategy_regime")
    if best_regime:
        focal_points_lines.append(f"- лучший режим: {best_regime['name']} ({best_regime['pnl_rub']:.2f} RUB)")
    if worst_regime:
        focal_points_lines.append(f"- худший режим: {worst_regime['name']} ({worst_regime['pnl_rub']:.2f} RUB)")
    if best_setup_quality:
        focal_points_lines.append(
            f"- лучшее качество сетапа: {best_setup_quality['name']} ({best_setup_quality['pnl_rub']:.2f} RUB)"
        )
    if worst_setup_quality:
        focal_points_lines.append(
            f"- худшее качество сетапа: {worst_setup_quality['name']} ({worst_setup_quality['pnl_rub']:.2f} RUB)"
        )
    if best_strategy_regime:
        focal_points_lines.append(
            f"- лучшая связка стратегия/режим: {best_strategy_regime['name']} ({best_strategy_regime['pnl_rub']:.2f} RUB)"
        )
    if worst_strategy_regime:
        focal_points_lines.append(
            f"- худшая связка стратегия/режим: {worst_strategy_regime['name']} ({worst_strategy_regime['pnl_rub']:.2f} RUB)"
        )
    if not focal_points_lines:
        focal_points_lines = ["- Недостаточно закрытых сделок для режимной сводки."]

    review_lines = [
        f"Дата: {target_day.isoformat()}",
        "",
        "Портфель:",
        *portfolio_lines,
        "",
        "Сводка по дню:",
        f"- закрыто: {summary['closed_count']}",
        f"- win rate: {summary['win_rate']:.1f}%",
        f"- итог по закрытым: {summary['total_pnl_rub']:.2f} RUB",
        "",
        "Итог по инструментам:",
        *by_symbol_lines,
        "",
        "Итог по стратегиям:",
        *by_strategy_lines,
        "",
        "Фокусные точки результата:",
        *focal_points_lines,
        "",
        "Итог по режимам рынка:",
        *by_regime_lines,
        "",
        "Итог по качеству сетапов:",
        *by_setup_quality_lines,
        "",
        "Итог по сочетаниям стратегия/режим:",
        *by_strategy_regime_lines,
        "",
        "Сильные сочетания за последние 3 дня:",
        *recent_positive_lines,
        "",
        "Токсичные сочетания за последние 3 дня:",
        *recent_negative_lines,
        "",
        "Открытые позиции:",
        *open_positions_lines,
        "",
        "Сделки:",
        *trades_lines,
        "",
        "Текущие сигналы:",
        *signal_lines,
        "",
        "Новости:",
        *news_lines,
        "",
        "Наблюдения по инструментам:",
        *[f"- {item}" for item in market_notes],
    ]
    return "\n".join(review_lines)


def first_summary_line(state: dict[str, Any]) -> str:
    summary = state.get("last_signal_summary") or []
    if isinstance(summary, list) and summary:
        return str(summary[0])
    return str(state.get("last_error") or "-")


def extract_output_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
        return payload["output_text"].strip()

    texts: list[str] = []
    for output_item in payload.get("output", []):
        if output_item.get("type") != "message":
            continue
        for content in output_item.get("content", []):
            if content.get("type") == "output_text":
                text = str(content.get("text") or "").strip()
                if text:
                    texts.append(text)
    return "\n\n".join(texts).strip()


def request_openai_review(api_key: str, model: str, prompt: str) -> str:
    return request_openai_text(api_key, model, SYSTEM_INSTRUCTIONS, prompt)


def request_openai_text(api_key: str, model: str, instructions: str, prompt: str) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "instructions": instructions,
        "input": prompt,
    }
    response = requests.post(OPENAI_RESPONSES_URL, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()
    text = extract_output_text(data)
    if not text:
        raise RuntimeError("OpenAI вернул пустой текст ответа")
    return text


def save_review(output_path: Path, target_day: date, model: str, review_text: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dated_path = output_path.parent / f"{target_day.isoformat()}_review.md"
    content = (
        f"# AI Review {target_day.isoformat()}\n\n"
        f"- Модель: `{model}`\n"
        f"- Сформировано: `{datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S МСК')}`\n\n"
        f"{review_text.strip()}\n"
    )
    output_path.write_text(content, encoding="utf-8")
    dated_path.write_text(content, encoding="utf-8")


def build_review_prompt(base_dir: Path, target_day: date) -> str:
    portfolio = load_json(get_portfolio_snapshot_path(base_dir))
    news = load_json(get_news_snapshot_path(base_dir))
    states = load_states(base_dir)
    trade_rows = load_trade_rows(base_dir, target_day)
    closed_trades = pair_closed_trades(trade_rows)
    recent_rows: list[dict[str, Any]] = []
    for offset in range(3):
        recent_rows.extend(load_trade_rows(base_dir, target_day - timedelta(days=offset)))
    recent_closed_trades = pair_closed_trades(recent_rows)
    return build_prompt(target_day, portfolio, news, states, closed_trades, recent_closed_trades)


def main() -> int:
    load_dotenv(BASE_DIR / ".env")
    args = parse_args()
    target_day = parse_target_date(args.target_date)
    base_dir = Path(args.base_dir).expanduser().resolve()
    prompt = build_review_prompt(base_dir, target_day)

    if args.preview:
        print("=== SYSTEM ===")
        print(SYSTEM_INSTRUCTIONS)
        print("\n=== USER ===")
        print(prompt)
        return 0

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Не найден OPENAI_API_KEY. Запусти скрипт с ключом или используй --preview.")

    review_text = request_openai_review(api_key, args.model, prompt)
    output_path = Path(args.output)
    save_review(output_path, target_day, args.model, review_text)
    print(review_text)
    print(f"\nСохранено в: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
