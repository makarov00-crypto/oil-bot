from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

DEFAULT_MODEL = "gpt-5-mini"
DEFAULT_OUTPUT_PATH = BASE_DIR / "logs" / "ai_reviews" / "latest_review.md"

SYSTEM_INSTRUCTIONS = """Ты торговый аналитик для фьючерсного бота на Мосбирже.

Твоя задача:
проанализировать торговый день по журналу сделок, текущим результатам, сигналам и новостному фону.
Не придумывай данные. Опирайся только на переданный контекст.

Что нужно вернуть:
1. Короткий итог дня в 3-5 предложениях.
2. Лучшие инструменты дня.
3. Худшие инструменты дня.
4. Главные ошибки:
   - ошибка входа
   - ошибка выхода
   - переторговка / churn
   - слишком жёсткий или слишком слабый фильтр
5. Что менять завтра:
   - не больше 3 конкретных изменений
6. Что НЕ менять завтра:
   - не больше 3 пунктов
7. Уровень риска на завтра:
   - низкий / средний / высокий
   - и почему

Правила:
- Пиши кратко, по делу, без воды.
- Не советуй полностью переписывать стратегию, если проблема локальная.
- Разделяй:
  - нормальную убыточную сделку
  - плохой вход
  - плохой выход
  - переторговку
- Если данных недостаточно, скажи это явно.
- Не делай общих выводов о всей системе по одному инструменту, если остальные инструменты вели себя иначе.
- Ответ дай на русском языке, в Markdown.
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
    if not trade_journal_path.exists():
        return rows
    for line in trade_journal_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        raw_time = str(row.get("time") or "")
        if not raw_time:
            continue
        try:
            dt = datetime.fromisoformat(raw_time)
        except ValueError:
            continue
        if dt.astimezone(MOSCOW_TZ).date() == target_day:
            row["_dt"] = dt
            rows.append(row)
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
    by_opens: dict[str, int] = defaultdict(int)
    for trade in trades:
        by_symbol[trade.symbol] += trade.pnl_rub
        by_strategy[trade.strategy] += trade.pnl_rub
        by_opens[trade.symbol] += 1

    return {
        "closed_count": len(trades),
        "wins": wins,
        "losses": losses,
        "total_pnl_rub": total,
        "win_rate": win_rate,
        "best_symbol": max(by_symbol.items(), key=lambda item: item[1]) if by_symbol else None,
        "worst_symbol": min(by_symbol.items(), key=lambda item: item[1]) if by_symbol else None,
        "best_strategy": max(by_strategy.items(), key=lambda item: item[1]) if by_strategy else None,
        "worst_strategy": min(by_strategy.items(), key=lambda item: item[1]) if by_strategy else None,
        "churn_symbols": sorted(by_opens.items(), key=lambda item: item[1], reverse=True)[:3],
    }


def build_known_issues(trades: list[ClosedTrade]) -> list[str]:
    issues: list[str] = []
    by_symbol_count: dict[str, int] = defaultdict(int)
    by_symbol_negative: dict[str, float] = defaultdict(float)

    for trade in trades:
        by_symbol_count[trade.symbol] += 1
        by_symbol_negative[trade.symbol] += trade.pnl_rub

    for symbol, count in sorted(by_symbol_count.items(), key=lambda item: item[1], reverse=True):
        if count >= 4:
            issues.append(f"{symbol}: повышенная частота сделок ({count})")
    for symbol, pnl in sorted(by_symbol_negative.items(), key=lambda item: item[1]):
        if pnl < -100:
            issues.append(f"{symbol}: существенный минус за день ({pnl:.2f} RUB)")
    return issues[:6]


def build_prompt(
    target_day: date,
    portfolio: dict[str, Any],
    news: dict[str, Any],
    states: dict[str, dict[str, Any]],
    closed_trades: list[ClosedTrade],
) -> str:
    summary = summarize_closed_trades(closed_trades)
    active_news = list(news.get("active_biases") or [])
    known_issues = build_known_issues(closed_trades)

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
            f"вход: {trade.entry_reason} | выход: {trade.exit_reason}"
        )
    if not trades_lines:
        trades_lines.append("- Закрытых сделок за день нет.")

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

    issues_lines = [f"- {item}" for item in known_issues] if known_issues else ["- Явных проблем не выделено автоматически."]

    review_lines = [
        f"Дата: {target_day.isoformat()}",
        "",
        "Портфель:",
        *portfolio_lines,
        "",
        "Обзор сделок:",
        f"- закрыто: {summary['closed_count']}",
        f"- win rate: {summary['win_rate']:.1f}%",
        f"- итог по закрытым: {summary['total_pnl_rub']:.2f} RUB",
        f"- лучший инструмент: {format_named_pnl(summary['best_symbol'])}",
        f"- худший инструмент: {format_named_pnl(summary['worst_symbol'])}",
        f"- лучшая стратегия: {format_named_pnl(summary['best_strategy'])}",
        f"- худшая стратегия: {format_named_pnl(summary['worst_strategy'])}",
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
        "Известные проблемы:",
        *issues_lines,
    ]
    return "\n".join(review_lines)


def first_summary_line(state: dict[str, Any]) -> str:
    summary = state.get("last_signal_summary") or []
    if isinstance(summary, list) and summary:
        return str(summary[0])
    return str(state.get("last_error") or "-")


def format_named_pnl(item: tuple[str, float] | None) -> str:
    if not item:
        return "-"
    name, value = item
    return f"{name} ({value:.2f} RUB)"


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
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "instructions": SYSTEM_INSTRUCTIONS,
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
    return build_prompt(target_day, portfolio, news, states, closed_trades)


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
