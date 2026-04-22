import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import ta
from dotenv import load_dotenv
from custom_instruments import merge_with_custom_symbols
from instrument_groups import (
    DEFAULT_SYMBOLS,
    get_instrument_group,
    is_currency_instrument as is_currency_symbol,
)
from news_bias import NewsBias, select_active_biases
from news_ingest import CHANNEL_URLS, detect_biases_for_posts, fetch_posts_for_day
from strategy_registry import get_secondary_strategies
from strategy_engine import evaluate_primary_signal_bundle
from trade_storage import append_trade_row, load_trade_rows as load_trade_rows_from_storage, sync_journal_to_db
from tinkoff.invest import (
    CandleInterval,
    Client,
    GetMaxLotsRequest,
    GetOperationsByCursorRequest,
    OrderDirection,
    OrderExecutionReportStatus,
    OrderType,
    OperationState,
    OperationType,
    RequestError,
)
from tinkoff.invest.constants import INVEST_GRPC_API, INVEST_GRPC_API_SANDBOX
from strategies.base import StrategyProfile
from strategies import evaluate_williams_currency_signal as evaluate_williams_signal
from strategies import get_strategy_profile as get_primary_strategy_profile


APP_NAME = "oil-bot-main"
WATCHLIST_REFRESH_SECONDS = 300
RECENT_STRATEGY_GUARD_DAYS = 1
RECENT_STRATEGY_GUARD_MIN_TRADES = 4
RECENT_STRATEGY_GUARD_MAX_WIN_RATE = 25.0
RECENT_STRATEGY_GUARD_MAX_NET_PNL_RUB = -250.0
RECENT_STRATEGY_GUARD_HARD_LOSS_RUB = -500.0
INTRADAY_CHOP_GUARD_STRATEGIES = {
    "failed_breakout",
    "opening_range_breakout",
    "range_break_continuation",
    "breakdown_continuation",
    "momentum_breakout",
    "trend_pullback",
    "trend_rollover",
}
STATE_DIR = Path(__file__).with_name("bot_state")
META_STATE_PATH = STATE_DIR / "_bot_meta.json"
PORTFOLIO_SNAPSHOT_PATH = STATE_DIR / "_portfolio_snapshot.json"
ACCOUNTING_HISTORY_PATH = STATE_DIR / "_accounting_history.json"
RUNTIME_STATUS_PATH = STATE_DIR / "_runtime_status.json"
NEWS_SNAPSHOT_PATH = STATE_DIR / "_news_snapshot.json"
LOG_DIR = Path(__file__).with_name("logs")
TRADE_JOURNAL_PATH = LOG_DIR / "trade_journal.jsonl"
TRADE_DB_PATH = STATE_DIR / "trade_analytics.sqlite3"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
UTC = timezone.utc
NEWS_CACHE_TTL_SECONDS = 300
NEWS_CACHE: dict[str, Any] = {"fetched_at": None, "biases": {}}
SUPPORTED_INTERVALS = {
    1: CandleInterval.CANDLE_INTERVAL_1_MIN,
    2: CandleInterval.CANDLE_INTERVAL_2_MIN,
    3: CandleInterval.CANDLE_INTERVAL_3_MIN,
    5: CandleInterval.CANDLE_INTERVAL_5_MIN,
    10: CandleInterval.CANDLE_INTERVAL_10_MIN,
    15: CandleInterval.CANDLE_INTERVAL_15_MIN,
    30: CandleInterval.CANDLE_INTERVAL_30_MIN,
}

VARMARGIN_OPERATION_TYPES = {
    OperationType.OPERATION_TYPE_ACCRUING_VARMARGIN,
    OperationType.OPERATION_TYPE_WRITING_OFF_VARMARGIN,
}

FEE_OPERATION_TYPES = {
    OperationType.OPERATION_TYPE_BROKER_FEE,
    OperationType.OPERATION_TYPE_SERVICE_FEE,
    OperationType.OPERATION_TYPE_MARGIN_FEE,
}

if hasattr(CandleInterval, "CANDLE_INTERVAL_HOUR"):
    SUPPORTED_INTERVALS[60] = CandleInterval.CANDLE_INTERVAL_HOUR


load_dotenv()


@dataclass
class BotConfig:
    token: str
    account_id: str
    target: str
    symbols: list[str]
    dry_run: bool
    allow_orders: bool
    tg_token: str | None
    tg_chat_id: str | None
    order_quantity: int
    max_order_quantity: int
    risk_per_trade_pct: float
    max_margin_usage_pct: float
    portfolio_usage_pct: float
    capital_reserve_pct: float
    base_trade_allocation_pct: float
    poll_seconds: int
    startup_retry_seconds: int
    candle_hours: int
    candle_interval: CandleInterval
    candle_interval_minutes: int
    higher_tf_interval: CandleInterval
    higher_tf_interval_minutes: int
    max_daily_loss: float
    max_consecutive_errors: int
    max_cycles: int
    stop_loss_pct: float
    trailing_stop_pct: float
    breakeven_profit_pct: float
    min_hold_minutes: int
    ema_slope_threshold: float
    near_ema20_pct: float
    volume_factor: float
    atr_min_pct: float
    long_rsi_min: float
    long_rsi_max: float
    short_rsi_min: float
    short_rsi_max: float
    rsi_exit_long: float
    rsi_exit_short: float


@dataclass
class InstrumentConfig:
    symbol: str
    figi: str
    display_name: str
    lot: int = 1
    min_price_increment: float = 0.0
    min_price_increment_amount: float = 0.0
    initial_margin_on_buy: float = 0.0
    initial_margin_on_sell: float = 0.0


@dataclass
class AccountSnapshot:
    total_portfolio: float
    free_rub: float
    blocked_guarantee_rub: float


@dataclass
class ExitProfile:
    min_hold_minutes: int
    breakeven_profit_pct: float
    trailing_stop_pct: float


@dataclass
class InstrumentState:
    entry_price: float | None = None
    max_price: float | None = None
    min_price: float | None = None
    position_qty: int = 0
    position_side: str = "FLAT"
    realized_pnl: float = 0.0
    trading_day: str = ""
    last_signal: str = "HOLD"
    last_error: str = ""
    breakeven_armed: bool = False
    last_status_candle: str = ""
    last_risk_stop_day: str = ""
    pending_order_id: str = ""
    pending_order_action: str = ""
    pending_order_side: str = ""
    pending_order_qty: int = 0
    pending_submitted_at: str = ""
    pending_entry_reason: str = ""
    pending_exit_reason: str = ""
    delayed_close_recovery_needed: bool = False
    delayed_close_side: str = ""
    delayed_close_qty: int = 0
    delayed_close_entry_price: float | None = None
    delayed_close_entry_commission_rub: float = 0.0
    delayed_close_strategy: str = ""
    delayed_close_reason: str = ""
    delayed_close_entry_time: str = ""
    delayed_close_submitted_at: str = ""
    delayed_close_queue: list[dict[str, Any]] = field(default_factory=list)
    execution_status: str = "idle"
    last_fill_price: float | None = None
    entry_time: str = ""
    entry_strategy: str = ""
    entry_reason: str = ""
    last_strategy_name: str = ""
    last_higher_tf_bias: str = ""
    last_news_bias: str = "NEUTRAL"
    last_news_impact: str = ""
    last_market_regime: str = ""
    last_setup_quality_label: str = ""
    last_setup_quality_score: int = 0
    last_volume_ratio: float = 0.0
    last_body_ratio: float = 0.0
    last_atr_pct: float = 0.0
    last_range_width_pct: float = 0.0
    last_signal_summary: list[str] = field(default_factory=list)
    last_allocator_summary: str = ""
    last_allocator_quantity: int = 0
    last_entry_allocator_summary: str = ""
    last_entry_allocator_quantity: int = 0
    last_entry_allocator_time: str = ""
    last_exit_time: str = ""
    last_exit_side: str = ""
    last_exit_reason: str = ""
    last_exit_pnl_rub: float = 0.0
    last_exit_price: float | None = None
    entry_commission_rub: float = 0.0
    entry_commission_accounted: bool = False
    realized_gross_pnl_rub: float = 0.0
    realized_commission_rub: float = 0.0
    last_market_price: float | None = None
    position_notional_rub: float = 0.0
    position_variation_margin_rub: float = 0.0
    position_pnl_pct: float = 0.0



def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("tinkoff").setLevel(logging.WARNING)
    logging.getLogger("grpc").setLevel(logging.WARNING)


def parse_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def parse_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def parse_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_symbols_env() -> list[str]:
    raw = os.getenv("T_INVEST_SYMBOLS", DEFAULT_SYMBOLS)
    base_symbols = [item.strip().upper() for item in raw.split(",") if item.strip()]
    return merge_with_custom_symbols(base_symbols)


def state_path_for(symbol: str) -> Path:
    safe_symbol = re.sub(r"[^A-Za-z0-9_-]+", "_", symbol)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"{safe_symbol}.json"


def load_meta_state() -> dict[str, Any]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not META_STATE_PATH.exists():
        return {}
    try:
        return json.loads(META_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_meta_state(meta: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    META_STATE_PATH.write_text(json.dumps(meta, ensure_ascii=True, indent=2), encoding="utf-8")


def save_portfolio_snapshot(payload: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    PORTFOLIO_SNAPSHOT_PATH.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def load_accounting_history() -> dict[str, Any]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if not ACCOUNTING_HISTORY_PATH.exists():
        return {}
    try:
        return json.loads(ACCOUNTING_HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_accounting_history(history: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    ACCOUNTING_HISTORY_PATH.write_text(
        json.dumps(history, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def save_runtime_status(payload: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_STATUS_PATH.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def save_news_snapshot(payload: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    NEWS_SNAPSHOT_PATH.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def load_state(symbol: str) -> InstrumentState:
    path = state_path_for(symbol)
    if not path.exists():
        return InstrumentState()
    try:
        return InstrumentState(**json.loads(path.read_text(encoding="utf-8")))
    except Exception as error:
        logging.warning("Не удалось прочитать state %s: %s", path, error)
        return InstrumentState()


def save_state(symbol: str, state: InstrumentState) -> None:
    state_path_for(symbol).write_text(
        json.dumps(asdict(state), ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def has_pending_order(state: InstrumentState) -> bool:
    return bool(state.pending_order_id)


def clear_pending_order(state: InstrumentState) -> None:
    state.pending_order_id = ""
    state.pending_order_action = ""
    state.pending_order_side = ""
    state.pending_order_qty = 0
    state.pending_submitted_at = ""
    state.pending_entry_reason = ""
    state.pending_exit_reason = ""


def clear_delayed_close_recovery(state: InstrumentState) -> None:
    state.delayed_close_recovery_needed = False
    state.delayed_close_side = ""
    state.delayed_close_qty = 0
    state.delayed_close_entry_price = None
    state.delayed_close_entry_commission_rub = 0.0
    state.delayed_close_strategy = ""
    state.delayed_close_reason = ""
    state.delayed_close_entry_time = ""
    state.delayed_close_submitted_at = ""
    state.delayed_close_queue = []


def build_delayed_close_snapshot(
    *,
    previous_side: str,
    previous_qty: int,
    previous_entry_price: float | None,
    previous_entry_commission: float,
    previous_strategy: str,
    previous_exit_reason: str,
    previous_entry_time: datetime | None,
    submitted_at: datetime,
) -> dict[str, Any]:
    return {
        "side": previous_side,
        "qty": int(previous_qty or 0),
        "entry_price": previous_entry_price,
        "entry_commission_rub": float(previous_entry_commission or 0.0),
        "strategy": previous_strategy or "",
        "reason": previous_exit_reason or "",
        "entry_time": previous_entry_time.isoformat() if previous_entry_time else "",
        "submitted_at": submitted_at.isoformat(),
    }


def sync_legacy_delayed_close_fields(state: InstrumentState) -> None:
    queue = list(state.delayed_close_queue or [])
    if not queue:
        state.delayed_close_recovery_needed = False
        state.delayed_close_side = ""
        state.delayed_close_qty = 0
        state.delayed_close_entry_price = None
        state.delayed_close_entry_commission_rub = 0.0
        state.delayed_close_strategy = ""
        state.delayed_close_reason = ""
        state.delayed_close_entry_time = ""
        state.delayed_close_submitted_at = ""
        return
    item = queue[0]
    state.delayed_close_recovery_needed = True
    state.delayed_close_side = str(item.get("side") or "")
    state.delayed_close_qty = int(item.get("qty") or 0)
    state.delayed_close_entry_price = item.get("entry_price")
    state.delayed_close_entry_commission_rub = float(item.get("entry_commission_rub") or 0.0)
    state.delayed_close_strategy = str(item.get("strategy") or "")
    state.delayed_close_reason = str(item.get("reason") or "")
    state.delayed_close_entry_time = str(item.get("entry_time") or "")
    state.delayed_close_submitted_at = str(item.get("submitted_at") or "")


def ensure_delayed_close_queue(state: InstrumentState) -> list[dict[str, Any]]:
    queue = list(state.delayed_close_queue or [])
    if not queue and state.delayed_close_recovery_needed and state.delayed_close_side and int(state.delayed_close_qty or 0) > 0:
        queue.append(
            {
                "side": state.delayed_close_side,
                "qty": int(state.delayed_close_qty or 0),
                "entry_price": state.delayed_close_entry_price,
                "entry_commission_rub": float(state.delayed_close_entry_commission_rub or 0.0),
                "strategy": state.delayed_close_strategy or "",
                "reason": state.delayed_close_reason or "",
                "entry_time": state.delayed_close_entry_time or "",
                "submitted_at": state.delayed_close_submitted_at or "",
            }
        )
    state.delayed_close_queue = queue
    sync_legacy_delayed_close_fields(state)
    return state.delayed_close_queue


def enqueue_delayed_close_snapshot(state: InstrumentState, snapshot: dict[str, Any]) -> None:
    queue = ensure_delayed_close_queue(state)
    queue.append(snapshot)
    queue.sort(key=lambda item: str(item.get("submitted_at") or ""))
    state.delayed_close_queue = queue
    sync_legacy_delayed_close_fields(state)


def parse_state_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def build_trade_event_context(state: InstrumentState | None) -> dict[str, Any]:
    if state is None:
        return {}
    signal_summary = [str(item) for item in list(state.last_signal_summary or [])[:4] if str(item).strip()]
    context = {
        "higher_tf_bias": str(state.last_higher_tf_bias or ""),
        "news_bias": str(state.last_news_bias or ""),
        "news_impact": str(state.last_news_impact or ""),
        "market_regime": str(state.last_market_regime or ""),
        "setup_quality_label": str(state.last_setup_quality_label or ""),
        "setup_quality_score": int(state.last_setup_quality_score or 0),
        "volume_ratio": round(float(state.last_volume_ratio or 0.0), 3),
        "body_ratio": round(float(state.last_body_ratio or 0.0), 3),
        "atr_pct": round(float(state.last_atr_pct or 0.0), 6),
        "range_width_pct": round(float(state.last_range_width_pct or 0.0), 6),
        "allocator_quantity": int(state.last_allocator_quantity or 0),
        "allocator_summary": str(state.last_allocator_summary or ""),
        "entry_allocator_quantity": int(state.last_entry_allocator_quantity or 0),
        "entry_allocator_summary": str(state.last_entry_allocator_summary or ""),
        "signal_summary": signal_summary,
        "execution_status": str(state.execution_status or ""),
    }
    return {key: value for key, value in context.items() if value not in ("", [], None)}


def append_trade_journal(
    instrument: InstrumentConfig,
    event: str,
    side: str,
    qty: int,
    price: float,
    *,
    event_time: datetime | str | None = None,
    pnl_rub: float | None = None,
    gross_pnl_rub: float | None = None,
    commission_rub: float | None = None,
    net_pnl_rub: float | None = None,
    reason: str = "",
    source: str = "",
    strategy: str = "",
    dry_run: bool = True,
    state: InstrumentState | None = None,
) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if isinstance(event_time, datetime):
        journal_time = event_time.astimezone(MOSCOW_TZ).isoformat()
    elif isinstance(event_time, str) and event_time.strip():
        journal_time = event_time.strip()
    else:
        journal_time = datetime.now(UTC).astimezone(MOSCOW_TZ).isoformat()
    event_name = str(event).upper()
    side_name = str(side).upper()
    strategy_name = str(strategy or "")
    reason_text = str(reason or "")
    source_text = str(source or "")
    try:
        existing_rows = load_trade_journal()[-20:]
    except Exception:
        existing_rows = []
    for existing in reversed(existing_rows):
        try:
            same_price = round(float(existing.get("price") or 0.0), 6) == round(float(price or 0.0), 6)
        except Exception:
            same_price = False
        same_core_identity = (
            str(existing.get("time", "")).strip() == journal_time
            and str(existing.get("symbol", "")).upper() == instrument.symbol.upper()
            and str(existing.get("event", "")).upper() == event_name
            and str(existing.get("side", "")).upper() == side_name
            and int(existing.get("qty_lots") or 0) == int(qty)
            and same_price
            and str(existing.get("strategy", "") or "") == strategy_name
        )
        # Recovery and portfolio-confirmation can describe the same OPEN with
        # different service metadata. Treat identical execution payloads as one event.
        if event_name == "OPEN" and same_core_identity:
            logging.info(
                "Пропускаю семантический дубль OPEN: %s %s %s %s @ %s",
                instrument.symbol,
                event_name,
                side_name,
                qty,
                journal_time,
            )
            return
        if (
            same_core_identity
            and str(existing.get("reason", "") or "") == reason_text
            and str(existing.get("source", "") or "") == source_text
            and str(existing.get("strategy", "") or "") == strategy_name
        ):
            logging.info(
                "Пропускаю дублирующую запись журнала: %s %s %s %s @ %s",
                instrument.symbol,
                event_name,
                side_name,
                qty,
                journal_time,
            )
            return
    row = {
        "time": journal_time,
        "symbol": instrument.symbol,
        "display_name": instrument.display_name,
        "event": event,
        "side": side,
        "qty_lots": qty,
        "lot_size": instrument.lot,
        "price": price,
        "pnl_rub": pnl_rub,
        "gross_pnl_rub": gross_pnl_rub,
        "commission_rub": commission_rub,
        "net_pnl_rub": net_pnl_rub,
        "reason": reason,
        "source": source,
        "strategy": strategy,
        "mode": "DRY_RUN" if dry_run else "LIVE",
        "session": get_market_session(),
    }
    context = build_trade_event_context(state)
    if context:
        row["context"] = context
    with TRADE_JOURNAL_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    append_trade_row(TRADE_DB_PATH, row)


def load_trade_journal() -> list[dict[str, Any]]:
    try:
        return load_trade_rows_from_storage(TRADE_JOURNAL_PATH, TRADE_DB_PATH)
    except Exception as error:
        logging.warning("Не удалось загрузить журнал сделок из trade storage: %s", error)
        if not TRADE_JOURNAL_PATH.exists():
            return []
        rows: list[dict[str, Any]] = []
        for line in TRADE_JOURNAL_PATH.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except Exception as inner_error:
                logging.warning("Не удалось прочитать строку журнала сделок: %s", inner_error)
        return rows


def save_trade_journal(rows: list[dict[str, Any]]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with TRADE_JOURNAL_PATH.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    sync_journal_to_db(TRADE_JOURNAL_PATH, TRADE_DB_PATH)


def update_latest_unclosed_open_journal_entry(
    symbol: str,
    side: str,
    *,
    not_before: datetime | None = None,
    commission_rub: float | None = None,
    net_pnl_rub: float | None = None,
) -> bool:
    rows = load_trade_journal()
    target_symbol = symbol.upper()
    target_side = side.upper()
    unmatched_closes = 0

    for row in reversed(rows):
        if str(row.get("symbol", "")).upper() != target_symbol:
            continue
        if str(row.get("side", "")).upper() != target_side:
            continue
        event = str(row.get("event", "")).upper()
        if event == "CLOSE":
            unmatched_closes += 1
            continue
        if event != "OPEN":
            continue
        if unmatched_closes > 0:
            unmatched_closes -= 1
            continue
        row_dt = parse_state_datetime(str(row.get("time") or ""))
        if not_before is not None and row_dt is not None and row_dt < not_before:
            continue
        changed = False
        if commission_rub is not None:
            row["commission_rub"] = round(float(commission_rub), 2)
            changed = True
        if net_pnl_rub is not None:
            row["net_pnl_rub"] = round(float(net_pnl_rub), 2)
            changed = True
        if changed:
            save_trade_journal(rows)
        return changed
    return False


def update_latest_close_journal_entry(
    symbol: str,
    side: str,
    *,
    not_before: datetime | None = None,
    price: float | None = None,
    gross_pnl_rub: float | None = None,
    commission_rub: float | None = None,
    net_pnl_rub: float | None = None,
    pnl_rub: float | None = None,
    event_time: datetime | None = None,
) -> bool:
    rows = load_trade_journal()
    target_symbol = symbol.upper()
    target_side = side.upper()

    for row in reversed(rows):
        if str(row.get("symbol", "")).upper() != target_symbol:
            continue
        if str(row.get("side", "")).upper() != target_side:
            continue
        if str(row.get("event", "")).upper() != "CLOSE":
            continue
        row_dt = parse_state_datetime(str(row.get("time") or ""))
        if not_before is not None and row_dt is not None and row_dt < not_before:
            continue
        changed = False
        if price is not None:
            row["price"] = price
            changed = True
        if gross_pnl_rub is not None:
            row["gross_pnl_rub"] = round(float(gross_pnl_rub), 2)
            changed = True
        if commission_rub is not None:
            row["commission_rub"] = round(float(commission_rub), 2)
            changed = True
        if net_pnl_rub is not None:
            row["net_pnl_rub"] = round(float(net_pnl_rub), 2)
            changed = True
        if pnl_rub is not None:
            row["pnl_rub"] = round(float(pnl_rub), 2)
            changed = True
        if event_time is not None:
            row["time"] = event_time.astimezone(MOSCOW_TZ).isoformat()
            changed = True
        if changed:
            save_trade_journal(rows)
        return changed
    return False


def get_today_trade_journal_rows() -> list[dict[str, Any]]:
    today = datetime.now(MOSCOW_TZ).date().isoformat()
    rows = []
    for row in load_trade_journal():
        row_time = str(row.get("time", ""))
        if row_time.startswith(today):
            rows.append(row)
    return rows


def _trade_net_pnl(row: dict[str, Any]) -> float:
    for key in ("net_pnl_rub", "pnl_rub"):
        if row.get(key) in (None, ""):
            continue
        try:
            return float(row.get(key) or 0.0)
        except Exception:
            return 0.0
    return 0.0


def aggregate_closed_strategy_trades(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trades: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for index, row in enumerate(rows):
        if str(row.get("event", "")).upper() != "CLOSE":
            continue
        broker_op_id = str(row.get("broker_op_id") or "").strip()
        if broker_op_id:
            key = (
                f"broker:{str(row.get('symbol', '')).upper()}:"
                f"{str(row.get('strategy', '') or '')}:"
                f"{str(row.get('side', '')).upper()}:"
                f"{broker_op_id}"
            )
        else:
            key = f"row:{index}"
        if key not in trades:
            ordered_keys.append(key)
            trades[key] = {
                "symbol": str(row.get("symbol", "")).upper(),
                "strategy": str(row.get("strategy", "") or ""),
                "side": str(row.get("side", "")).upper(),
                "time": str(row.get("time") or ""),
                "rows": 0,
                "qty_lots": 0,
                "net_pnl_rub": 0.0,
            }
        item = trades[key]
        item["rows"] = int(item["rows"]) + 1
        item["qty_lots"] = int(item["qty_lots"]) + max(1, int(row.get("qty_lots") or 0))
        item["net_pnl_rub"] = round(float(item["net_pnl_rub"]) + _trade_net_pnl(row), 2)
    return [trades[key] for key in ordered_keys]


def get_trade_journal_rows_since(start_day: date) -> list[dict[str, Any]]:
    start_value = start_day.isoformat()
    rows = []
    for row in load_trade_journal():
        row_day = str(row.get("time", ""))[:10]
        if row_day >= start_value:
            rows.append(row)
    return rows


def calculate_closed_trade_totals(rows: list[dict[str, Any]] | None = None) -> dict[str, float]:
    source_rows = rows if rows is not None else get_today_trade_journal_rows()
    gross = 0.0
    commission = 0.0
    net = 0.0
    for row in source_rows:
        if str(row.get("event", "")).upper() != "CLOSE":
            continue
        try:
            gross += float(row.get("gross_pnl_rub") or 0.0)
        except Exception:
            pass
        try:
            commission += float(row.get("commission_rub") or 0.0)
        except Exception:
            pass
        if row.get("net_pnl_rub") not in (None, ""):
            try:
                net += float(row.get("net_pnl_rub") or 0.0)
                continue
            except Exception:
                pass
        try:
            net += float(row.get("pnl_rub") or 0.0)
        except Exception:
            pass
    return {
        "gross_pnl_rub": round(gross, 2),
        "commission_rub": round(commission, 2),
        "net_pnl_rub": round(net, 2),
    }


def get_today_closed_net_pnl_rub(rows: list[dict[str, Any]] | None = None) -> float:
    return float(calculate_closed_trade_totals(rows)["net_pnl_rub"])


def reconcile_state_accounting(symbol: str, state: InstrumentState) -> None:
    gross = 0.0
    commission = 0.0
    net = 0.0
    unmatched_open_rows: list[dict[str, Any]] = []
    for row in get_today_trade_journal_rows():
        if str(row.get("symbol", "")).upper() != symbol.upper():
            continue
        event = str(row.get("event", "")).upper()
        if event == "OPEN":
            unmatched_open_rows.append(row)
            continue
        if event != "CLOSE":
            continue
        if unmatched_open_rows:
            unmatched_open_rows.pop(0)
        try:
            gross += float(row.get("gross_pnl_rub") or 0.0)
        except Exception:
            pass
        try:
            commission += float(row.get("commission_rub") or 0.0)
        except Exception:
            pass
        if row.get("net_pnl_rub") not in (None, ""):
            try:
                net += float(row.get("net_pnl_rub") or 0.0)
                continue
            except Exception:
                pass
        try:
            net += float(row.get("pnl_rub") or 0.0)
        except Exception:
            pass

    if state.position_side != "FLAT" and unmatched_open_rows:
        active_open = unmatched_open_rows[-1]
        try:
            open_commission = float(active_open.get("commission_rub") or 0.0)
        except Exception:
            open_commission = 0.0
        commission += open_commission
        net -= open_commission
        state.entry_commission_rub = open_commission
        state.entry_commission_accounted = open_commission > 0
    elif state.position_side == "FLAT":
        state.entry_commission_rub = 0.0
        state.entry_commission_accounted = False

    state.realized_gross_pnl_rub = round(gross, 2)
    state.realized_commission_rub = round(commission, 2)
    state.realized_pnl = round(net, 2)


def has_today_active_open_journal_entry(symbol: str, side: str) -> bool:
    return get_active_journal_lots(symbol, side) > 0


def get_active_journal_lots(symbol: str, side: str, rows: list[dict[str, Any]] | None = None) -> int:
    target_symbol = symbol.upper()
    target_side = side.upper()
    active_lots = 0
    source_rows = rows if rows is not None else get_today_trade_journal_rows()
    for row in source_rows:
        if str(row.get("symbol", "")).upper() != target_symbol:
            continue
        if str(row.get("side", "")).upper() != target_side:
            continue
        event = str(row.get("event", "")).upper()
        qty = int(row.get("qty_lots") or 0)
        if event == "OPEN":
            active_lots += qty
        elif event == "CLOSE":
            active_lots = max(0, active_lots - qty)
    return max(0, active_lots)


def has_journal_event_since(
    symbol: str,
    side: str,
    event: str,
    *,
    not_before: datetime | None = None,
    tolerance_seconds: float = 0.0,
) -> bool:
    target_symbol = symbol.upper()
    target_side = side.upper()
    target_event = event.upper()
    source_rows = load_trade_journal()
    adjusted_not_before = not_before
    if adjusted_not_before is not None and tolerance_seconds > 0:
        adjusted_not_before = adjusted_not_before - timedelta(seconds=tolerance_seconds)
    for row in reversed(source_rows):
        if str(row.get("symbol", "")).upper() != target_symbol:
            continue
        if str(row.get("side", "")).upper() != target_side:
            continue
        if str(row.get("event", "")).upper() != target_event:
            continue
        row_dt = parse_state_datetime(str(row.get("time") or ""))
        if adjusted_not_before is not None and row_dt is not None and row_dt < adjusted_not_before:
            continue
        return True
    return False


def find_recent_live_open_details(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    side: str,
    qty: int,
    entry_price: float,
    not_before: datetime | None = None,
) -> tuple[datetime | None, float | None]:
    from_utc, to_utc = get_moscow_day_bounds_utc()
    cursor = ""
    fee_by_parent: dict[str, float] = {}
    candidates: list[tuple[datetime, str]] = []
    expected_type = (
        OperationType.OPERATION_TYPE_BUY if side.upper() == "LONG" else OperationType.OPERATION_TYPE_SELL
    )
    tolerance = max(instrument.min_price_increment * 2, 1e-6)

    while True:
        response = client.operations.get_operations_by_cursor(
            GetOperationsByCursorRequest(
                account_id=config.account_id,
                from_=from_utc,
                to=to_utc,
                cursor=cursor,
                limit=200,
                state=OperationState.OPERATION_STATE_EXECUTED,
                without_commissions=False,
                without_overnights=False,
                without_trades=False,
            )
        )
        for item in getattr(response, "items", []) or []:
            if str(getattr(item, "figi", "") or "") != instrument.figi:
                continue
            op_type = getattr(item, "type", None)
            op_id = str(getattr(item, "id", "") or "")
            parent_id = str(getattr(item, "parent_operation_id", "") or "")
            payment = quotation_to_float(getattr(item, "payment", None))
            if op_type in FEE_OPERATION_TYPES and parent_id:
                fee_by_parent[parent_id] = fee_by_parent.get(parent_id, 0.0) + abs(payment)
                continue
            if op_type != expected_type:
                continue
            op_qty = int(getattr(item, "quantity", 0) or 0)
            if qty > 0 and op_qty not in {0, qty}:
                continue
            op_price = quotation_to_float(getattr(item, "price", None))
            if entry_price > 0 and op_price > 0 and abs(op_price - entry_price) > tolerance:
                continue
            op_time = getattr(item, "date", None)
            if isinstance(op_time, datetime):
                if not_before is not None:
                    compare_dt = op_time
                    if compare_dt.tzinfo is None:
                        compare_dt = compare_dt.replace(tzinfo=UTC)
                    if compare_dt < not_before:
                        continue
                candidates.append((op_time, op_id))

        next_cursor = str(getattr(response, "next_cursor", "") or "")
        if not getattr(response, "has_next", False) or not next_cursor:
            break
        cursor = next_cursor

    if not candidates:
        return None, None

    candidates.sort(key=lambda item: item[0])
    op_time, op_id = candidates[-1]
    return op_time, fee_by_parent.get(op_id)


def find_recent_live_close_details(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    previous_side: str,
    qty: int,
    not_before: datetime | None = None,
) -> tuple[datetime | None, float | None, float | None]:
    from_utc, to_utc = get_moscow_day_bounds_utc()
    cursor = ""
    fee_by_parent: dict[str, float] = {}
    candidates: list[tuple[datetime, str, float]] = []
    expected_type = (
        OperationType.OPERATION_TYPE_BUY if previous_side.upper() == "SHORT" else OperationType.OPERATION_TYPE_SELL
    )

    while True:
        response = client.operations.get_operations_by_cursor(
            GetOperationsByCursorRequest(
                account_id=config.account_id,
                from_=from_utc,
                to=to_utc,
                cursor=cursor,
                limit=200,
                state=OperationState.OPERATION_STATE_EXECUTED,
                without_commissions=False,
                without_overnights=False,
                without_trades=False,
            )
        )
        for item in getattr(response, "items", []) or []:
            if str(getattr(item, "figi", "") or "") != instrument.figi:
                continue
            op_type = getattr(item, "type", None)
            op_id = str(getattr(item, "id", "") or "")
            parent_id = str(getattr(item, "parent_operation_id", "") or "")
            payment = quotation_to_float(getattr(item, "payment", None))
            if op_type in FEE_OPERATION_TYPES and parent_id:
                fee_by_parent[parent_id] = fee_by_parent.get(parent_id, 0.0) + abs(payment)
                continue
            if op_type != expected_type:
                continue
            op_qty = int(getattr(item, "quantity", 0) or 0)
            if qty > 0 and op_qty not in {0, qty}:
                continue
            op_time = getattr(item, "date", None)
            if isinstance(op_time, datetime):
                if not_before is not None:
                    compare_dt = op_time
                    if compare_dt.tzinfo is None:
                        compare_dt = compare_dt.replace(tzinfo=UTC)
                    if compare_dt < not_before:
                        continue
                op_price = quotation_to_float(getattr(item, "price", None))
                candidates.append((op_time, op_id, op_price))

        next_cursor = str(getattr(response, "next_cursor", "") or "")
        if not getattr(response, "has_next", False) or not next_cursor:
            break
        cursor = next_cursor

    if not candidates:
        return None, None, None

    candidates.sort(key=lambda item: item[0])
    op_time, op_id, op_price = candidates[-1]
    return op_time, fee_by_parent.get(op_id), op_price


def confirm_pending_open_from_broker(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    *,
    not_before: datetime | None = None,
) -> bool:
    if (
        state.position_qty <= 0
        or state.position_side == "FLAT"
        or state.position_side != state.pending_order_side
        or state.position_qty < state.pending_order_qty
        or state.entry_price is None
    ):
        return False
    operation_time, entry_fee_rub = find_recent_live_open_details(
        client,
        config,
        instrument,
        state.position_side,
        state.position_qty,
        state.entry_price,
        not_before=not_before,
    )
    if operation_time is not None:
        state.entry_time = operation_time.isoformat()
    active_open_lots = get_active_journal_lots(instrument.symbol, state.position_side)
    missing_open_lots = max(0, int(state.position_qty) - int(active_open_lots))
    has_matching_open_entry = (
        has_journal_event_since(
            instrument.symbol,
            state.position_side,
            "OPEN",
            not_before=not_before,
        )
        if not_before is not None
        else active_open_lots > 0
    )
    if not has_matching_open_entry and active_open_lots >= int(state.position_qty):
        has_matching_open_entry = True
    if not has_matching_open_entry and missing_open_lots > 0:
        entry_reason = compact_reason(
            state.pending_entry_reason
            or state.entry_reason
            or "Позиция подтверждена по брокерскому портфелю."
        )
        state.entry_reason = entry_reason
        append_trade_journal(
            instrument,
            "OPEN",
            state.position_side,
            missing_open_lots,
            state.entry_price,
            event_time=operation_time,
            gross_pnl_rub=0.0,
            commission_rub=entry_fee_rub,
            net_pnl_rub=-entry_fee_rub if entry_fee_rub is not None else None,
            reason=entry_reason,
            source="portfolio_confirmation",
            strategy=state.entry_strategy or state.last_strategy_name or "recovered_position",
            dry_run=config.dry_run,
            state=state,
        )
    if entry_fee_rub is not None and entry_fee_rub > 0:
        state.entry_commission_rub = entry_fee_rub
        state.entry_commission_accounted = True
        update_latest_unclosed_open_journal_entry(
            instrument.symbol,
            state.position_side,
            not_before=not_before or operation_time,
            commission_rub=entry_fee_rub,
            net_pnl_rub=-entry_fee_rub,
        )
    state.execution_status = "confirmed_open"
    state.last_error = ""
    clear_pending_order(state)
    save_state(instrument.symbol, state)
    logging.info(
        "symbol=%s status=pending_open_confirmed_via_portfolio qty=%s",
        instrument.symbol,
        state.position_qty,
    )
    return True


def confirm_pending_close_from_broker(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    *,
    previous_side: str,
    previous_qty: int,
    previous_entry_price: float | None,
    previous_entry_commission: float,
    previous_strategy: str,
    previous_exit_reason: str,
    previous_entry_time: datetime | None,
    source: str,
    recovered_status: str,
    not_before: datetime | None = None,
) -> bool:
    if previous_side == "FLAT" or previous_qty <= 0:
        return False
    close_time, close_fee_rub, close_price = find_recent_live_close_details(
        client,
        config,
        instrument,
        previous_side,
        previous_qty,
        not_before=not_before,
    )
    if close_time is None:
        return False
    recovered_entry_commission = previous_entry_commission
    if recovered_entry_commission <= 0 and previous_entry_price is not None:
        _, recovered_open_fee = find_recent_live_open_details(
            client,
            config,
            instrument,
            previous_side,
            previous_qty,
            previous_entry_price,
            not_before=previous_entry_time,
        )
        if recovered_open_fee is not None and recovered_open_fee > 0:
            recovered_entry_commission = recovered_open_fee
    if close_price is None or close_price <= 0:
        close_price = get_last_price(client, instrument)
    gross_pnl = 0.0
    if previous_entry_price is not None and close_price is not None:
        gross_pnl = calculate_futures_pnl_rub(
            instrument,
            previous_entry_price,
            close_price,
            previous_qty,
            previous_side,
        )
    reset_daily_pnl_if_needed(state)
    close_fee_only = float(close_fee_rub or 0.0)
    recovered_entry_delta = max(0.0, recovered_entry_commission - previous_entry_commission)
    total_trade_commission = recovered_entry_commission + close_fee_only
    net_trade_pnl = gross_pnl - total_trade_commission
    if recovered_entry_delta > 0:
        state.realized_commission_rub += recovered_entry_delta
        state.realized_pnl -= recovered_entry_delta
    state.realized_gross_pnl_rub += gross_pnl
    state.realized_commission_rub += close_fee_only
    state.realized_pnl += gross_pnl - close_fee_only
    state.last_exit_time = (close_time or datetime.now(UTC)).isoformat()
    state.last_exit_side = previous_side
    state.last_exit_reason = previous_exit_reason
    state.last_exit_pnl_rub = net_trade_pnl
    state.last_exit_price = close_price
    state.execution_status = recovered_status
    append_trade_journal(
        instrument,
        "CLOSE",
        previous_side,
        previous_qty,
        close_price,
        event_time=close_time,
        pnl_rub=net_trade_pnl,
        gross_pnl_rub=gross_pnl,
        commission_rub=total_trade_commission,
        net_pnl_rub=net_trade_pnl,
        reason=previous_exit_reason,
        source=source,
        strategy=previous_strategy,
        dry_run=False,
        state=state,
    )
    update_latest_close_journal_entry(
        instrument.symbol,
        previous_side,
        not_before=not_before,
        price=close_price,
        gross_pnl_rub=gross_pnl,
        commission_rub=total_trade_commission,
        net_pnl_rub=net_trade_pnl,
        pnl_rub=net_trade_pnl,
        event_time=close_time,
    )
    state.last_error = ""
    clear_pending_order(state)
    save_state(instrument.symbol, state)
    logging.info(
        "symbol=%s status=pending_close_confirmed_via_portfolio qty=%s source=%s",
        instrument.symbol,
        previous_qty,
        source,
    )
    return True


DELAYED_CLOSE_RECOVERY_MAX_AGE_SECONDS = 6 * 60 * 60


def reconcile_delayed_close_from_broker(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
) -> bool:
    queue = ensure_delayed_close_queue(state)
    if not queue:
        return False

    changed = False
    for item in list(queue):
        previous_side = str(item.get("side") or "")
        previous_qty = int(item.get("qty") or 0)
        if previous_side == "FLAT" or previous_qty <= 0:
            queue.remove(item)
            changed = True
            continue

        previous_entry_time = parse_state_datetime(str(item.get("entry_time") or ""))
        delayed_submitted_at = parse_state_datetime(str(item.get("submitted_at") or ""))
        close_not_before = previous_entry_time
        if delayed_submitted_at is not None and (close_not_before is None or delayed_submitted_at > close_not_before):
            close_not_before = delayed_submitted_at

        if has_journal_event_since(
            instrument.symbol,
            previous_side,
            "CLOSE",
            not_before=close_not_before,
            tolerance_seconds=60.0,
        ):
            queue.remove(item)
            changed = True
            logging.info("symbol=%s status=delayed_close_already_in_journal", instrument.symbol)
            continue

        if confirm_pending_close_from_broker(
            client,
            config,
            instrument,
            state,
            previous_side=previous_side,
            previous_qty=previous_qty,
            previous_entry_price=item.get("entry_price"),
            previous_entry_commission=float(item.get("entry_commission_rub") or 0.0),
            previous_strategy=str(item.get("strategy") or ""),
            previous_exit_reason=str(item.get("reason") or "Закрытие подтверждено брокерской операцией"),
            previous_entry_time=previous_entry_time,
            source="delayed_broker_ops_recovery",
            recovered_status="recovered_close",
            not_before=close_not_before,
        ):
            queue.remove(item)
            state.delayed_close_queue = queue
            sync_legacy_delayed_close_fields(state)
            save_state(instrument.symbol, state)
            logging.info("symbol=%s status=delayed_close_recovered", instrument.symbol)
            return True

        if delayed_submitted_at is not None:
            age_seconds = (datetime.now(UTC) - delayed_submitted_at).total_seconds()
            if age_seconds > DELAYED_CLOSE_RECOVERY_MAX_AGE_SECONDS:
                state.last_error = (
                    "Закрытие позиции пока не удалось подтвердить по брокерским операциям. "
                    "Recovery будет продолжать попытки автоматически."
                )
                logging.warning("symbol=%s status=delayed_close_recovery_overdue", instrument.symbol)
    if changed:
        state.delayed_close_queue = queue
        sync_legacy_delayed_close_fields(state)
        save_state(instrument.symbol, state)
    return False


@dataclass
class BrokerTradeOp:
    symbol: str
    display_name: str
    figi: str
    op_id: str
    parent_id: str
    op_type: Any
    side: str
    qty: int
    price: float
    dt: datetime


def fetch_trade_operations_for_day(
    client: Client,
    config: BotConfig,
    target_day: date,
    watchlist: list[InstrumentConfig],
) -> tuple[list[BrokerTradeOp], dict[str, float]]:
    from_utc, to_utc = get_day_bounds_utc_for_date(target_day)
    cursor = ""
    fee_by_parent: dict[str, float] = defaultdict(float)
    figi_to_symbol = {item.figi: item.symbol for item in watchlist if item.figi}
    symbol_to_name = {item.symbol: item.display_name for item in watchlist}
    trade_ops: list[BrokerTradeOp] = []

    while True:
        response = client.operations.get_operations_by_cursor(
            GetOperationsByCursorRequest(
                account_id=config.account_id,
                from_=from_utc,
                to=to_utc,
                cursor=cursor,
                limit=500,
                state=OperationState.OPERATION_STATE_EXECUTED,
                without_commissions=False,
                without_overnights=False,
                without_trades=False,
            )
        )
        for item in getattr(response, "items", []) or []:
            figi = str(getattr(item, "figi", "") or "")
            symbol = figi_to_symbol.get(figi)
            if not symbol:
                continue
            op_type = getattr(item, "type", None)
            op_id = str(getattr(item, "id", "") or "")
            parent_id = str(getattr(item, "parent_operation_id", "") or "")
            payment = quotation_to_float(getattr(item, "payment", None))

            if op_type in FEE_OPERATION_TYPES and parent_id:
                fee_by_parent[parent_id] += abs(payment)
                continue

            if op_type not in {
                OperationType.OPERATION_TYPE_BUY,
                OperationType.OPERATION_TYPE_SELL,
            }:
                continue

            op_dt = getattr(item, "date", None)
            if not isinstance(op_dt, datetime):
                continue
            if op_dt.tzinfo is None:
                op_dt = op_dt.replace(tzinfo=UTC)

            trade_ops.append(
                BrokerTradeOp(
                    symbol=symbol,
                    display_name=symbol_to_name.get(symbol, symbol),
                    figi=figi,
                    op_id=op_id,
                    parent_id=parent_id,
                    op_type=op_type,
                    side="LONG" if op_type == OperationType.OPERATION_TYPE_BUY else "SHORT",
                    qty=int(getattr(item, "quantity", 0) or 0),
                    price=quotation_to_float(getattr(item, "price", None)),
                    dt=op_dt,
                )
            )

        next_cursor = str(getattr(response, "next_cursor", "") or "")
        if not getattr(response, "has_next", False) or not next_cursor:
            break
        cursor = next_cursor

    trade_ops.sort(key=lambda item: item.dt)
    return trade_ops, dict(fee_by_parent)


def build_trade_journal_queues_for_day(
    rows: list[dict[str, Any]],
    target_day: date,
) -> tuple[list[dict[str, Any]], set[str]]:
    day_key = target_day.isoformat()
    queues: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    existing_close_signatures: set[str] = set()
    close_unit_counts: dict[tuple[str, str, int], int] = defaultdict(int)

    target_rows = sorted(
        [row for row in rows if str(row.get("time", "")).startswith(day_key)],
        key=lambda row: parse_state_datetime(str(row.get("time") or "")) or datetime.min.replace(tzinfo=UTC),
    )

    for row in target_rows:
        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").upper()
        event = str(row.get("event") or "").upper()
        if not symbol or not side:
            continue
        if event == "OPEN":
            qty = max(1, int(row.get("qty_lots") or 0))
            for _ in range(qty):
                split_row = dict(row)
                split_row["qty_lots"] = 1
                for money_key in ("commission_rub", "net_pnl_rub", "pnl_rub", "gross_pnl_rub"):
                    if split_row.get(money_key) is None:
                        continue
                    try:
                        split_row[money_key] = round(float(split_row[money_key]) / qty, 2)
                    except Exception:
                        pass
                queues[(symbol, side)].append(split_row)
            continue
        if event != "CLOSE":
            continue
        row_dt = parse_state_datetime(str(row.get("time") or ""))
        close_qty = max(1, int(row.get("qty_lots") or 0))
        broker_op_id = str(row.get("broker_op_id") or "")
        if row_dt is not None:
            for unit in range(close_qty):
                if broker_op_id:
                    broker_unit = int(row.get("broker_op_unit") or unit)
                    existing_close_signatures.add(f"{symbol}:{side}:op:{broker_op_id}:{broker_unit}")
                    continue
                timestamp = int(row_dt.timestamp())
                next_unit = close_unit_counts[(symbol, side, timestamp)]
                existing_close_signatures.add(f"{symbol}:{side}:ts:{timestamp}:{next_unit}")
                close_unit_counts[(symbol, side, timestamp)] = next_unit + 1
        queue = queues[(symbol, side)]
        remaining = close_qty
        while queue and remaining > 0:
            queue.pop(0)
            remaining -= 1

    unmatched = [row for queue in queues.values() for row in queue]
    unmatched.sort(key=lambda row: parse_state_datetime(str(row.get("time") or "")) or datetime.min.replace(tzinfo=UTC))
    return unmatched, existing_close_signatures


def infer_open_fee_for_recovery(
    open_row: dict[str, Any],
    instrument: InstrumentConfig,
    trade_ops: list[BrokerTradeOp],
    fee_by_parent: dict[str, float],
) -> float:
    try:
        existing_fee = float(open_row.get("commission_rub") or 0.0)
    except Exception:
        existing_fee = 0.0
    if existing_fee > 0:
        return existing_fee

    open_dt = parse_state_datetime(str(open_row.get("time") or ""))
    open_price = float(open_row.get("price") or 0.0)
    qty = int(open_row.get("qty_lots") or 0)
    side = str(open_row.get("side") or "").upper()
    if open_dt is None or qty <= 0 or side not in {"LONG", "SHORT"}:
        return 0.0

    expected_type = OperationType.OPERATION_TYPE_BUY if side == "LONG" else OperationType.OPERATION_TYPE_SELL
    tolerance = max(float(getattr(instrument, "min_price_increment", 0.0) or 0.0) * 3, 1e-4)

    for op in trade_ops:
        if op.symbol != str(open_row.get("symbol") or "").upper():
            continue
        if op.op_type != expected_type:
            continue
        if op.dt < open_dt - timedelta(minutes=2) or op.dt > open_dt + timedelta(minutes=10):
            continue
        if qty > 0 and op.qty not in {0, qty}:
            continue
        if open_price > 0 and op.price > 0 and abs(op.price - open_price) > tolerance:
            continue
        return round(float(fee_by_parent.get(op.op_id, 0.0)), 2)
    return 0.0


def infer_close_reason_for_recovery(symbol: str, side: str, qty: int, close_dt: datetime) -> str:
    state = load_state(symbol)
    delayed_reason = compact_reason(str(state.delayed_close_reason or ""))
    delayed_side = str(state.delayed_close_side or "").upper()
    delayed_qty = int(state.delayed_close_qty or 0)
    delayed_at = parse_state_datetime(state.delayed_close_submitted_at)
    if delayed_reason and delayed_side == side and delayed_qty == qty and delayed_at is not None:
        if abs((close_dt - delayed_at).total_seconds()) <= 12 * 60 * 60:
            return delayed_reason

    last_reason = compact_reason(str(state.last_exit_reason or ""))
    last_side = str(state.last_exit_side or "").upper()
    last_time = parse_state_datetime(state.last_exit_time)
    if last_reason and last_side == side and last_time is not None:
        if abs((close_dt - last_time).total_seconds()) <= 12 * 60 * 60:
            return last_reason

    pending_reason = compact_reason(str(state.pending_exit_reason or ""))
    if pending_reason:
        return pending_reason

    return "Торговая причина выхода не сохранилась, закрытие подтверждено брокерскими операциями."


def reconcile_missing_trade_closes_from_broker(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
    *,
    target_day: date | None = None,
) -> int:
    target_day = target_day or current_moscow_time().date()
    try:
        trade_ops, fee_by_parent = fetch_trade_operations_for_day(client, config, target_day, watchlist)
    except RequestError as error:
        logging.warning("journal_auto_recovery skipped for %s: %s", target_day.isoformat(), error)
        return 0

    rows = load_trade_journal()
    unmatched_opens, existing_close_signatures = build_trade_journal_queues_for_day(rows, target_day)
    instruments_by_symbol = {item.symbol: item for item in watchlist}
    matches: list[dict[str, Any]] = []
    used_op_qty: dict[str, int] = defaultdict(int)

    for open_row in unmatched_opens:
        symbol = str(open_row.get("symbol") or "").upper()
        side = str(open_row.get("side") or "").upper()
        qty = int(open_row.get("qty_lots") or 0)
        open_dt = parse_state_datetime(str(open_row.get("time") or ""))
        instrument = instruments_by_symbol.get(symbol)
        if not symbol or side not in {"LONG", "SHORT"} or qty <= 0 or open_dt is None or instrument is None:
            continue

        expected_type = OperationType.OPERATION_TYPE_SELL if side == "LONG" else OperationType.OPERATION_TYPE_BUY
        candidate: BrokerTradeOp | None = None

        for op in trade_ops:
            if op.symbol != symbol or op.op_type != expected_type:
                continue
            if op.dt <= open_dt:
                continue
            op_total_qty = max(1, int(op.qty or 0))
            op_used_qty = used_op_qty.get(op.op_id, 0)
            op_remaining_qty = max(0, op_total_qty - op_used_qty)
            if op_remaining_qty < qty:
                continue
            signature = f"{symbol}:{side}:op:{op.op_id}:{op_used_qty}"
            legacy_signature = f"{symbol}:{side}:ts:{int(op.dt.timestamp())}:{op_used_qty}"
            if signature in existing_close_signatures or legacy_signature in existing_close_signatures:
                continue
            candidate = op
            break

        if candidate is None:
            continue

        entry_price = float(open_row.get("price") or 0.0)
        open_fee = infer_open_fee_for_recovery(open_row, instrument, trade_ops, fee_by_parent)
        close_fee = round(float(fee_by_parent.get(candidate.op_id, 0.0)), 2)
        op_total_qty = max(1, int(candidate.qty or 0))
        op_used_qty = used_op_qty.get(candidate.op_id, 0)
        close_fee_for_qty = round(close_fee * qty / op_total_qty, 2)
        gross = calculate_futures_pnl_rub(instrument, entry_price, candidate.price, qty, side)
        total_commission = round(open_fee + close_fee_for_qty, 2)
        net = round(gross - total_commission, 2)
        matches.append(
            {
                "time": candidate.dt.astimezone(MOSCOW_TZ).isoformat(),
                "symbol": symbol,
                "display_name": open_row.get("display_name") or candidate.display_name,
                "event": "CLOSE",
                "side": side,
                "qty_lots": qty,
                "lot_size": open_row.get("lot_size") or getattr(instrument, "lot", 1),
                "price": candidate.price,
                "pnl_rub": net,
                "gross_pnl_rub": round(gross, 2),
                "commission_rub": total_commission,
                "net_pnl_rub": net,
                "reason": infer_close_reason_for_recovery(symbol, side, qty, candidate.dt),
                "source": "broker_ops_auto_recovery",
                "broker_op_id": candidate.op_id,
                "broker_op_unit": op_used_qty,
                "strategy": open_row.get("strategy") or "",
                "mode": open_row.get("mode") or ("DRY_RUN" if config.dry_run else "LIVE"),
                "session": open_row.get("session") or get_market_session(),
            }
        )
        used_op_qty[candidate.op_id] = op_used_qty + qty
        existing_close_signatures.add(f"{symbol}:{side}:op:{candidate.op_id}:{op_used_qty}")

    if not matches:
        return 0

    rows.extend(matches)
    rows.sort(key=lambda row: parse_state_datetime(str(row.get("time") or "")) or datetime.min.replace(tzinfo=UTC))
    save_trade_journal(rows)
    logging.warning(
        "journal_auto_recovery recovered_closes=%s symbols=%s",
        len(matches),
        [row["symbol"] for row in matches],
    )
    return len(matches)


def defer_close_recovery_to_broker_ops(
    instrument: InstrumentConfig,
    state: InstrumentState,
    *,
    previous_side: str,
    previous_qty: int,
    previous_entry_price: float | None,
    previous_entry_commission: float,
    previous_strategy: str,
    previous_exit_reason: str,
    previous_entry_time: datetime | None,
    pending_submitted_at: datetime | None,
    grace_seconds: float | None,
) -> bool:
    if previous_side == "FLAT" or previous_qty <= 0:
        return False
    effective_submitted_at = pending_submitted_at or datetime.now(UTC)
    wait_seconds = (datetime.now(UTC) - effective_submitted_at).total_seconds()
    if grace_seconds is not None and pending_submitted_at is not None and wait_seconds < grace_seconds:
        state.execution_status = "submitted_close"
        state.last_error = (
            f"Статус заявки {state.pending_order_id} не найден у брокера, "
            "ждём появления операции закрытия в истории."
        )
        state.last_signal_summary = [state.last_error, *state.last_signal_summary[:2]]
        save_state(instrument.symbol, state)
        logging.info(
            "symbol=%s status=close_waiting_broker_ops seconds=%.0f",
            instrument.symbol,
            wait_seconds,
        )
        return True

    enqueue_delayed_close_snapshot(
        state,
        build_delayed_close_snapshot(
            previous_side=previous_side,
            previous_qty=previous_qty,
            previous_entry_price=previous_entry_price,
            previous_entry_commission=previous_entry_commission,
            previous_strategy=previous_strategy,
            previous_exit_reason=previous_exit_reason,
            previous_entry_time=previous_entry_time,
            submitted_at=effective_submitted_at,
        ),
    )
    state.execution_status = "submitted_close"
    state.last_error = (
        f"Статус заявки {state.pending_order_id} не найден у брокера, "
        "закрытие будет дозапрошено по брокерским операциям."
    )
    state.last_signal_summary = [state.last_error, *state.last_signal_summary[:2]]
    clear_pending_order(state)
    save_state(instrument.symbol, state)
    logging.warning(
        "symbol=%s status=close_deferred_to_broker_ops seconds=%.0f",
        instrument.symbol,
        wait_seconds,
    )
    return False


def pair_trade_journal_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    open_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    closed_reviews: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol", ""))
        side = str(row.get("side", "")).upper()
        event = str(row.get("event", "")).upper()
        if not symbol or not side:
            continue
        key = (symbol, side)
        if event == "OPEN":
            open_by_key.setdefault(key, []).append(row)
            continue
        if event != "CLOSE":
            continue
        open_row = None
        if open_by_key.get(key):
            open_row = open_by_key[key].pop(0)
        closed_reviews.append(
            {
                "symbol": symbol,
                "side": row.get("side", open_row.get("side") if open_row else ""),
                "strategy": row.get("strategy") or (open_row.get("strategy") if open_row else ""),
                "entry_time": open_row.get("time") if open_row else "",
                "exit_time": row.get("time", ""),
                "entry_price": open_row.get("price") if open_row else None,
                "exit_price": row.get("price"),
                "qty_lots": row.get("qty_lots") or (open_row.get("qty_lots") if open_row else 0),
                "pnl_rub": row.get("pnl_rub"),
                "gross_pnl_rub": row.get("gross_pnl_rub"),
                "commission_rub": row.get("commission_rub"),
                "net_pnl_rub": row.get("net_pnl_rub"),
                "entry_reason": open_row.get("reason") if open_row else "",
                "exit_reason": row.get("reason", ""),
            }
        )
    current_open = {symbol: items[-1] for (symbol, _side), items in open_by_key.items() if items}
    return closed_reviews, current_open


def load_config() -> BotConfig:
    token = os.getenv("T_INVEST_TOKEN", "").strip()
    account_id = os.getenv("T_INVEST_ACCOUNT_ID", "").strip()
    symbols = parse_symbols_env()
    missing = [
        name
        for name, value in (("T_INVEST_TOKEN", token), ("T_INVEST_ACCOUNT_ID", account_id))
        if not value
    ]
    if missing:
        raise RuntimeError(f"Не заданы обязательные переменные окружения: {', '.join(missing)}")
    if not symbols:
        raise RuntimeError("Не задан ни один инструмент в T_INVEST_SYMBOLS")

    target_name = os.getenv("T_INVEST_TARGET", "PROD").strip().upper()
    target = INVEST_GRPC_API_SANDBOX if target_name == "SANDBOX" else INVEST_GRPC_API

    tf_minutes = parse_int_env("OIL_CANDLE_INTERVAL_MINUTES", 5)
    higher_tf_minutes = parse_int_env("OIL_HIGHER_TF_MINUTES", 15)
    if tf_minutes not in SUPPORTED_INTERVALS:
        raise RuntimeError(f"Неподдерживаемый таймфрейм OIL_CANDLE_INTERVAL_MINUTES={tf_minutes}")
    if higher_tf_minutes not in SUPPORTED_INTERVALS:
        raise RuntimeError(f"Неподдерживаемый таймфрейм OIL_HIGHER_TF_MINUTES={higher_tf_minutes}")

    return BotConfig(
        token=token,
        account_id=account_id,
        target=target,
        symbols=symbols,
        dry_run=parse_bool_env("OIL_DRY_RUN", True),
        allow_orders=parse_bool_env("OIL_ALLOW_ORDERS", False),
        tg_token=os.getenv("TG_TOKEN", "").strip() or None,
        tg_chat_id=os.getenv("TG_CHAT_ID", "").strip() or None,
        order_quantity=parse_int_env("OIL_ORDER_QUANTITY", 1),
        max_order_quantity=parse_int_env("OIL_MAX_ORDER_QUANTITY", parse_int_env("OIL_ORDER_QUANTITY", 1)),
        risk_per_trade_pct=parse_float_env("OIL_RISK_PER_TRADE_PCT", 0.0),
        max_margin_usage_pct=parse_float_env("OIL_MAX_MARGIN_USAGE_PCT", 0.35),
        portfolio_usage_pct=parse_float_env("OIL_PORTFOLIO_USAGE_PCT", 0.85),
        capital_reserve_pct=parse_float_env("OIL_CAPITAL_RESERVE_PCT", 0.35),
        base_trade_allocation_pct=parse_float_env("OIL_BASE_TRADE_ALLOCATION_PCT", 0.28),
        poll_seconds=parse_int_env("OIL_POLL_SECONDS", 10),
        startup_retry_seconds=parse_int_env("OIL_STARTUP_RETRY_SECONDS", 15),
        candle_hours=parse_int_env("OIL_CANDLE_LOOKBACK_HOURS", 12),
        candle_interval=SUPPORTED_INTERVALS[tf_minutes],
        candle_interval_minutes=tf_minutes,
        higher_tf_interval=SUPPORTED_INTERVALS[higher_tf_minutes],
        higher_tf_interval_minutes=higher_tf_minutes,
        max_daily_loss=parse_float_env("OIL_MAX_DAILY_LOSS", 5.0),
        max_consecutive_errors=parse_int_env("OIL_MAX_CONSECUTIVE_ERRORS", 10),
        max_cycles=parse_int_env("OIL_MAX_CYCLES", 0),
        stop_loss_pct=parse_float_env("OIL_STOP_LOSS_PCT", 0.007),
        trailing_stop_pct=parse_float_env("OIL_TRAILING_STOP_PCT", 0.004),
        breakeven_profit_pct=parse_float_env("OIL_BREAKEVEN_PROFIT_PCT", 0.005),
        min_hold_minutes=parse_int_env("OIL_MIN_HOLD_MINUTES", 15),
        ema_slope_threshold=parse_float_env("OIL_EMA_SLOPE_THRESHOLD", 0.0005),
        near_ema20_pct=parse_float_env("OIL_NEAR_EMA20_PCT", 0.003),
        volume_factor=parse_float_env("OIL_VOLUME_FACTOR", 1.2),
        atr_min_pct=parse_float_env("OIL_ATR_MIN_PCT", 0.0015),
        long_rsi_min=parse_float_env("OIL_LONG_RSI_MIN", 42.0),
        long_rsi_max=parse_float_env("OIL_LONG_RSI_MAX", 48.0),
        short_rsi_min=parse_float_env("OIL_SHORT_RSI_MIN", 52.0),
        short_rsi_max=parse_float_env("OIL_SHORT_RSI_MAX", 58.0),
        rsi_exit_long=parse_float_env("OIL_RSI_EXIT_LONG", 65.0),
        rsi_exit_short=parse_float_env("OIL_RSI_EXIT_SHORT", 35.0),
    )


def quotation_to_float(value: Any) -> float:
    if value is None:
        return 0.0
    return float(getattr(value, "units", 0) or 0) + float(getattr(value, "nano", 0) or 0) / 1e9


def extract_order_commission_rub(order_state: Any) -> float:
    executed_commission = quotation_to_float(getattr(order_state, "executed_commission", None))
    service_commission = quotation_to_float(getattr(order_state, "service_commission", None))
    return executed_commission + service_commission


def send_msg(config: BotConfig, text: str) -> None:
    if not config.tg_token or not config.tg_chat_id:
        return
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{config.tg_token}/sendMessage",
            json={"chat_id": config.tg_chat_id, "text": text},
            timeout=5,
        )
        response.raise_for_status()
    except requests.RequestException as error:
        logging.warning("Не удалось отправить сообщение в Telegram: %s", error)


def format_instrument_title(instrument: InstrumentConfig) -> str:
    return f"{instrument.symbol} ({instrument.display_name})"


def signal_emoji(signal: str) -> str:
    return {
        "LONG": "🟢",
        "SHORT": "🔴",
        "HOLD": "🟡",
    }.get(signal, "ℹ️")


def compact_reason(reason: str) -> str:
    cleaned = reason.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.replace("Сигнал HOLD: ", "")
    cleaned = cleaned.replace("Сигнал LONG: ", "")
    cleaned = cleaned.replace("Сигнал SHORT: ", "")
    cleaned = cleaned.replace("long не подтверждён ", "Long: ")
    cleaned = cleaned.replace("short не подтверждён ", "Short: ")
    return cleaned


def parse_bias_label(value: str) -> tuple[str, str]:
    raw = (value or "").strip().upper()
    if not raw:
        return "NEUTRAL", ""
    if "/" in raw:
        bias, strength = raw.split("/", 1)
        return bias.strip(), strength.strip()
    return raw, ""


def format_news_bias_label(news_bias: NewsBias | None) -> str:
    if news_bias is None:
        return "NEUTRAL"
    return f"{news_bias.bias}/{news_bias.strength}"


def describe_news_bias_impact(signal: str, news_bias: NewsBias | None) -> str:
    if news_bias is None or news_bias.bias == "NEUTRAL":
        return "новости не вмешиваются"
    if news_bias.bias == "BLOCK":
        return "новости блокируют новый вход"
    if signal == "HOLD":
        return f"новости задают контекст {news_bias.bias}, но техника вход не подтвердила"
    if news_bias.bias == signal:
        return f"новости усиливают сигнал {signal}"
    return f"новости конфликтуют с сигналом {signal}"


def format_news_bias_lines(news_bias: NewsBias | None) -> list[str]:
    if news_bias is None:
        return ["• News bias: NEUTRAL"]
    return [
        f"• News bias: {news_bias.bias} ({news_bias.strength})",
        f"• Источник: {news_bias.source}",
        f"• Причина: {news_bias.reason}",
    ]


def floor_time_slot(dt: datetime, minutes: int) -> datetime:
    floored_minute = (dt.minute // minutes) * minutes
    return dt.replace(minute=floored_minute, second=0, microsecond=0)


def get_status_slot(candle_time: str, minutes: int) -> str:
    dt = datetime.strptime(candle_time, "%Y-%m-%d %H:%M")
    return floor_time_slot(dt, minutes).strftime("%Y-%m-%d %H:%M")


def format_reason_multiline(reason: str) -> list[str]:
    text = compact_reason(reason)
    lines: list[str] = []

    if "Главные блокеры long:" in text:
        main_part, long_blockers_part = text.split("Главные блокеры long:", 1)
    else:
        main_part, long_blockers_part = text, ""

    short_blockers_part = ""
    if "Главные блокеры short:" in long_blockers_part:
        long_blockers_part, short_blockers_part = long_blockers_part.split("Главные блокеры short:", 1)

    main_part = main_part.strip().rstrip(".")
    if "Short:" in main_part and "Long:" in main_part:
        long_part, short_part = main_part.split("Short:", 1)
        long_part = long_part.replace("Long:", "").strip().strip("[] ").rstrip(".")
        short_part = short_part.strip().strip("[] ").rstrip(".")
        if long_part:
            lines.append("Long:")
            lines.extend(f"• {item.strip()}" for item in long_part.split(";") if item.strip())
        if short_part:
            lines.append("Short:")
            lines.extend(f"• {item.strip()}" for item in short_part.split(";") if item.strip())
    elif main_part:
        lines.extend(f"• {item.strip()}" for item in main_part.split(";") if item.strip())

    long_blockers_part = long_blockers_part.strip().rstrip(".")
    short_blockers_part = short_blockers_part.strip().rstrip(".")
    if long_blockers_part:
        lines.append("Главные блокеры long:")
        lines.extend(f"• {item.strip()}" for item in long_blockers_part.split(";") if item.strip())
    if short_blockers_part:
        lines.append("Главные блокеры short:")
        lines.extend(f"• {item.strip()}" for item in short_blockers_part.split(";") if item.strip())

    return lines


def extract_blocker_sections(reason: str) -> tuple[list[str], list[str]]:
    text = compact_reason(reason)
    long_blockers: list[str] = []
    short_blockers: list[str] = []

    if "Главные блокеры long:" in text:
        after_long = text.split("Главные блокеры long:", 1)[1]
        if "Главные блокеры short:" in after_long:
            long_part, short_part = after_long.split("Главные блокеры short:", 1)
            long_blockers = [item.strip().strip(".") for item in long_part.split(";") if item.strip()]
            short_blockers = [item.strip().strip(".") for item in short_part.split(";") if item.strip()]
        else:
            long_blockers = [item.strip().strip(".") for item in after_long.split(";") if item.strip()]
    return long_blockers, short_blockers


def summarize_signal_reason(signal: str, reason: str) -> list[str]:
    if signal == "HOLD":
        long_blockers, short_blockers = extract_blocker_sections(reason)
        summary: list[str] = []
        if long_blockers:
            summary.append(f"Long: {', '.join(long_blockers[:2])}")
        if short_blockers:
            summary.append(f"Short: {', '.join(short_blockers[:2])}")
        return summary[:2] or ["Нет подтверждённого входа"]

    compact = compact_reason(reason)
    compact = compact.split("Главные блокеры", 1)[0].strip().rstrip(".")
    parts = [item.strip() for item in compact.split(";") if item.strip()]
    filtered = [item for item in parts if not item.startswith(("Long:", "Short:"))]
    return filtered[:3] or [compact]


def is_currency_instrument(symbol: str) -> bool:
    return is_currency_symbol(symbol)


def build_telegram_card(title: str, emoji: str, lines: list[str]) -> str:
    body = "\n".join(line for line in lines if line)
    return f"{emoji} {title}\n\n{body}"


SIGNAL_STATUS_INTERVAL_MINUTES = 60
HOURLY_REPORT_INTERVAL_MINUTES = 60
SUMMARY_STATUS_INTERVAL_MINUTES = 120
BROKER_CLOSE_CONFIRMATION_GRACE_SECONDS = 120


def get_active_news_biases(force: bool = False) -> dict[str, NewsBias]:
    now = datetime.now(UTC)
    fetched_at = NEWS_CACHE.get("fetched_at")
    if (
        not force
        and isinstance(fetched_at, datetime)
        and (now - fetched_at).total_seconds() < NEWS_CACHE_TTL_SECONDS
    ):
        return NEWS_CACHE.get("biases", {})

    all_biases: list[NewsBias] = []
    target_day = current_moscow_time().date()
    for channel in CHANNEL_URLS:
        try:
            posts = fetch_posts_for_day(channel, target_day=target_day)
            for _, biases in detect_biases_for_posts(posts):
                all_biases.extend(biases)
        except Exception as error:
            logging.warning("Не удалось обновить новости из %s: %s", channel, error)

    active = select_active_biases(all_biases, now=now)
    NEWS_CACHE["fetched_at"] = now
    NEWS_CACHE["biases"] = active
    save_news_snapshot(
        {
            "fetched_at": now.isoformat(),
            "fetched_at_moscow": now.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
            "active_biases": [
                {
                    "symbol": item.symbol,
                    "bias": item.bias,
                    "strength": item.strength,
                    "source": item.source,
                    "reason": item.reason,
                    "message_text": item.message_text,
                    "expires_at": item.expires_at.isoformat(),
                    "expires_at_moscow": item.expires_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
                    "score": item.score,
                }
                for item in sorted(active.values(), key=lambda x: (-x.score, x.symbol))
            ],
        }
    )
    return active


def apply_news_bias_to_signal(signal: str, reason: str, news_bias: NewsBias | None) -> tuple[str, str]:
    if news_bias is None:
        return signal, reason
    if news_bias.bias == "BLOCK" and signal in {"LONG", "SHORT"}:
        return "HOLD", f"{reason}. News bias BLOCK: {news_bias.reason}."
    if signal == "LONG" and news_bias.bias == "SHORT":
        return "HOLD", f"{reason}. News bias конфликтует с LONG: {news_bias.reason}."
    if signal == "SHORT" and news_bias.bias == "LONG":
        return "HOLD", f"{reason}. News bias конфликтует с SHORT: {news_bias.reason}."
    if signal in {"LONG", "SHORT"} and news_bias.bias == signal:
        return signal, f"{reason}. News bias подтверждает сигнал: {news_bias.reason}."
    return signal, reason


def get_strategy_profile(config: BotConfig, instrument: InstrumentConfig) -> StrategyProfile:
    return get_primary_strategy_profile(config, instrument)


def resolve_instruments(client: Client, config: BotConfig) -> list[InstrumentConfig]:
    futures = client.instruments.futures().instruments
    lookup = {item.ticker.upper(): item for item in futures if item.ticker}
    result: list[InstrumentConfig] = []
    missing: list[str] = []
    for symbol in config.symbols:
        item = lookup.get(symbol)
        if item is None:
            missing.append(symbol)
            continue
        result.append(
            InstrumentConfig(
                symbol=symbol,
                figi=item.figi,
                display_name=item.name or symbol,
                lot=int(getattr(item, "lot", 1) or 1),
                min_price_increment=quotation_to_float(getattr(item, "min_price_increment", None)),
                min_price_increment_amount=quotation_to_float(getattr(item, "min_price_increment_amount", None)),
                initial_margin_on_buy=quotation_to_float(getattr(item, "initial_margin_on_buy", None)),
                initial_margin_on_sell=quotation_to_float(getattr(item, "initial_margin_on_sell", None)),
            )
        )
    if missing:
        raise RuntimeError(f"Не удалось разрешить тикеры: {', '.join(missing)}")
    return result


def refresh_watchlist_if_needed(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
    last_refresh_monotonic: float,
) -> tuple[list[InstrumentConfig], float]:
    now_monotonic = time.monotonic()
    if now_monotonic - last_refresh_monotonic < WATCHLIST_REFRESH_SECONDS:
        return watchlist, last_refresh_monotonic
    try:
        config.symbols = parse_symbols_env()
        refreshed = resolve_instruments(client, config)
        before = [item.symbol for item in watchlist]
        after = [item.symbol for item in refreshed]
        if after != before:
            logging.warning("watchlist_changed before=%s after=%s", before, after)
        else:
            logging.info("watchlist_refresh symbols=%s", after)
        return refreshed, now_monotonic
    except Exception as error:
        logging.warning("Не удалось обновить watchlist: %s", error)
        return watchlist, now_monotonic


def reset_daily_pnl_if_needed(state: InstrumentState) -> None:
    today = datetime.now(UTC).date().isoformat()
    if state.trading_day != today:
        state.trading_day = today
        state.realized_pnl = 0.0
        state.realized_gross_pnl_rub = 0.0
        state.realized_commission_rub = 0.0
        state.last_risk_stop_day = ""


def current_moscow_time() -> datetime:
    return datetime.now(MOSCOW_TZ)


def get_moscow_day_bounds_utc(now: datetime | None = None) -> tuple[datetime, datetime]:
    now_msk = (now or current_moscow_time()).astimezone(MOSCOW_TZ)
    start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_msk.astimezone(UTC), now_msk.astimezone(UTC)


def get_day_bounds_utc_for_date(target_day: date) -> tuple[datetime, datetime]:
    start_msk = datetime(
        target_day.year,
        target_day.month,
        target_day.day,
        0,
        0,
        0,
        tzinfo=MOSCOW_TZ,
    )
    end_msk = start_msk + timedelta(days=1)
    return start_msk.astimezone(UTC), end_msk.astimezone(UTC)


def get_market_session(now: datetime | None = None) -> str:
    now = now or current_moscow_time()
    current_minutes = now.hour * 60 + now.minute
    if now.weekday() >= 5:
        if current_minutes < 19 * 60:
            return "WEEKEND"
        return "CLOSED"
    if current_minutes < 8 * 60 + 50:
        return "CLOSED"
    if current_minutes < 10 * 60:
        return "MORNING"
    if current_minutes < 19 * 60:
        return "DAY"
    if current_minutes < 23 * 60 + 50:
        return "EVENING"
    return "CLOSED"


def get_session_position_multiplier(session_name: str, symbol: str | None = None) -> float:
    if session_name == "CLOSED":
        return 0.0
    if session_name == "WEEKEND":
        if symbol and is_currency_symbol(symbol):
            return 0.0
        return 0.35
    return {
        "MORNING": 0.5,
        "DAY": 1.0,
        "EVENING": 0.5,
    }.get(session_name, 1.0)


def session_allows_new_entries(session_name: str, symbol: str) -> bool:
    if session_name == "CLOSED":
        return False
    if session_name == "WEEKEND":
        return not is_currency_symbol(symbol)
    return True


def session_signal_quality_ok(df: pd.DataFrame, signal: str, session_name: str, symbol: str) -> bool:
    if session_name == "CLOSED":
        return False
    if session_name == "WEEKEND" and is_currency_symbol(symbol):
        return False
    if session_name in {"DAY", "MORNING"}:
        return True

    last = df.iloc[-1]
    prev = df.iloc[-2]
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = float(last["body"])
    body_avg = float(last["body_avg"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])

    volume_factor = 1.0
    impulse_factor = 0.75
    min_score = 2
    if session_name == "WEEKEND":
        volume_factor = 1.10
        impulse_factor = 0.95
        min_score = 3

    volume_ok = volume_avg > 0 and volume >= volume_avg * volume_factor
    impulse_ok = body_avg > 0 and body >= body_avg * impulse_factor
    if signal == "LONG":
        macd_ok = macd > macd_signal and macd >= prev_macd and prev_macd >= prev_macd_signal
    else:
        macd_ok = macd < macd_signal and macd <= prev_macd and prev_macd <= prev_macd_signal
    return sum([volume_ok, impulse_ok, macd_ok]) >= min_score


def get_candles(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    interval: CandleInterval,
    lookback_hours: int | None = None,
) -> pd.DataFrame:
    now = datetime.now(UTC)
    candles = client.market_data.get_candles(
        figi=instrument.figi,
        from_=now - timedelta(hours=lookback_hours or config.candle_hours),
        to=now,
        interval=interval,
    )
    rows: list[dict[str, float]] = []
    for candle in candles.candles:
        rows.append(
            {
                "time": getattr(candle, "time", None),
                "is_complete": bool(getattr(candle, "is_complete", True)),
                "open": quotation_to_float(candle.open),
                "high": quotation_to_float(candle.high),
                "low": quotation_to_float(candle.low),
                "close": quotation_to_float(candle.close),
                "volume": float(getattr(candle, "volume", 0) or 0),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"API не вернул свечи для {instrument.symbol}")
    return df


def get_lower_tf_lookback_hours(config: BotConfig, symbol: str | None = None) -> int:
    base_hours = max(config.candle_hours, int((config.candle_interval_minutes * 240) / 60) + 1)
    # For EMA200 on 5m candles, exchange session gaps mean a simple 30h wall-clock
    # lookback can still contain too few actual candles on mornings and after weekends.
    lookback_hours = max(base_hours, 72)
    if symbol and get_instrument_group(symbol).name == "fx":
        # Currency futures can start the week with a shorter effective trading history
        # inside the same wall-clock window, so we keep a longer bootstrap window.
        lookback_hours = max(lookback_hours, 120)
    if get_market_session() == "WEEKEND":
        return max(lookback_hours, 72)
    return lookback_hours


def get_higher_tf_lookback_hours(config: BotConfig, symbol: str | None = None) -> int:
    base_hours = max(120, int((config.higher_tf_interval_minutes * 240) / 60) + 1)
    if symbol and get_instrument_group(symbol).name == "fx":
        return max(base_hours, 168)
    return base_hours


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "time" in result.columns:
        result["time"] = pd.to_datetime(result["time"], utc=True, errors="coerce")
        result = result.sort_values("time").reset_index(drop=True)
    if "is_complete" in result.columns:
        result = result[result["is_complete"]].reset_index(drop=True)
    # Early in the session the API can return only a short tail of completed candles.
    # Guard before TA-lib style helpers to avoid window-size index errors.
    if len(result) < 14:
        raise RuntimeError("Недостаточно данных для стратегии")
    result["ema20"] = ta.trend.EMAIndicator(result["close"], window=20).ema_indicator()
    result["ema50"] = ta.trend.EMAIndicator(result["close"], window=50).ema_indicator()
    result["ema200"] = ta.trend.EMAIndicator(result["close"], window=200).ema_indicator()
    result["ema50_slope"] = result["ema50"].pct_change()
    result["rsi"] = ta.momentum.RSIIndicator(result["close"], window=14).rsi()
    macd = ta.trend.MACD(result["close"], window_slow=26, window_fast=12, window_sign=9)
    result["macd"] = macd.macd()
    result["macd_signal"] = macd.macd_signal()
    bb = ta.volatility.BollingerBands(result["close"], window=20, window_dev=2)
    result["bb_upper"] = bb.bollinger_hband()
    result["bb_lower"] = bb.bollinger_lband()
    result["bb_mid"] = bb.bollinger_mavg()
    result["atr"] = ta.volatility.AverageTrueRange(
        result["high"], result["low"], result["close"], window=14
    ).average_true_range()
    result["volume_avg"] = result["volume"].rolling(20).mean()
    body = (result["close"] - result["open"]).abs()
    result["body"] = body
    result["body_avg"] = body.rolling(10).mean()
    result = result.dropna().reset_index(drop=True)
    if len(result) < 3:
        raise RuntimeError("Недостаточно данных для стратегии")
    return result


def add_williams_indicators(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "time" in result.columns:
        result["time"] = pd.to_datetime(result["time"], utc=True, errors="coerce")
        result = result.sort_values("time").reset_index(drop=True)
    if "is_complete" in result.columns:
        result = result[result["is_complete"]].reset_index(drop=True)

    median_price = (result["high"] + result["low"]) / 2
    result["median_price"] = median_price

    # Smoothed averages approximating Williams Alligator lines.
    result["alligator_lips"] = median_price.ewm(alpha=1 / 5, adjust=False).mean().shift(3)
    result["alligator_teeth"] = median_price.ewm(alpha=1 / 8, adjust=False).mean().shift(5)
    result["alligator_jaws"] = median_price.ewm(alpha=1 / 13, adjust=False).mean().shift(8)

    ao_fast = median_price.rolling(5).mean()
    ao_slow = median_price.rolling(20).mean()
    result["ao"] = ao_fast - ao_slow

    high_low_range = (result["high"] - result["low"]).astype(float).replace(0.0, float("nan"))
    money_flow_multiplier = ((result["close"] - result["low"]) - (result["high"] - result["close"])) / high_low_range
    money_flow_multiplier = money_flow_multiplier.fillna(0.0).astype(float)
    money_flow_volume = money_flow_multiplier * result["volume"]
    adl = money_flow_volume.cumsum()
    result["chaikin"] = adl.ewm(span=5, adjust=False).mean() - adl.ewm(span=20, adjust=False).mean()

    line_max = result[["alligator_lips", "alligator_teeth", "alligator_jaws"]].max(axis=1)
    line_min = result[["alligator_lips", "alligator_teeth", "alligator_jaws"]].min(axis=1)
    result["alligator_spread_pct"] = (line_max - line_min) / result["close"]

    result = result.dropna().reset_index(drop=True)
    if len(result) < 3:
        raise RuntimeError("Недостаточно данных для стратегии Williams")
    return result


def get_higher_tf_bias(client: Client, config: BotConfig, instrument: InstrumentConfig) -> str:
    tf_specs = [
        (15, CandleInterval.CANDLE_INTERVAL_15_MIN, 1),
        (30, CandleInterval.CANDLE_INTERVAL_30_MIN, 2),
        (60, CandleInterval.CANDLE_INTERVAL_HOUR, 3),
    ]
    long_score = 0
    short_score = 0

    for interval_minutes, interval, weight in tf_specs:
        try:
            lookback_hours = max(120, int((interval_minutes * 120) / 60) + 1)
            df = get_candles(client, config, instrument, interval, lookback_hours=lookback_hours)
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
                df = df.sort_values("time").reset_index(drop=True)
            if "is_complete" in df.columns:
                df = df[df["is_complete"]].reset_index(drop=True)
            if len(df) < 50:
                continue
            df["ema50"] = ta.trend.EMAIndicator(df["close"], window=50).ema_indicator()
            df = df.dropna().reset_index(drop=True)
            if df.empty:
                continue
            last = df.iloc[-1]
            close = float(last["close"])
            ema50 = float(last["ema50"])
            if close > ema50:
                long_score += weight
            elif close < ema50:
                short_score += weight
        except RuntimeError:
            continue

    if long_score > short_score:
        return "LONG"
    if short_score > long_score:
        return "SHORT"
    return "FLAT"


def get_configured_higher_tf_df(client: Client, config: BotConfig, instrument: InstrumentConfig) -> pd.DataFrame:
    return add_indicators(
        get_candles(
            client,
            config,
            instrument,
            config.higher_tf_interval,
            lookback_hours=get_higher_tf_lookback_hours(config, instrument.symbol),
        )
    )


def evaluate_signal(
    df: pd.DataFrame,
    config: BotConfig,
    instrument: InstrumentConfig,
    higher_tf_bias: str,
) -> tuple[str, str, str]:
    return evaluate_primary_signal_bundle(df, config, instrument, higher_tf_bias)


def evaluate_williams_currency_signal(
    df: pd.DataFrame,
    higher_tf_bias: str,
) -> tuple[str, str]:
    return evaluate_williams_signal(df, higher_tf_bias)


def build_market_view_lines(
    df: pd.DataFrame,
    config: BotConfig,
    instrument: InstrumentConfig,
    higher_tf_bias: str,
) -> list[str]:
    profile = get_strategy_profile(config, instrument)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    close = float(last["close"])
    ema50 = float(last["ema50"])
    rsi = float(last["rsi"])
    atr_pct = float(last["atr"]) / close if close else 0.0
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = float(last["body"])
    body_avg = float(last["body_avg"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_prev = df.iloc[-3]
    prev_prev_macd = float(prev_prev["macd"])

    volume_ok = volume_avg > 0 and volume >= volume_avg * profile.volume_factor
    impulse_ok = body_avg > 0 and body >= body_avg * profile.impulse_body_factor
    macd_turn_up = macd > macd_signal and macd > prev_macd and prev_macd >= prev_prev_macd
    macd_turn_down = macd < macd_signal and macd < prev_macd and prev_macd <= prev_prev_macd

    if macd_turn_up:
        macd_text = "вверх, есть подтверждение long"
    elif macd_turn_down:
        macd_text = "вниз, есть подтверждение short"
    elif macd > macd_signal:
        macd_text = "вверх, но без подтверждения long"
    elif macd < macd_signal:
        macd_text = "вниз, но без подтверждения short"
    else:
        macd_text = "нейтрально"

    return [
        "📌 Что видит бот",
        f"• Старший тренд: {higher_tf_bias}",
        f"• Цена: {'выше EMA50' if close > ema50 else 'ниже EMA50'}",
        f"• RSI: {rsi:.2f}",
        f"• MACD: {macd_text}",
        f"• ATR: {'норма' if atr_pct >= profile.atr_min_pct else 'ниже нормы'}",
        f"• Объём: {'норма' if volume_ok else 'слабый'}",
        f"• Импульс: {'есть' if impulse_ok else 'слабый'}",
    ]


def classify_market_regime(df: pd.DataFrame, higher_tf_bias: str) -> tuple[str, dict[str, float]]:
    last = df.iloc[-1]
    recent = df.iloc[-5:] if len(df) >= 5 else df
    close = float(last["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    atr_pct = float(last["atr"]) / close if close else 0.0
    volume_avg = float(last["volume_avg"])
    body_avg = float(last["body_avg"])
    volume_ratio = (float(last["volume"]) / volume_avg) if volume_avg > 0 else 0.0
    body_ratio = (float(last["body"]) / body_avg) if body_avg > 0 else 0.0
    range_width_pct = (
        (float(recent["high"].max()) - float(recent["low"].min())) / close if close else 0.0
    )
    long_trend = close > ema20 and ema20 >= ema50
    short_trend = close < ema20 and ema20 <= ema50
    near_ema20 = abs(close - ema20) / close <= 0.0025 if close else False

    if volume_ratio < 0.9 and body_ratio < 0.7 and range_width_pct <= max(atr_pct * 2.0, 0.0035):
        regime = "compression"
    elif atr_pct <= 0.00055 and range_width_pct <= 0.0045:
        regime = "chop"
    elif higher_tf_bias == "LONG" and long_trend and near_ema20:
        regime = "trend_pullback"
    elif higher_tf_bias == "SHORT" and short_trend and near_ema20:
        regime = "trend_pullback"
    elif (higher_tf_bias == "LONG" and long_trend) or (higher_tf_bias == "SHORT" and short_trend):
        regime = "trend_expansion"
    elif volume_ratio >= 1.15 and body_ratio >= 1.0 and atr_pct >= 0.0008:
        regime = "impulse"
    else:
        regime = "mixed"

    return regime, {
        "atr_pct": atr_pct,
        "volume_ratio": volume_ratio,
        "body_ratio": body_ratio,
        "range_width_pct": range_width_pct,
    }


def estimate_setup_quality(
    signal: str,
    higher_tf_bias: str,
    market_regime: str,
    metrics: dict[str, float],
    news_bias: NewsBias | None = None,
) -> tuple[int, str]:
    if signal not in {"LONG", "SHORT"}:
        return 0, "none"
    score = 1
    if higher_tf_bias == signal:
        score += 1
    if float(metrics.get("volume_ratio") or 0.0) >= 1.0:
        score += 1
    if float(metrics.get("body_ratio") or 0.0) >= 0.8:
        score += 1
    if market_regime in {"trend_expansion", "trend_pullback", "impulse"}:
        score += 1
    if news_bias is not None and news_bias.bias == signal:
        score += 1
    if score >= 5:
        return score, "strong"
    if score >= 3:
        return score, "medium"
    return score, "weak"


def regime_entry_block_reason(
    strategy_name: str,
    signal: str,
    market_regime: str,
    metrics: dict[str, float],
) -> str:
    strategy = (strategy_name or "").strip()
    if signal not in {"LONG", "SHORT"}:
        return ""

    atr_pct = float(metrics.get("atr_pct") or 0.0)
    volume_ratio = float(metrics.get("volume_ratio") or 0.0)
    body_ratio = float(metrics.get("body_ratio") or 0.0)

    if market_regime == "compression":
        if strategy in {"opening_range_breakout", "momentum_breakout", "range_break_continuation", "breakdown_continuation"}:
            return f"режим {market_regime} не подходит для стратегии {strategy}: рынок слишком сжат"

    if market_regime == "chop":
        if strategy in {"opening_range_breakout", "momentum_breakout", "range_break_continuation", "breakdown_continuation"}:
            return f"режим {market_regime} не подходит для стратегии {strategy}: слишком высокий риск ложного пробоя"

    if strategy == "trend_pullback" and market_regime not in {"trend_pullback", "trend_expansion"}:
        return f"режим {market_regime} не подходит для стратегии {strategy}: нет направленного отката в тренде"

    if strategy in {"opening_range_breakout", "momentum_breakout"} and market_regime not in {"trend_expansion", "impulse"}:
        return f"режим {market_regime} не подходит для стратегии {strategy}: нужен импульсный или расширяющийся рынок"

    if strategy in {"range_break_continuation", "breakdown_continuation"}:
        if market_regime not in {"trend_expansion", "impulse"}:
            return f"режим {market_regime} не подходит для стратегии {strategy}: нужен зрелый направленный пробой"
        if atr_pct < 0.00045 and volume_ratio < 0.95:
            return f"режим {market_regime} не подходит для стратегии {strategy}: не хватает волатильности и объёма"

    if strategy == "failed_breakout" and market_regime == "impulse" and body_ratio >= 1.15:
        return f"режим {market_regime} не подходит для стратегии {strategy}: движение слишком импульсное против контртрендового входа"

    return ""


def build_periodic_status_message(
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    signal: str,
    price: float,
    reason: str,
    candle_time: str,
    higher_tf_bias: str,
    df: pd.DataFrame,
    news_bias: NewsBias | None = None,
    compare_lines: list[str] | None = None,
) -> str:
    position_text = "нет" if state.position_side == "FLAT" else f"{state.position_side}, лотов={state.position_qty}"
    session_name = get_market_session()
    summary = summarize_signal_reason(signal, reason)
    lines = [
        f"Инструмент: {format_instrument_title(instrument)}",
        f"Сигнал: {signal_emoji(signal)} {signal}",
        f"⏱ Свеча: {candle_time}",
        f"🕒 Сессия: {session_name}",
        f"💵 Цена: {price:.2f}",
        f"🧾 Позиция: {position_text}",
        f"📌 Стратегия: {state.entry_strategy or state.last_strategy_name or '-'}",
        f"📈 Старший тренд: {higher_tf_bias}",
        f"📰 Новости: {format_news_bias_label(news_bias)}",
        f"• Влияние: {describe_news_bias_impact(signal, news_bias)}",
        "",
        "Коротко почему:",
        *[f"• {line}" for line in summary[:3]],
    ]
    return build_telegram_card("Торговый сигнал", signal_emoji(signal), lines)


def notify_signal_change(
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    signal: str,
    price: float,
    reason: str,
    news_bias: NewsBias | None = None,
) -> None:
    if signal == state.last_signal:
        return
    mode = "DRY_RUN" if config.dry_run else "LIVE"
    impact = describe_news_bias_impact(signal, news_bias)
    send_msg(
        config,
        build_telegram_card(
            "Изменение сигнала",
            signal_emoji(signal),
            [
                f"Инструмент: {format_instrument_title(instrument)}",
                f"Режим: {mode}",
                f"Цена: {price:.4f}",
                f"Сигнал: {signal_emoji(signal)} {signal}",
                f"Новости: {format_news_bias_label(news_bias)}",
                f"Влияние: {impact}",
                "",
                "Коротко почему:",
                *[f"• {line}" for line in summarize_signal_reason(signal, reason)],
            ],
        ),
    )


def notify_periodic_status(
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    signal: str,
    price: float,
    reason: str,
    candle_time: str,
    higher_tf_bias: str,
    df: pd.DataFrame,
    news_bias: NewsBias | None = None,
    compare_lines: list[str] | None = None,
) -> None:
    # Периодические сигнальные карточки в Telegram отключены: оставляем только
    # изменения сигнала и общий часовой отчёт.
    status_slot = get_status_slot(candle_time, SIGNAL_STATUS_INTERVAL_MINUTES)
    state.last_status_candle = status_slot


def build_global_diagnostic_message(
    config: BotConfig,
    client: Client,
    watchlist: list[InstrumentConfig],
) -> str:
    news = get_active_news_biases()
    session_name = get_market_session()
    lines = [
        f"🕒 Сессия: {session_name}",
        f"🧭 Режим: {'DRY_RUN' if config.dry_run else 'LIVE'}",
        "",
    ]

    for instrument in watchlist:
        news_bias = news.get(instrument.symbol)
        try:
            lower_df = add_indicators(
                get_candles(
                    client,
                    config,
                    instrument,
                    config.candle_interval,
                    lookback_hours=get_lower_tf_lookback_hours(config, instrument.symbol),
                )
            )
            higher_tf_bias = get_higher_tf_bias(client, config, instrument)
            signal, reason, strategy_name = evaluate_signal(lower_df, config, instrument, higher_tf_bias)
            signal, reason = apply_news_bias_to_signal(signal, reason, news_bias)
            summary = summarize_signal_reason(signal, reason)
            lines.extend(
                [
                    f"{signal_emoji(signal)} {instrument.symbol}: {signal}",
                    f"• Стратегия: {strategy_name} | Старший ТФ: {higher_tf_bias}",
                    f"• Влияние: {describe_news_bias_impact(signal, news_bias)}",
                    f"• Ключевое: {summary[0] if summary else 'нет явного вывода'}",
                    "",
                ]
            )
        except RuntimeError as error:
            lines.extend(
                [
                    f"{signal_emoji('HOLD')} {instrument.symbol}: HOLD",
                    "• Стратегия: ожидание данных",
                    f"• Новости: {format_news_bias_label(news_bias)}",
                    f"• Влияние: {describe_news_bias_impact('HOLD', news_bias)}",
                    f"• Статус: {str(error)}",
                    "",
                ]
            )

    return build_telegram_card("Общая диагностика", "📊", lines[:-1] if lines and lines[-1] == "" else lines)


def maybe_send_global_diagnostic(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> None:
    return


def build_portfolio_snapshot_message(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> str:
    payload = build_portfolio_snapshot_payload(client, config, watchlist)
    lines = [
        f"🧭 Режим: {'DRY_RUN' if config.dry_run else 'LIVE'}",
        f"🕓 Срез: {payload.get('generated_at_moscow', '-')}",
        f"💼 Портфель: {float(payload['total_portfolio_rub']):.2f} RUB",
        f"💵 Свободно: {float(payload['free_rub']):.2f} RUB",
        f"🛡 ГО: {float(payload['blocked_guarantee_rub']):.2f} RUB",
        f"📒 NET закрытых сделок: {float(payload['bot_realized_pnl_rub']):.2f} RUB",
        f"💸 Комиссия по счёту: {float(payload['bot_actual_fee_rub']):.2f} RUB",
        f"🏦 Клиринговая ВМ: {float(payload['bot_actual_varmargin_rub']):.2f} RUB",
        f"📈 Текущая вар. маржа позиций: {float(payload['bot_estimated_variation_margin_rub']):.2f} RUB",
        f"🧾 Общая вар. маржа: {float(payload['bot_total_varmargin_rub']):.2f} RUB",
        f"🧮 Итог по боту: {float(payload['bot_total_pnl_rub']):.2f} RUB",
        f"📌 Открытых позиций: {int(payload['open_positions_count'])}",
    ]
    return build_telegram_card("Портфель бота", "💼", lines)


def build_portfolio_snapshot_payload(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> dict[str, Any]:
    snapshot = get_account_snapshot(client, config)
    accounting = get_today_accounting_snapshot(client, config, watchlist)
    live_positions = get_live_portfolio_positions(client, config, watchlist)
    closed_totals = calculate_closed_trade_totals()
    open_positions = len(live_positions)
    unrealized_pnl = sum(float(item.get("variation_margin_rub") or 0.0) for item in live_positions.values())
    broker_open_positions_pnl = sum(float(item.get("expected_yield_rub") or 0.0) for item in live_positions.values())
    realized_pnl = float(closed_totals["net_pnl_rub"])
    realized_gross_pnl = float(closed_totals["gross_pnl_rub"])
    realized_commission = float(closed_totals["commission_rub"])
    total_varmargin_rub = realized_gross_pnl + broker_open_positions_pnl
    total_bot_pnl = total_varmargin_rub - float(accounting["actual_fee_expense_rub"])

    generated_at = datetime.now(timezone.utc)
    return {
        "mode": "DRY_RUN" if config.dry_run else "LIVE",
        "report_date": current_moscow_time().date().isoformat(),
        "selected_date": current_moscow_time().date().isoformat(),
        "selected_date_moscow": current_moscow_time().strftime("%d.%m.%Y"),
        "total_portfolio_rub": round(snapshot.total_portfolio, 2),
        "free_rub": round(snapshot.free_rub, 2),
        "free_cash_rub": round(snapshot.free_rub, 2),
        "blocked_guarantee_rub": round(snapshot.blocked_guarantee_rub, 2),
        "open_positions_count": open_positions,
        "bot_realized_gross_pnl_rub": round(realized_gross_pnl, 2),
        "bot_realized_commission_rub": round(realized_commission, 2),
        "bot_realized_pnl_rub": round(realized_pnl, 2),
        "bot_actual_varmargin_rub": float(accounting["actual_varmargin_rub"]),
        "bot_actual_fee_rub": float(accounting["actual_fee_expense_rub"]),
        "bot_actual_cash_effect_rub": float(accounting["actual_account_cash_effect_rub"]),
        "bot_actual_varmargin_by_symbol": dict(accounting.get("varmargin_by_symbol") or {}),
        "bot_estimated_variation_margin_rub": round(unrealized_pnl, 2),
        "bot_total_varmargin_rub": round(total_varmargin_rub, 2),
        "bot_total_variation_margin_rub": round(total_varmargin_rub, 2),
        "bot_broker_day_pnl_rub": round(broker_open_positions_pnl, 2),
        "bot_total_pnl_rub": round(total_bot_pnl, 2),
        "broker_open_positions": list(live_positions.values()),
        "generated_at": generated_at.isoformat(),
        "generated_at_moscow": generated_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
    }


def maybe_refresh_portfolio_snapshot(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
    refresh_seconds: int = 60,
) -> None:
    meta = load_meta_state()
    now = datetime.now(timezone.utc)
    raw_ts = str(meta.get("portfolio_snapshot_refreshed_at") or "").strip()
    if raw_ts:
        try:
            last_ts = datetime.fromisoformat(raw_ts)
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            if (now - last_ts).total_seconds() < refresh_seconds:
                return
        except Exception:
            pass
    payload = build_portfolio_snapshot_payload(client, config, watchlist)
    save_portfolio_snapshot(payload)
    history = load_accounting_history()
    today_key = current_moscow_time().date().isoformat()
    history[today_key] = {
        "date": today_key,
        "generated_at": payload.get("generated_at"),
        "generated_at_moscow": payload.get("generated_at_moscow"),
        "actual_varmargin_rub": payload.get("bot_actual_varmargin_rub", 0.0),
        "actual_fee_expense_rub": payload.get("bot_actual_fee_rub", 0.0),
        "actual_account_cash_effect_rub": payload.get("bot_actual_cash_effect_rub", 0.0),
        "total_varmargin_rub": payload.get("bot_total_varmargin_rub", 0.0),
        "broker_open_positions_pnl_rub": payload.get("bot_broker_day_pnl_rub", 0.0),
        "total_pnl_rub": payload.get("bot_total_pnl_rub", 0.0),
        "varmargin_by_symbol": payload.get("bot_actual_varmargin_by_symbol", {}),
    }
    save_accounting_history(history)
    meta["portfolio_snapshot_refreshed_at"] = now.isoformat()
    save_meta_state(meta)


def update_accounting_history_for_day(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
    target_day: date,
) -> dict[str, Any]:
    accounting = get_accounting_snapshot_for_day(client, config, target_day, watchlist)
    generated_at = datetime.now(timezone.utc)
    entry = {
        "date": target_day.isoformat(),
        "generated_at": generated_at.isoformat(),
        "generated_at_moscow": generated_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
        "actual_varmargin_rub": float(accounting["actual_varmargin_rub"]),
        "actual_fee_expense_rub": float(accounting["actual_fee_expense_rub"]),
        "actual_account_cash_effect_rub": float(accounting["actual_account_cash_effect_rub"]),
        "total_varmargin_rub": 0.0,
        "broker_open_positions_pnl_rub": 0.0,
        "total_pnl_rub": 0.0,
        "varmargin_by_symbol": dict(accounting.get("varmargin_by_symbol") or {}),
    }
    history = load_accounting_history()
    history[target_day.isoformat()] = entry
    save_accounting_history(history)
    return entry


def maybe_refresh_news_snapshot(refresh_seconds: int = 300) -> None:
    meta = load_meta_state()
    now = datetime.now(timezone.utc)
    raw_ts = str(meta.get("news_snapshot_refreshed_at") or "").strip()
    if raw_ts:
        try:
            last_ts = datetime.fromisoformat(raw_ts)
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            if (now - last_ts).total_seconds() < refresh_seconds:
                return
        except Exception:
            pass
    get_active_news_biases(force=True)
    meta["news_snapshot_refreshed_at"] = now.isoformat()
    save_meta_state(meta)


def build_runtime_status_payload(
    *,
    mode: str,
    session_name: str,
    started_at: datetime,
    cycle_count: int,
    consecutive_errors: int,
    state: str,
    last_cycle_at: datetime | None = None,
    last_error: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "mode": mode,
        "state": state,
        "session": session_name,
        "started_at": started_at.isoformat(),
        "started_at_moscow": started_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
        "updated_at": now.isoformat(),
        "updated_at_moscow": now.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК"),
        "last_cycle_at": last_cycle_at.isoformat() if last_cycle_at else "",
        "last_cycle_at_moscow": last_cycle_at.astimezone(MOSCOW_TZ).strftime("%d.%m %H:%M:%S МСК") if last_cycle_at else "",
        "cycle_count": cycle_count,
        "consecutive_errors": consecutive_errors,
        "last_error": last_error,
    }


def build_trade_results_message(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> str:
    payload = build_portfolio_snapshot_payload(client, config, watchlist)
    open_positions = payload.get("broker_open_positions") or []
    lines = [
        f"🕓 Срез: {payload.get('generated_at_moscow', '-')}",
        f"📒 NET сделок: {float(payload['bot_realized_pnl_rub']):.2f} RUB",
        f"💸 Комиссия по счёту: {float(payload['bot_actual_fee_rub']):.2f} RUB",
        f"🏦 Клиринговая ВМ: {float(payload['bot_actual_varmargin_rub']):.2f} RUB",
        f"📈 Текущая вар. маржа: {float(payload['bot_estimated_variation_margin_rub']):.2f} RUB",
        f"🧮 Итог по боту: {float(payload['bot_total_pnl_rub']):.2f} RUB",
        "",
        "Открытые позиции:",
    ]
    if open_positions:
        for item in open_positions:
            lines.append(
                f"• {item['symbol']}: {item['side']} {int(item['qty'])} | "
                f"вар. маржа {float(item.get('variation_margin_rub') or 0.0):.2f} RUB"
            )
    else:
        lines.append("• Сейчас открытых позиций нет")
    return build_telegram_card("Результат торговли", "📒", lines)


def build_trade_review_message() -> str:
    rows = get_today_trade_journal_rows()
    closed_reviews, current_open = pair_trade_journal_rows(rows)

    closed_total = len(closed_reviews)
    wins = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) > 0)
    losses = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) < 0)
    pnl_total = sum(float(item.get("pnl_rub") or 0.0) for item in closed_reviews)

    lines = [
        f"📘 Закрыто сделок: {closed_total}",
        f"✅ Плюсовых: {wins}",
        f"⚠️ Минусовых: {losses}",
        f"💰 Итог по закрытым: {pnl_total:.2f} RUB",
        "",
    ]

    if closed_reviews:
        lines.append("Последние закрытые:")
        for item in closed_reviews[:5]:
            net = float(item.get("net_pnl_rub") or item.get("pnl_rub") or 0.0)
            gross = float(item.get("gross_pnl_rub") or 0.0)
            commission = float(item.get("commission_rub") or 0.0)
            lines.append(
                f"• {item['symbol']} {item['side']} | {item['strategy'] or 'unknown'} | "
                f"брутто {gross:.2f} | ком. {commission:.2f} | net {net:.2f} RUB"
            )
            exit_reason = str(item.get("exit_reason") or "").strip()
            if exit_reason:
                lines.append(f"  Выход: {compact_reason(exit_reason)[:140]}")
        lines.append("")
    else:
        lines.extend(["Последние закрытые:", "• Пока нет закрытых сделок в журнале", ""])

    if current_open:
        lines.append("Текущие открытые по журналу:")
        for symbol, row in current_open.items():
            lines.append(
                f"• {symbol} {row.get('side', '')} | {row.get('strategy') or 'unknown'} | "
                f"{row.get('qty_lots', 0)} лот. | вход {row.get('price')}"
            )
    else:
        lines.extend(["Текущие открытые по журналу:", "• Нет открытых записей в журнале"])

    return build_telegram_card("Разбор сделок", "🧾", lines)


def maybe_send_portfolio_snapshot(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> None:
    return


def maybe_send_trade_results(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> None:
    return


def maybe_send_trade_review(config: BotConfig) -> None:
    return


def build_hourly_summary_message(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> str:
    portfolio = build_portfolio_snapshot_payload(client, config, watchlist)
    review_rows = get_today_trade_journal_rows()
    closed_reviews, current_open = pair_trade_journal_rows(review_rows)
    news = get_active_news_biases()

    wins = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) > 0)
    losses = sum(1 for item in closed_reviews if float(item.get("pnl_rub") or 0.0) < 0)
    session_name = get_market_session()
    recent_closed = sorted(
        closed_reviews,
        key=lambda item: str(item.get("exit_time") or item.get("close_time") or ""),
        reverse=True,
    )[:3]
    lines = [
        f"🕓 Срез: {portfolio.get('generated_at_moscow', '-')}",
        f"🕒 Сессия: {session_name} | Закрыто: {len(closed_reviews)} | ✅ {wins} / ⚠️ {losses}",
        f"💰 Закрытые: {float(portfolio['bot_realized_pnl_rub']):.2f} | "
        f"📈 Текущая ВМ: {float(portfolio['bot_estimated_variation_margin_rub']):.2f} | "
        f"🧮 Итог: {float(portfolio['bot_total_pnl_rub']):.2f} RUB",
        f"💼 Портфель: {float(portfolio['total_portfolio_rub']):.2f} | "
        f"💵 Свободно: {float(portfolio['free_rub']):.2f} | "
        f"🛡 ГО: {float(portfolio['blocked_guarantee_rub']):.2f} RUB",
    ]

    if recent_closed:
        lines.extend(["", "Последние закрытые"])
        for item in recent_closed:
            net = float(item.get("net_pnl_rub") or item.get("pnl_rub") or 0.0)
            exit_reason = compact_reason(str(item.get("exit_reason") or ""))[:90]
            lines.append(
                f"• {item['symbol']} {item['side']} | {item.get('strategy') or '-'} | "
                f"net {net:.2f} RUB | {exit_reason or 'без причины'}"
            )

    if current_open:
        lines.extend(["", "Открытые позиции"])
        for symbol, row in current_open.items():
            lines.append(
                f"• {symbol} {row.get('side', '')} | {row.get('strategy') or '-'} | "
                f"{row.get('qty_lots', 0)} лот. | вход {row.get('price')}"
            )
    else:
        lines.extend(["", "Открытые позиции", "• нет"])

    lines.extend(["", "Диагностика"])
    for instrument in watchlist:
        state = load_state(instrument.symbol)
        signal = state.last_signal or "HOLD"
        strategy_name = state.last_strategy_name or state.entry_strategy or "-"
        news_label = format_news_bias_label(news.get(instrument.symbol))
        lines.append(
            f"• {instrument.symbol}: {signal} | {strategy_name} | "
            f"ТФ {state.last_higher_tf_bias or '-'} | Новости {news_label}"
        )

    return build_telegram_card("Часовой отчёт", "🧾", lines)


def maybe_send_hourly_summary(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> None:
    session_name = get_market_session()
    if session_name == "CLOSED":
        return
    now_slot = floor_time_slot(datetime.now(MOSCOW_TZ), HOURLY_REPORT_INTERVAL_MINUTES).strftime("%Y-%m-%d %H:%M")
    meta = load_meta_state()
    if meta.get("last_hourly_summary_slot") == now_slot:
        return
    send_msg(config, build_hourly_summary_message(client, config, watchlist))
    meta["last_hourly_summary_slot"] = now_slot
    save_meta_state(meta)


def get_last_price(client: Client, instrument: InstrumentConfig) -> float:
    response = client.market_data.get_last_prices(figi=[instrument.figi])
    if not response.last_prices:
        raise RuntimeError(f"Не удалось получить последнюю цену для {instrument.symbol}")
    return quotation_to_float(response.last_prices[0].price)


def extract_position_data(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
) -> tuple[int, float | None, float | None, float | None, float | None]:
    portfolio = client.operations.get_portfolio(account_id=config.account_id)
    for position in portfolio.positions:
        if position.figi != instrument.figi:
            continue
        qty = int(round(quotation_to_float(getattr(position, "quantity", None))))
        avg = quotation_to_float(getattr(position, "average_position_price", None))
        current_price = quotation_to_float(getattr(position, "current_price", None))
        var_margin = quotation_to_float(getattr(position, "var_margin", None))
        expected_yield = quotation_to_float(getattr(position, "expected_yield", None))
        return (
            qty,
            (avg if avg > 0 else None),
            (current_price if current_price > 0 else None),
            var_margin,
            expected_yield,
        )
    return 0, None, None, None, None


def get_live_portfolio_positions(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> dict[str, dict[str, float | int | str | None]]:
    figi_to_instrument = {item.figi: item for item in watchlist}
    portfolio = client.operations.get_portfolio(account_id=config.account_id)
    positions: dict[str, dict[str, float | int | str | None]] = {}
    for position in portfolio.positions:
        instrument = figi_to_instrument.get(position.figi)
        if instrument is None:
            continue
        qty_signed = int(round(quotation_to_float(getattr(position, "quantity", None))))
        qty = abs(qty_signed)
        if qty <= 0:
            continue
        side = "LONG" if qty_signed > 0 else "SHORT"
        avg_price = quotation_to_float(getattr(position, "average_position_price", None))
        current_price = quotation_to_float(getattr(position, "current_price", None))
        var_margin = quotation_to_float(getattr(position, "var_margin", None))
        expected_yield = quotation_to_float(getattr(position, "expected_yield", None))
        positions[instrument.symbol] = {
            "symbol": instrument.symbol,
            "side": side,
            "qty": qty,
            "entry_price": avg_price if avg_price > 0 else None,
            "current_price": current_price if current_price > 0 else None,
            "notional_rub": calculate_futures_notional_rub(instrument, current_price, qty) if current_price > 0 else 0.0,
            "variation_margin_rub": var_margin if var_margin is not None else expected_yield,
            "expected_yield_rub": expected_yield,
        }
    return positions


def sync_state_with_portfolio(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
) -> int:
    qty, avg, broker_current_price, broker_var_margin, broker_expected_yield = extract_position_data(client, config, instrument)
    if state.delayed_close_recovery_needed:
        reconcile_delayed_close_from_broker(client, config, instrument, state)
    state.position_qty = abs(qty)
    if qty == 0:
        state.entry_price = None
        state.entry_commission_rub = 0.0
        state.entry_commission_accounted = False
        state.max_price = None
        state.min_price = None
        state.position_side = "FLAT"
        state.breakeven_armed = False
        state.entry_time = ""
        state.entry_strategy = ""
        state.entry_reason = ""
        state.position_notional_rub = 0.0
        state.position_variation_margin_rub = 0.0
        state.position_pnl_pct = 0.0
        if not state.pending_order_id:
            state.execution_status = "idle"
        return 0
    last_price = broker_current_price if broker_current_price is not None and broker_current_price > 0 else get_last_price(client, instrument)
    if state.entry_price is None:
        state.entry_price = avg if avg is not None else last_price
        state.max_price = last_price
        state.min_price = last_price
        state.position_side = "LONG" if qty > 0 else "SHORT"
        state.entry_time = datetime.now(UTC).isoformat()
    else:
        state.position_side = "LONG" if qty > 0 else "SHORT"
        if not state.entry_time:
            state.entry_time = datetime.now(UTC).isoformat()
    state.max_price = max(state.max_price or last_price, last_price)
    state.min_price = min(state.min_price or last_price, last_price)
    pending_open_submitted_at = (
        parse_state_datetime(state.pending_submitted_at)
        if state.pending_order_id and state.pending_order_action == "OPEN"
        else None
    )
    active_open_lots = get_active_journal_lots(instrument.symbol, state.position_side)
    missing_open_lots = max(0, int(state.position_qty) - int(active_open_lots))
    has_matching_open_entry = (
        has_journal_event_since(
            instrument.symbol,
            state.position_side,
            "OPEN",
            not_before=pending_open_submitted_at,
        )
        if pending_open_submitted_at is not None
        else active_open_lots > 0
    )
    if state.position_side != "FLAT" and missing_open_lots > 0 and not state.delayed_close_recovery_needed:
        entry_reason_text = compact_reason(
            state.pending_entry_reason or state.entry_reason or ""
        )
        if state.pending_order_id and state.pending_order_action == "OPEN":
            recovery_reason = entry_reason_text or "Позиция подтверждена по брокерскому портфелю."
            recovery_source = "portfolio_confirmation"
            state.execution_status = "confirmed_open"
        else:
            recovery_reason = entry_reason_text or "Восстановлено после рестарта по брокерскому портфелю."
            recovery_source = "portfolio_recovery"
            state.execution_status = "recovered_open"
        state.entry_reason = recovery_reason
        operation_time, entry_fee_rub = find_recent_live_open_details(
            client,
            config,
            instrument,
            state.position_side,
            state.position_qty,
            state.entry_price or last_price,
        )
        if operation_time is not None:
            state.entry_time = operation_time.isoformat()
        if entry_fee_rub is not None and entry_fee_rub > 0:
            state.entry_commission_rub = entry_fee_rub
            if not state.entry_commission_accounted:
                state.realized_commission_rub += entry_fee_rub
                state.realized_pnl -= entry_fee_rub
                state.entry_commission_accounted = True
        append_trade_journal(
            instrument,
            "OPEN",
            state.position_side,
            missing_open_lots,
            state.entry_price or last_price,
            event_time=operation_time,
            gross_pnl_rub=None,
            commission_rub=entry_fee_rub,
            net_pnl_rub=-entry_fee_rub if entry_fee_rub is not None else None,
            reason=recovery_reason,
            source=recovery_source,
            strategy=state.entry_strategy or state.last_strategy_name or "recovered_position",
            dry_run=config.dry_run,
            state=state,
        )
        save_state(instrument.symbol, state)
    elif state.position_side != "FLAT" and not has_matching_open_entry and state.delayed_close_recovery_needed:
        logging.info(
            "symbol=%s status=open_recovery_deferred_until_close_reconciled",
            instrument.symbol,
        )
    refresh_position_snapshot(state, instrument, last_price)
    if (
        state.position_side != "FLAT"
        and state.position_qty > 0
        and state.entry_price is not None
        and (not state.entry_commission_accounted or float(state.entry_commission_rub or 0.0) <= 0)
    ):
        operation_time, entry_fee_rub = find_recent_live_open_details(
            client,
            config,
            instrument,
            state.position_side,
            state.position_qty,
            state.entry_price,
            not_before=None,
        )
        if operation_time is not None and entry_fee_rub is not None and entry_fee_rub > 0:
            state.entry_time = operation_time.isoformat()
            state.entry_commission_rub = entry_fee_rub
            state.entry_commission_accounted = True
            update_latest_unclosed_open_journal_entry(
                instrument.symbol,
                state.position_side,
                not_before=operation_time,
                commission_rub=entry_fee_rub,
                net_pnl_rub=-entry_fee_rub,
            )
    if state.position_side != "FLAT" and not state.pending_order_id and state.execution_status in {"idle", "rejected"}:
        state.execution_status = "confirmed_open"
    if broker_var_margin is not None:
        state.position_variation_margin_rub = broker_var_margin
    elif broker_expected_yield is not None:
        state.position_variation_margin_rub = broker_expected_yield
    save_state(instrument.symbol, state)
    return qty


def ensure_risk_limits(
    client: Client,
    state: InstrumentState,
    config: BotConfig,
) -> bool:
    reset_daily_pnl_if_needed(state)
    return get_daily_loss_limit_status(client, config)["allowed"]


def get_daily_loss_limit_status(client: Client, config: BotConfig) -> dict[str, Any]:
    if config.max_daily_loss <= 0:
        return {
            "allowed": True,
            "enabled": False,
            "net_pnl_rub": get_today_closed_net_pnl_rub(),
            "limit_rub": 0.0,
            "equity_rub": 0.0,
        }
    snapshot = get_account_snapshot(client, config)
    equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
    if equity <= 0:
        return {
            "allowed": True,
            "enabled": True,
            "net_pnl_rub": get_today_closed_net_pnl_rub(),
            "limit_rub": 0.0,
            "equity_rub": 0.0,
        }
    daily_loss_limit_rub = equity * (abs(config.max_daily_loss) / 100.0)
    net_pnl_rub = get_today_closed_net_pnl_rub()
    return {
        "allowed": net_pnl_rub > -daily_loss_limit_rub,
        "enabled": True,
        "net_pnl_rub": round(net_pnl_rub, 2),
        "limit_rub": round(daily_loss_limit_rub, 2),
        "equity_rub": round(equity, 2),
    }


def format_daily_loss_block_reason(status: dict[str, Any]) -> str:
    return (
        "глобальный дневной стоп: закрытый NET "
        f"{float(status.get('net_pnl_rub') or 0.0):.2f} RUB <= "
        f"-{float(status.get('limit_rub') or 0.0):.2f} RUB "
        f"({float(status.get('equity_rub') or 0.0):.2f} RUB * "
        f"{float(status.get('limit_pct') or 0.0):.2f}%)."
    )


def get_global_daily_loss_block_reason(client: Client, config: BotConfig) -> str:
    status = get_daily_loss_limit_status(client, config)
    if status.get("allowed"):
        return ""
    status["limit_pct"] = abs(float(config.max_daily_loss or 0.0))
    return format_daily_loss_block_reason(status)


def estimate_round_trip_commission_rub(state: InstrumentState) -> float:
    entry_commission = float(state.entry_commission_rub or 0.0)
    if entry_commission > 0:
        return max(entry_commission * 2.0, entry_commission + 1.0)
    return 10.0


def build_profit_lock_exit_reason(
    instrument: InstrumentConfig,
    state: InstrumentState,
    current_price: float,
) -> str:
    if state.entry_price is None or state.position_qty <= 0 or state.position_side == "FLAT":
        return ""
    if state.position_side == "LONG":
        best_price = max(float(state.max_price or current_price), current_price)
    else:
        best_price = min(float(state.min_price or current_price), current_price)
    best_gross = calculate_futures_pnl_rub(
        instrument,
        state.entry_price,
        best_price,
        state.position_qty,
        state.position_side,
    )
    current_gross = calculate_futures_pnl_rub(
        instrument,
        state.entry_price,
        current_price,
        state.position_qty,
        state.position_side,
    )
    round_trip_commission = estimate_round_trip_commission_rub(state)
    lock_trigger = max(round_trip_commission * 2.5, 50.0)
    lock_floor = max(round_trip_commission * 0.75, 10.0)
    if best_gross < lock_trigger:
        return ""
    if current_gross > lock_floor:
        return ""
    return (
        "Profit-lock: позиция уже давала "
        f"{best_gross:.2f} RUB gross, текущий результат {current_gross:.2f} RUB; "
        f"защищаем движение после покрытия комиссии ~{round_trip_commission:.2f} RUB."
    )


def calculate_today_strategy_performance(symbol: str, strategy_name: str) -> dict[str, Any]:
    return calculate_recent_strategy_performance(
        symbol,
        strategy_name,
        lookback_days=1,
        rows=get_today_trade_journal_rows(),
    )


def intraday_chop_block_reason(symbol: str, strategy_name: str) -> str:
    if strategy_name not in INTRADAY_CHOP_GUARD_STRATEGIES:
        return ""
    stats = calculate_today_strategy_performance(symbol, strategy_name)
    closed_count = int(stats["closed_count"])
    losses = int(stats["losses"])
    wins = int(stats["wins"])
    net_pnl = float(stats["net_pnl_rub"])
    if closed_count < 2 or losses < 2 or wins > 0 or net_pnl > -50.0:
        return ""
    return (
        f"anti-chop guard: {stats['symbol']} {stats['strategy']} сегодня "
        f"закрытий={closed_count}, убытков={losses}, net={net_pnl:.2f} RUB. "
        "Новые breakout/range входы по этой связке заблокированы до конца дня."
    )


def calculate_recent_strategy_performance(
    symbol: str,
    strategy_name: str,
    *,
    lookback_days: int = RECENT_STRATEGY_GUARD_DAYS,
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    start_day = datetime.now(MOSCOW_TZ).date() - timedelta(days=max(1, lookback_days - 1))
    if rows is None:
        source_rows = get_trade_journal_rows_since(start_day)
    else:
        start_value = start_day.isoformat()
        source_rows = [
            row
            for row in rows
            if not str(row.get("time", ""))[:10] or str(row.get("time", ""))[:10] >= start_value
        ]
    target_symbol = str(symbol or "").upper()
    target_strategy = str(strategy_name or "")
    closed_trades = [
        trade
        for trade in aggregate_closed_strategy_trades(source_rows)
        if str(trade.get("symbol", "")).upper() == target_symbol
        and str(trade.get("strategy", "") or "") == target_strategy
    ]
    total_net = 0.0
    wins = 0
    losses = 0
    for trade in closed_trades:
        try:
            net = float(trade.get("net_pnl_rub") or 0.0)
        except Exception:
            net = 0.0
        total_net += net
        if net > 0:
            wins += 1
        elif net < 0:
            losses += 1
    total = len(closed_trades)
    win_rate = (wins / total * 100.0) if total else 0.0
    return {
        "symbol": target_symbol,
        "strategy": target_strategy,
        "lookback_days": lookback_days,
        "closed_count": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "net_pnl_rub": round(total_net, 2),
    }


def recent_strategy_performance_block_reason(symbol: str, strategy_name: str) -> str:
    chop_reason = intraday_chop_block_reason(symbol, strategy_name)
    if chop_reason:
        return chop_reason
    stats = calculate_today_strategy_performance(symbol, strategy_name)
    closed_count = int(stats["closed_count"])
    losses = int(stats["losses"])
    wins = int(stats["wins"])
    win_rate = float(stats["win_rate"])
    net_pnl = float(stats["net_pnl_rub"])
    toxic_series = (
        closed_count >= RECENT_STRATEGY_GUARD_MIN_TRADES
        and win_rate <= RECENT_STRATEGY_GUARD_MAX_WIN_RATE
        and net_pnl <= RECENT_STRATEGY_GUARD_MAX_NET_PNL_RUB
    )
    hard_loss = closed_count >= 2 and net_pnl <= RECENT_STRATEGY_GUARD_HARD_LOSS_RUB
    no_win_loss_streak = closed_count >= 3 and wins == 0 and losses >= 3 and net_pnl <= -150.0
    if not (toxic_series or hard_loss or no_win_loss_streak):
        return ""
    return (
        f"daily performance guard: {stats['symbol']} {stats['strategy']} сегодня "
        f"закрытий={closed_count}, win rate={win_rate:.1f}%, net={net_pnl:.2f} RUB. "
        "Новые входы по этой связке заблокированы до следующего торгового дня."
    )


def get_recovery_mode_status(symbol: str, strategy_name: str) -> dict[str, Any]:
    stats = calculate_today_strategy_performance(symbol, strategy_name)
    closed_count = int(stats["closed_count"])
    losses = int(stats["losses"])
    wins = int(stats["wins"])
    net_pnl = float(stats["net_pnl_rub"])
    active = (
        (closed_count >= 2 and net_pnl <= -120.0)
        or (closed_count >= 3 and wins == 0 and losses >= 3)
    )
    return {
        "active": active,
        "closed_count": closed_count,
        "wins": wins,
        "losses": losses,
        "net_pnl_rub": net_pnl,
        "reason": (
            f"recovery mode: {symbol} {strategy_name} сегодня закрытий={closed_count}, "
            f"wins={wins}, losses={losses}, net={net_pnl:.2f} RUB."
        ),
    }


def get_strategy_health_score(symbol: str, strategy_name: str) -> tuple[float, str]:
    score = 1.0
    reasons: list[str] = []

    today_stats = calculate_today_strategy_performance(symbol, strategy_name)
    recent_stats = calculate_recent_strategy_performance(symbol, strategy_name, lookback_days=3)

    today_closed = int(today_stats["closed_count"])
    today_net = float(today_stats["net_pnl_rub"])
    today_win_rate = float(today_stats["win_rate"])
    recent_closed = int(recent_stats["closed_count"])
    recent_net = float(recent_stats["net_pnl_rub"])
    recent_win_rate = float(recent_stats["win_rate"])

    if recent_closed >= 4 and recent_net <= -250.0:
        score *= 0.82
        reasons.append("связка слаба 3 дня")
    elif recent_closed >= 4 and recent_net >= 180.0 and recent_win_rate >= 55.0:
        score *= 1.08
        reasons.append("связка сильна 3 дня")
    elif recent_closed >= 4 and recent_win_rate < 35.0:
        score *= 0.88
        reasons.append("низкий win rate 3 дня")

    if today_closed >= 2 and today_net <= -120.0:
        score *= 0.85
        reasons.append("сегодняшняя просадка")
    elif today_closed >= 2 and today_net >= 80.0 and today_win_rate >= 50.0:
        score *= 1.05
        reasons.append("сегодняшний edge")

    score = max(0.70, min(score, 1.20))
    return score, ", ".join(reasons) if reasons else "нейтральная форма связки"


def recovery_mode_block_reason(
    state: InstrumentState,
    symbol: str,
    strategy_name: str,
    signal: str,
) -> str:
    status = get_recovery_mode_status(symbol, strategy_name)
    if not status["active"] or signal not in {"LONG", "SHORT"}:
        return ""

    allowed_strategies = {"trend_pullback", "failed_breakout", "trend_rollover"}
    setup_label = str(state.last_setup_quality_label or "").strip().lower()
    regime = str(state.last_market_regime or "").strip().lower()
    higher_tf_bias = str(state.last_higher_tf_bias or "").strip().upper()

    if strategy_name not in allowed_strategies:
        return f"{status['reason']} Разрешены только точечные стратегии после серии минусов."
    if setup_label != "strong":
        return f"{status['reason']} Нужен только strong setup."
    if regime not in {"trend_pullback", "trend_expansion", "impulse"}:
        return f"{status['reason']} Текущий режим {regime or '-'} слишком слабый для recovery mode."
    if higher_tf_bias != signal:
        return f"{status['reason']} Нужен вход только по направлению старшего ТФ."
    return ""


def mark_daily_risk_stop_if_needed(state: InstrumentState) -> None:
    today = datetime.now(MOSCOW_TZ).date().isoformat()
    if state.last_risk_stop_day != today:
        state.last_risk_stop_day = today


def get_account_snapshot(client: Client, config: BotConfig) -> AccountSnapshot:
    portfolio = client.operations.get_portfolio(account_id=config.account_id)
    limits = client.operations.get_withdraw_limits(account_id=config.account_id)

    total_portfolio = quotation_to_float(getattr(portfolio, "total_amount_portfolio", None))
    free_rub = 0.0
    blocked_guarantee_rub = 0.0

    for money in getattr(limits, "money", []) or []:
        if getattr(money, "currency", "").upper() == "RUB":
            free_rub += quotation_to_float(money)
    for money in getattr(limits, "blocked_guarantee", []) or []:
        if getattr(money, "currency", "").upper() == "RUB":
            blocked_guarantee_rub += quotation_to_float(money)

    return AccountSnapshot(
        total_portfolio=total_portfolio,
        free_rub=free_rub,
        blocked_guarantee_rub=blocked_guarantee_rub,
    )


def get_accounting_snapshot_for_day(
    client: Client,
    config: BotConfig,
    target_day: date,
    watchlist: list[InstrumentConfig] | None = None,
) -> dict[str, Any]:
    def history_fallback() -> dict[str, Any]:
        history_entry = (load_accounting_history() or {}).get(target_day.isoformat()) or {}
        return {
            "date": target_day.isoformat(),
            "actual_varmargin_rub": round(float(history_entry.get("actual_varmargin_rub") or 0.0), 2),
            "actual_fee_expense_rub": round(float(history_entry.get("actual_fee_expense_rub") or 0.0), 2),
            "actual_fee_cash_effect_rub": round(
                float(history_entry.get("actual_fee_cash_effect_rub") or 0.0),
                2,
            ),
            "actual_account_cash_effect_rub": round(
                float(history_entry.get("actual_account_cash_effect_rub") or 0.0),
                2,
            ),
            "varmargin_by_symbol": dict(history_entry.get("varmargin_by_symbol") or {}),
            "source": "history_fallback",
        }

    from_utc, to_utc = get_day_bounds_utc_for_date(target_day)
    cursor = ""
    actual_varmargin_rub = 0.0
    actual_fee_expense_rub = 0.0
    actual_fee_cash_effect_rub = 0.0
    figi_to_symbol = {item.figi: item.symbol for item in (watchlist or []) if item.figi}
    varmargin_by_symbol: dict[str, float] = {}

    while True:
        try:
            response = client.operations.get_operations_by_cursor(
                GetOperationsByCursorRequest(
                    account_id=config.account_id,
                    from_=from_utc,
                    to=to_utc,
                    cursor=cursor,
                    limit=200,
                    state=OperationState.OPERATION_STATE_EXECUTED,
                    without_commissions=False,
                    without_overnights=False,
                    without_trades=False,
                )
            )
        except RequestError as error:
            logging.warning(
                "Не удалось получить операции за %s: %s. Использую сохранённую историю.",
                target_day.isoformat(),
                error,
            )
            fallback = history_fallback()
            if any(
                abs(float(fallback.get(key) or 0.0)) > 0.0
                for key in (
                    "actual_varmargin_rub",
                    "actual_fee_expense_rub",
                    "actual_fee_cash_effect_rub",
                    "actual_account_cash_effect_rub",
                )
            ) or fallback.get("varmargin_by_symbol"):
                return fallback
            return {
                "date": target_day.isoformat(),
                "actual_varmargin_rub": round(actual_varmargin_rub, 2),
                "actual_fee_expense_rub": round(actual_fee_expense_rub, 2),
                "actual_fee_cash_effect_rub": round(actual_fee_cash_effect_rub, 2),
                "actual_account_cash_effect_rub": round(actual_varmargin_rub + actual_fee_cash_effect_rub, 2),
                "varmargin_by_symbol": varmargin_by_symbol,
                "source": "partial_live_fallback",
            }
        for item in getattr(response, "items", []) or []:
            op_type = getattr(item, "type", None)
            payment = quotation_to_float(getattr(item, "payment", None))
            figi = str(getattr(item, "figi", "") or "")
            symbol = figi_to_symbol.get(figi)
            if op_type in VARMARGIN_OPERATION_TYPES:
                actual_varmargin_rub += payment
                if symbol:
                    varmargin_by_symbol[symbol] = round(varmargin_by_symbol.get(symbol, 0.0) + payment, 2)
            elif op_type in FEE_OPERATION_TYPES:
                actual_fee_cash_effect_rub += payment
                actual_fee_expense_rub += abs(payment)

        next_cursor = str(getattr(response, "next_cursor", "") or "")
        if not getattr(response, "has_next", False) or not next_cursor:
            break
        cursor = next_cursor

    return {
        "date": target_day.isoformat(),
        "actual_varmargin_rub": round(actual_varmargin_rub, 2),
        "actual_fee_expense_rub": round(actual_fee_expense_rub, 2),
        "actual_fee_cash_effect_rub": round(actual_fee_cash_effect_rub, 2),
        "actual_account_cash_effect_rub": round(actual_varmargin_rub + actual_fee_cash_effect_rub, 2),
        "varmargin_by_symbol": varmargin_by_symbol,
        "source": "live",
    }


def get_today_accounting_snapshot(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig] | None = None,
) -> dict[str, Any]:
    return get_accounting_snapshot_for_day(client, config, current_moscow_time().date(), watchlist)


def describe_capacity_block_reason(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    entry_price: float,
    signal: str,
    strategy_name: str = "",
) -> str:
    sizing = calculate_position_sizing_context(client, config, instrument, state, entry_price, signal, strategy_name)
    margin_per_lot = float(sizing.get("margin_per_lot_rub") or 0.0)
    if margin_per_lot > 0:
        free_rub = float(sizing.get("free_rub") or 0.0)
        working_margin_budget = float(sizing.get("working_margin_budget_rub") or 0.0)
        allocatable_margin = float(sizing.get("allocatable_margin_rub") or 0.0)
        reserve_rub = float(sizing.get("reserve_rub") or 0.0)
        if free_rub < margin_per_lot:
            return (
                f"не хватает средств/ГО для {instrument.symbol}: "
                f"на 1 лот нужно примерно {margin_per_lot:.2f} RUB, свободно {free_rub:.2f} RUB."
            )
        if working_margin_budget < margin_per_lot:
            return (
                f"рабочий бюджет капитала не позволяет открыть {instrument.symbol}: "
                f"на 1 лот нужно {margin_per_lot:.2f} RUB, в рабочем бюджете доступно {working_margin_budget:.2f} RUB."
            )
        if allocatable_margin < margin_per_lot:
            return (
                f"аллокатор капитала пока не даёт новый вход по {instrument.symbol}: "
                f"на 1 лот нужно {margin_per_lot:.2f} RUB, под новые сигналы сейчас доступно {allocatable_margin:.2f} RUB "
                f"(после резерва {reserve_rub:.2f} RUB)."
            )

    try:
        max_lots = client.orders.get_max_lots(
            GetMaxLotsRequest(
                account_id=config.account_id,
                instrument_id=instrument.figi,
            )
        )
        if signal == "LONG":
            broker_limit = int(getattr(getattr(max_lots, "buy_limits", None), "buy_max_market_lots", 0) or 0)
        else:
            broker_limit = int(getattr(getattr(max_lots, "sell_limits", None), "sell_max_lots", 0) or 0)
        if broker_limit <= 0:
            return (
                f"брокер не разрешает открыть {instrument.symbol}: "
                f"доступный лимит по заявке сейчас 0 лотов."
            )
    except Exception:
        pass

    return f"не удалось открыть {instrument.symbol}: размер позиции получился 0 лотов."


def summarize_order_request_error(instrument: InstrumentConfig, error: RequestError) -> str:
    text = str(error).strip()
    lowered = text.lower()
    if any(needle in lowered for needle in {"insufficient", "not enough", "недостат", "недост", "не хватает", "lack"}):
        return f"заявка по {instrument.symbol} отклонена: не хватает средств/ГО. {text}"
    if any(needle in lowered for needle in {"margin", "го", "guarantee"}):
        return f"заявка по {instrument.symbol} отклонена из-за ограничений по ГО/марже. {text}"
    return f"заявка по {instrument.symbol} отклонена брокером. {text}"


def summarize_pending_order_rejection(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
) -> str:
    action = state.pending_order_action or "UNKNOWN"
    side = state.pending_order_side or "UNKNOWN"
    base_reason = (
        f"заявка по {instrument.symbol} не исполнена: действие={action}, направление={side}."
    )
    if action != "OPEN" or side not in {"LONG", "SHORT"}:
        return base_reason

    try:
        price = get_last_price(client, instrument)
        block_reason = describe_capacity_block_reason(client, config, instrument, state, price, side, state.entry_strategy)
        return f"{base_reason} Причина: {block_reason}"
    except Exception as error:
        logging.warning("Не удалось уточнить причину отклонения заявки по %s: %s", instrument.symbol, error)
        return base_reason


def is_order_not_found_error(error: RequestError) -> bool:
    text = str(error).lower()
    return "order not found" in text or "50005" in text or "not_found" in text


def get_margin_per_lot(instrument: InstrumentConfig, signal: str) -> float:
    margin = instrument.initial_margin_on_buy if signal == "LONG" else instrument.initial_margin_on_sell
    if margin > 0:
        return margin
    fallback_margin = instrument.initial_margin_on_buy or instrument.initial_margin_on_sell
    if fallback_margin > 0:
        return fallback_margin
    return 0.0


def get_margin_headroom_rub(client: Client, config: BotConfig, snapshot: AccountSnapshot) -> float:
    try:
        margin = client.users.get_margin_attributes(account_id=config.account_id)
        missing = quotation_to_float(getattr(margin, "amount_of_missing_funds", None))
        if missing is not None:
            return max(0.0, -missing)
    except Exception as error:
        logging.warning("Не удалось получить margin attributes: %s", error)

    equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
    if equity <= 0:
        return max(0.0, snapshot.free_rub)
    if config.max_margin_usage_pct > 0:
        return max(0.0, equity * config.max_margin_usage_pct - snapshot.blocked_guarantee_rub)
    return max(0.0, snapshot.free_rub)


def get_instrument_allocation_weight(symbol: str) -> tuple[str, float]:
    if symbol in {"BRK6", "USDRUBF", "NGJ6"}:
        return "тяжёлый", 0.85
    if symbol in {"GNM6", "SRM6"}:
        return "средний", 1.0
    if symbol in {"IMOEXF", "CNYRUBF"}:
        return "лёгкий", 1.35
    return "базовый", 1.0


def get_signal_conviction_weight(state: InstrumentState, signal: str, strategy_name: str) -> float:
    weight = 1.0
    if state.last_higher_tf_bias == signal:
        weight += 0.15
    news_bias, news_strength = parse_bias_label(state.last_news_bias)
    if news_bias == signal:
        if news_strength == "HIGH":
            weight += 0.30
        elif news_strength == "MEDIUM":
            weight += 0.18
        elif news_strength == "LOW":
            weight += 0.08
    if strategy_name in {"momentum_breakout", "opening_range_breakout"}:
        weight += 0.10
    elif strategy_name == "range_break_continuation":
        weight += 0.05
    return min(weight, 1.65)


def get_adaptive_entry_size_multiplier(
    state: InstrumentState,
    symbol: str,
    strategy_name: str,
) -> tuple[float, str]:
    multiplier = 1.0
    reasons: list[str] = []

    setup_label = str(state.last_setup_quality_label or "").strip().lower()
    if setup_label == "strong":
        multiplier *= 1.10
        reasons.append("сильный сетап")
    elif setup_label == "medium":
        multiplier *= 0.85
        reasons.append("средний сетап")
    elif setup_label == "weak":
        multiplier *= 0.60
        reasons.append("слабый сетап")

    regime = str(state.last_market_regime or "").strip().lower()
    if regime in {"trend_expansion", "impulse"}:
        multiplier *= 1.05
        reasons.append(f"режим {regime}")
    elif regime in {"compression", "chop"}:
        multiplier *= 0.75
        reasons.append(f"режим {regime}")

    health_score, health_reason = get_strategy_health_score(symbol, strategy_name)
    multiplier *= health_score
    if health_reason:
        reasons.append(health_reason)

    recovery_status = get_recovery_mode_status(symbol, strategy_name)
    if recovery_status["active"]:
        multiplier *= 0.65
        reasons.append("recovery mode")

    multiplier = max(0.35, min(multiplier, 1.35))
    return multiplier, ", ".join(reasons) if reasons else "нейтральный размер"


def calculate_position_sizing_context(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    entry_price: float,
    signal: str,
    strategy_name: str = "",
) -> dict[str, Any]:
    session_name = get_market_session()
    session_multiplier = get_session_position_multiplier(session_name, instrument.symbol)
    snapshot = get_account_snapshot(client, config)
    equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
    margin_per_lot = get_margin_per_lot(instrument, signal)
    margin_headroom = get_margin_headroom_rub(client, config, snapshot)
    # Основой аллокации должен быть реальный маржинальный запас брокера, а не
    # только текущая оценка портфеля. Резерв ограничивает суммарную загрузку
    # счёта, но не должен чрезмерно занижать целевой размер одной сделки.
    working_margin_budget = max(0.0, margin_headroom)
    reserve_rub = working_margin_budget * max(0.0, min(config.capital_reserve_pct, 0.95))
    allocatable_margin = max(0.0, working_margin_budget - reserve_rub)
    instrument_class, instrument_weight = get_instrument_allocation_weight(instrument.symbol)
    conviction_weight = get_signal_conviction_weight(state, signal, strategy_name)
    adaptive_size_multiplier, adaptive_size_reason = get_adaptive_entry_size_multiplier(
        state,
        instrument.symbol,
        strategy_name,
    )
    strategy_health_score, strategy_health_reason = get_strategy_health_score(
        instrument.symbol,
        strategy_name,
    )
    recovery_status = get_recovery_mode_status(instrument.symbol, strategy_name)
    base_trade_share = max(0.05, min(config.base_trade_allocation_pct, 1.0))
    trade_aggression = max(0.35, min(config.portfolio_usage_pct, 1.0))
    target_trade_margin = (
        working_margin_budget
        * trade_aggression
        * base_trade_share
        * instrument_weight
        * conviction_weight
        * adaptive_size_multiplier
        * max(session_multiplier, 0.0)
    )
    qty_by_target = int(target_trade_margin // margin_per_lot) if margin_per_lot > 0 else 0
    qty_by_allocatable = int(allocatable_margin // margin_per_lot) if margin_per_lot > 0 else 0
    qty_by_working = int(working_margin_budget // margin_per_lot) if margin_per_lot > 0 else 0
    qty_by_headroom = int(margin_headroom // margin_per_lot) if margin_per_lot > 0 else 0

    step_price = instrument.min_price_increment
    step_money = instrument.min_price_increment_amount
    stop_distance = entry_price * config.stop_loss_pct
    money_risk_per_contract = 0.0
    risk_budget = 0.0
    if step_price > 0 and step_money > 0 and stop_distance > 0 and equity > 0 and config.risk_per_trade_pct > 0:
        risk_budget = equity * config.risk_per_trade_pct * max(session_multiplier, 0.0)
        money_risk_per_contract = (stop_distance / step_price) * step_money

    raw_qty = min(qty_by_target, qty_by_allocatable) if qty_by_allocatable > 0 else 0
    if raw_qty < 1 and margin_per_lot > 0:
        can_afford_min_lot = qty_by_working >= 1
        can_afford_min_lot_by_headroom = qty_by_headroom >= 1
        near_min_lot_by_working = working_margin_budget >= margin_per_lot * 0.96
        if (
            instrument.symbol == "BRK6"
            and can_afford_min_lot_by_headroom
            and conviction_weight >= 1.10
            and signal in {"LONG", "SHORT"}
        ):
            raw_qty = 1
        elif (
            instrument.symbol == "SRM6"
            and signal == "SHORT"
            and near_min_lot_by_working
            and conviction_weight >= 1.15
        ):
            raw_qty = 1
        elif can_afford_min_lot and (instrument_class == "тяжёлый" or conviction_weight >= 1.25):
            raw_qty = 1
        elif qty_by_allocatable >= 1:
            raw_qty = 1

    broker_limit = 0
    try:
        max_lots = client.orders.get_max_lots(
            GetMaxLotsRequest(
                account_id=config.account_id,
                instrument_id=instrument.figi,
            )
        )
        if signal == "LONG":
            broker_limit = int(getattr(getattr(max_lots, "buy_limits", None), "buy_max_market_lots", 0) or 0)
        else:
            broker_limit = int(getattr(getattr(max_lots, "sell_limits", None), "sell_max_lots", 0) or 0)
    except Exception as error:
        logging.warning("Не удалось получить max lots для %s: %s", instrument.symbol, error)

    if broker_limit > 0:
        raw_qty = min(raw_qty, broker_limit)
    if config.max_order_quantity > 0:
        raw_qty = min(raw_qty, config.max_order_quantity)

    return {
        "session_name": session_name,
        "session_multiplier": session_multiplier,
        "equity": equity,
        "free_rub": snapshot.free_rub,
        "blocked_guarantee_rub": snapshot.blocked_guarantee_rub,
        "margin_headroom_rub": margin_headroom,
        "working_margin_budget_rub": working_margin_budget,
        "reserve_rub": reserve_rub,
        "allocatable_margin_rub": allocatable_margin,
        "trade_aggression": trade_aggression,
        "instrument_class": instrument_class,
        "instrument_weight": instrument_weight,
        "conviction_weight": conviction_weight,
        "adaptive_size_multiplier": adaptive_size_multiplier,
        "adaptive_size_reason": adaptive_size_reason,
        "strategy_health_score": strategy_health_score,
        "strategy_health_reason": strategy_health_reason,
        "recovery_mode_active": bool(recovery_status["active"]),
        "base_trade_share": base_trade_share,
        "target_trade_margin_rub": target_trade_margin,
        "margin_per_lot_rub": margin_per_lot,
        "qty_by_target": qty_by_target,
        "qty_by_allocatable": qty_by_allocatable,
        "qty_by_working": qty_by_working,
        "qty_by_headroom": qty_by_headroom,
        "broker_limit": broker_limit,
        "money_risk_per_contract_rub": money_risk_per_contract,
        "risk_budget_rub": risk_budget,
        "quantity": max(0, raw_qty),
    }


def calculate_order_quantity(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    entry_price: float,
    signal: str,
    strategy_name: str = "",
) -> int:
    sizing = calculate_position_sizing_context(client, config, instrument, state, entry_price, signal, strategy_name)
    return int(sizing.get("quantity") or 0)


def build_position_sizing_lines(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    entry_price: float,
    signal: str,
    quantity: int,
    strategy_name: str = "",
) -> list[str]:
    sizing = calculate_position_sizing_context(client, config, instrument, state, entry_price, signal, strategy_name)
    lines = [
        f"Сессия: {sizing['session_name']}",
        f"Множитель размера: {sizing['session_multiplier']:.2f}",
        f"Лотов: {quantity}",
        f"Размер биржевого лота: {instrument.lot}",
        f"Класс инструмента: {sizing['instrument_class']}",
        f"Вес инструмента: {sizing['instrument_weight']:.2f}",
        f"Вес сигнала: {sizing['conviction_weight']:.2f}",
    ]
    lines.append(f"Портфель: {sizing['equity']:.2f} RUB")
    lines.append(f"Свободно: {sizing['free_rub']:.2f} RUB")
    lines.append(f"ГО занято: {sizing['blocked_guarantee_rub']:.2f} RUB")
    lines.append(f"Маржинальный запас: {sizing['margin_headroom_rub']:.2f} RUB")
    lines.append(f"Рабочий маржинальный бюджет: {sizing['working_margin_budget_rub']:.2f} RUB")
    lines.append(f"Резерв капитала: {sizing['reserve_rub']:.2f} RUB")
    lines.append(f"Доступно под новые входы: {sizing['allocatable_margin_rub']:.2f} RUB")
    lines.append(f"Глубина по марже: {int(sizing['qty_by_headroom'])} лот(а)")
    lines.append(f"Агрессивность аллокации: {float(sizing['trade_aggression']):.2f}")
    if sizing["margin_per_lot_rub"] > 0:
        lines.append(f"ГО на 1 лот: {sizing['margin_per_lot_rub']:.2f} RUB")
    lines.append(f"Целевой бюджет сделки: {sizing['target_trade_margin_rub']:.2f} RUB")
    if sizing["risk_budget_rub"] > 0 and sizing["money_risk_per_contract_rub"] > 0:
        lines.append(f"Риск-бюджет (справочно): {sizing['risk_budget_rub']:.2f} RUB")
        lines.append(f"Риск на 1 контракт (справочно): {sizing['money_risk_per_contract_rub']:.2f} RUB")
    if sizing["broker_limit"] > 0:
        lines.append(f"Максимум у брокера: {sizing['broker_limit']} лотов")
    return lines


def build_allocator_summary_text(sizing: dict[str, Any]) -> str:
    quantity = int(sizing.get("quantity") or 0)
    instrument_class = str(sizing.get("instrument_class") or "базовый")
    conviction_weight = float(sizing.get("conviction_weight") or 0.0)
    allocatable_margin = float(sizing.get("allocatable_margin_rub") or 0.0)
    margin_headroom = float(sizing.get("margin_headroom_rub") or 0.0)
    target_trade_margin = float(sizing.get("target_trade_margin_rub") or 0.0)
    margin_per_lot = float(sizing.get("margin_per_lot_rub") or 0.0)
    broker_limit = int(sizing.get("broker_limit") or 0)
    qty_by_headroom = int(sizing.get("qty_by_headroom") or 0)
    strategy_health_score = float(sizing.get("strategy_health_score") or 1.0)
    recovery_mode_active = bool(sizing.get("recovery_mode_active"))
    recovery_hint = ", recovery mode" if recovery_mode_active else ""
    if quantity <= 0:
        return (
            f"Аллокатор: вход не проходит. Класс {instrument_class}, "
            f"вес сигнала {conviction_weight:.2f}, health {strategy_health_score:.2f}{recovery_hint}, запас {margin_headroom:.0f} RUB, "
            f"доступно {allocatable_margin:.0f} RUB, цель {target_trade_margin:.0f} RUB, "
            f"ГО 1 лота {margin_per_lot:.0f} RUB, глубина {qty_by_headroom} лот(а)."
        )
    broker_hint = f", лимит брокера {broker_limit}" if broker_limit > 0 else ""
    return (
        f"Аллокатор: класс {instrument_class}, вес сигнала {conviction_weight:.2f}, "
        f"health {strategy_health_score:.2f}{recovery_hint}, "
        f"запас {margin_headroom:.0f} RUB, доступно {allocatable_margin:.0f} RUB, "
        f"цель {target_trade_margin:.0f} RUB, ГО 1 лота {margin_per_lot:.0f} RUB, "
        f"глубина {qty_by_headroom} лот(а){broker_hint} -> {quantity} лот(а)."
    )


def calculate_futures_pnl_rub(
    instrument: InstrumentConfig,
    entry_price: float,
    exit_price: float,
    qty: int,
    side: str,
) -> float:
    if qty <= 0:
        return 0.0
    price_diff = exit_price - entry_price if side == "LONG" else entry_price - exit_price
    if instrument.min_price_increment > 0 and instrument.min_price_increment_amount > 0:
        ticks = price_diff / instrument.min_price_increment
        return ticks * instrument.min_price_increment_amount * qty
    return price_diff * qty


def calculate_futures_notional_rub(
    instrument: InstrumentConfig,
    current_price: float,
    qty: int,
) -> float:
    if qty <= 0 or current_price <= 0:
        return 0.0
    if instrument.min_price_increment > 0 and instrument.min_price_increment_amount > 0:
        ticks = current_price / instrument.min_price_increment
        return ticks * instrument.min_price_increment_amount * qty
    return current_price * qty


def refresh_position_snapshot(state: InstrumentState, instrument: InstrumentConfig, current_price: float) -> None:
    state.last_market_price = current_price
    if state.position_side == "FLAT" or state.position_qty <= 0 or state.entry_price is None:
        state.position_notional_rub = 0.0
        state.position_variation_margin_rub = 0.0
        state.position_pnl_pct = 0.0
        return

    state.position_notional_rub = calculate_futures_notional_rub(
        instrument,
        current_price,
        state.position_qty,
    )
    state.position_variation_margin_rub = calculate_futures_pnl_rub(
        instrument,
        state.entry_price,
        current_price,
        state.position_qty,
        state.position_side,
    )
    if state.entry_price > 0:
        if state.position_side == "LONG":
            state.position_pnl_pct = ((current_price - state.entry_price) / state.entry_price) * 100.0
        else:
            state.position_pnl_pct = ((state.entry_price - current_price) / state.entry_price) * 100.0
    else:
        state.position_pnl_pct = 0.0


def get_exit_profile(config: BotConfig, strategy_name: str) -> ExitProfile:
    strategy = (strategy_name or "").strip()
    if strategy == "opening_range_breakout":
        return ExitProfile(
            min_hold_minutes=max(config.min_hold_minutes, 25),
            breakeven_profit_pct=max(config.breakeven_profit_pct, 0.0075),
            trailing_stop_pct=max(config.trailing_stop_pct, 0.0055),
        )
    if strategy in {"range_break_continuation", "breakdown_continuation"}:
        return ExitProfile(
            min_hold_minutes=max(config.min_hold_minutes, 25),
            breakeven_profit_pct=max(config.breakeven_profit_pct, 0.0080),
            trailing_stop_pct=max(config.trailing_stop_pct, 0.0060),
        )
    if strategy == "momentum_breakout":
        return ExitProfile(
            min_hold_minutes=max(config.min_hold_minutes, 25),
            breakeven_profit_pct=max(config.breakeven_profit_pct, 0.0090),
            trailing_stop_pct=max(config.trailing_stop_pct, 0.0070),
        )
    if strategy == "trend_rollover":
        return ExitProfile(
            min_hold_minutes=max(config.min_hold_minutes, 35),
            breakeven_profit_pct=max(config.breakeven_profit_pct, 0.0100),
            trailing_stop_pct=max(config.trailing_stop_pct, 0.0075),
        )
    if strategy == "trend_pullback":
        return ExitProfile(
            min_hold_minutes=max(config.min_hold_minutes, 30),
            breakeven_profit_pct=max(config.breakeven_profit_pct, 0.0090),
            trailing_stop_pct=max(config.trailing_stop_pct, 0.0065),
        )
    return ExitProfile(
        min_hold_minutes=config.min_hold_minutes,
        breakeven_profit_pct=config.breakeven_profit_pct,
        trailing_stop_pct=config.trailing_stop_pct,
    )


def get_adaptive_exit_profile(
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    base_profile: ExitProfile,
) -> tuple[ExitProfile, str]:
    profile = base_profile
    reasons: list[str] = []
    regime = str(state.last_market_regime or "").strip().lower()
    setup_label = str(state.last_setup_quality_label or "").strip().lower()
    recovery_active = False
    if state.entry_strategy:
        recovery_active = bool(get_recovery_mode_status(instrument.symbol, state.entry_strategy)["active"])

    if regime in {"compression", "chop"} or setup_label == "weak" or recovery_active:
        profile = ExitProfile(
            min_hold_minutes=min(profile.min_hold_minutes, 20),
            breakeven_profit_pct=min(profile.breakeven_profit_pct, 0.0045),
            trailing_stop_pct=min(profile.trailing_stop_pct, 0.0040),
        )
        if regime in {"compression", "chop"}:
            reasons.append(f"режим {regime}")
        if setup_label == "weak":
            reasons.append("weak setup")
        if recovery_active:
            reasons.append("recovery mode")
    elif regime in {"trend_expansion", "impulse"} and setup_label == "strong":
        profile = ExitProfile(
            min_hold_minutes=max(profile.min_hold_minutes, 30),
            breakeven_profit_pct=max(profile.breakeven_profit_pct, 0.0060),
            trailing_stop_pct=max(profile.trailing_stop_pct, 0.0075),
        )
        reasons.append(f"режим {regime}")
        reasons.append("strong setup")
    elif regime == "trend_pullback" and setup_label in {"strong", "medium"}:
        profile = ExitProfile(
            min_hold_minutes=max(profile.min_hold_minutes, 25),
            breakeven_profit_pct=max(profile.breakeven_profit_pct, 0.0055),
            trailing_stop_pct=max(profile.trailing_stop_pct, 0.0065),
        )
        reasons.append("trend pullback context")

    return profile, ", ".join(reasons) if reasons else ""


def position_held_long_enough(state: InstrumentState, config: BotConfig, min_hold_minutes: int | None = None) -> bool:
    if not state.entry_time:
        return True
    try:
        opened_at = datetime.fromisoformat(state.entry_time)
    except ValueError:
        return True
    if opened_at.tzinfo is None:
        opened_at = opened_at.replace(tzinfo=UTC)
    held_for = datetime.now(UTC) - opened_at.astimezone(UTC)
    required_minutes = min_hold_minutes if min_hold_minutes is not None else config.min_hold_minutes
    return held_for >= timedelta(minutes=required_minutes)


def select_exit_indicator_df(
    instrument: InstrumentConfig,
    lower_df: pd.DataFrame,
    higher_tf_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    higher_tf_exit_symbols = {"GNM6", "NGJ6", "RBM6", "SRM6", "VBM6", "IMOEXF"}
    if (
        (instrument.symbol in higher_tf_exit_symbols or get_instrument_group(instrument.symbol).name == "fx")
        and higher_tf_df is not None
        and len(higher_tf_df) >= 3
    ):
        return higher_tf_df
    return lower_df


def macd_crossed_down_with_ema_loss(df: pd.DataFrame) -> bool:
    if len(df) < 3:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    prev2_macd = float(prev2["macd"])
    prev2_macd_signal = float(prev2["macd_signal"])
    close = float(last["close"])
    ema20 = float(last["ema20"])
    crossed = prev_macd >= prev_macd_signal and macd < macd_signal
    confirmed = prev2_macd >= prev2_macd_signal and prev_macd < prev_macd_signal and macd < macd_signal
    return close < ema20 and (crossed or confirmed)


def macd_crossed_up_with_ema_reclaim(df: pd.DataFrame) -> bool:
    if len(df) < 3:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    prev2_macd = float(prev2["macd"])
    prev2_macd_signal = float(prev2["macd_signal"])
    close = float(last["close"])
    ema20 = float(last["ema20"])
    crossed = prev_macd <= prev_macd_signal and macd > macd_signal
    confirmed = prev2_macd <= prev2_macd_signal and prev_macd > prev_macd_signal and macd > macd_signal
    return close > ema20 and (crossed or confirmed)


def rbm6_sideways_exhaustion_exit_reason(
    instrument: InstrumentConfig,
    state: InstrumentState,
    df: pd.DataFrame,
    current_price: float,
) -> str:
    if state.entry_price is None or state.position_qty <= 0 or state.position_side == "FLAT":
        return ""
    if len(df) < 8:
        return ""
    best_price = max(float(state.max_price or current_price), current_price) if state.position_side == "LONG" else min(float(state.min_price or current_price), current_price)
    best_gross = calculate_futures_pnl_rub(
        instrument,
        state.entry_price,
        best_price,
        state.position_qty,
        state.position_side,
    )
    current_gross = calculate_futures_pnl_rub(
        instrument,
        state.entry_price,
        current_price,
        state.position_qty,
        state.position_side,
    )
    round_trip_commission = estimate_round_trip_commission_rub(state)
    if best_gross < max(round_trip_commission * 2.0, 25.0):
        return ""
    recent = df.iloc[-5:]
    previous = df.iloc[-8:-5]
    last = df.iloc[-1]
    prev = df.iloc[-2]
    macd_hist = float(last["macd"]) - float(last["macd_signal"])
    prev_hist = float(prev["macd"]) - float(prev["macd_signal"])
    if state.position_side == "LONG":
        no_new_high = float(recent["high"].max()) <= float(previous["high"].max()) * 1.0004
        momentum_fades = macd_hist <= prev_hist and float(last["rsi"]) <= 58.0
        gave_back = current_gross <= best_gross * 0.65
        if no_new_high and momentum_fades and gave_back:
            return (
                "RBM6 profit-lock: после импульса нет нового high на 15m, "
                f"MACD затухает; фиксируем {current_gross:.2f} из max {best_gross:.2f} RUB gross."
            )
    else:
        no_new_low = float(recent["low"].min()) >= float(previous["low"].min()) * 0.9996
        momentum_fades = macd_hist >= prev_hist and float(last["rsi"]) >= 42.0
        gave_back = current_gross <= best_gross * 0.65
        if no_new_low and momentum_fades and gave_back:
            return (
                "RBM6 profit-lock: после импульса нет нового low на 15m, "
                f"MACD затухает; фиксируем {current_gross:.2f} из max {best_gross:.2f} RUB gross."
            )
    return ""


def price_has_new_extreme_since_exit(
    instrument: InstrumentConfig,
    signal: str,
    current_price: float,
    last_exit_price: float | None,
    min_steps: int = 1,
) -> bool:
    if last_exit_price is None:
        return True
    step = instrument.min_price_increment if instrument.min_price_increment > 0 else max(abs(last_exit_price) * 0.0002, 0.0001)
    threshold = step * max(min_steps, 1)
    if signal == "SHORT":
        return current_price <= last_exit_price - threshold
    if signal == "LONG":
        return current_price >= last_exit_price + threshold
    return True


def position_reentry_allowed(
    state: InstrumentState,
    instrument: InstrumentConfig,
    signal: str,
    current_price: float,
) -> tuple[bool, str]:
    def exit_reason_has_macd_ema_reversal() -> bool:
        reason = str(state.last_exit_reason or "").lower()
        return "macd" in reason or "ema20" in reason or "цена вернулась" in reason or "цена потеряла" in reason

    def strong_same_side_reentry_confirmed(min_steps: int) -> bool:
        return price_has_new_extreme_since_exit(
            instrument,
            signal,
            current_price,
            state.last_exit_price,
            min_steps=min_steps,
        )

    def brk6_fresh_impulse_override() -> bool:
        if instrument.symbol != "BRK6":
            return False
        if signal != "LONG":
            return False
        if state.last_exit_side != "LONG" or state.last_exit_pnl_rub <= 0:
            return False
        if state.last_higher_tf_bias != "LONG":
            return False
        news_bias, news_strength = parse_bias_label(state.last_news_bias)
        if news_bias != "LONG" or news_strength not in {"MEDIUM", "HIGH"}:
            return False
        return price_has_new_extreme_since_exit(
            instrument,
            signal,
            current_price,
            state.last_exit_price,
            min_steps=6,
        )

    group_name = get_instrument_group(instrument.symbol).name
    guarded_symbols = {"GNM6", "USDRUBF", "SRM6", "BRK6", "NGJ6", "IMOEXF", "CNYRUBF", "UCM6", "VBM6"}
    if instrument.symbol not in guarded_symbols and group_name not in {"fx", "equity_index", "equity_futures"}:
        return True, ""
    if not state.last_exit_time:
        return True, ""
    try:
        last_exit_at = datetime.fromisoformat(state.last_exit_time)
    except ValueError:
        return True, ""
    if last_exit_at.tzinfo is None:
        last_exit_at = last_exit_at.replace(tzinfo=UTC)
    try:
        trading_day = datetime.strptime(str(state.trading_day or ""), "%Y-%m-%d").date()
    except ValueError:
        trading_day = None
    if trading_day and last_exit_at.astimezone(MOSCOW_TZ).date() < trading_day:
        return True, ""

    if group_name == "fx" and state.last_exit_side == signal and (state.last_exit_pnl_rub < 0 or exit_reason_has_macd_ema_reversal()):
        min_steps = 1 if instrument.symbol == "CNYRUBF" else 2
        if not strong_same_side_reentry_confirmed(min_steps):
            return (
                False,
                f"для {instrument.symbol} повторный вход после MACD/EMA-выхода разрешён только после нового экстремума цены.",
            )

    if (
        group_name in {"equity_index", "equity_futures"}
        and state.last_exit_side == signal
        and (state.last_exit_pnl_rub < 0 or exit_reason_has_macd_ema_reversal())
        and not strong_same_side_reentry_confirmed(2)
    ):
        return (
            False,
            f"для {instrument.symbol} повторный вход после слабого выхода разрешён только после нового экстремума цены.",
        )

    if (
        instrument.symbol == "NGJ6"
        and state.last_exit_side == signal
        and state.last_exit_pnl_rub > 0
        and "RSI вышел" in state.last_exit_reason
        and not strong_same_side_reentry_confirmed(3)
    ):
        return False, "для NGJ6 повторный вход после RSI-фиксации прибыли разрешён только после нового экстремума."

    cooldown_minutes = 0
    if instrument.symbol == "GNM6":
        if state.last_exit_side == signal == "LONG" and state.last_exit_pnl_rub > 0:
            cooldown_minutes = max(cooldown_minutes, 35)
        if state.last_exit_pnl_rub < 0:
            cooldown_minutes = 45
        if "RSI вышел" in state.last_exit_reason and state.last_exit_side == signal and state.last_exit_pnl_rub > 0:
            cooldown_minutes = max(cooldown_minutes, 45)
        if "Стоп-лосс" in state.last_exit_reason:
            cooldown_minutes = max(cooldown_minutes, 75)
        if "противоположный сигнал" in state.last_exit_reason.lower():
            cooldown_minutes = max(cooldown_minutes, 60)
        if state.last_exit_side and state.last_exit_side != signal:
            cooldown_minutes = max(cooldown_minutes, 75)
    elif instrument.symbol == "USDRUBF":
        if "RSI вышел" in state.last_exit_reason and state.last_exit_side == signal and state.last_exit_pnl_rub > 0:
            cooldown_minutes = max(cooldown_minutes, 25)
        if state.last_exit_pnl_rub < 0:
            cooldown_minutes = 60
        if "Трейлинг-стоп" in state.last_exit_reason or "MACD" in state.last_exit_reason:
            cooldown_minutes = max(cooldown_minutes, 45)
        if state.last_exit_side and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            cooldown_minutes = max(cooldown_minutes, 75)
    elif instrument.symbol == "SRM6":
        if "RSI вышел" in state.last_exit_reason and state.last_exit_side == signal and state.last_exit_pnl_rub > 0:
            cooldown_minutes = max(cooldown_minutes, 25)
        if state.last_exit_pnl_rub < 0:
            cooldown_minutes = 45
        if "противоположный сигнал" in state.last_exit_reason.lower():
            cooldown_minutes = max(cooldown_minutes, 60)
        if state.last_exit_side and state.last_exit_side != signal:
            cooldown_minutes = max(cooldown_minutes, 75)
    elif instrument.symbol == "BRK6":
        if state.last_exit_side == signal and state.last_exit_pnl_rub > 0:
            cooldown_minutes = max(cooldown_minutes, 40)
        if state.last_exit_pnl_rub < 0:
            cooldown_minutes = 45
        if state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            cooldown_minutes = max(cooldown_minutes, 60)
        if "Трейлинг-стоп" in state.last_exit_reason or "Стоп-лосс" in state.last_exit_reason:
            cooldown_minutes = max(cooldown_minutes, 60)
    elif instrument.symbol == "NGJ6":
        if state.last_exit_pnl_rub < 0:
            cooldown_minutes = 60
        if state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            cooldown_minutes = max(cooldown_minutes, 90)
        if "Трейлинг-стоп" in state.last_exit_reason:
            cooldown_minutes = max(cooldown_minutes, 90)
    elif instrument.symbol == "IMOEXF":
        if state.last_exit_pnl_rub < 0:
            cooldown_minutes = 35
        if state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            cooldown_minutes = max(cooldown_minutes, 45)
        if "MACD" in state.last_exit_reason and state.last_exit_side == signal:
            cooldown_minutes = max(cooldown_minutes, 45)

    if cooldown_minutes <= 0:
        if instrument.symbol in {"USDRUBF", "SRM6"} and "RSI вышел" in state.last_exit_reason and state.last_exit_side == signal and state.last_exit_pnl_rub > 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=2):
                return False, f"для {instrument.symbol} повторный вход в ту же сторону разрешён только после нового экстремума после фиксации прибыли."
        if brk6_fresh_impulse_override():
            return True, ""
        if instrument.symbol == "BRK6" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=3):
                return False, "для BRK6 повторный вход после убыточного выхода разрешён только после обновления экстремума."
        if instrument.symbol == "NGJ6" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=4):
                return False, "для NGJ6 повторный вход после убыточного выхода разрешён только после нового экстремума."
        if instrument.symbol == "GNM6" and state.last_exit_side == signal == "LONG" and state.last_exit_pnl_rub > 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=3):
                return False, "для GNM6 повторный LONG после прибыльного выхода разрешён только после нового экстремума."
        if instrument.symbol == "IMOEXF" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=2):
                return False, "для IMOEXF повторный вход после убыточного выхода разрешён только после нового экстремума."
        return True, ""

    next_allowed = last_exit_at + timedelta(minutes=cooldown_minutes)
    now = datetime.now(UTC)
    if brk6_fresh_impulse_override():
        return True, ""
    if now >= next_allowed:
        if instrument.symbol in {"USDRUBF", "SRM6"} and "RSI вышел" in state.last_exit_reason and state.last_exit_side == signal and state.last_exit_pnl_rub > 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=2):
                return False, f"для {instrument.symbol} повторный вход в ту же сторону разрешён только после нового экстремума после фиксации прибыли."
        if instrument.symbol == "BRK6" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=3):
                return False, "для BRK6 повторный вход после убыточного выхода разрешён только после обновления экстремума."
        if instrument.symbol == "NGJ6" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=4):
                return False, "для NGJ6 повторный вход после убыточного выхода разрешён только после нового экстремума."
        if instrument.symbol == "GNM6" and state.last_exit_side == signal == "LONG" and state.last_exit_pnl_rub > 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=3):
                return False, "для GNM6 повторный LONG после прибыльного выхода разрешён только после нового экстремума."
        if instrument.symbol == "IMOEXF" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=2):
                return False, "для IMOEXF повторный вход после убыточного выхода разрешён только после нового экстремума."
        return True, ""

    remaining = int((next_allowed - now).total_seconds() // 60) + 1
    if instrument.symbol == "BRK6" and state.last_exit_side == signal and state.last_exit_pnl_rub > 0:
        return False, f"для BRK6 после прибыльного выхода нужен либо новый сильный импульс по новостям и цене, либо пауза ещё ~{remaining} мин."
    return False, f"для {instrument.symbol} действует cooldown после выхода: ждать ещё ~{remaining} мин."


def sync_pending_order(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
) -> bool:
    if not has_pending_order(state):
        return False
    previous_side = state.position_side
    previous_qty = state.position_qty
    previous_entry_price = state.entry_price
    previous_entry_commission = float(state.entry_commission_rub or 0.0)
    previous_strategy = state.entry_strategy
    previous_exit_reason = state.pending_exit_reason or "Заявка на закрытие подтверждена синхронизацией портфеля"
    pending_action = state.pending_order_action
    previous_entry_time = parse_state_datetime(state.entry_time) if previous_entry_price is not None else None
    pending_submitted_at = parse_state_datetime(state.pending_submitted_at)

    if pending_action == "CLOSE":
        if previous_side == "FLAT" and state.delayed_close_side:
            previous_side = state.delayed_close_side
        if previous_qty <= 0 and state.delayed_close_qty:
            previous_qty = int(state.delayed_close_qty or 0)
        if previous_entry_price is None and state.delayed_close_entry_price is not None:
            previous_entry_price = state.delayed_close_entry_price
        if previous_entry_commission <= 0 and state.delayed_close_entry_commission_rub:
            previous_entry_commission = float(state.delayed_close_entry_commission_rub or 0.0)
        if not previous_strategy and state.delayed_close_strategy:
            previous_strategy = state.delayed_close_strategy
        if (
            previous_exit_reason == "Заявка на закрытие подтверждена синхронизацией портфеля"
            and state.delayed_close_reason
        ):
            previous_exit_reason = state.delayed_close_reason
        if previous_entry_time is None and state.delayed_close_entry_time:
            previous_entry_time = parse_state_datetime(state.delayed_close_entry_time)

    try:
        synced_qty = sync_state_with_portfolio(client, config, instrument, state)
    except Exception as sync_error:
        logging.warning(
            "Не удалось предварительно синхронизировать pending-заявку по %s: %s",
            instrument.symbol,
            sync_error,
        )
        synced_qty = state.position_qty

    if pending_action == "OPEN":
        if confirm_pending_open_from_broker(
            client,
            config,
            instrument,
            state,
            not_before=pending_submitted_at,
        ):
            return False
    elif pending_action == "CLOSE":
        close_not_before = previous_entry_time
        if pending_submitted_at is not None and (
            close_not_before is None or pending_submitted_at > close_not_before
        ):
            close_not_before = pending_submitted_at
        if confirm_pending_close_from_broker(
            client,
            config,
            instrument,
            state,
            previous_side=previous_side,
            previous_qty=previous_qty,
            previous_entry_price=previous_entry_price,
            previous_entry_commission=previous_entry_commission,
            previous_strategy=previous_strategy,
            previous_exit_reason=previous_exit_reason,
            previous_entry_time=previous_entry_time,
            source="portfolio_confirmation",
            recovered_status="confirmed_close",
            not_before=close_not_before,
        ):
            return False

    try:
        order_state = client.orders.get_order_state(
            account_id=config.account_id,
            order_id=state.pending_order_id,
        )
    except RequestError as error:
        if not is_order_not_found_error(error):
            raise

        try:
            synced_qty = sync_state_with_portfolio(client, config, instrument, state)
            close_not_before = previous_entry_time
            if pending_submitted_at is not None and (
                close_not_before is None or pending_submitted_at > close_not_before
            ):
                close_not_before = pending_submitted_at
            if pending_action == "CLOSE" and confirm_pending_close_from_broker(
                client,
                config,
                instrument,
                state,
                previous_side=previous_side,
                previous_qty=previous_qty,
                previous_entry_price=previous_entry_price,
                previous_entry_commission=previous_entry_commission,
                previous_strategy=previous_strategy,
                previous_exit_reason=previous_exit_reason,
                previous_entry_time=previous_entry_time,
                source="pending_order_recovery",
                recovered_status="recovered_close",
                not_before=close_not_before,
                ):
                state.last_error = (
                    f"Статус заявки {state.pending_order_id} не найден у брокера, "
                    "закрытие подтверждено по операциям и портфелю."
                )
                return False
            elif pending_action == "CLOSE" and previous_side != "FLAT":
                return defer_close_recovery_to_broker_ops(
                    instrument,
                    state,
                    previous_side=previous_side,
                    previous_qty=previous_qty,
                    previous_entry_price=previous_entry_price,
                    previous_entry_commission=previous_entry_commission,
                    previous_strategy=previous_strategy,
                    previous_exit_reason=previous_exit_reason,
                    previous_entry_time=previous_entry_time,
                    pending_submitted_at=pending_submitted_at,
                    grace_seconds=BROKER_CLOSE_CONFIRMATION_GRACE_SECONDS,
                )
            elif pending_action == "OPEN" and confirm_pending_open_from_broker(
                client,
                config,
                instrument,
                state,
                not_before=pending_submitted_at,
            ):
                state.execution_status = "recovered_open"
                state.last_error = (
                    f"Статус заявки {state.pending_order_id} не найден у брокера, "
                    "позиция синхронизирована по портфелю."
                )
            elif synced_qty != 0 and state.entry_price is not None:
                refresh_position_snapshot(state, instrument, get_last_price(client, instrument))
                state.execution_status = "recovered_open"
                state.last_error = (
                    f"Статус заявки {state.pending_order_id} не найден у брокера, "
                    "позиция синхронизирована по портфелю."
                )
            else:
                state.execution_status = "rejected"
                state.last_error = (
                    f"Статус заявки {state.pending_order_id} не найден у брокера. "
                    "Подвисшая заявка очищена, открытой позиции нет."
                )
        except Exception as sync_error:
            logging.warning(
                "Не удалось синхронизировать состояние после Order not found по %s: %s",
                instrument.symbol,
                sync_error,
            )
            if pending_action == "CLOSE" and previous_side != "FLAT":
                return defer_close_recovery_to_broker_ops(
                    instrument,
                    state,
                    previous_side=previous_side,
                    previous_qty=previous_qty,
                    previous_entry_price=previous_entry_price,
                    previous_entry_commission=previous_entry_commission,
                    previous_strategy=previous_strategy,
                    previous_exit_reason=previous_exit_reason,
                    previous_entry_time=previous_entry_time,
                    pending_submitted_at=pending_submitted_at,
                    grace_seconds=None,
                )
            state.execution_status = "rejected"
            state.last_error = (
                f"Статус заявки {state.pending_order_id} не найден у брокера. "
                "Заявка очищена без подтверждения позиции."
            )
        state.last_signal_summary = [state.last_error, *state.last_signal_summary[:2]]
        clear_pending_order(state)
        save_state(instrument.symbol, state)
        logging.warning("symbol=%s status=stale_pending_cleared reason=%s", instrument.symbol, state.last_error)
        return False
    status = order_state.execution_report_status

    if status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
        fill_price = quotation_to_float(getattr(order_state, "executed_order_price", None))
        if fill_price <= 0:
            fill_price = quotation_to_float(getattr(order_state, "average_position_price", None))
        if fill_price <= 0:
            fill_price = get_last_price(client, instrument)
        filled_qty = int(getattr(order_state, "lots_executed", 0) or 0) or state.pending_order_qty
        fill_commission_rub = extract_order_commission_rub(order_state)
        state.last_fill_price = fill_price

        if state.pending_order_action == "OPEN":
            state.entry_price = fill_price
            state.entry_commission_rub = fill_commission_rub
            state.entry_commission_accounted = True
            state.max_price = fill_price
            state.min_price = fill_price
            state.position_qty = filled_qty
            state.position_side = state.pending_order_side
            state.breakeven_armed = False
            state.entry_time = datetime.now(UTC).isoformat()
            state.entry_reason = compact_reason(state.pending_entry_reason or "Сделка исполнена по рыночному входу")
            state.execution_status = "confirmed_open"
            reset_daily_pnl_if_needed(state)
            state.realized_commission_rub += fill_commission_rub
            state.realized_pnl -= fill_commission_rub
            refresh_position_snapshot(state, instrument, fill_price)
            append_trade_journal(
                instrument,
                "OPEN",
                state.position_side,
                filled_qty,
                fill_price,
                gross_pnl_rub=0.0,
                commission_rub=fill_commission_rub,
                net_pnl_rub=-fill_commission_rub,
                reason=state.entry_reason,
                source="order_fill",
                strategy=state.entry_strategy,
                dry_run=False,
                state=state,
            )
        elif state.pending_order_action == "CLOSE":
            gross_pnl = 0.0
            if state.entry_price is not None:
                gross_pnl = calculate_futures_pnl_rub(
                    instrument,
                    state.entry_price,
                    fill_price,
                    state.position_qty,
                    state.position_side,
                )
            reset_daily_pnl_if_needed(state)
            total_trade_commission = float(state.entry_commission_rub or 0.0) + fill_commission_rub
            net_trade_pnl = gross_pnl - total_trade_commission
            state.realized_gross_pnl_rub += gross_pnl
            state.realized_commission_rub += fill_commission_rub
            state.realized_pnl += gross_pnl - fill_commission_rub
            exit_reason = state.pending_exit_reason or "Заявка на закрытие исполнена"
            append_trade_journal(
                instrument,
                "CLOSE",
                state.position_side,
                state.position_qty,
                fill_price,
                pnl_rub=net_trade_pnl,
                gross_pnl_rub=gross_pnl,
                commission_rub=total_trade_commission,
                net_pnl_rub=net_trade_pnl,
                reason=exit_reason,
                source="order_fill",
                strategy=state.entry_strategy,
                dry_run=False,
                state=state,
            )
            state.last_exit_time = datetime.now(UTC).isoformat()
            state.last_exit_side = state.position_side
            state.last_exit_reason = exit_reason
            state.last_exit_pnl_rub = net_trade_pnl
            state.last_exit_price = fill_price
            state.execution_status = "confirmed_close"
            state.position_notional_rub = 0.0
            state.position_variation_margin_rub = 0.0
            state.position_pnl_pct = 0.0
            state.entry_price = None
            state.entry_commission_rub = 0.0
            state.entry_commission_accounted = False
            state.max_price = None
            state.min_price = None
            state.position_qty = 0
            state.position_side = "FLAT"
            state.breakeven_armed = False
            state.entry_time = ""
            state.entry_strategy = ""
            state.entry_reason = ""

        clear_pending_order(state)
        save_state(instrument.symbol, state)
        return False

    if status in {
        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED,
        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED,
    }:
        rejection_reason = summarize_pending_order_rejection(client, config, instrument, state)
        state.execution_status = "rejected"
        state.last_error = rejection_reason
        state.last_signal_summary = [rejection_reason, *state.last_signal_summary[:2]]
        clear_pending_order(state)
        save_state(instrument.symbol, state)
        return False

    if status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL:
        executed = int(getattr(order_state, "lots_executed", 0) or 0)
        state.execution_status = "partial_fill"
        partial_message = (
            f"Заявка {state.pending_order_id} исполнена частично: "
            f"{executed}/{state.pending_order_qty}. Ждём финальный статус брокера."
        )
        should_notify_partial = state.last_error != partial_message
        state.last_error = partial_message
        state.last_signal_summary = [partial_message, *state.last_signal_summary[:2]]
        try:
            synced_qty = sync_state_with_portfolio(client, config, instrument, state)
            if synced_qty != 0 and state.entry_price is not None:
                refresh_position_snapshot(state, instrument, get_last_price(client, instrument))
        except Exception as error:
            logging.warning(
                "Не удалось синхронизировать частичное исполнение по %s: %s",
                instrument.symbol,
                error,
            )
        save_state(instrument.symbol, state)
        return True

    return True


def place_market_order(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    quantity: int,
    direction: OrderDirection,
) -> str:
    if not config.allow_orders:
        raise RuntimeError("Отправка ордеров запрещена: установи OIL_ALLOW_ORDERS=true только когда будешь готов.")
    order_id = str(uuid4())
    client.orders.post_order(
        figi=instrument.figi,
        quantity=quantity,
        price=None,
        direction=direction,
        account_id=config.account_id,
        order_type=OrderType.ORDER_TYPE_MARKET,
        order_id=order_id,
    )
    return order_id


def open_position(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    signal: str,
    strategy_name: str = "",
    entry_reason: str = "",
) -> None:
    if state.position_qty > 0 or has_pending_order(state):
        return
    session_name = get_market_session()
    if not session_allows_new_entries(session_name, instrument.symbol):
        session_block_reason = f"Новые входы заблокированы для сессии {session_name}."
        state.last_error = session_block_reason
        state.last_signal_summary = [session_block_reason, *state.last_signal_summary[:2]]
        state.last_allocator_summary = f"Аллокатор заблокирован: {session_block_reason}"
        state.last_allocator_quantity = 0
        save_state(instrument.symbol, state)
        logging.info(
            "symbol=%s session=%s status=entry_blocked_session",
            instrument.symbol,
            session_name,
        )
        return
    risk_block_reason = get_global_daily_loss_block_reason(client, config)
    if risk_block_reason:
        mark_daily_risk_stop_if_needed(state)
        state.last_error = risk_block_reason
        state.last_signal_summary = [risk_block_reason, *state.last_signal_summary[:2]]
        state.last_allocator_summary = f"Аллокатор заблокирован: {risk_block_reason}"
        state.last_allocator_quantity = 0
        save_state(instrument.symbol, state)
        logging.warning("symbol=%s status=entry_blocked_daily_loss reason=%s", instrument.symbol, risk_block_reason)
        return
    performance_block_reason = recent_strategy_performance_block_reason(instrument.symbol, strategy_name)
    if performance_block_reason:
        state.last_error = performance_block_reason
        state.last_signal_summary = [performance_block_reason, *state.last_signal_summary[:2]]
        state.last_allocator_summary = f"Аллокатор заблокирован: {performance_block_reason}"
        state.last_allocator_quantity = 0
        save_state(instrument.symbol, state)
        logging.warning(
            "symbol=%s strategy=%s status=entry_blocked_performance reason=%s",
            instrument.symbol,
            strategy_name,
            performance_block_reason,
        )
        return
    price = get_last_price(client, instrument)
    quantity = calculate_order_quantity(client, config, instrument, state, price, signal, strategy_name)
    if quantity <= 0:
        block_reason = describe_capacity_block_reason(client, config, instrument, state, price, signal, strategy_name)
        state.last_error = block_reason
        state.last_signal_summary = [block_reason, *state.last_signal_summary[:2]]
        save_state(instrument.symbol, state)
        logging.info("symbol=%s status=entry_blocked reason=%s", instrument.symbol, block_reason)
        return
    sizing_lines = build_position_sizing_lines(client, config, instrument, state, price, signal, quantity, strategy_name)
    try:
        allocator_sizing = calculate_position_sizing_context(
            client,
            config,
            instrument,
            state,
            price,
            signal,
            strategy_name,
        )
        state.last_entry_allocator_quantity = quantity
        state.last_entry_allocator_summary = build_allocator_summary_text(allocator_sizing)
        state.last_entry_allocator_time = datetime.now(UTC).isoformat()
    except Exception:
        state.last_entry_allocator_quantity = quantity
        state.last_entry_allocator_summary = f"Последний вход: {quantity} лот(а), подробный расчёт аллокатора недоступен."
        state.last_entry_allocator_time = datetime.now(UTC).isoformat()
    side = "LONG" if signal == "LONG" else "SHORT"
    direction = (
        OrderDirection.ORDER_DIRECTION_BUY
        if signal == "LONG"
        else OrderDirection.ORDER_DIRECTION_SELL
    )
    if config.dry_run:
        state.entry_price = price
        state.entry_commission_rub = 0.0
        state.entry_commission_accounted = False
        state.max_price = price
        state.min_price = price
        state.position_qty = quantity
        state.position_side = side
        state.breakeven_armed = False
        state.entry_time = datetime.now(UTC).isoformat()
        state.entry_strategy = strategy_name
        state.entry_reason = compact_reason(entry_reason or "Тестовый вход по стратегии.")
        state.execution_status = "confirmed_open"
        save_state(instrument.symbol, state)
        append_trade_journal(
            instrument,
            "OPEN",
            side,
            quantity,
            price,
            gross_pnl_rub=0.0,
            commission_rub=0.0,
            net_pnl_rub=0.0,
            reason=state.entry_reason,
            source="dry_run",
            strategy=strategy_name,
            dry_run=True,
            state=state,
        )
        return

    try:
        order_id = place_market_order(client, config, instrument, quantity, direction)
    except RequestError as error:
        request_reason = summarize_order_request_error(instrument, error)
        state.execution_status = "rejected"
        state.last_error = request_reason
        state.last_signal_summary = [request_reason, *state.last_signal_summary[:2]]
        save_state(instrument.symbol, state)
        logging.warning("symbol=%s status=order_rejected reason=%s", instrument.symbol, request_reason)
        return
    state.entry_strategy = strategy_name
    state.pending_entry_reason = compact_reason(entry_reason)
    state.pending_order_id = order_id
    state.pending_order_action = "OPEN"
    state.pending_order_side = side
    state.pending_order_qty = quantity
    state.pending_submitted_at = datetime.now(UTC).isoformat()
    state.pending_exit_reason = ""
    state.execution_status = "submitted_open"
    save_state(instrument.symbol, state)
    logging.info("symbol=%s status=open_submitted order_id=%s", instrument.symbol, order_id)


def close_position(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    exit_reason: str,
) -> None:
    if state.position_qty <= 0 or state.position_side == "FLAT" or has_pending_order(state):
        return
    price = get_last_price(client, instrument)
    qty = state.position_qty
    direction = (
        OrderDirection.ORDER_DIRECTION_SELL
        if state.position_side == "LONG"
        else OrderDirection.ORDER_DIRECTION_BUY
    )
    if config.dry_run:
        pnl = 0.0
        if state.entry_price is not None:
            pnl = calculate_futures_pnl_rub(
                instrument,
                state.entry_price,
                price,
                qty,
                state.position_side,
            )
        reset_daily_pnl_if_needed(state)
        state.realized_gross_pnl_rub += pnl
        state.realized_pnl += pnl
        append_trade_journal(
            instrument,
            "CLOSE",
            state.position_side,
            qty,
            price,
            pnl_rub=pnl,
            gross_pnl_rub=pnl,
            commission_rub=0.0,
            net_pnl_rub=pnl,
            reason=exit_reason,
            source="dry_run",
            strategy=state.entry_strategy,
            dry_run=True,
            state=state,
        )
        state.last_exit_time = datetime.now(UTC).isoformat()
        state.last_exit_side = state.position_side
        state.last_exit_reason = exit_reason
        state.last_exit_pnl_rub = pnl
        state.last_exit_price = price
        state.execution_status = "confirmed_close"
        state.entry_price = None
        state.entry_commission_rub = 0.0
        state.entry_commission_accounted = False
        state.max_price = None
        state.min_price = None
        state.position_qty = 0
        state.position_side = "FLAT"
        state.breakeven_armed = False
        state.entry_time = ""
        state.entry_strategy = ""
        state.entry_reason = ""
        state.position_notional_rub = 0.0
        state.position_variation_margin_rub = 0.0
        state.position_pnl_pct = 0.0
        state.execution_status = "idle"
        save_state(instrument.symbol, state)
        return

    try:
        order_id = place_market_order(client, config, instrument, qty, direction)
    except RequestError as error:
        request_reason = summarize_order_request_error(instrument, error)
        state.execution_status = "rejected"
        state.last_error = request_reason
        state.last_signal_summary = [request_reason, *state.last_signal_summary[:2]]
        save_state(instrument.symbol, state)
        logging.warning("symbol=%s status=close_rejected reason=%s", instrument.symbol, request_reason)
        return
    state.pending_order_id = order_id
    state.pending_order_action = "CLOSE"
    state.pending_order_side = state.position_side
    state.pending_order_qty = qty
    state.pending_submitted_at = datetime.now(UTC).isoformat()
    state.pending_exit_reason = exit_reason
    # Снимок позиции на момент отправки CLOSE нужен, потому что live sync может
    # увидеть FLAT раньше, чем брокерская операция появится в истории.
    state.delayed_close_side = state.position_side
    state.delayed_close_qty = qty
    state.delayed_close_entry_price = state.entry_price
    state.delayed_close_entry_commission_rub = float(state.entry_commission_rub or 0.0)
    state.delayed_close_strategy = state.entry_strategy
    state.delayed_close_reason = exit_reason
    state.delayed_close_entry_time = state.entry_time or ""
    state.delayed_close_submitted_at = state.pending_submitted_at
    state.execution_status = "submitted_close"
    save_state(instrument.symbol, state)
    logging.info("symbol=%s status=close_submitted order_id=%s", instrument.symbol, order_id)


def check_exit(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
    df: pd.DataFrame,
    fresh_signal: str,
    higher_tf_df: pd.DataFrame | None = None,
) -> None:
    if state.position_qty <= 0 or state.position_side == "FLAT" or state.entry_price is None:
        return

    price = get_last_price(client, instrument)
    if not state.entry_time:
        state.entry_time = datetime.now(UTC).isoformat()
    state.max_price = max(state.max_price or price, price)
    state.min_price = min(state.min_price or price, price)
    exit_df = select_exit_indicator_df(instrument, df, higher_tf_df)
    last = exit_df.iloc[-1]
    profile = get_strategy_profile(config, instrument)
    exit_profile = get_exit_profile(config, state.entry_strategy)
    if get_instrument_group(instrument.symbol).name == "fx":
        exit_profile = ExitProfile(
            min_hold_minutes=min(exit_profile.min_hold_minutes, 15),
            breakeven_profit_pct=min(exit_profile.breakeven_profit_pct, 0.0035),
            trailing_stop_pct=min(exit_profile.trailing_stop_pct, 0.0035),
        )
    if instrument.symbol == "NGJ6":
        exit_profile = ExitProfile(
            min_hold_minutes=max(exit_profile.min_hold_minutes, 30),
            breakeven_profit_pct=max(exit_profile.breakeven_profit_pct, 0.0060),
            trailing_stop_pct=max(exit_profile.trailing_stop_pct, 0.0080),
        )
    if instrument.symbol == "GNM6":
        exit_profile = ExitProfile(
            min_hold_minutes=max(exit_profile.min_hold_minutes, 30),
            breakeven_profit_pct=max(exit_profile.breakeven_profit_pct, 0.0060),
            trailing_stop_pct=max(exit_profile.trailing_stop_pct, 0.0070),
        )
    if instrument.symbol == "VBM6":
        exit_profile = ExitProfile(
            min_hold_minutes=max(exit_profile.min_hold_minutes, 25),
            breakeven_profit_pct=max(exit_profile.breakeven_profit_pct, 0.0050),
            trailing_stop_pct=max(exit_profile.trailing_stop_pct, 0.0070),
        )
    exit_profile, adaptive_exit_reason = get_adaptive_exit_profile(config, instrument, state, exit_profile)
    if adaptive_exit_reason:
        logging.info(
            "symbol=%s strategy=%s adaptive_exit_profile=%s",
            instrument.symbol,
            state.entry_strategy,
            adaptive_exit_reason,
        )
    is_trend_rollover = state.entry_strategy == "trend_rollover"
    prev = exit_df.iloc[-2]
    prev2 = exit_df.iloc[-3]
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    prev2_macd = float(prev2["macd"])
    prev2_macd_signal = float(prev2["macd_signal"])
    rsi = float(last["rsi"])
    ema20 = float(last["ema20"])
    prev_close = float(prev["close"])
    close = float(last["close"])

    if state.position_side == "LONG":
        profit_pct = (price - state.entry_price) / state.entry_price
        if profit_pct >= exit_profile.breakeven_profit_pct:
            state.breakeven_armed = True
        stop_price = state.entry_price * (1 - config.stop_loss_pct)
        if state.breakeven_armed:
            stop_price = max(stop_price, state.entry_price)
        trailing_price = (state.max_price or price) * (1 - exit_profile.trailing_stop_pct)
        if (instrument.symbol in {"GNM6", "NGJ6", "RBM6", "SRM6", "VBM6", "IMOEXF"} or get_instrument_group(instrument.symbol).name == "fx") and higher_tf_df is not None:
            macd_down = macd_crossed_down_with_ema_loss(exit_df)
        else:
            macd_down = (
                prev2_macd >= prev2_macd_signal
                and prev_macd < prev_macd_signal
                and macd < macd_signal
                and close < ema20
            )
        opposite_signal_confirmed = fresh_signal == "SHORT" and close < ema20 and close <= prev_close
        min_hold_passed = position_held_long_enough(state, config, exit_profile.min_hold_minutes)
        profit_lock_reason = build_profit_lock_exit_reason(instrument, state, price)
        if instrument.symbol == "RBM6" and higher_tf_df is not None:
            profit_lock_reason = profit_lock_reason or rbm6_sideways_exhaustion_exit_reason(instrument, state, exit_df, price)
        if price <= stop_price:
            close_position(client, config, instrument, state, f"Стоп-лосс: цена {price:.4f} <= {stop_price:.4f}")
        elif min_hold_passed and profit_lock_reason:
            close_position(client, config, instrument, state, profit_lock_reason)
        elif (
            instrument.symbol not in {"GNM6", "NGJ6", "RBM6", "SRM6", "VBM6", "IMOEXF"}
            and get_instrument_group(instrument.symbol).name != "fx"
            or state.breakeven_armed
        ) and price <= trailing_price:
            close_position(client, config, instrument, state, f"Трейлинг-стоп: цена {price:.4f} <= {trailing_price:.4f}")
        elif min_hold_passed and state.breakeven_armed and rsi >= profile.rsi_exit_long and not is_trend_rollover:
            close_position(client, config, instrument, state, f"RSI вышел в зону перегрева: {rsi:.2f} >= {profile.rsi_exit_long:.2f}")
        elif min_hold_passed and macd_down:
            close_position(client, config, instrument, state, "MACD подтверждённо развернулся вниз и цена потеряла EMA20")
        elif min_hold_passed and opposite_signal_confirmed:
            close_position(client, config, instrument, state, "Появился подтверждённый противоположный сигнал SHORT")
    else:
        profit_pct = (state.entry_price - price) / state.entry_price
        if profit_pct >= exit_profile.breakeven_profit_pct:
            state.breakeven_armed = True
        stop_price = state.entry_price * (1 + config.stop_loss_pct)
        if state.breakeven_armed:
            stop_price = min(stop_price, state.entry_price)
        trailing_price = (state.min_price or price) * (1 + exit_profile.trailing_stop_pct)
        if (instrument.symbol in {"GNM6", "NGJ6", "RBM6", "SRM6", "VBM6", "IMOEXF"} or get_instrument_group(instrument.symbol).name == "fx") and higher_tf_df is not None:
            macd_up = macd_crossed_up_with_ema_reclaim(exit_df)
        else:
            macd_up = (
                prev2_macd <= prev2_macd_signal
                and prev_macd > prev_macd_signal
                and macd > macd_signal
                and close > ema20
            )
        opposite_signal_confirmed = fresh_signal == "LONG" and close > ema20 and close >= prev_close
        min_hold_passed = position_held_long_enough(state, config, exit_profile.min_hold_minutes)
        profit_lock_reason = build_profit_lock_exit_reason(instrument, state, price)
        if instrument.symbol == "RBM6" and higher_tf_df is not None:
            profit_lock_reason = profit_lock_reason or rbm6_sideways_exhaustion_exit_reason(instrument, state, exit_df, price)
        if price >= stop_price:
            close_position(client, config, instrument, state, f"Стоп-лосс: цена {price:.4f} >= {stop_price:.4f}")
        elif min_hold_passed and profit_lock_reason:
            close_position(client, config, instrument, state, profit_lock_reason)
        elif (
            instrument.symbol not in {"GNM6", "NGJ6", "RBM6", "SRM6", "VBM6", "IMOEXF"}
            and get_instrument_group(instrument.symbol).name != "fx"
            or state.breakeven_armed
        ) and price >= trailing_price:
            close_position(client, config, instrument, state, f"Трейлинг-стоп: цена {price:.4f} >= {trailing_price:.4f}")
        elif (
            min_hold_passed
            and state.breakeven_armed
            and rsi <= profile.rsi_exit_short
            and not is_trend_rollover
            and (
                instrument.symbol not in {"VBM6", "USDRUBF"}
                or macd_up
                or opposite_signal_confirmed
                or close > ema20
            )
        ):
            close_position(client, config, instrument, state, f"RSI вышел в зону перепроданности: {rsi:.2f} <= {profile.rsi_exit_short:.2f}")
        elif min_hold_passed and macd_up:
            close_position(client, config, instrument, state, "MACD подтверждённо развернулся вверх и цена вернулась выше EMA20")
        elif min_hold_passed and opposite_signal_confirmed:
            close_position(client, config, instrument, state, "Появился подтверждённый противоположный сигнал LONG")


def process_instrument(client: Client, config: BotConfig, instrument: InstrumentConfig) -> None:
    state = load_state(instrument.symbol)
    reconcile_state_accounting(instrument.symbol, state)
    if not config.dry_run:
        reconcile_delayed_close_from_broker(client, config, instrument, state)
        state = load_state(instrument.symbol)
    if not config.dry_run and sync_pending_order(client, config, instrument, state):
        return
    session_name = get_market_session()
    if session_name == "CLOSED":
        closed_message = "Вне торговой сессии срочного рынка Мосбиржи."
        if state.last_error != closed_message or state.last_signal != "HOLD":
            state.last_error = closed_message
            state.last_signal = "HOLD"
            state.last_news_impact = "торговая сессия закрыта"
            save_state(instrument.symbol, state)
            logging.info("symbol=%s status=session_closed", instrument.symbol)
        else:
            state.last_error = closed_message
        return
    if session_name == "WEEKEND" and is_currency_symbol(instrument.symbol):
        weekend_message = "Выходной день: валютный фьючерс не торгуется."
        if state.position_side != "FLAT" or has_pending_order(state):
            state.last_signal_summary = [weekend_message, *state.last_signal_summary[:2]]
        elif state.last_error != weekend_message or state.last_signal != "HOLD":
            state.last_error = weekend_message
            state.last_signal = "HOLD"
            state.last_news_impact = "инструмент недоступен на выходных"
            save_state(instrument.symbol, state)
            logging.info("symbol=%s status=weekend_currency_closed", instrument.symbol)
        else:
            state.last_error = weekend_message
        if state.position_side == "FLAT" and not has_pending_order(state):
            state.last_signal = "HOLD"
            return

    try:
        lower_df = add_indicators(
            get_candles(
                client,
                config,
                instrument,
                config.candle_interval,
                lookback_hours=get_lower_tf_lookback_hours(config, instrument.symbol),
            )
        )
    except RuntimeError as error:
        if "Недостаточно данных для стратегии" in str(error) or "API не вернул свечи" in str(error):
            state.last_error = str(error)
            save_state(instrument.symbol, state)
            logging.info("symbol=%s status=waiting_for_candles", instrument.symbol)
            return
        raise

    higher_tf_df: pd.DataFrame | None = None
    if instrument.symbol in {"GNM6", "NGJ6", "RBM6", "SRM6", "VBM6", "IMOEXF"} or get_instrument_group(instrument.symbol).name == "fx":
        try:
            higher_tf_df = get_configured_higher_tf_df(client, config, instrument)
        except RuntimeError as error:
            logging.info("symbol=%s status=waiting_for_higher_tf_exit_context reason=%s", instrument.symbol, error)

    higher_tf_bias = get_higher_tf_bias(client, config, instrument)
    signal, reason, primary_strategy_name = evaluate_signal(lower_df, config, instrument, higher_tf_bias)
    news_bias = get_active_news_biases().get(instrument.symbol)
    signal, reason = apply_news_bias_to_signal(signal, reason, news_bias)
    signal_summary = summarize_signal_reason(signal, reason)
    compare_lines: list[str] = []
    secondary_strategies = set(get_secondary_strategies(instrument.symbol))
    compare_lines.append(f"Основная: {signal_emoji(signal)} {signal} ({primary_strategy_name})")
    compare_lines.append(f"News bias: {format_news_bias_label(news_bias)}")
    if "williams" in secondary_strategies:
        try:
            williams_df = add_williams_indicators(
                get_candles(
                    client,
                    config,
                    instrument,
                    config.candle_interval,
                    lookback_hours=get_lower_tf_lookback_hours(config, instrument.symbol),
                )
            )
        except RuntimeError as error:
            if "Недостаточно данных для стратегии Williams" in str(error) or "API не вернул свечи" in str(error):
                williams_df = None
            else:
                raise
        if williams_df is not None:
            williams_signal, williams_reason = evaluate_williams_currency_signal(williams_df, higher_tf_bias)
            compare_lines.extend(
                [
                f"Тест Williams: {signal_emoji(williams_signal)} {williams_signal}",
                compact_reason(williams_reason),
                ]
            )
    current_price = float(lower_df.iloc[-1]["close"])
    refresh_position_snapshot(state, instrument, current_price)
    candle_time_value = lower_df.iloc[-1].get("time")
    candle_time = (
        candle_time_value.tz_convert("Europe/Moscow").strftime("%Y-%m-%d %H:%M")
        if hasattr(candle_time_value, "tz_convert")
        else str(candle_time_value)
    )
    signal_changed = signal != state.last_signal

    notify_signal_change(config, instrument, state, signal, current_price, reason, news_bias)

    if not config.dry_run:
        sync_state_with_portfolio(client, config, instrument, state)
        if has_pending_order(state):
            reconcile_state_accounting(instrument.symbol, state)
            save_state(instrument.symbol, state)
            return

    notify_periodic_status(
        config,
        instrument,
        state,
        signal,
        current_price,
        reason,
        candle_time,
        higher_tf_bias,
        lower_df,
        news_bias,
        compare_lines,
    )

    state.last_error = ""
    state.last_signal = signal
    state.last_strategy_name = primary_strategy_name
    state.last_higher_tf_bias = higher_tf_bias
    state.last_news_bias = format_news_bias_label(news_bias)
    state.last_news_impact = describe_news_bias_impact(signal, news_bias)
    market_regime, regime_metrics = classify_market_regime(lower_df, higher_tf_bias)
    setup_quality_score, setup_quality_label = estimate_setup_quality(
        signal,
        higher_tf_bias,
        market_regime,
        regime_metrics,
        news_bias,
    )
    state.last_market_regime = market_regime
    state.last_setup_quality_score = setup_quality_score
    state.last_setup_quality_label = setup_quality_label
    state.last_volume_ratio = float(regime_metrics.get("volume_ratio") or 0.0)
    state.last_body_ratio = float(regime_metrics.get("body_ratio") or 0.0)
    state.last_atr_pct = float(regime_metrics.get("atr_pct") or 0.0)
    state.last_range_width_pct = float(regime_metrics.get("range_width_pct") or 0.0)
    state.last_signal_summary = signal_summary
    state.last_allocator_quantity = 0
    if signal not in {"LONG", "SHORT"}:
        state.last_allocator_summary = "Аллокатор не активен: сейчас нет сигнала на вход."
    elif state.position_side != "FLAT":
        last_entry_hint = ""
        if state.last_entry_allocator_summary:
            last_entry_hint = f" Последний вход: {state.last_entry_allocator_summary}"
        elif state.entry_time:
            last_entry_hint = " Последний вход был открыт до включения расширенной диагностики аллокатора."
        state.last_allocator_summary = (
            f"Аллокатор не активен: по инструменту уже открыта позиция {state.position_side} "
            f"{state.position_qty} лот(а).{last_entry_hint}"
        )
    elif has_pending_order(state):
        pending_action = state.pending_order_action or "UNKNOWN"
        pending_side = state.pending_order_side or "UNKNOWN"
        state.last_allocator_summary = (
            f"Аллокатор ждёт завершения заявки: действие {pending_action}, "
            f"направление {pending_side}, лотов {state.pending_order_qty}."
        )
    elif not session_allows_new_entries(session_name, instrument.symbol):
        state.last_allocator_summary = f"Аллокатор заблокирован: новые входы недоступны для сессии {session_name}."
    else:
        try:
            daily_loss_block_reason = get_global_daily_loss_block_reason(client, config)
            if daily_loss_block_reason:
                mark_daily_risk_stop_if_needed(state)
                state.last_allocator_quantity = 0
                state.last_allocator_summary = f"Аллокатор заблокирован: {daily_loss_block_reason}"
            else:
                performance_block_reason = recent_strategy_performance_block_reason(
                    instrument.symbol,
                    primary_strategy_name,
                )
                if performance_block_reason:
                    state.last_allocator_quantity = 0
                    state.last_allocator_summary = f"Аллокатор заблокирован: {performance_block_reason}"
                else:
                    recovery_block_reason = recovery_mode_block_reason(
                        state,
                        instrument.symbol,
                        primary_strategy_name,
                        signal,
                    )
                    if recovery_block_reason:
                        state.last_allocator_quantity = 0
                        state.last_allocator_summary = f"Аллокатор в recovery mode: {recovery_block_reason}"
                    else:
                        regime_block_reason = regime_entry_block_reason(
                            primary_strategy_name,
                            signal,
                            market_regime,
                            regime_metrics,
                        )
                        if regime_block_reason:
                            state.last_allocator_quantity = 0
                            state.last_allocator_summary = f"Аллокатор заблокирован: {regime_block_reason}"
                        else:
                            allocator_sizing = calculate_position_sizing_context(
                                client,
                                config,
                                instrument,
                                state,
                                current_price,
                                signal,
                                primary_strategy_name,
                            )
                            state.last_allocator_quantity = int(allocator_sizing.get("quantity") or 0)
                            state.last_allocator_summary = build_allocator_summary_text(allocator_sizing)
        except Exception as error:
            state.last_allocator_summary = f"Аллокатор временно недоступен: {error}"
            logging.info("symbol=%s allocator_summary_error=%s", instrument.symbol, error)

    if signal_changed:
        logging.info("symbol=%s signal=%s side=%s qty=%s", instrument.symbol, signal, state.position_side, state.position_qty)

    if state.position_side == "FLAT":
        if signal in {"LONG", "SHORT"} and session_allows_new_entries(session_name, instrument.symbol) and session_signal_quality_ok(lower_df, signal, session_name, instrument.symbol):
            daily_loss_block_reason = get_global_daily_loss_block_reason(client, config)
            if daily_loss_block_reason:
                mark_daily_risk_stop_if_needed(state)
                state.last_error = daily_loss_block_reason
                state.last_signal_summary = [daily_loss_block_reason, *state.last_signal_summary[:2]]
                logging.warning(
                    "symbol=%s status=entry_blocked_daily_loss reason=%s",
                    instrument.symbol,
                    daily_loss_block_reason,
                )
            else:
                performance_block_reason = recent_strategy_performance_block_reason(
                    instrument.symbol,
                    primary_strategy_name,
                )
                if performance_block_reason:
                    state.last_error = performance_block_reason
                    state.last_signal_summary = [performance_block_reason, *state.last_signal_summary[:2]]
                    logging.warning(
                        "symbol=%s strategy=%s status=entry_blocked_performance reason=%s",
                        instrument.symbol,
                        primary_strategy_name,
                        performance_block_reason,
                    )
                else:
                    reentry_allowed, reentry_reason = position_reentry_allowed(state, instrument, signal, current_price)
                    if reentry_allowed:
                        recovery_block_reason = recovery_mode_block_reason(
                            state,
                            instrument.symbol,
                            primary_strategy_name,
                            signal,
                        )
                        if recovery_block_reason:
                            state.last_error = recovery_block_reason
                            state.last_signal_summary = [recovery_block_reason, *state.last_signal_summary[:2]]
                            logging.info(
                                "symbol=%s strategy=%s status=entry_blocked_recovery reason=%s",
                                instrument.symbol,
                                primary_strategy_name,
                                recovery_block_reason,
                            )
                        else:
                            regime_block_reason = regime_entry_block_reason(
                                primary_strategy_name,
                                signal,
                                market_regime,
                                regime_metrics,
                            )
                            if regime_block_reason:
                                state.last_error = regime_block_reason
                                state.last_signal_summary = [regime_block_reason, *state.last_signal_summary[:2]]
                                logging.info("symbol=%s strategy=%s status=entry_blocked_regime reason=%s", instrument.symbol, primary_strategy_name, regime_block_reason)
                            else:
                                open_position(client, config, instrument, state, signal, primary_strategy_name, reason)
                    else:
                        logging.info("symbol=%s status=reentry_cooldown reason=%s", instrument.symbol, reentry_reason)
                        state.last_signal_summary = [reentry_reason, *state.last_signal_summary[:2]]
    else:
        check_exit(client, config, instrument, state, lower_df, signal, higher_tf_df=higher_tf_df)

    reconcile_state_accounting(instrument.symbol, state)
    save_state(instrument.symbol, state)


def run_bot() -> int:
    setup_logging()
    config = load_config()
    if not config.dry_run and not config.allow_orders:
        raise RuntimeError("LIVE-режим заблокирован: сначала включи OIL_ALLOW_ORDERS=true осознанно.")

    mode = "DRY_RUN" if config.dry_run else "LIVE"
    started_at = datetime.now(timezone.utc)
    last_cycle_at: datetime | None = None
    save_runtime_status(
        build_runtime_status_payload(
            mode=mode,
            session_name=get_market_session(),
            started_at=started_at,
            cycle_count=0,
            consecutive_errors=0,
            state="starting",
        )
    )
    consecutive_errors = 0
    cycle_count = 0
    startup_error_notified = False
    while True:
        try:
            with Client(config.token, app_name=APP_NAME, target=config.target) as client:
                watchlist = resolve_instruments(client, config)
                logging.info("watchlist_resolved symbols=%s", [item.symbol for item in watchlist])
                watchlist_refresh_at = time.monotonic()
                startup_error_notified = False
                while True:
                    try:
                        watchlist, watchlist_refresh_at = refresh_watchlist_if_needed(
                            client,
                            config,
                            watchlist,
                            watchlist_refresh_at,
                        )
                        maybe_refresh_news_snapshot()
                        for instrument in watchlist:
                            process_instrument(client, config, instrument)
                        recovered_closes = reconcile_missing_trade_closes_from_broker(client, config, watchlist)
                        if recovered_closes:
                            logging.warning("journal_auto_recovery_applied recovered_closes=%s", recovered_closes)
                        maybe_refresh_portfolio_snapshot(client, config, watchlist)
                        maybe_send_hourly_summary(client, config, watchlist)
                        consecutive_errors = 0
                        cycle_count += 1
                        last_cycle_at = datetime.now(timezone.utc)
                        save_runtime_status(
                            build_runtime_status_payload(
                                mode=mode,
                                session_name=get_market_session(),
                                started_at=started_at,
                                cycle_count=cycle_count,
                                consecutive_errors=0,
                                state="running",
                                last_cycle_at=last_cycle_at,
                            )
                        )
                        if config.max_cycles > 0 and cycle_count >= config.max_cycles:
                            return 0
                        time.sleep(config.poll_seconds)
                    except RequestError as error:
                        consecutive_errors += 1
                        logging.exception("Ошибка API T-Invest")
                        save_runtime_status(
                            build_runtime_status_payload(
                                mode=mode,
                                session_name=get_market_session(),
                                started_at=started_at,
                                cycle_count=cycle_count,
                                consecutive_errors=consecutive_errors,
                                state="api_error",
                                last_cycle_at=last_cycle_at,
                                last_error=str(error),
                            )
                        )
                    except Exception as error:
                        consecutive_errors += 1
                        logging.exception("Внутренняя ошибка бота")
                        save_runtime_status(
                            build_runtime_status_payload(
                                mode=mode,
                                session_name=get_market_session(),
                                started_at=started_at,
                                cycle_count=cycle_count,
                                consecutive_errors=consecutive_errors,
                                state="internal_error",
                                last_cycle_at=last_cycle_at,
                                last_error=str(error),
                            )
                        )
                    if consecutive_errors >= config.max_consecutive_errors:
                        save_runtime_status(
                            build_runtime_status_payload(
                                mode=mode,
                                session_name=get_market_session(),
                                started_at=started_at,
                                cycle_count=cycle_count,
                                consecutive_errors=consecutive_errors,
                                state="stopped_after_errors",
                                last_cycle_at=last_cycle_at,
                                last_error=f"Слишком много ошибок подряд: {consecutive_errors}",
                            )
                        )
                        return 1
                    time.sleep(5)
        except RequestError as error:
            logging.exception("Стартовый сбой API T-Invest")
            save_runtime_status(
                build_runtime_status_payload(
                    mode=mode,
                    session_name=get_market_session(),
                    started_at=started_at,
                    cycle_count=cycle_count,
                    consecutive_errors=consecutive_errors,
                    state="startup_api_retry",
                    last_cycle_at=last_cycle_at,
                    last_error=str(error),
                )
            )
            if not startup_error_notified:
                startup_error_notified = True
            time.sleep(config.startup_retry_seconds)
        except Exception as error:
            logging.exception("Стартовый внутренний сбой")
            save_runtime_status(
                build_runtime_status_payload(
                    mode=mode,
                    session_name=get_market_session(),
                    started_at=started_at,
                    cycle_count=cycle_count,
                    consecutive_errors=consecutive_errors,
                    state="startup_internal_retry",
                    last_cycle_at=last_cycle_at,
                    last_error=str(error),
                )
            )
            if not startup_error_notified:
                startup_error_notified = True
            time.sleep(config.startup_retry_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(run_bot())
    except KeyboardInterrupt:
        raise SystemExit(130)
