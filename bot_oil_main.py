import json
import logging
import os
import re
import sys
import time
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
from instrument_groups import (
    DEFAULT_SYMBOLS,
    get_instrument_group,
    is_currency_instrument as is_currency_symbol,
)
from news_bias import NewsBias, select_active_biases
from news_ingest import CHANNEL_URLS, detect_biases_for_posts, fetch_posts_for_day
from strategy_registry import get_secondary_strategies
from strategy_engine import evaluate_primary_signal_bundle
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
STATE_DIR = Path(__file__).with_name("bot_state")
META_STATE_PATH = STATE_DIR / "_bot_meta.json"
PORTFOLIO_SNAPSHOT_PATH = STATE_DIR / "_portfolio_snapshot.json"
ACCOUNTING_HISTORY_PATH = STATE_DIR / "_accounting_history.json"
RUNTIME_STATUS_PATH = STATE_DIR / "_runtime_status.json"
NEWS_SNAPSHOT_PATH = STATE_DIR / "_news_snapshot.json"
LOG_DIR = Path(__file__).with_name("logs")
TRADE_JOURNAL_PATH = LOG_DIR / "trade_journal.jsonl"
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
    pending_exit_reason: str = ""
    execution_status: str = "idle"
    last_fill_price: float | None = None
    entry_time: str = ""
    entry_strategy: str = ""
    last_strategy_name: str = ""
    last_higher_tf_bias: str = ""
    last_news_bias: str = "NEUTRAL"
    last_news_impact: str = ""
    last_signal_summary: list[str] = field(default_factory=list)
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
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


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
    state.pending_exit_reason = ""


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
        if (
            str(existing.get("time", "")).strip() == journal_time
            and str(existing.get("symbol", "")).upper() == instrument.symbol.upper()
            and str(existing.get("event", "")).upper() == event_name
            and str(existing.get("side", "")).upper() == side_name
            and int(existing.get("qty_lots") or 0) == int(qty)
            and same_price
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
    with TRADE_JOURNAL_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_trade_journal() -> list[dict[str, Any]]:
    if not TRADE_JOURNAL_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in TRADE_JOURNAL_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception as error:
            logging.warning("Не удалось прочитать строку журнала сделок: %s", error)
    return rows


def save_trade_journal(rows: list[dict[str, Any]]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with TRADE_JOURNAL_PATH.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def update_latest_unclosed_open_journal_entry(
    symbol: str,
    side: str,
    *,
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
    target_symbol = symbol.upper()
    target_side = side.upper()
    open_count = 0
    close_count = 0
    for row in get_today_trade_journal_rows():
        if str(row.get("symbol", "")).upper() != target_symbol:
            continue
        if str(row.get("side", "")).upper() != target_side:
            continue
        event = str(row.get("event", "")).upper()
        if event == "OPEN":
            open_count += 1
        elif event == "CLOSE":
            close_count += 1
    return open_count > close_count


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
    if entry_fee_rub is not None and entry_fee_rub > 0:
        state.entry_commission_rub = entry_fee_rub
        state.entry_commission_accounted = True
        update_latest_unclosed_open_journal_entry(
            instrument.symbol,
            state.position_side,
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
    if previous_side == "FLAT" or previous_qty <= 0 or state.position_qty != 0:
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


def pair_trade_journal_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    open_by_symbol: dict[str, list[dict[str, Any]]] = {}
    closed_reviews: list[dict[str, Any]] = []
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
    current_open = {symbol: items[-1] for symbol, items in open_by_symbol.items() if items}
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
    if now.weekday() >= 5:
        return "WEEKEND"
    current_minutes = now.hour * 60 + now.minute
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
    if session_name != "WEEKEND":
        return True
    return not is_currency_symbol(symbol)


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

    high_low_range = (result["high"] - result["low"]).replace(0, pd.NA)
    money_flow_multiplier = ((result["close"] - result["low"]) - (result["high"] - result["close"])) / high_low_range
    money_flow_multiplier = money_flow_multiplier.fillna(0.0)
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
    status_slot = get_status_slot(candle_time, SIGNAL_STATUS_INTERVAL_MINUTES)
    if status_slot == state.last_status_candle:
        return
    send_msg(
        config,
        build_periodic_status_message(
            config,
            instrument,
            state,
            signal,
            price,
            reason,
            candle_time,
            higher_tf_bias,
            df,
            news_bias,
            compare_lines,
        ),
    )
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
    session_name = get_market_session()
    if session_name == "CLOSED":
        return
    now_slot = floor_time_slot(datetime.now(MOSCOW_TZ), SUMMARY_STATUS_INTERVAL_MINUTES).strftime("%Y-%m-%d %H:%M")
    meta = load_meta_state()
    if meta.get("last_global_status_slot") == now_slot:
        return
    send_msg(config, build_global_diagnostic_message(config, client, watchlist))
    meta["last_global_status_slot"] = now_slot
    save_meta_state(meta)


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
        "total_portfolio_rub": round(snapshot.total_portfolio, 2),
        "free_rub": round(snapshot.free_rub, 2),
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
    session_name = get_market_session()
    if session_name == "CLOSED":
        return
    now_slot = floor_time_slot(datetime.now(MOSCOW_TZ), SUMMARY_STATUS_INTERVAL_MINUTES).strftime("%Y-%m-%d %H:%M")
    meta = load_meta_state()
    if meta.get("last_portfolio_snapshot_slot") == now_slot:
        return
    send_msg(config, build_portfolio_snapshot_message(client, config, watchlist))
    meta["last_portfolio_snapshot_slot"] = now_slot
    save_meta_state(meta)


def maybe_send_trade_results(
    client: Client,
    config: BotConfig,
    watchlist: list[InstrumentConfig],
) -> None:
    session_name = get_market_session()
    if session_name == "CLOSED":
        return
    now_slot = floor_time_slot(datetime.now(MOSCOW_TZ), SUMMARY_STATUS_INTERVAL_MINUTES).strftime("%Y-%m-%d %H:%M")
    meta = load_meta_state()
    if meta.get("last_trade_results_slot") == now_slot:
        return
    send_msg(config, build_trade_results_message(client, config, watchlist))
    meta["last_trade_results_slot"] = now_slot
    save_meta_state(meta)


def maybe_send_trade_review(config: BotConfig) -> None:
    session_name = get_market_session()
    if session_name == "CLOSED":
        return
    now_slot = floor_time_slot(datetime.now(MOSCOW_TZ), SUMMARY_STATUS_INTERVAL_MINUTES).strftime("%Y-%m-%d %H:%M")
    meta = load_meta_state()
    if meta.get("last_trade_review_slot") == now_slot:
        return
    send_msg(config, build_trade_review_message())
    meta["last_trade_review_slot"] = now_slot
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
    if state.position_side != "FLAT" and not has_today_active_open_journal_entry(instrument.symbol, state.position_side):
        if state.pending_order_id and state.pending_order_action == "OPEN":
            recovery_reason = "Позиция подтверждена по брокерскому портфелю."
            recovery_source = "portfolio_confirmation"
            state.execution_status = "confirmed_open"
        else:
            recovery_reason = "Восстановлено после рестарта по брокерскому портфелю."
            recovery_source = "portfolio_recovery"
            state.execution_status = "recovered_open"
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
            state.position_qty,
            state.entry_price or last_price,
            event_time=operation_time,
            gross_pnl_rub=None,
            commission_rub=entry_fee_rub,
            net_pnl_rub=-entry_fee_rub if entry_fee_rub is not None else None,
            reason=recovery_reason,
            source=recovery_source,
            strategy=state.entry_strategy or state.last_strategy_name or "recovered_position",
            dry_run=config.dry_run,
        )
        save_state(instrument.symbol, state)
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
    if config.max_daily_loss <= 0:
        return True
    snapshot = get_account_snapshot(client, config)
    equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
    if equity <= 0:
        return True
    daily_loss_limit_rub = equity * (abs(config.max_daily_loss) / 100.0)
    return state.realized_pnl > -daily_loss_limit_rub


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
    from_utc, to_utc = get_day_bounds_utc_for_date(target_day)
    cursor = ""
    actual_varmargin_rub = 0.0
    actual_fee_expense_rub = 0.0
    actual_fee_cash_effect_rub = 0.0
    figi_to_symbol = {item.figi: item.symbol for item in (watchlist or []) if item.figi}
    varmargin_by_symbol: dict[str, float] = {}

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
    entry_price: float,
    signal: str,
) -> str:
    snapshot = get_account_snapshot(client, config)
    equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
    margin_per_lot = get_margin_per_lot(instrument, signal)
    if margin_per_lot > 0:
        allowed_margin_total = equity * config.max_margin_usage_pct if config.max_margin_usage_pct > 0 else 0.0
        available_margin_budget = allowed_margin_total - snapshot.blocked_guarantee_rub
        if snapshot.free_rub < margin_per_lot:
            return (
                f"не хватает средств/ГО для {instrument.symbol}: "
                f"на 1 лот нужно примерно {margin_per_lot:.2f} RUB, свободно {snapshot.free_rub:.2f} RUB."
            )
        if config.max_margin_usage_pct > 0 and available_margin_budget < margin_per_lot:
            return (
                f"внутренний лимит ГО не позволяет открыть {instrument.symbol}: "
                f"на 1 лот нужно {margin_per_lot:.2f} RUB, в лимите ГО доступно {max(0.0, available_margin_budget):.2f} RUB."
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

    step_price = instrument.min_price_increment
    step_money = instrument.min_price_increment_amount
    stop_distance = entry_price * config.stop_loss_pct
    if step_price > 0 and step_money > 0 and stop_distance > 0 and equity > 0 and config.risk_per_trade_pct > 0:
        risk_budget = equity * config.risk_per_trade_pct * get_session_position_multiplier(get_market_session(), instrument.symbol)
        money_risk_per_contract = (stop_distance / step_price) * step_money
        if money_risk_per_contract > 0 and risk_budget < money_risk_per_contract:
            return (
                f"риск-бюджет слишком мал для {instrument.symbol}: "
                f"на сделку выделено {risk_budget:.2f} RUB, на 1 контракт нужно {money_risk_per_contract:.2f} RUB."
            )

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
        block_reason = describe_capacity_block_reason(client, config, instrument, price, side)
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


def calculate_order_quantity(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    entry_price: float,
    signal: str,
) -> int:
    session_name = get_market_session()
    session_multiplier = get_session_position_multiplier(session_name, instrument.symbol)
    if session_multiplier <= 0:
        return 0
    if config.risk_per_trade_pct <= 0:
        return max(1, int(round(max(1, config.order_quantity) * session_multiplier)))

    snapshot = get_account_snapshot(client, config)
    equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
    if equity <= 0:
        return max(1, config.order_quantity)

    risk_budget = equity * config.risk_per_trade_pct * session_multiplier
    stop_distance = entry_price * config.stop_loss_pct
    step_price = instrument.min_price_increment
    step_money = instrument.min_price_increment_amount

    if step_price <= 0 or step_money <= 0 or stop_distance <= 0:
        return max(1, config.order_quantity)

    money_risk_per_contract = (stop_distance / step_price) * step_money
    if money_risk_per_contract <= 0:
        return max(1, config.order_quantity)

    raw_qty = int(risk_budget // money_risk_per_contract)
    if raw_qty < 1:
        raw_qty = 1

    margin_per_lot = get_margin_per_lot(instrument, signal)
    if margin_per_lot > 0 and config.max_margin_usage_pct > 0:
        allowed_margin_total = equity * config.max_margin_usage_pct
        available_margin_budget = allowed_margin_total - snapshot.blocked_guarantee_rub
        if available_margin_budget <= 0:
            if snapshot.free_rub >= margin_per_lot:
                margin_cap_qty = 1
            else:
                return 0
        else:
            margin_cap_qty = int(available_margin_budget // margin_per_lot)
            if margin_cap_qty < 1 and snapshot.free_rub >= margin_per_lot:
                margin_cap_qty = 1
            if margin_cap_qty < 1:
                return 0
        raw_qty = min(raw_qty, margin_cap_qty)

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
        if broker_limit > 0:
            raw_qty = min(raw_qty, broker_limit)
    except Exception as error:
        logging.warning("Не удалось получить max lots для %s: %s", instrument.symbol, error)

    if config.max_order_quantity > 0:
        raw_qty = min(raw_qty, config.max_order_quantity)

    return max(1, raw_qty)


def build_position_sizing_lines(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    entry_price: float,
    signal: str,
    quantity: int,
) -> list[str]:
    session_name = get_market_session()
    session_multiplier = get_session_position_multiplier(session_name, instrument.symbol)
    lines = [
        f"Сессия: {session_name}",
        f"Множитель размера: {session_multiplier:.2f}",
        f"Лотов: {quantity}",
        f"Размер биржевого лота: {instrument.lot}",
    ]
    try:
        snapshot = get_account_snapshot(client, config)
        equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
        lines.append(f"Портфель: {equity:.2f} RUB")
        lines.append(f"Свободно: {snapshot.free_rub:.2f} RUB")
        lines.append(f"ГО занято: {snapshot.blocked_guarantee_rub:.2f} RUB")
        if config.risk_per_trade_pct > 0:
            risk_budget = equity * config.risk_per_trade_pct * session_multiplier
            stop_distance = entry_price * config.stop_loss_pct
            step_price = instrument.min_price_increment
            step_money = instrument.min_price_increment_amount
            if step_price > 0 and step_money > 0 and stop_distance > 0:
                money_risk_per_contract = (stop_distance / step_price) * step_money
                lines.append(f"Риск на сделку: {risk_budget:.2f} RUB")
                lines.append(f"Риск на 1 контракт: {money_risk_per_contract:.2f} RUB")
        margin_per_lot = get_margin_per_lot(instrument, signal)
        if margin_per_lot > 0:
            allowed_margin_total = equity * config.max_margin_usage_pct
            available_margin_budget = max(0.0, allowed_margin_total - snapshot.blocked_guarantee_rub)
            lines.append(f"ГО на 1 лот: {margin_per_lot:.2f} RUB")
            lines.append(f"Лимит ГО: {allowed_margin_total:.2f} RUB")
            lines.append(f"Свободно под ГО: {available_margin_budget:.2f} RUB")
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
        if broker_limit > 0:
            lines.append(f"Максимум у брокера: {broker_limit} лотов")
    except Exception as error:
        logging.warning("Не удалось собрать sizing info для %s: %s", instrument.symbol, error)
    return lines


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
    if instrument.symbol not in {"GNM6", "USDRUBF", "SRM6", "BRK6", "NGJ6", "IMOEXF"}:
        return True, ""
    if not state.last_exit_time:
        return True, ""
    try:
        last_exit_at = datetime.fromisoformat(state.last_exit_time)
    except ValueError:
        return True, ""
    if last_exit_at.tzinfo is None:
        last_exit_at = last_exit_at.replace(tzinfo=UTC)

    cooldown_minutes = 0
    if instrument.symbol == "GNM6":
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
        if instrument.symbol == "BRK6" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=3):
                return False, "для BRK6 повторный вход после убыточного выхода разрешён только после обновления экстремума."
        if instrument.symbol == "NGJ6" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=4):
                return False, "для NGJ6 повторный вход после убыточного выхода разрешён только после нового экстремума."
        if instrument.symbol == "IMOEXF" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=2):
                return False, "для IMOEXF повторный вход после убыточного выхода разрешён только после нового экстремума."
        return True, ""

    next_allowed = last_exit_at + timedelta(minutes=cooldown_minutes)
    now = datetime.now(UTC)
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
        if instrument.symbol == "IMOEXF" and state.last_exit_side == signal and state.last_exit_pnl_rub < 0:
            if not price_has_new_extreme_since_exit(instrument, signal, current_price, state.last_exit_price, min_steps=2):
                return False, "для IMOEXF повторный вход после убыточного выхода разрешён только после нового экстремума."
        return True, ""

    remaining = int((next_allowed - now).total_seconds() // 60) + 1
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
            elif (
                pending_action == "CLOSE"
                and synced_qty == 0
                and previous_side != "FLAT"
                and pending_submitted_at is not None
            ):
                wait_seconds = (datetime.now(UTC) - pending_submitted_at).total_seconds()
                if wait_seconds < BROKER_CLOSE_CONFIRMATION_GRACE_SECONDS:
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
                reason="live fill",
                source="order_fill",
                strategy=state.entry_strategy,
                dry_run=False,
            )
            send_msg(
                config,
                build_telegram_card(
                    "Позиция открыта",
                    "🟢" if state.position_side == "LONG" else "🔴",
                    [
                        f"Инструмент: {format_instrument_title(instrument)}",
                        f"Направление: {state.position_side}",
                        f"Лотов: {filled_qty}",
                        f"Цена исполнения: {fill_price:.4f}",
                        f"Комиссия: {fill_commission_rub:.2f} RUB",
                        f"ID заявки: {state.pending_order_id}",
                    ],
                ),
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
            send_msg(
                config,
                build_telegram_card(
                    "Позиция закрыта",
                    "✅" if net_trade_pnl >= 0 else "⚠️",
                    [
                        f"Инструмент: {format_instrument_title(instrument)}",
                        f"Причина выхода: {exit_reason}",
                        f"Цена исполнения: {fill_price:.4f}",
                        f"Gross: {gross_pnl:.2f} RUB",
                        f"Комиссия: {total_trade_commission:.2f} RUB",
                        f"Net: {net_trade_pnl:.2f} RUB",
                        f"ID заявки: {state.pending_order_id}",
                    ],
                ),
            )
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
        send_msg(
            config,
            build_telegram_card(
                "Заявка не исполнена",
                "⚠️",
                [
                    f"Инструмент: {format_instrument_title(instrument)}",
                    f"Действие: {state.pending_order_action or 'UNKNOWN'}",
                    f"Направление: {state.pending_order_side or 'UNKNOWN'}",
                    f"Статус: {status.name}",
                    f"Причина: {rejection_reason}",
                    f"ID заявки: {state.pending_order_id}",
                ],
            ),
        )
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
        if should_notify_partial:
            send_msg(
                config,
                build_telegram_card(
                    "Заявка исполнена частично",
                    "⚠️",
                    [
                        f"Инструмент: {format_instrument_title(instrument)}",
                        f"Действие: {state.pending_order_action or 'UNKNOWN'}",
                        f"Направление: {state.pending_order_side or 'UNKNOWN'}",
                        f"Исполнено: {executed}/{state.pending_order_qty}",
                        "Бот сохранил промежуточное состояние и ждёт финальный статус.",
                        f"ID заявки: {state.pending_order_id}",
                    ],
                ),
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
) -> None:
    if state.position_qty > 0 or has_pending_order(state):
        return
    session_name = get_market_session()
    price = get_last_price(client, instrument)
    quantity = calculate_order_quantity(client, config, instrument, price, signal)
    if quantity <= 0:
        block_reason = describe_capacity_block_reason(client, config, instrument, price, signal)
        state.last_error = block_reason
        state.last_signal_summary = [block_reason, *state.last_signal_summary[:2]]
        save_state(instrument.symbol, state)
        logging.info("symbol=%s status=entry_blocked reason=%s", instrument.symbol, block_reason)
        return
    sizing_lines = build_position_sizing_lines(client, config, instrument, price, signal, quantity)
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
            reason="dry_run open",
            source="dry_run",
            strategy=strategy_name,
            dry_run=True,
        )
        send_msg(
            config,
            build_telegram_card(
                "Тестовое открытие позиции",
                "🚀" if side == "LONG" else "📉",
                [
                    f"Инструмент: {format_instrument_title(instrument)}",
                    f"Направление: {side}",
                    f"Стратегия: {strategy_name or 'unknown'}",
                    *sizing_lines,
                    f"Режим входа: {session_name}",
                    f"Ориентировочная цена: {price:.4f}",
                    "",
                    "Сделка не отправлена в брокер: включён DRY_RUN.",
                ],
            ),
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
        send_msg(
            config,
            build_telegram_card(
                "Заявка на открытие отклонена",
                "⚠️",
                [
                    f"Инструмент: {format_instrument_title(instrument)}",
                    f"Направление: {side}",
                    f"Стратегия: {strategy_name or 'unknown'}",
                    request_reason,
                ],
            ),
        )
        return
    state.entry_strategy = strategy_name
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
        state.position_notional_rub = 0.0
        state.position_variation_margin_rub = 0.0
        state.position_pnl_pct = 0.0
        state.execution_status = "idle"
        save_state(instrument.symbol, state)
        text = build_telegram_card(
            "Тестовое закрытие позиции",
            "✅" if pnl >= 0 else "⚠️",
            [
                f"Инструмент: {format_instrument_title(instrument)}",
                f"Причина выхода: {exit_reason}",
                f"Ориентировочная цена: {price:.4f}",
                f"Результат по позиции: {pnl:.2f} RUB",
            ],
        )
        send_msg(config, text)
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
        send_msg(
            config,
            build_telegram_card(
                "Заявка на закрытие отклонена",
                "⚠️",
                [
                    f"Инструмент: {format_instrument_title(instrument)}",
                    f"Причина выхода: {exit_reason}",
                    f"Направление позиции: {state.position_side}",
                    request_reason,
                ],
            ),
        )
        return
    state.pending_order_id = order_id
    state.pending_order_action = "CLOSE"
    state.pending_order_side = state.position_side
    state.pending_order_qty = qty
    state.pending_submitted_at = datetime.now(UTC).isoformat()
    state.pending_exit_reason = exit_reason
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
) -> None:
    if state.position_qty <= 0 or state.position_side == "FLAT" or state.entry_price is None:
        return

    price = get_last_price(client, instrument)
    if not state.entry_time:
        state.entry_time = datetime.now(UTC).isoformat()
    state.max_price = max(state.max_price or price, price)
    state.min_price = min(state.min_price or price, price)
    last = df.iloc[-1]
    profile = get_strategy_profile(config, instrument)
    exit_profile = get_exit_profile(config, state.entry_strategy)
    is_trend_rollover = state.entry_strategy == "trend_rollover"
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
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
        macd_down = (
            prev2_macd >= prev2_macd_signal
            and prev_macd < prev_macd_signal
            and macd < macd_signal
            and close < ema20
        )
        opposite_signal_confirmed = fresh_signal == "SHORT" and close < ema20 and close <= prev_close
        min_hold_passed = position_held_long_enough(state, config, exit_profile.min_hold_minutes)
        if price <= stop_price:
            close_position(client, config, instrument, state, f"Стоп-лосс: цена {price:.4f} <= {stop_price:.4f}")
        elif price <= trailing_price:
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
        macd_up = (
            prev2_macd <= prev2_macd_signal
            and prev_macd > prev_macd_signal
            and macd > macd_signal
            and close > ema20
        )
        opposite_signal_confirmed = fresh_signal == "LONG" and close > ema20 and close >= prev_close
        min_hold_passed = position_held_long_enough(state, config, exit_profile.min_hold_minutes)
        if price >= stop_price:
            close_position(client, config, instrument, state, f"Стоп-лосс: цена {price:.4f} >= {stop_price:.4f}")
        elif price >= trailing_price:
            close_position(client, config, instrument, state, f"Трейлинг-стоп: цена {price:.4f} >= {trailing_price:.4f}")
        elif min_hold_passed and state.breakeven_armed and rsi <= profile.rsi_exit_short and not is_trend_rollover:
            close_position(client, config, instrument, state, f"RSI вышел в зону перепроданности: {rsi:.2f} <= {profile.rsi_exit_short:.2f}")
        elif min_hold_passed and macd_up:
            close_position(client, config, instrument, state, "MACD подтверждённо развернулся вверх и цена вернулась выше EMA20")
        elif min_hold_passed and opposite_signal_confirmed:
            close_position(client, config, instrument, state, "Появился подтверждённый противоположный сигнал LONG")


def process_instrument(client: Client, config: BotConfig, instrument: InstrumentConfig) -> None:
    state = load_state(instrument.symbol)
    reconcile_state_accounting(instrument.symbol, state)
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
        if state.last_error != weekend_message or state.last_signal != "HOLD":
            state.last_error = weekend_message
            state.last_signal = "HOLD"
            state.last_news_impact = "инструмент недоступен на выходных"
            save_state(instrument.symbol, state)
            logging.info("symbol=%s status=weekend_currency_closed", instrument.symbol)
        else:
            state.last_error = weekend_message
        state.last_signal = "HOLD"
        return
    if not ensure_risk_limits(client, state, config):
        today = datetime.now(UTC).date().isoformat()
        if state.last_risk_stop_day != today:
            send_msg(
                config,
                build_telegram_card(
                    "Торговля остановлена",
                    "🛑",
                    [
                        f"Инструмент: {format_instrument_title(instrument)}",
                        "Причина: достигнут дневной лимит убытка.",
                    ],
                ),
            )
            state.last_risk_stop_day = today
            save_state(instrument.symbol, state)
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
    state.last_signal_summary = signal_summary

    if signal_changed:
        logging.info("symbol=%s signal=%s side=%s qty=%s", instrument.symbol, signal, state.position_side, state.position_qty)

    if state.position_side == "FLAT":
        if signal in {"LONG", "SHORT"} and session_allows_new_entries(session_name, instrument.symbol) and session_signal_quality_ok(lower_df, signal, session_name, instrument.symbol):
            reentry_allowed, reentry_reason = position_reentry_allowed(state, instrument, signal, current_price)
            if reentry_allowed:
                open_position(client, config, instrument, state, signal, primary_strategy_name)
            else:
                logging.info("symbol=%s status=reentry_cooldown reason=%s", instrument.symbol, reentry_reason)
                state.last_signal_summary = [reentry_reason, *state.last_signal_summary[:2]]
    else:
        check_exit(client, config, instrument, state, lower_df, signal)

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
    if get_market_session() != "CLOSED":
        send_msg(
            config,
            build_telegram_card(
                "Бот запущен",
                "🤖",
                [
                    f"Режим: {mode}",
                    f"Младший ТФ: {config.candle_interval_minutes} минут",
                    f"Старший ТФ: {config.higher_tf_interval_minutes} минут",
                    f"Инструменты: {', '.join(config.symbols)}",
                    "",
                    "Базовые торговые сигналы будут приходить раз в 60 минут.",
                    "Диагностика, портфель, результат и разбор сделок будут приходить раз в 2 часа.",
                ],
            ),
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
                        maybe_refresh_portfolio_snapshot(client, config, watchlist)
                        maybe_send_global_diagnostic(client, config, watchlist)
                        maybe_send_portfolio_snapshot(client, config, watchlist)
                        maybe_send_trade_results(client, config, watchlist)
                        maybe_send_trade_review(config)
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
                            send_msg(
                                config,
                                build_telegram_card(
                                    "Тестовый прогон завершён",
                                    "🏁",
                                    [f"Количество циклов: {cycle_count}"],
                                ),
                            )
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
                        send_msg(
                            config,
                            build_telegram_card(
                                "Ошибка API T-Invest",
                                "⚠️",
                                [str(error)],
                            ),
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
                        send_msg(
                            config,
                            build_telegram_card(
                                "Внутренняя ошибка бота",
                                "⚠️",
                                [str(error)],
                            ),
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
                        send_msg(
                            config,
                            build_telegram_card(
                                "Бот остановлен",
                                "🛑",
                                [f"Слишком много ошибок подряд: {consecutive_errors}"],
                            ),
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
                send_msg(
                    config,
                    build_telegram_card(
                        "Проблема запуска",
                        "⚠️",
                        [
                            "Не удалось подключиться к T-Invest при старте.",
                            str(error),
                            f"Повторная попытка через {config.startup_retry_seconds} сек.",
                        ],
                    ),
                )
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
                send_msg(
                    config,
                    build_telegram_card(
                        "Проблема запуска",
                        "⚠️",
                        [
                            "Бот не смог корректно стартовать.",
                            str(error),
                            f"Повторная попытка через {config.startup_retry_seconds} сек.",
                        ],
                    ),
                )
                startup_error_notified = True
            time.sleep(config.startup_retry_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(run_bot())
    except KeyboardInterrupt:
        raise SystemExit(130)
