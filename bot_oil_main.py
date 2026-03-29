import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import ta
from dotenv import load_dotenv
from instrument_groups import DEFAULT_SYMBOLS, is_currency_instrument as is_currency_symbol
from news_bias import NewsBias, select_active_biases
from news_ingest import CHANNEL_URLS, detect_biases_for_posts, fetch_posts_for_day
from strategy_registry import get_secondary_strategies
from strategy_engine import evaluate_primary_signal_bundle
from tinkoff.invest import (
    CandleInterval,
    Client,
    GetMaxLotsRequest,
    OrderDirection,
    OrderExecutionReportStatus,
    OrderType,
    RequestError,
)
from tinkoff.invest.constants import INVEST_GRPC_API, INVEST_GRPC_API_SANDBOX
from strategies.base import StrategyProfile
from strategies import evaluate_williams_currency_signal as evaluate_williams_signal
from strategies import get_strategy_profile as get_primary_strategy_profile


APP_NAME = "oil-bot-main"
STATE_DIR = Path(__file__).with_name("bot_state")
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
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
    poll_seconds: int
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
    pending_exit_reason: str = ""
    last_fill_price: float | None = None



def setup_logging() -> None:
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
    state.pending_exit_reason = ""


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
        poll_seconds=parse_int_env("OIL_POLL_SECONDS", 10),
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


def format_news_bias_lines(news_bias: NewsBias | None) -> list[str]:
    if news_bias is None:
        return ["• News bias: NEUTRAL"]
    return [
        f"• News bias: {news_bias.bias} ({news_bias.strength})",
        f"• Источник: {news_bias.source}",
        f"• Причина: {news_bias.reason}",
    ]


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


def is_currency_instrument(symbol: str) -> bool:
    return is_currency_symbol(symbol)


def build_telegram_card(title: str, emoji: str, lines: list[str]) -> str:
    body = "\n".join(line for line in lines if line)
    return f"{emoji} {title}\n\n{body}"


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


def reset_daily_pnl_if_needed(state: InstrumentState) -> None:
    today = datetime.now(UTC).date().isoformat()
    if state.trading_day != today:
        state.trading_day = today
        state.realized_pnl = 0.0
        state.last_risk_stop_day = ""


def current_moscow_time() -> datetime:
    return datetime.now(MOSCOW_TZ)


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

    volume_factor = 1.05
    impulse_factor = 0.85
    if session_name == "WEEKEND":
        volume_factor = 1.10
        impulse_factor = 0.95

    volume_ok = volume_avg > 0 and volume >= volume_avg * volume_factor
    impulse_ok = body_avg > 0 and body >= body_avg * impulse_factor
    if signal == "LONG":
        macd_ok = macd > macd_signal and macd >= prev_macd and prev_macd >= prev_macd_signal
    else:
        macd_ok = macd < macd_signal and macd <= prev_macd and prev_macd <= prev_macd_signal
    return volume_ok and impulse_ok and macd_ok


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


def get_lower_tf_lookback_hours(config: BotConfig) -> int:
    base_hours = max(config.candle_hours, int((config.candle_interval_minutes * 240) / 60) + 1)
    if get_market_session() == "WEEKEND":
        return max(base_hours, 72)
    return base_hours


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "time" in result.columns:
        result["time"] = pd.to_datetime(result["time"], utc=True, errors="coerce")
        result = result.sort_values("time").reset_index(drop=True)
    if "is_complete" in result.columns:
        result = result[result["is_complete"]].reset_index(drop=True)
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
    lookback_hours = max(config.candle_hours, int((config.higher_tf_interval_minutes * 80) / 60) + 1)
    df = get_candles(client, config, instrument, config.higher_tf_interval, lookback_hours=lookback_hours)
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df = df.sort_values("time").reset_index(drop=True)
    if "is_complete" in df.columns:
        df = df[df["is_complete"]].reset_index(drop=True)
    df["ema50"] = ta.trend.EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema50_slope"] = df["ema50"].pct_change()
    df = df.dropna().reset_index(drop=True)
    if df.empty:
        return "FLAT"
    last = df.iloc[-1]
    close = float(last["close"])
    ema50 = float(last["ema50"])
    if close > ema50:
        return "LONG"
    if close < ema50:
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
    position_text = "нет" if state.position_side == "FLAT" else f"{state.position_side}, qty={state.position_qty}"
    session_name = get_market_session()
    header = f"{signal_emoji(signal)} {instrument.symbol} — {signal}"
    lines = [
        header,
        "",
        f"⏱ Свеча: {candle_time}",
        f"🕒 Сессия: {session_name}",
        f"💵 Цена: {price:.2f}",
        f"🧾 Позиция: {position_text}",
        "",
        *build_market_view_lines(df, config, instrument, higher_tf_bias),
        "",
        "📰 Новостный фон",
        *format_news_bias_lines(news_bias),
    ]

    if compare_lines:
        lines.extend(["", "⚖️ Сравнение стратегий", *compare_lines])

    if signal == "HOLD":
        long_blockers, short_blockers = extract_blocker_sections(reason)
        lines.extend(["", "🚫 Почему нет входа", "Long:"])
        lines.extend(f"• {item}" for item in long_blockers[:3])
        if short_blockers:
            lines.append("")
            lines.append("Short:")
            lines.extend(f"• {item}" for item in short_blockers[:3])
    else:
        lines.extend(["", "✅ Почему есть вход", *format_reason_multiline(reason)])

    return "\n".join(lines)


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
    send_msg(
        config,
        build_telegram_card(
            "Изменение сигнала",
            signal_emoji(signal),
            [
                f"Инструмент: {format_instrument_title(instrument)}",
                f"Режим: {mode}",
                f"Цена: {price:.4f}",
                f"Новый сигнал: {signal_emoji(signal)} {signal}",
                f"News bias: {format_news_bias_label(news_bias)}",
                "",
                "Причины:",
                *format_reason_multiline(reason),
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
    if candle_time == state.last_status_candle:
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
    state.last_status_candle = candle_time


def get_last_price(client: Client, instrument: InstrumentConfig) -> float:
    response = client.market_data.get_last_prices(figi=[instrument.figi])
    if not response.last_prices:
        raise RuntimeError(f"Не удалось получить последнюю цену для {instrument.symbol}")
    return quotation_to_float(response.last_prices[0].price)


def extract_position_data(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
) -> tuple[int, float | None]:
    portfolio = client.operations.get_portfolio(account_id=config.account_id)
    for position in portfolio.positions:
        if position.figi != instrument.figi:
            continue
        qty = int(round(quotation_to_float(getattr(position, "quantity", None))))
        avg = quotation_to_float(getattr(position, "average_position_price", None))
        return qty, (avg if avg > 0 else None)
    return 0, None


def sync_state_with_portfolio(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
) -> int:
    qty, avg = extract_position_data(client, config, instrument)
    state.position_qty = abs(qty)
    if qty == 0:
        state.entry_price = None
        state.max_price = None
        state.min_price = None
        state.position_side = "FLAT"
        state.breakeven_armed = False
        return 0
    last_price = get_last_price(client, instrument)
    if state.entry_price is None:
        state.entry_price = avg if avg is not None else last_price
        state.max_price = last_price
        state.min_price = last_price
        state.position_side = "LONG" if qty > 0 else "SHORT"
    else:
        state.position_side = "LONG" if qty > 0 else "SHORT"
    state.max_price = max(state.max_price or last_price, last_price)
    state.min_price = min(state.min_price or last_price, last_price)
    return qty


def ensure_risk_limits(state: InstrumentState, config: BotConfig) -> bool:
    reset_daily_pnl_if_needed(state)
    return state.realized_pnl > -abs(config.max_daily_loss)


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
    lines = [f"Сессия: {session_name}", f"Множитель размера: {session_multiplier:.2f}", f"Количество: {quantity}"]
    try:
        snapshot = get_account_snapshot(client, config)
        equity = snapshot.total_portfolio if snapshot.total_portfolio > 0 else snapshot.free_rub
        lines.append(f"Портфель: {equity:.2f} RUB")
        lines.append(f"Свободно: {snapshot.free_rub:.2f} RUB")
        if config.risk_per_trade_pct > 0:
            risk_budget = equity * config.risk_per_trade_pct
            stop_distance = entry_price * config.stop_loss_pct
            step_price = instrument.min_price_increment
            step_money = instrument.min_price_increment_amount
            if step_price > 0 and step_money > 0 and stop_distance > 0:
                money_risk_per_contract = (stop_distance / step_price) * step_money
                lines.append(f"Риск на сделку: {risk_budget:.2f} RUB")
                lines.append(f"Риск на 1 контракт: {money_risk_per_contract:.2f} RUB")
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
            lines.append(f"Максимум у брокера: {broker_limit}")
    except Exception as error:
        logging.warning("Не удалось собрать sizing info для %s: %s", instrument.symbol, error)
    return lines


def sync_pending_order(
    client: Client,
    config: BotConfig,
    instrument: InstrumentConfig,
    state: InstrumentState,
) -> bool:
    if not has_pending_order(state):
        return False

    order_state = client.orders.get_order_state(
        account_id=config.account_id,
        order_id=state.pending_order_id,
    )
    status = order_state.execution_report_status

    if status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
        fill_price = quotation_to_float(getattr(order_state, "executed_order_price", None))
        if fill_price <= 0:
            fill_price = quotation_to_float(getattr(order_state, "average_position_price", None))
        if fill_price <= 0:
            fill_price = get_last_price(client, instrument)
        filled_qty = int(getattr(order_state, "lots_executed", 0) or 0) or state.pending_order_qty
        state.last_fill_price = fill_price

        if state.pending_order_action == "OPEN":
            state.entry_price = fill_price
            state.max_price = fill_price
            state.min_price = fill_price
            state.position_qty = filled_qty
            state.position_side = state.pending_order_side
            state.breakeven_armed = False
            send_msg(
                config,
                build_telegram_card(
                    "Позиция открыта",
                    "🟢" if state.position_side == "LONG" else "🔴",
                    [
                        f"Инструмент: {format_instrument_title(instrument)}",
                        f"Направление: {state.position_side}",
                        f"Количество: {filled_qty}",
                        f"Цена исполнения: {fill_price:.4f}",
                        f"ID заявки: {state.pending_order_id}",
                    ],
                ),
            )
        elif state.pending_order_action == "CLOSE":
            pnl = 0.0
            if state.entry_price is not None:
                pnl = (
                    (fill_price - state.entry_price) * state.position_qty
                    if state.position_side == "LONG"
                    else (state.entry_price - fill_price) * state.position_qty
                )
            reset_daily_pnl_if_needed(state)
            state.realized_pnl += pnl
            exit_reason = state.pending_exit_reason or "Заявка на закрытие исполнена"
            send_msg(
                config,
                build_telegram_card(
                    "Позиция закрыта",
                    "✅" if pnl >= 0 else "⚠️",
                    [
                        f"Инструмент: {format_instrument_title(instrument)}",
                        f"Причина выхода: {exit_reason}",
                        f"Цена исполнения: {fill_price:.4f}",
                        f"Результат по позиции: {pnl:.4f}",
                        f"ID заявки: {state.pending_order_id}",
                    ],
                ),
            )
            state.entry_price = None
            state.max_price = None
            state.min_price = None
            state.position_qty = 0
            state.position_side = "FLAT"
            state.breakeven_armed = False

        clear_pending_order(state)
        save_state(instrument.symbol, state)
        return False

    if status in {
        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED,
        OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED,
    }:
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
                    f"ID заявки: {state.pending_order_id}",
                ],
            ),
        )
        clear_pending_order(state)
        save_state(instrument.symbol, state)
        return False

    if status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL:
        state.last_error = (
            f"Заявка {state.pending_order_id} исполнена частично: "
            f"{getattr(order_state, 'lots_executed', 0)}/{state.pending_order_qty}"
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
) -> None:
    if state.position_qty > 0 or has_pending_order(state):
        return
    session_name = get_market_session()
    price = get_last_price(client, instrument)
    quantity = calculate_order_quantity(client, config, instrument, price, signal)
    if quantity <= 0:
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
        state.max_price = price
        state.min_price = price
        state.position_qty = quantity
        state.position_side = side
        state.breakeven_armed = False
        save_state(instrument.symbol, state)
        send_msg(
            config,
            build_telegram_card(
                "Тестовое открытие позиции",
                "🚀" if side == "LONG" else "📉",
                [
                    f"Инструмент: {format_instrument_title(instrument)}",
                    f"Направление: {side}",
                    *sizing_lines,
                    f"Режим входа: {session_name}",
                    f"Ориентировочная цена: {price:.4f}",
                    "",
                    "Сделка не отправлена в брокер: включён DRY_RUN.",
                ],
            ),
        )
        return

    order_id = place_market_order(client, config, instrument, quantity, direction)
    state.pending_order_id = order_id
    state.pending_order_action = "OPEN"
    state.pending_order_side = side
    state.pending_order_qty = quantity
    state.pending_exit_reason = ""
    save_state(instrument.symbol, state)
    send_msg(
        config,
        build_telegram_card(
            "Открытие позиции отправлено",
            "🚀" if side == "LONG" else "📉",
            [
                f"Инструмент: {format_instrument_title(instrument)}",
                f"Направление: {side}",
                *sizing_lines,
                f"Режим входа: {session_name}",
                f"Ориентировочная цена: {price:.4f}",
                f"ID заявки: {order_id}",
            ],
        ),
    )


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
            pnl = (
                (price - state.entry_price) * qty
                if state.position_side == "LONG"
                else (state.entry_price - price) * qty
            )
        reset_daily_pnl_if_needed(state)
        state.realized_pnl += pnl
        state.entry_price = None
        state.max_price = None
        state.min_price = None
        state.position_qty = 0
        state.position_side = "FLAT"
        state.breakeven_armed = False
        save_state(instrument.symbol, state)
        text = build_telegram_card(
            "Тестовое закрытие позиции",
            "✅" if pnl >= 0 else "⚠️",
            [
                f"Инструмент: {format_instrument_title(instrument)}",
                f"Причина выхода: {exit_reason}",
                f"Ориентировочная цена: {price:.4f}",
                f"Результат по позиции: {pnl:.4f}",
            ],
        )
        send_msg(config, text)
        return

    order_id = place_market_order(client, config, instrument, qty, direction)
    state.pending_order_id = order_id
    state.pending_order_action = "CLOSE"
    state.pending_order_side = state.position_side
    state.pending_order_qty = qty
    state.pending_exit_reason = exit_reason
    save_state(instrument.symbol, state)
    send_msg(
        config,
        build_telegram_card(
            "Закрытие позиции отправлено",
            "✅",
            [
                f"Инструмент: {format_instrument_title(instrument)}",
                f"Причина выхода: {exit_reason}",
                f"Ориентировочная цена: {price:.4f}",
                f"ID заявки: {order_id}",
            ],
        ),
    )


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
    state.max_price = max(state.max_price or price, price)
    state.min_price = min(state.min_price or price, price)
    last = df.iloc[-1]
    profile = get_strategy_profile(config, instrument)
    prev = df.iloc[-2]
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    rsi = float(last["rsi"])

    if state.position_side == "LONG":
        profit_pct = (price - state.entry_price) / state.entry_price
        if profit_pct >= config.breakeven_profit_pct:
            state.breakeven_armed = True
        stop_price = state.entry_price * (1 - config.stop_loss_pct)
        if state.breakeven_armed:
            stop_price = max(stop_price, state.entry_price)
        trailing_price = (state.max_price or price) * (1 - config.trailing_stop_pct)
        macd_down = prev_macd >= prev_macd_signal and macd < macd_signal
        if price <= stop_price:
            close_position(client, config, instrument, state, f"Стоп-лосс: цена {price:.4f} <= {stop_price:.4f}")
        elif price <= trailing_price:
            close_position(client, config, instrument, state, f"Трейлинг-стоп: цена {price:.4f} <= {trailing_price:.4f}")
        elif rsi >= profile.rsi_exit_long:
            close_position(client, config, instrument, state, f"RSI вышел в зону перегрева: {rsi:.2f} >= {profile.rsi_exit_long:.2f}")
        elif macd_down:
            close_position(client, config, instrument, state, "MACD развернулся вниз против позиции")
        elif fresh_signal == "SHORT":
            close_position(client, config, instrument, state, "Появился противоположный сигнал SHORT")
    else:
        profit_pct = (state.entry_price - price) / state.entry_price
        if profit_pct >= config.breakeven_profit_pct:
            state.breakeven_armed = True
        stop_price = state.entry_price * (1 + config.stop_loss_pct)
        if state.breakeven_armed:
            stop_price = min(stop_price, state.entry_price)
        trailing_price = (state.min_price or price) * (1 + config.trailing_stop_pct)
        macd_up = prev_macd <= prev_macd_signal and macd > macd_signal
        if price >= stop_price:
            close_position(client, config, instrument, state, f"Стоп-лосс: цена {price:.4f} >= {stop_price:.4f}")
        elif price >= trailing_price:
            close_position(client, config, instrument, state, f"Трейлинг-стоп: цена {price:.4f} >= {trailing_price:.4f}")
        elif rsi <= profile.rsi_exit_short:
            close_position(client, config, instrument, state, f"RSI вышел в зону перепроданности: {rsi:.2f} <= {profile.rsi_exit_short:.2f}")
        elif macd_up:
            close_position(client, config, instrument, state, "MACD развернулся вверх против позиции")
        elif fresh_signal == "LONG":
            close_position(client, config, instrument, state, "Появился противоположный сигнал LONG")


def process_instrument(client: Client, config: BotConfig, instrument: InstrumentConfig) -> None:
    state = load_state(instrument.symbol)
    if not config.dry_run and sync_pending_order(client, config, instrument, state):
        return
    session_name = get_market_session()
    if session_name == "CLOSED":
        closed_message = "Вне торговой сессии срочного рынка Мосбиржи."
        if state.last_error != closed_message or state.last_signal != "HOLD":
            state.last_error = closed_message
            state.last_signal = "HOLD"
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
            save_state(instrument.symbol, state)
            logging.info("symbol=%s status=weekend_currency_closed", instrument.symbol)
        else:
            state.last_error = weekend_message
        state.last_signal = "HOLD"
        return
    if not ensure_risk_limits(state, config):
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
                lookback_hours=get_lower_tf_lookback_hours(config),
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
                    lookback_hours=get_lower_tf_lookback_hours(config),
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

    if signal_changed:
        logging.info("symbol=%s signal=%s side=%s qty=%s", instrument.symbol, signal, state.position_side, state.position_qty)

    if state.position_side == "FLAT":
        if signal in {"LONG", "SHORT"} and session_allows_new_entries(session_name, instrument.symbol) and session_signal_quality_ok(lower_df, signal, session_name, instrument.symbol):
            open_position(client, config, instrument, state, signal)
    else:
        check_exit(client, config, instrument, state, lower_df, signal)

    save_state(instrument.symbol, state)


def run_bot() -> int:
    setup_logging()
    config = load_config()
    if not config.dry_run and not config.allow_orders:
        raise RuntimeError("LIVE-режим заблокирован: сначала включи OIL_ALLOW_ORDERS=true осознанно.")

    mode = "DRY_RUN" if config.dry_run else "LIVE"
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
                    "Статусы будут приходить по закрытию каждой 5-минутной свечи.",
                ],
            ),
        )

    consecutive_errors = 0
    cycle_count = 0
    with Client(config.token, app_name=APP_NAME, target=config.target) as client:
        watchlist = resolve_instruments(client, config)
        while True:
            try:
                for instrument in watchlist:
                    process_instrument(client, config, instrument)
                consecutive_errors = 0
                cycle_count += 1
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
                send_msg(
                    config,
                    build_telegram_card(
                        "Внутренняя ошибка бота",
                        "⚠️",
                        [str(error)],
                    ),
                )
            if consecutive_errors >= config.max_consecutive_errors:
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


if __name__ == "__main__":
    try:
        raise SystemExit(run_bot())
    except KeyboardInterrupt:
        raise SystemExit(130)
