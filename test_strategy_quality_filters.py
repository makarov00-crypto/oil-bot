import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd

from bot_oil_main import BotConfig, InstrumentConfig
from strategy_registry import get_primary_strategies
from strategies.breakdown_continuation import evaluate_signal as evaluate_range_break
from strategies.opening_range_breakout import evaluate_signal as evaluate_opening_range


def make_config() -> BotConfig:
    return BotConfig(
        token="",
        account_id="",
        target="",
        symbols=[],
        tg_token="",
        tg_chat_id="",
        dry_run=True,
        allow_orders=False,
        order_quantity=1,
        max_order_quantity=1,
        risk_per_trade_pct=0.0,
        max_margin_usage_pct=0.0,
        portfolio_usage_pct=0.0,
        capital_reserve_pct=0.0,
        base_trade_allocation_pct=0.0,
        poll_seconds=1,
        startup_retry_seconds=1,
        candle_hours=1,
        candle_interval=SimpleNamespace(),
        candle_interval_minutes=5,
        higher_tf_interval=SimpleNamespace(),
        higher_tf_interval_minutes=15,
        max_daily_loss=0.0,
        max_consecutive_errors=1,
        max_cycles=1,
        stop_loss_pct=0.0,
        trailing_stop_pct=0.0,
        breakeven_profit_pct=0.0,
        min_hold_minutes=0,
        ema_slope_threshold=0.0,
        near_ema20_pct=0.0,
        volume_factor=1.0,
        atr_min_pct=0.0,
        long_rsi_min=0.0,
        long_rsi_max=100.0,
        short_rsi_min=0.0,
        short_rsi_max=100.0,
        rsi_exit_long=70.0,
        rsi_exit_short=30.0,
    )


def candle_rows(rows: list[dict]) -> pd.DataFrame:
    base_time = datetime(2026, 4, 15, 6, 0, tzinfo=timezone.utc)
    result = []
    for index, row in enumerate(rows):
        item = {
            "time": pd.Timestamp(base_time + pd.Timedelta(minutes=5 * index)),
            "open": row.get("open", row["close"]),
            "high": row.get("high", row["close"]),
            "low": row.get("low", row["close"]),
            "close": row["close"],
            "ema20": row.get("ema20", row["close"]),
            "ema50": row.get("ema50", row["close"]),
            "rsi": row.get("rsi", 50.0),
            "macd": row.get("macd", 0.1),
            "macd_signal": row.get("macd_signal", 0.0),
            "atr": row.get("atr", 0.1),
            "volume": row.get("volume", 100.0),
            "volume_avg": row.get("volume_avg", 100.0),
            "body": row.get("body", abs(row.get("close", 0.0) - row.get("open", row.get("close", 0.0)))),
            "body_avg": row.get("body_avg", 0.1),
        }
        result.append(item)
    return pd.DataFrame(result)


class StrategyQualityFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = make_config()

    def test_opening_range_blocks_expensive_fx_without_commission_room(self) -> None:
        df = candle_rows(
            [
                {"close": 75.00, "high": 75.01, "low": 74.99, "ema20": 75.00, "ema50": 75.00},
                {"close": 75.01, "high": 75.02, "low": 75.00, "ema20": 75.00, "ema50": 75.00},
                {"close": 75.02, "high": 75.03, "low": 75.01, "ema20": 75.00, "ema50": 75.00},
                {"close": 75.03, "high": 75.04, "low": 75.02, "ema20": 75.00, "ema50": 75.00},
                {"close": 75.04, "high": 75.05, "low": 75.03, "ema20": 75.00, "ema50": 75.00},
                {"close": 75.05, "high": 75.06, "low": 75.04, "ema20": 75.00, "ema50": 75.00},
                {
                    "open": 75.05,
                    "close": 75.07,
                    "high": 75.08,
                    "low": 75.05,
                    "ema20": 75.03,
                    "ema50": 75.02,
                    "rsi": 55.0,
                    "macd": 0.20,
                    "macd_signal": 0.10,
                    "atr": 0.03,
                    "volume": 120.0,
                    "volume_avg": 100.0,
                    "body": 0.02,
                    "body_avg": 0.02,
                },
            ]
        )
        instrument = InstrumentConfig(symbol="USDRUBF", figi="FIGI", display_name="USD/RUB")

        signal, reason = evaluate_opening_range(df, self.config, instrument, "LONG")

        self.assertEqual(signal, "HOLD")
        self.assertIn("ожидаемое движение мало относительно комиссии", reason)

    def test_imoexf_short_requires_hard_breakdown(self) -> None:
        df = candle_rows(
            [
                {"open": 100.0, "close": 99.8, "high": 100.0, "low": 99.5, "ema20": 100.5, "ema50": 101.0, "macd": -0.20, "macd_signal": -0.10, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.2, "body_avg": 0.2},
                {"open": 99.8, "close": 99.6, "high": 99.9, "low": 99.4, "ema20": 100.3, "ema50": 100.8, "macd": -0.22, "macd_signal": -0.12, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.2, "body_avg": 0.2},
                {"open": 99.6, "close": 99.4, "high": 99.7, "low": 99.3, "ema20": 100.1, "ema50": 100.6, "macd": -0.24, "macd_signal": -0.14, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.2, "body_avg": 0.2},
                {"open": 99.4, "close": 99.2, "high": 99.5, "low": 99.1, "ema20": 99.9, "ema50": 100.4, "macd": -0.26, "macd_signal": -0.16, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.2, "body_avg": 0.2},
                {"open": 99.2, "close": 99.0, "high": 99.3, "low": 98.9, "ema20": 99.7, "ema50": 100.2, "macd": -0.28, "macd_signal": -0.18, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.2, "body_avg": 0.2},
                {"open": 99.0, "close": 98.9, "high": 99.1, "low": 98.8, "ema20": 99.5, "ema50": 100.0, "macd": -0.30, "macd_signal": -0.20, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.1, "body_avg": 0.2},
                {"open": 98.9, "close": 98.82, "high": 98.95, "low": 98.81, "ema20": 99.3, "ema50": 99.9, "macd": -0.32, "macd_signal": -0.22, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.08, "body_avg": 0.2},
                {"open": 98.82, "close": 98.81, "high": 98.9, "low": 98.8, "ema20": 99.2, "ema50": 99.8, "macd": -0.34, "macd_signal": -0.24, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.01, "body_avg": 0.2},
                {"open": 98.81, "close": 98.79, "high": 98.85, "low": 98.78, "ema20": 99.0, "ema50": 99.7, "macd": -0.36, "macd_signal": -0.26, "atr": 2.0, "volume": 160, "volume_avg": 100, "body": 0.02, "body_avg": 0.2},
            ]
        )
        instrument = InstrumentConfig(symbol="IMOEXF", figi="FIGI", display_name="IMOEX")

        signal, reason = evaluate_range_break(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "HOLD")
        self.assertIn("пробой вниз диапазона", reason)

    def test_brk6_uses_rollover_strategy_for_intraday_reversal(self) -> None:
        self.assertIn("trend_rollover", get_primary_strategies("BRK6"))


if __name__ == "__main__":
    unittest.main()
