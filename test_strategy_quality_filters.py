import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import bot_oil_main as mod
from bot_oil_main import BotConfig, InstrumentConfig
from strategy_registry import get_primary_strategies
from strategies.breakdown_continuation import evaluate_signal as evaluate_range_break
from strategies.failed_breakout import evaluate_signal as evaluate_failed_breakout
from strategies.momentum_breakout import evaluate_signal as evaluate_momentum_breakout
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
            "ema200": row.get("ema200", row.get("ema50", row["close"])),
            "ema50_slope": row.get("ema50_slope", 0.0),
            "bb_upper": row.get("bb_upper", row["close"] * 1.01),
            "bb_mid": row.get("bb_mid", row.get("ema20", row["close"])),
            "bb_lower": row.get("bb_lower", row["close"] * 0.99),
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

    def test_opening_range_hands_fx_to_continuation_after_mature_breakout(self) -> None:
        df = candle_rows(
            [
                {"close": 11.100, "high": 11.105, "low": 11.095, "ema20": 11.100, "ema50": 11.100},
                {"close": 11.098, "high": 11.102, "low": 11.094, "ema20": 11.099, "ema50": 11.100},
                {"close": 11.095, "high": 11.100, "low": 11.093, "ema20": 11.098, "ema50": 11.100},
                {"close": 11.092, "high": 11.096, "low": 11.090, "ema20": 11.097, "ema50": 11.099},
                {"close": 11.089, "high": 11.093, "low": 11.086, "ema20": 11.096, "ema50": 11.098},
                {"close": 11.086, "high": 11.090, "low": 11.083, "ema20": 11.094, "ema50": 11.097},
                {"close": 11.080, "high": 11.084, "low": 11.078, "ema20": 11.091, "ema50": 11.096, "rsi": 46.0, "macd": -0.016, "macd_signal": -0.013, "atr": 0.012, "volume": 130.0, "volume_avg": 100.0, "body": 0.006, "body_avg": 0.006},
                {"close": 11.078, "high": 11.082, "low": 11.074, "ema20": 11.089, "ema50": 11.095, "rsi": 45.0, "macd": -0.017, "macd_signal": -0.014, "atr": 0.012, "volume": 130.0, "volume_avg": 100.0, "body": 0.006, "body_avg": 0.006},
                {"close": 11.076, "high": 11.080, "low": 11.072, "ema20": 11.087, "ema50": 11.094, "rsi": 44.0, "macd": -0.018, "macd_signal": -0.015, "atr": 0.012, "volume": 130.0, "volume_avg": 100.0, "body": 0.006, "body_avg": 0.006},
                {"close": 11.074, "high": 11.078, "low": 11.070, "ema20": 11.085, "ema50": 11.093, "rsi": 43.0, "macd": -0.019, "macd_signal": -0.016, "atr": 0.012, "volume": 130.0, "volume_avg": 100.0, "body": 0.006, "body_avg": 0.006},
                {"close": 11.073, "high": 11.077, "low": 11.069, "ema20": 11.083, "ema50": 11.092, "rsi": 42.0, "macd": -0.020, "macd_signal": -0.017, "atr": 0.012, "volume": 130.0, "volume_avg": 100.0, "body": 0.006, "body_avg": 0.006},
                {"close": 11.072, "high": 11.076, "low": 11.068, "ema20": 11.081, "ema50": 11.091, "rsi": 42.0, "macd": -0.021, "macd_signal": -0.018, "atr": 0.012, "volume": 130.0, "volume_avg": 100.0, "body": 0.006, "body_avg": 0.006},
                {"close": 11.071, "high": 11.075, "low": 11.067, "ema20": 11.079, "ema50": 11.090, "rsi": 42.0, "macd": -0.022, "macd_signal": -0.019, "atr": 0.012, "volume": 130.0, "volume_avg": 100.0, "body": 0.006, "body_avg": 0.006},
                {
                    "open": 11.085,
                    "close": 11.075,
                    "high": 11.086,
                    "low": 11.070,
                    "ema20": 11.090,
                    "ema50": 11.095,
                    "rsi": 45.0,
                    "macd": -0.020,
                    "macd_signal": -0.015,
                    "atr": 0.012,
                    "volume": 140.0,
                    "volume_avg": 100.0,
                    "body": 0.010,
                    "body_avg": 0.008,
                },
            ]
        )
        instrument = InstrumentConfig(symbol="CNYRUBF", figi="FIGI", display_name="CNY/RUB")

        signal, reason = evaluate_opening_range(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "HOLD")
        self.assertIn("пробой вниз opening range уже зрелый", reason)

    def test_usdrubf_mature_trend_short_continuation_allows_soft_volume(self) -> None:
        df = candle_rows(
            [
                {"open": 75.62, "close": 75.58, "high": 75.64, "low": 75.56, "ema20": 75.70, "ema50": 75.82, "macd": -0.04, "macd_signal": -0.02, "atr": 0.05, "volume": 95, "volume_avg": 100, "body": 0.04, "body_avg": 0.05},
                {"open": 75.58, "close": 75.54, "high": 75.60, "low": 75.52, "ema20": 75.68, "ema50": 75.80, "macd": -0.05, "macd_signal": -0.03, "atr": 0.05, "volume": 94, "volume_avg": 100, "body": 0.04, "body_avg": 0.05},
                {"open": 75.54, "close": 75.50, "high": 75.56, "low": 75.48, "ema20": 75.65, "ema50": 75.78, "macd": -0.06, "macd_signal": -0.04, "atr": 0.05, "volume": 92, "volume_avg": 100, "body": 0.04, "body_avg": 0.05},
                {"open": 75.50, "close": 75.48, "high": 75.53, "low": 75.46, "ema20": 75.62, "ema50": 75.75, "macd": -0.07, "macd_signal": -0.05, "atr": 0.05, "volume": 91, "volume_avg": 100, "body": 0.02, "body_avg": 0.05},
                {"open": 75.48, "close": 75.45, "high": 75.50, "low": 75.43, "ema20": 75.59, "ema50": 75.72, "macd": -0.08, "macd_signal": -0.06, "atr": 0.05, "volume": 90, "volume_avg": 100, "body": 0.03, "body_avg": 0.05},
                {"open": 75.45, "close": 75.42, "high": 75.47, "low": 75.40, "ema20": 75.56, "ema50": 75.69, "macd": -0.09, "macd_signal": -0.07, "atr": 0.05, "volume": 90, "volume_avg": 100, "body": 0.03, "body_avg": 0.05},
                {"open": 75.42, "close": 75.39, "high": 75.44, "low": 75.37, "ema20": 75.53, "ema50": 75.66, "rsi": 39.0, "macd": -0.10, "macd_signal": -0.08, "atr": 0.05, "volume": 88, "volume_avg": 100, "body": 0.03, "body_avg": 0.05},
                {"open": 75.39, "close": 75.36, "high": 75.42, "low": 75.34, "ema20": 75.50, "ema50": 75.63, "rsi": 38.0, "macd": -0.11, "macd_signal": -0.09, "atr": 0.05, "volume": 86, "volume_avg": 100, "body": 0.03, "body_avg": 0.05},
            ]
        )
        instrument = InstrumentConfig(symbol="USDRUBF", figi="FIGI", display_name="USD/RUB")

        signal, reason = evaluate_range_break(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "SHORT")
        self.assertIn("старший ТФ=SHORT", reason)

    def test_ucm6_prefers_pullback_before_breakout_chase(self) -> None:
        self.assertEqual(get_primary_strategies("UCM6")[:2], ["trend_pullback", "range_break_continuation"])

    def test_ucm6_allows_higher_tf_long_pullback_reclaim(self) -> None:
        from strategies.trend_pullback import evaluate_signal as evaluate_trend_pullback

        df = candle_rows(
            [
                {"open": 6.798, "close": 6.800, "high": 6.802, "low": 6.796, "ema20": 6.795, "ema50": 6.792, "ema50_slope": 0.00001, "macd": 0.0003, "macd_signal": 0.0001, "atr": 0.004, "volume": 85, "volume_avg": 100, "body": 0.002, "body_avg": 0.003, "bb_mid": 6.795, "bb_upper": 6.815},
                {"open": 6.800, "close": 6.803, "high": 6.806, "low": 6.799, "ema20": 6.797, "ema50": 6.793, "ema50_slope": 0.00001, "macd": 0.0006, "macd_signal": 0.0002, "atr": 0.004, "volume": 88, "volume_avg": 100, "body": 0.003, "body_avg": 0.003, "bb_mid": 6.797, "bb_upper": 6.817},
                {"open": 6.803, "close": 6.807, "high": 6.810, "low": 6.801, "ema20": 6.799, "ema50": 6.794, "ema50_slope": 0.00001, "macd": 0.0009, "macd_signal": 0.0003, "atr": 0.004, "volume": 90, "volume_avg": 100, "body": 0.004, "body_avg": 0.003, "bb_mid": 6.799, "bb_upper": 6.819},
                {"open": 6.807, "close": 6.805, "high": 6.811, "low": 6.802, "ema20": 6.801, "ema50": 6.795, "ema50_slope": 0.00001, "macd": 0.0010, "macd_signal": 0.0004, "atr": 0.004, "volume": 82, "volume_avg": 100, "body": 0.002, "body_avg": 0.003, "bb_mid": 6.801, "bb_upper": 6.820},
                {"open": 6.805, "close": 6.802, "high": 6.807, "low": 6.800, "ema20": 6.802, "ema50": 6.796, "ema50_slope": 0.00001, "macd": 0.0008, "macd_signal": 0.0005, "atr": 0.004, "volume": 78, "volume_avg": 100, "body": 0.003, "body_avg": 0.003, "bb_mid": 6.802, "bb_upper": 6.820},
                {"open": 6.802, "close": 6.804, "high": 6.806, "low": 6.800, "ema20": 6.803, "ema50": 6.797, "ema50_slope": 0.00001, "macd": 0.0007, "macd_signal": 0.0005, "atr": 0.004, "volume": 78, "volume_avg": 100, "body": 0.002, "body_avg": 0.003, "bb_mid": 6.803, "bb_upper": 6.820},
                {"open": 6.804, "close": 6.809, "high": 6.812, "low": 6.803, "ema20": 6.804, "ema50": 6.798, "ema50_slope": 0.00001, "rsi": 58.0, "macd": 0.0010, "macd_signal": 0.0006, "atr": 0.004, "volume": 86, "volume_avg": 100, "body": 0.005, "body_avg": 0.003, "bb_mid": 6.804, "bb_upper": 6.821},
            ]
        )
        instrument = InstrumentConfig(symbol="UCM6", figi="FIGI", display_name="CNY/USD")

        signal, reason = evaluate_trend_pullback(df, self.config, instrument, "LONG")

        self.assertEqual(signal, "LONG")
        self.assertIn("старший ТФ=LONG", reason)

    def test_ucm6_allows_fast_short_reversal_before_full_ema50_rollover(self) -> None:
        from strategies.trend_pullback import evaluate_signal as evaluate_trend_pullback

        df = candle_rows(
            [
                {"open": 6.812, "close": 6.814, "high": 6.815, "low": 6.810, "ema20": 6.809, "ema50": 6.804, "ema50_slope": 0.00001, "macd": 0.004, "macd_signal": 0.002, "atr": 0.004, "volume": 90, "volume_avg": 100, "body": 0.002, "body_avg": 0.003, "bb_mid": 6.809, "bb_upper": 6.818, "bb_lower": 6.800},
                {"open": 6.814, "close": 6.813, "high": 6.815, "low": 6.811, "ema20": 6.810, "ema50": 6.805, "ema50_slope": 0.00001, "macd": 0.003, "macd_signal": 0.002, "atr": 0.004, "volume": 92, "volume_avg": 100, "body": 0.001, "body_avg": 0.003, "bb_mid": 6.810, "bb_upper": 6.818, "bb_lower": 6.801},
                {"open": 6.813, "close": 6.811, "high": 6.814, "low": 6.809, "ema20": 6.810, "ema50": 6.806, "ema50_slope": 0.00001, "macd": 0.002, "macd_signal": 0.002, "atr": 0.004, "volume": 95, "volume_avg": 100, "body": 0.002, "body_avg": 0.003, "bb_mid": 6.810, "bb_upper": 6.818, "bb_lower": 6.801},
                {"open": 6.811, "close": 6.808, "high": 6.812, "low": 6.806, "ema20": 6.809, "ema50": 6.806, "ema50_slope": 0.00001, "macd": 0.000, "macd_signal": 0.001, "atr": 0.004, "volume": 98, "volume_avg": 100, "body": 0.003, "body_avg": 0.003, "bb_mid": 6.809, "bb_upper": 6.817, "bb_lower": 6.800},
                {"open": 6.808, "close": 6.804, "high": 6.809, "low": 6.801, "ema20": 6.808, "ema50": 6.806, "ema50_slope": 0.00000, "macd": -0.002, "macd_signal": 0.000, "atr": 0.004, "volume": 110, "volume_avg": 100, "body": 0.004, "body_avg": 0.003, "bb_mid": 6.808, "bb_upper": 6.816, "bb_lower": 6.799},
                {"open": 6.804, "close": 6.799, "high": 6.805, "low": 6.796, "ema20": 6.806, "ema50": 6.805, "ema50_slope": 0.00000, "macd": -0.004, "macd_signal": -0.001, "atr": 0.004, "volume": 120, "volume_avg": 100, "body": 0.005, "body_avg": 0.003, "bb_mid": 6.806, "bb_upper": 6.815, "bb_lower": 6.796},
                {"open": 6.799, "close": 6.794, "high": 6.800, "low": 6.790, "ema20": 6.803, "ema50": 6.804, "ema50_slope": 0.00000, "rsi": 44.0, "macd": -0.006, "macd_signal": -0.002, "atr": 0.004, "volume": 128, "volume_avg": 100, "body": 0.005, "body_avg": 0.003, "bb_mid": 6.803, "bb_upper": 6.813, "bb_lower": 6.792},
            ]
        )
        instrument = InstrumentConfig(symbol="UCM6", figi="FIGI", display_name="CNY/USD")

        signal, reason = evaluate_trend_pullback(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "SHORT")
        self.assertIn("старший ТФ=SHORT", reason)

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

    def test_imoexf_failed_breakout_allows_reversal_long_against_higher_tf_short(self) -> None:
        df = candle_rows(
            [
                {"open": 2739.0, "close": 2738.0, "high": 2740.0, "low": 2737.0, "ema20": 2738.0, "ema50": 2740.0, "macd": -1.8, "macd_signal": -1.5, "atr": 4.0, "volume": 140, "volume_avg": 100, "body": 1.0, "body_avg": 1.0},
                {"open": 2738.0, "close": 2737.0, "high": 2739.0, "low": 2736.0, "ema20": 2737.5, "ema50": 2739.5, "macd": -1.9, "macd_signal": -1.6, "atr": 4.0, "volume": 140, "volume_avg": 100, "body": 1.0, "body_avg": 1.0},
                {"open": 2737.0, "close": 2736.0, "high": 2738.0, "low": 2735.0, "ema20": 2737.0, "ema50": 2739.0, "macd": -2.0, "macd_signal": -1.7, "atr": 4.0, "volume": 140, "volume_avg": 100, "body": 1.0, "body_avg": 1.0},
                {"open": 2736.0, "close": 2735.0, "high": 2737.0, "low": 2734.0, "ema20": 2736.5, "ema50": 2738.5, "macd": -2.1, "macd_signal": -1.8, "atr": 4.0, "volume": 140, "volume_avg": 100, "body": 1.0, "body_avg": 1.0},
                {"open": 2735.0, "close": 2734.0, "high": 2736.0, "low": 2733.0, "ema20": 2736.0, "ema50": 2738.0, "macd": -2.2, "macd_signal": -1.9, "atr": 4.0, "volume": 140, "volume_avg": 100, "body": 1.0, "body_avg": 1.0},
                {"open": 2734.0, "close": 2733.5, "high": 2735.0, "low": 2732.0, "ema20": 2735.0, "ema50": 2737.0, "macd": -2.3, "macd_signal": -2.0, "atr": 4.0, "volume": 140, "volume_avg": 100, "body": 0.5, "body_avg": 1.0},
                {"open": 2733.5, "close": 2732.5, "high": 2734.0, "low": 2728.0, "ema20": 2734.0, "ema50": 2736.0, "rsi": 39.0, "macd": -2.2, "macd_signal": -2.1, "atr": 4.0, "volume": 170, "volume_avg": 100, "body": 1.0, "body_avg": 1.0},
                {"open": 2732.5, "close": 2735.2, "high": 2736.0, "low": 2732.0, "ema20": 2734.0, "ema50": 2736.0, "rsi": 50.0, "macd": -1.8, "macd_signal": -2.0, "atr": 4.0, "volume": 180, "volume_avg": 100, "body": 2.7, "body_avg": 1.0},
            ]
        )
        instrument = InstrumentConfig(symbol="IMOEXF", figi="FIGI", display_name="IMOEX")

        signal, reason = evaluate_failed_breakout(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "LONG")
        self.assertIn("ложный пробой вниз", reason)

    def test_brk6_uses_rollover_strategy_for_intraday_reversal(self) -> None:
        self.assertIn("trend_rollover", get_primary_strategies("BRK6"))

    def test_gnm6_prefers_pullback_before_breakout_and_rollover(self) -> None:
        self.assertEqual(get_primary_strategies("GNM6")[:3], ["trend_pullback", "momentum_breakout", "trend_rollover"])

    def test_gnm6_allows_higher_tf_long_pullback_on_15m_structure(self) -> None:
        from strategies.trend_pullback import evaluate_signal as evaluate_trend_pullback

        df = candle_rows(
            [
                {"open": 4818.0, "close": 4822.0, "high": 4828.0, "low": 4814.0, "ema20": 4816.0, "ema50": 4808.0, "ema200": 4790.0, "ema50_slope": 0.20, "macd": 1.0, "macd_signal": 0.4, "atr": 7.0, "volume": 82, "volume_avg": 100, "body": 4.0, "body_avg": 5.0, "bb_mid": 4816.0, "bb_upper": 4865.0},
                {"open": 4822.0, "close": 4832.0, "high": 4836.0, "low": 4820.0, "ema20": 4820.0, "ema50": 4811.0, "ema200": 4792.0, "ema50_slope": 0.20, "macd": 1.5, "macd_signal": 0.6, "atr": 7.0, "volume": 86, "volume_avg": 100, "body": 10.0, "body_avg": 5.0, "bb_mid": 4820.0, "bb_upper": 4868.0},
                {"open": 4832.0, "close": 4844.0, "high": 4848.0, "low": 4828.0, "ema20": 4824.0, "ema50": 4814.0, "ema200": 4794.0, "ema50_slope": 0.20, "macd": 2.2, "macd_signal": 0.9, "atr": 7.0, "volume": 100, "volume_avg": 100, "body": 12.0, "body_avg": 5.0, "bb_mid": 4824.0, "bb_upper": 4870.0},
                {"open": 4844.0, "close": 4835.0, "high": 4846.0, "low": 4828.0, "ema20": 4828.0, "ema50": 4817.0, "ema200": 4796.0, "ema50_slope": 0.20, "macd": 2.0, "macd_signal": 1.0, "atr": 7.0, "volume": 78, "volume_avg": 100, "body": 9.0, "body_avg": 5.0, "bb_mid": 4828.0, "bb_upper": 4870.0},
                {"open": 4835.0, "close": 4829.0, "high": 4838.0, "low": 4825.0, "ema20": 4829.0, "ema50": 4820.0, "ema200": 4798.0, "ema50_slope": 0.20, "macd": 1.7, "macd_signal": 1.1, "atr": 7.0, "volume": 76, "volume_avg": 100, "body": 6.0, "body_avg": 5.0, "bb_mid": 4829.0, "bb_upper": 4868.0},
                {"open": 4829.0, "close": 4831.0, "high": 4836.0, "low": 4824.0, "ema20": 4830.0, "ema50": 4822.0, "ema200": 4800.0, "ema50_slope": 0.20, "macd": 1.8, "macd_signal": 1.2, "atr": 7.0, "volume": 78, "volume_avg": 100, "body": 2.0, "body_avg": 5.0, "bb_mid": 4830.0, "bb_upper": 4868.0},
                {"open": 4831.0, "close": 4842.0, "high": 4848.0, "low": 4830.0, "ema20": 4832.0, "ema50": 4825.0, "ema200": 4802.0, "ema50_slope": 0.20, "rsi": 58.0, "macd": 2.4, "macd_signal": 1.4, "atr": 7.0, "volume": 90, "volume_avg": 100, "body": 11.0, "body_avg": 5.0, "bb_mid": 4832.0, "bb_upper": 4868.0},
            ]
        )
        instrument = InstrumentConfig(symbol="GNM6", figi="FIGI", display_name="Gold")

        signal, reason = evaluate_trend_pullback(df, self.config, instrument, "LONG")

        self.assertEqual(signal, "LONG")
        self.assertIn("старший ТФ=LONG", reason)

    def test_imoexf_uses_failed_breakout_strategy_for_reversal(self) -> None:
        self.assertIn("failed_breakout", get_primary_strategies("IMOEXF"))

    def test_rbm6_is_bond_index_with_reversal_first_strategy(self) -> None:
        self.assertEqual(mod.get_instrument_group("RBM6").name, "bond_index")
        self.assertEqual(get_primary_strategies("RBM6")[:2], ["failed_breakout", "range_break_continuation"])

    def test_rbm6_failed_breakout_allows_morning_reversal_long(self) -> None:
        df = candle_rows(
            [
                {"open": 12110.0, "close": 12108.0, "high": 12112.0, "low": 12102.0, "ema20": 12110.0, "ema50": 12108.0, "macd": -2.4, "macd_signal": -1.7, "atr": 20.0, "volume": 80, "volume_avg": 100, "body": 2.0, "body_avg": 4.0},
                {"open": 12108.0, "close": 12106.0, "high": 12110.0, "low": 12101.0, "ema20": 12109.0, "ema50": 12108.0, "macd": -2.5, "macd_signal": -1.8, "atr": 20.0, "volume": 90, "volume_avg": 100, "body": 2.0, "body_avg": 4.0},
                {"open": 12106.0, "close": 12105.0, "high": 12109.0, "low": 12100.0, "ema20": 12108.0, "ema50": 12107.0, "macd": -2.6, "macd_signal": -1.9, "atr": 20.0, "volume": 95, "volume_avg": 100, "body": 1.0, "body_avg": 4.0},
                {"open": 12105.0, "close": 12104.0, "high": 12108.0, "low": 12100.0, "ema20": 12107.0, "ema50": 12107.0, "macd": -2.7, "macd_signal": -2.0, "atr": 20.0, "volume": 100, "volume_avg": 100, "body": 1.0, "body_avg": 4.0},
                {"open": 12104.0, "close": 12103.0, "high": 12107.0, "low": 12100.0, "ema20": 12106.0, "ema50": 12106.0, "macd": -2.8, "macd_signal": -2.1, "atr": 20.0, "volume": 105, "volume_avg": 100, "body": 1.0, "body_avg": 4.0},
                {"open": 12103.0, "close": 12101.0, "high": 12105.0, "low": 12100.0, "ema20": 12105.0, "ema50": 12106.0, "macd": -2.9, "macd_signal": -2.2, "atr": 20.0, "volume": 110, "volume_avg": 100, "body": 2.0, "body_avg": 4.0},
                {"open": 12101.0, "close": 12095.0, "high": 12102.0, "low": 12088.0, "ema20": 12100.0, "ema50": 12104.0, "macd": -2.4, "macd_signal": -2.3, "atr": 20.0, "volume": 180, "volume_avg": 100, "body": 6.0, "body_avg": 4.0},
                {"open": 12095.0, "close": 12118.0, "high": 12121.0, "low": 12094.0, "ema20": 12105.0, "ema50": 12108.0, "rsi": 42.0, "macd": -1.7, "macd_signal": -2.0, "atr": 20.0, "volume": 190, "volume_avg": 100, "body": 23.0, "body_avg": 4.0},
            ]
        )
        instrument = InstrumentConfig(symbol="RBM6", figi="FIGI", display_name="RGBI")

        signal, reason = evaluate_failed_breakout(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "LONG")
        self.assertIn("ложный пробой вниз", reason)

    def test_rbm6_volume_reversal_short_can_override_lagging_higher_tf_long(self) -> None:
        df = candle_rows(
            [
                {"open": 12145.0, "close": 12148.0, "high": 12150.0, "low": 12142.0, "ema20": 12138.0, "ema50": 12130.0, "macd": 4.0, "macd_signal": 2.0, "atr": 18.0, "volume": 90, "volume_avg": 100, "body": 3.0, "body_avg": 6.0},
                {"open": 12148.0, "close": 12146.0, "high": 12152.0, "low": 12144.0, "ema20": 12139.0, "ema50": 12131.0, "macd": 4.2, "macd_signal": 2.4, "atr": 18.0, "volume": 95, "volume_avg": 100, "body": 2.0, "body_avg": 6.0},
                {"open": 12146.0, "close": 12143.0, "high": 12149.0, "low": 12140.0, "ema20": 12140.0, "ema50": 12132.0, "macd": 4.0, "macd_signal": 2.8, "atr": 18.0, "volume": 100, "volume_avg": 100, "body": 3.0, "body_avg": 6.0},
                {"open": 12143.0, "close": 12139.0, "high": 12145.0, "low": 12136.0, "ema20": 12141.0, "ema50": 12133.0, "macd": 3.2, "macd_signal": 3.0, "atr": 18.0, "volume": 105, "volume_avg": 100, "body": 4.0, "body_avg": 6.0},
                {"open": 12139.0, "close": 12135.0, "high": 12142.0, "low": 12132.0, "ema20": 12141.0, "ema50": 12134.0, "macd": 2.0, "macd_signal": 3.0, "atr": 18.0, "volume": 110, "volume_avg": 100, "body": 4.0, "body_avg": 6.0},
                {"open": 12135.0, "close": 12131.0, "high": 12138.0, "low": 12129.0, "ema20": 12140.0, "ema50": 12135.0, "macd": 1.0, "macd_signal": 2.5, "atr": 18.0, "volume": 115, "volume_avg": 100, "body": 4.0, "body_avg": 6.0},
                {"open": 12131.0, "close": 12129.0, "high": 12134.0, "low": 12127.0, "ema20": 12138.0, "ema50": 12136.0, "macd": 0.2, "macd_signal": 1.8, "atr": 18.0, "volume": 120, "volume_avg": 100, "body": 2.0, "body_avg": 6.0},
                {"open": 12129.0, "close": 12120.0, "high": 12130.0, "low": 12118.0, "ema20": 12135.0, "ema50": 12132.0, "rsi": 44.0, "macd": -1.4, "macd_signal": 0.2, "atr": 18.0, "volume": 180, "volume_avg": 100, "body": 9.0, "body_avg": 6.0},
            ]
        )
        instrument = InstrumentConfig(symbol="RBM6", figi="FIGI", display_name="RGBI")

        signal, reason = evaluate_range_break(df, self.config, instrument, "LONG")

        self.assertEqual(signal, "SHORT")
        self.assertIn("старший ТФ=LONG", reason)

    def test_rbm6_allows_mature_trend_short_in_persistent_higher_tf_selloff(self) -> None:
        df = candle_rows(
            [
                {"open": 12130.0, "close": 12128.0, "high": 12131.0, "low": 12127.0, "ema20": 12127.0, "ema50": 12120.0, "macd": 2.0, "macd_signal": 1.0, "atr": 8.0, "volume": 90, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
                {"open": 12128.0, "close": 12126.0, "high": 12129.0, "low": 12125.0, "ema20": 12127.5, "ema50": 12121.0, "macd": 1.6, "macd_signal": 1.1, "atr": 8.0, "volume": 92, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
                {"open": 12126.0, "close": 12124.0, "high": 12127.0, "low": 12123.0, "ema20": 12127.0, "ema50": 12122.0, "macd": 1.0, "macd_signal": 1.1, "atr": 8.0, "volume": 95, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
                {"open": 12124.0, "close": 12122.0, "high": 12125.0, "low": 12121.0, "ema20": 12126.0, "ema50": 12122.5, "macd": 0.4, "macd_signal": 1.0, "atr": 8.0, "volume": 96, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
                {"open": 12122.0, "close": 12120.0, "high": 12123.0, "low": 12119.0, "ema20": 12124.5, "ema50": 12122.8, "macd": -0.2, "macd_signal": 0.8, "atr": 8.0, "volume": 98, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
                {"open": 12120.0, "close": 12118.0, "high": 12121.0, "low": 12117.0, "ema20": 12123.0, "ema50": 12122.7, "macd": -0.8, "macd_signal": 0.5, "atr": 8.0, "volume": 102, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
                {"open": 12118.0, "close": 12116.0, "high": 12119.0, "low": 12115.0, "ema20": 12121.5, "ema50": 12122.4, "macd": -1.5, "macd_signal": 0.1, "atr": 8.0, "volume": 104, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
                {"open": 12116.0, "close": 12114.0, "high": 12117.0, "low": 12113.0, "ema20": 12120.0, "ema50": 12122.0, "rsi": 46.0, "macd": -2.0, "macd_signal": -0.3, "atr": 8.0, "volume": 106, "volume_avg": 100, "body": 2.0, "body_avg": 3.0},
            ]
        )
        instrument = InstrumentConfig(symbol="RBM6", figi="FIGI", display_name="RGBI")

        signal, reason = evaluate_range_break(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "SHORT")
        self.assertIn("старший ТФ=SHORT", reason)

    def test_srm6_failed_breakout_allows_reversal_short_against_flat_higher_tf(self) -> None:
        df = candle_rows(
            [
                {"open": 33240.0, "close": 33255.0, "high": 33265.0, "low": 33235.0, "ema20": 33235.0, "ema50": 33220.0, "macd": 18.0, "macd_signal": 14.0, "atr": 90.0, "volume": 130, "volume_avg": 100, "body": 15.0, "body_avg": 20.0},
                {"open": 33255.0, "close": 33270.0, "high": 33290.0, "low": 33250.0, "ema20": 33240.0, "ema50": 33225.0, "macd": 20.0, "macd_signal": 15.0, "atr": 90.0, "volume": 140, "volume_avg": 100, "body": 15.0, "body_avg": 20.0},
                {"open": 33270.0, "close": 33280.0, "high": 33300.0, "low": 33260.0, "ema20": 33245.0, "ema50": 33230.0, "macd": 22.0, "macd_signal": 16.0, "atr": 90.0, "volume": 150, "volume_avg": 100, "body": 10.0, "body_avg": 20.0},
                {"open": 33280.0, "close": 33310.0, "high": 33335.0, "low": 33275.0, "ema20": 33255.0, "ema50": 33235.0, "macd": 24.0, "macd_signal": 17.0, "atr": 90.0, "volume": 180, "volume_avg": 100, "body": 30.0, "body_avg": 20.0},
                {"open": 33310.0, "close": 33320.0, "high": 33350.0, "low": 33300.0, "ema20": 33270.0, "ema50": 33245.0, "macd": 25.0, "macd_signal": 18.0, "atr": 90.0, "volume": 210, "volume_avg": 100, "body": 10.0, "body_avg": 20.0},
                {"open": 33320.0, "close": 33305.0, "high": 33360.0, "low": 33295.0, "ema20": 33285.0, "ema50": 33255.0, "macd": 24.0, "macd_signal": 19.0, "atr": 90.0, "volume": 220, "volume_avg": 100, "body": 15.0, "body_avg": 20.0},
                {"open": 33305.0, "close": 33270.0, "high": 33310.0, "low": 33260.0, "ema20": 33290.0, "ema50": 33265.0, "rsi": 48.0, "macd": 18.0, "macd_signal": 19.0, "atr": 90.0, "volume": 190, "volume_avg": 100, "body": 35.0, "body_avg": 20.0},
            ]
        )
        instrument = InstrumentConfig(symbol="SRM6", figi="FIGI", display_name="SBER")

        signal, reason = evaluate_failed_breakout(df, self.config, instrument, "FLAT")

        self.assertEqual(signal, "SHORT")
        self.assertIn("ложный пробой вверх", reason)

    def test_vbm6_post_gap_chop_blocks_continuation(self) -> None:
        df = candle_rows(
            [
                {"open": 9760.0, "close": 9758.0, "high": 9765.0, "low": 9755.0, "ema20": 9760.0, "ema50": 9762.0, "macd": -1.0, "macd_signal": -0.6, "atr": 18.0, "volume": 80, "volume_avg": 100, "body": 2.0, "body_avg": 10.0},
                {"open": 9758.0, "close": 9755.0, "high": 9761.0, "low": 9752.0, "ema20": 9758.0, "ema50": 9760.0, "macd": -1.2, "macd_signal": -0.7, "atr": 18.0, "volume": 85, "volume_avg": 100, "body": 3.0, "body_avg": 10.0},
                {"open": 9755.0, "close": 9752.0, "high": 9758.0, "low": 9750.0, "ema20": 9756.0, "ema50": 9758.0, "macd": -1.4, "macd_signal": -0.8, "atr": 18.0, "volume": 90, "volume_avg": 100, "body": 3.0, "body_avg": 10.0},
                {"open": 9860.0, "close": 9890.0, "high": 9915.0, "low": 9845.0, "ema20": 9780.0, "ema50": 9765.0, "macd": 7.0, "macd_signal": 1.0, "atr": 20.0, "volume": 230, "volume_avg": 100, "body": 30.0, "body_avg": 10.0},
                {"open": 9890.0, "close": 9870.0, "high": 9905.0, "low": 9860.0, "ema20": 9800.0, "ema50": 9775.0, "macd": 8.0, "macd_signal": 2.0, "atr": 20.0, "volume": 180, "volume_avg": 100, "body": 20.0, "body_avg": 10.0},
                {"open": 9870.0, "close": 9882.0, "high": 9900.0, "low": 9865.0, "ema20": 9820.0, "ema50": 9788.0, "macd": 8.2, "macd_signal": 3.0, "atr": 20.0, "volume": 140, "volume_avg": 100, "body": 12.0, "body_avg": 10.0},
                {"open": 9882.0, "close": 9888.0, "high": 9902.0, "low": 9870.0, "ema20": 9840.0, "ema50": 9800.0, "rsi": 58.0, "macd": 7.0, "macd_signal": 4.0, "atr": 18.0, "volume": 80, "volume_avg": 100, "body": 6.0, "body_avg": 10.0},
                {"open": 9888.0, "close": 9878.0, "high": 9898.0, "low": 9868.0, "ema20": 9855.0, "ema50": 9812.0, "rsi": 56.0, "macd": 6.0, "macd_signal": 4.2, "atr": 18.0, "volume": 78, "volume_avg": 100, "body": 10.0, "body_avg": 10.0},
                {"open": 9878.0, "close": 9884.0, "high": 9894.0, "low": 9872.0, "ema20": 9865.0, "ema50": 9825.0, "rsi": 57.0, "macd": 5.0, "macd_signal": 4.0, "atr": 18.0, "volume": 75, "volume_avg": 100, "body": 6.0, "body_avg": 10.0},
                {"open": 9884.0, "close": 9886.0, "high": 9896.0, "low": 9875.0, "ema20": 9875.0, "ema50": 9840.0, "rsi": 58.0, "macd": 4.5, "macd_signal": 3.9, "atr": 18.0, "volume": 72, "volume_avg": 100, "body": 2.0, "body_avg": 10.0},
                {"open": 9886.0, "close": 9880.0, "high": 9892.0, "low": 9873.0, "ema20": 9880.0, "ema50": 9852.0, "rsi": 56.0, "macd": 4.0, "macd_signal": 3.8, "atr": 18.0, "volume": 70, "volume_avg": 100, "body": 6.0, "body_avg": 10.0},
            ]
        )
        instrument = InstrumentConfig(symbol="VBM6", figi="FIGI", display_name="VTB")

        signal, reason = evaluate_range_break(df, self.config, instrument, "LONG")

        self.assertEqual(signal, "HOLD")
        self.assertIn("VBM6 после гэпа", reason)

    def test_vbm6_post_gap_chop_blocks_failed_breakout(self) -> None:
        df = candle_rows(
            [
                {"open": 9760.0, "close": 9758.0, "high": 9765.0, "low": 9755.0, "ema20": 9760.0, "ema50": 9762.0, "macd": -1.0, "macd_signal": -0.6, "atr": 18.0, "volume": 80, "volume_avg": 100, "body": 2.0, "body_avg": 10.0},
                {"open": 9758.0, "close": 9755.0, "high": 9761.0, "low": 9752.0, "ema20": 9758.0, "ema50": 9760.0, "macd": -1.2, "macd_signal": -0.7, "atr": 18.0, "volume": 85, "volume_avg": 100, "body": 3.0, "body_avg": 10.0},
                {"open": 9755.0, "close": 9752.0, "high": 9758.0, "low": 9750.0, "ema20": 9756.0, "ema50": 9758.0, "macd": -1.4, "macd_signal": -0.8, "atr": 18.0, "volume": 90, "volume_avg": 100, "body": 3.0, "body_avg": 10.0},
                {"open": 9860.0, "close": 9890.0, "high": 9915.0, "low": 9845.0, "ema20": 9780.0, "ema50": 9765.0, "macd": 7.0, "macd_signal": 1.0, "atr": 20.0, "volume": 230, "volume_avg": 100, "body": 30.0, "body_avg": 10.0},
                {"open": 9890.0, "close": 9870.0, "high": 9905.0, "low": 9860.0, "ema20": 9800.0, "ema50": 9775.0, "macd": 8.0, "macd_signal": 2.0, "atr": 20.0, "volume": 180, "volume_avg": 100, "body": 20.0, "body_avg": 10.0},
                {"open": 9870.0, "close": 9882.0, "high": 9900.0, "low": 9865.0, "ema20": 9820.0, "ema50": 9788.0, "macd": 8.2, "macd_signal": 3.0, "atr": 20.0, "volume": 140, "volume_avg": 100, "body": 12.0, "body_avg": 10.0},
                {"open": 9882.0, "close": 9868.0, "high": 9902.0, "low": 9862.0, "ema20": 9875.0, "ema50": 9840.0, "rsi": 52.0, "macd": 5.5, "macd_signal": 4.0, "atr": 18.0, "volume": 80, "volume_avg": 100, "body": 14.0, "body_avg": 10.0},
                {"open": 9868.0, "close": 9882.0, "high": 9898.0, "low": 9865.0, "ema20": 9876.0, "ema50": 9845.0, "rsi": 55.0, "macd": 5.0, "macd_signal": 4.2, "atr": 18.0, "volume": 78, "volume_avg": 100, "body": 14.0, "body_avg": 10.0},
                {"open": 9882.0, "close": 9876.0, "high": 9896.0, "low": 9868.0, "ema20": 9877.0, "ema50": 9850.0, "rsi": 54.0, "macd": 4.7, "macd_signal": 4.1, "atr": 18.0, "volume": 75, "volume_avg": 100, "body": 6.0, "body_avg": 10.0},
                {"open": 9876.0, "close": 9880.0, "high": 9894.0, "low": 9870.0, "ema20": 9878.0, "ema50": 9855.0, "rsi": 55.0, "macd": 4.4, "macd_signal": 4.0, "atr": 18.0, "volume": 72, "volume_avg": 100, "body": 4.0, "body_avg": 10.0},
                {"open": 9880.0, "close": 9874.0, "high": 9892.0, "low": 9871.0, "ema20": 9879.0, "ema50": 9860.0, "rsi": 53.0, "macd": 4.2, "macd_signal": 4.0, "atr": 18.0, "volume": 70, "volume_avg": 100, "body": 6.0, "body_avg": 10.0},
            ]
        )
        instrument = InstrumentConfig(symbol="VBM6", figi="FIGI", display_name="VTB")

        signal, reason = evaluate_failed_breakout(df, self.config, instrument, "FLAT")

        self.assertEqual(signal, "HOLD")
        self.assertIn("VBM6 после гэпа", reason)

    def test_vbm6_failed_breakout_does_not_open_early_long_against_persistent_short_bias(self) -> None:
        df = candle_rows(
            [
                {"open": 9895.0, "close": 9880.0, "high": 9902.0, "low": 9876.0, "ema20": 9892.0, "ema50": 9870.0, "macd": -4.0, "macd_signal": -2.0, "atr": 18.0, "volume": 120, "volume_avg": 100, "body": 15.0, "body_avg": 12.0},
                {"open": 9880.0, "close": 9868.0, "high": 9884.0, "low": 9862.0, "ema20": 9888.0, "ema50": 9868.0, "macd": -5.0, "macd_signal": -2.8, "atr": 18.0, "volume": 130, "volume_avg": 100, "body": 12.0, "body_avg": 12.0},
                {"open": 9868.0, "close": 9852.0, "high": 9870.0, "low": 9848.0, "ema20": 9880.0, "ema50": 9865.0, "macd": -6.0, "macd_signal": -3.5, "atr": 18.0, "volume": 150, "volume_avg": 100, "body": 16.0, "body_avg": 12.0},
                {"open": 9852.0, "close": 9835.0, "high": 9855.0, "low": 9828.0, "ema20": 9868.0, "ema50": 9860.0, "macd": -7.0, "macd_signal": -4.2, "atr": 18.0, "volume": 180, "volume_avg": 100, "body": 17.0, "body_avg": 12.0},
                {"open": 9835.0, "close": 9820.0, "high": 9838.0, "low": 9816.0, "ema20": 9858.0, "ema50": 9854.0, "macd": -8.0, "macd_signal": -5.0, "atr": 18.0, "volume": 190, "volume_avg": 100, "body": 15.0, "body_avg": 12.0},
                {"open": 9820.0, "close": 9810.0, "high": 9825.0, "low": 9808.0, "ema20": 9848.0, "ema50": 9848.0, "macd": -8.4, "macd_signal": -5.8, "atr": 18.0, "volume": 195, "volume_avg": 100, "body": 10.0, "body_avg": 12.0},
                {"open": 9810.0, "close": 9867.0, "high": 9872.0, "low": 9806.0, "ema20": 9840.0, "ema50": 9846.0, "rsi": 49.8, "macd": -6.6, "macd_signal": -6.8, "atr": 20.0, "volume": 150, "volume_avg": 100, "body": 57.0, "body_avg": 12.0},
            ]
        )
        instrument = InstrumentConfig(symbol="VBM6", figi="FIGI", display_name="VTB")

        signal, reason = evaluate_failed_breakout(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "HOLD")
        self.assertIn("старший ТФ не LONG, а SHORT", reason)

    def test_ngj6_prefers_pullback_before_momentum_breakout(self) -> None:
        self.assertEqual(get_primary_strategies("NGJ6")[:2], ["trend_pullback", "momentum_breakout"])

    def test_ngj6_blocks_late_momentum_long_chase(self) -> None:
        df = candle_rows(
            [
                {"open": 2.700, "close": 2.705, "high": 2.707, "low": 2.698, "ema20": 2.700, "ema50": 2.690, "macd": 0.001, "macd_signal": 0.000, "volume": 120, "volume_avg": 100, "body": 0.005, "body_avg": 0.004},
                {"open": 2.705, "close": 2.712, "high": 2.714, "low": 2.704, "ema20": 2.703, "ema50": 2.692, "macd": 0.002, "macd_signal": 0.000, "volume": 125, "volume_avg": 100, "body": 0.007, "body_avg": 0.004},
                {"open": 2.712, "close": 2.718, "high": 2.720, "low": 2.710, "ema20": 2.706, "ema50": 2.695, "macd": 0.003, "macd_signal": 0.001, "volume": 130, "volume_avg": 100, "body": 0.006, "body_avg": 0.004},
                {"open": 2.718, "close": 2.724, "high": 2.726, "low": 2.716, "ema20": 2.710, "ema50": 2.698, "macd": 0.004, "macd_signal": 0.001, "volume": 132, "volume_avg": 100, "body": 0.006, "body_avg": 0.004},
                {"open": 2.724, "close": 2.732, "high": 2.734, "low": 2.722, "ema20": 2.714, "ema50": 2.702, "macd": 0.006, "macd_signal": 0.002, "volume": 140, "volume_avg": 100, "body": 0.008, "body_avg": 0.004},
                {"open": 2.732, "close": 2.741, "high": 2.743, "low": 2.730, "ema20": 2.720, "ema50": 2.708, "macd": 0.008, "macd_signal": 0.003, "volume": 145, "volume_avg": 100, "body": 0.009, "body_avg": 0.005},
                {"open": 2.741, "close": 2.748, "high": 2.750, "low": 2.739, "ema20": 2.726, "ema50": 2.714, "macd": 0.010, "macd_signal": 0.004, "volume": 150, "volume_avg": 100, "body": 0.007, "body_avg": 0.005},
                {"open": 2.748, "close": 2.754, "high": 2.756, "low": 2.746, "ema20": 2.732, "ema50": 2.720, "macd": 0.011, "macd_signal": 0.005, "volume": 155, "volume_avg": 100, "body": 0.006, "body_avg": 0.005},
                {"open": 2.754, "close": 2.762, "high": 2.764, "low": 2.752, "ema20": 2.740, "ema50": 2.728, "rsi": 64.0, "macd": 0.012, "macd_signal": 0.006, "atr": 0.006, "volume": 170, "volume_avg": 100, "body": 0.008, "body_avg": 0.005, "bb_upper": 2.766, "bb_mid": 2.740},
            ]
        )
        instrument = InstrumentConfig(symbol="NGJ6", figi="FIGI", display_name="Natural Gas")

        signal, reason = evaluate_momentum_breakout(df, self.config, instrument, "LONG")

        self.assertEqual(signal, "HOLD")
        self.assertIn("поздний breakout", reason)

    def test_ngj6_allows_volume_reversal_short_against_lagging_higher_tf_long(self) -> None:
        df = candle_rows(
            [
                {"open": 2.740, "close": 2.746, "high": 2.748, "low": 2.738, "ema20": 2.735, "ema50": 2.728, "macd": 0.008, "macd_signal": 0.004, "volume": 120, "volume_avg": 100, "body": 0.006, "body_avg": 0.005},
                {"open": 2.746, "close": 2.752, "high": 2.754, "low": 2.744, "ema20": 2.738, "ema50": 2.730, "macd": 0.010, "macd_signal": 0.005, "volume": 125, "volume_avg": 100, "body": 0.006, "body_avg": 0.005},
                {"open": 2.752, "close": 2.758, "high": 2.760, "low": 2.750, "ema20": 2.742, "ema50": 2.733, "macd": 0.011, "macd_signal": 0.006, "volume": 130, "volume_avg": 100, "body": 0.006, "body_avg": 0.005},
                {"open": 2.758, "close": 2.762, "high": 2.764, "low": 2.756, "ema20": 2.746, "ema50": 2.736, "macd": 0.010, "macd_signal": 0.007, "volume": 135, "volume_avg": 100, "body": 0.004, "body_avg": 0.005},
                {"open": 2.762, "close": 2.755, "high": 2.764, "low": 2.752, "ema20": 2.748, "ema50": 2.739, "macd": 0.006, "macd_signal": 0.007, "volume": 150, "volume_avg": 100, "body": 0.007, "body_avg": 0.005},
                {"open": 2.755, "close": 2.748, "high": 2.756, "low": 2.746, "ema20": 2.749, "ema50": 2.741, "macd": 0.002, "macd_signal": 0.006, "volume": 160, "volume_avg": 100, "body": 0.007, "body_avg": 0.005},
                {"open": 2.748, "close": 2.742, "high": 2.750, "low": 2.740, "ema20": 2.748, "ema50": 2.742, "macd": -0.002, "macd_signal": 0.004, "volume": 170, "volume_avg": 100, "body": 0.006, "body_avg": 0.005},
                {"open": 2.742, "close": 2.736, "high": 2.744, "low": 2.734, "ema20": 2.746, "ema50": 2.743, "macd": -0.006, "macd_signal": 0.001, "volume": 180, "volume_avg": 100, "body": 0.006, "body_avg": 0.005},
                {"open": 2.736, "close": 2.728, "high": 2.738, "low": 2.726, "ema20": 2.742, "ema50": 2.740, "rsi": 42.0, "macd": -0.012, "macd_signal": -0.002, "atr": 0.006, "volume": 210, "volume_avg": 100, "body": 0.008, "body_avg": 0.005, "bb_mid": 2.742},
            ]
        )
        instrument = InstrumentConfig(symbol="NGJ6", figi="FIGI", display_name="Natural Gas")

        signal, reason = evaluate_momentum_breakout(df, self.config, instrument, "LONG")

        self.assertEqual(signal, "SHORT")
        self.assertIn("старший ТФ=LONG", reason)

    def test_ngj6_allows_pullback_reclaim_long_against_lagging_higher_tf_short(self) -> None:
        df = candle_rows(
            [
                {"open": 2.722, "close": 2.718, "high": 2.724, "low": 2.716, "ema20": 2.726, "ema50": 2.720, "macd": -0.006, "macd_signal": -0.003, "volume": 105, "volume_avg": 100, "body": 0.004, "body_avg": 0.005, "bb_mid": 2.726, "bb_upper": 2.738},
                {"open": 2.718, "close": 2.714, "high": 2.720, "low": 2.712, "ema20": 2.724, "ema50": 2.719, "macd": -0.008, "macd_signal": -0.004, "volume": 108, "volume_avg": 100, "body": 0.004, "body_avg": 0.005, "bb_mid": 2.724, "bb_upper": 2.736},
                {"open": 2.714, "close": 2.709, "high": 2.716, "low": 2.706, "ema20": 2.721, "ema50": 2.718, "macd": -0.010, "macd_signal": -0.005, "volume": 112, "volume_avg": 100, "body": 0.005, "body_avg": 0.005, "bb_mid": 2.721, "bb_upper": 2.734},
                {"open": 2.709, "close": 2.704, "high": 2.711, "low": 2.700, "ema20": 2.718, "ema50": 2.716, "macd": -0.011, "macd_signal": -0.006, "volume": 118, "volume_avg": 100, "body": 0.005, "body_avg": 0.005, "bb_mid": 2.718, "bb_upper": 2.732},
                {"open": 2.704, "close": 2.699, "high": 2.706, "low": 2.696, "ema20": 2.714, "ema50": 2.714, "macd": -0.012, "macd_signal": -0.007, "volume": 122, "volume_avg": 100, "body": 0.005, "body_avg": 0.005, "bb_mid": 2.714, "bb_upper": 2.730},
                {"open": 2.699, "close": 2.703, "high": 2.705, "low": 2.697, "ema20": 2.710, "ema50": 2.712, "macd": -0.010, "macd_signal": -0.008, "volume": 110, "volume_avg": 100, "body": 0.004, "body_avg": 0.005, "bb_mid": 2.710, "bb_upper": 2.728},
                {"open": 2.703, "close": 2.711, "high": 2.713, "low": 2.701, "ema20": 2.708, "ema50": 2.711, "macd": -0.007, "macd_signal": -0.008, "volume": 118, "volume_avg": 100, "body": 0.008, "body_avg": 0.005, "bb_mid": 2.708, "bb_upper": 2.726},
                {"open": 2.711, "close": 2.719, "high": 2.721, "low": 2.709, "ema20": 2.709, "ema50": 2.710, "rsi": 54.0, "macd": -0.003, "macd_signal": -0.007, "atr": 0.006, "volume": 122, "volume_avg": 100, "body": 0.008, "body_avg": 0.005, "bb_mid": 2.709, "bb_upper": 2.725},
            ]
        )
        instrument = InstrumentConfig(symbol="NGJ6", figi="FIGI", display_name="Natural Gas")

        signal, reason = evaluate_momentum_breakout(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "LONG")
        self.assertIn("старший ТФ=SHORT", reason)

    def test_ngj6_blocks_weak_short_when_price_reclaims_bollinger_mid(self) -> None:
        df = candle_rows(
            [
                {"open": 2.716, "close": 2.713, "high": 2.718, "low": 2.711, "ema20": 2.720, "ema50": 2.717, "macd": -0.006, "macd_signal": -0.003, "volume": 108, "volume_avg": 100, "body": 0.003, "body_avg": 0.005, "bb_mid": 2.720, "bb_upper": 2.734},
                {"open": 2.713, "close": 2.709, "high": 2.715, "low": 2.707, "ema20": 2.718, "ema50": 2.716, "macd": -0.008, "macd_signal": -0.004, "volume": 110, "volume_avg": 100, "body": 0.004, "body_avg": 0.005, "bb_mid": 2.718, "bb_upper": 2.732},
                {"open": 2.709, "close": 2.704, "high": 2.711, "low": 2.702, "ema20": 2.715, "ema50": 2.714, "macd": -0.010, "macd_signal": -0.005, "volume": 112, "volume_avg": 100, "body": 0.005, "body_avg": 0.005, "bb_mid": 2.715, "bb_upper": 2.730},
                {"open": 2.704, "close": 2.700, "high": 2.706, "low": 2.698, "ema20": 2.712, "ema50": 2.712, "macd": -0.011, "macd_signal": -0.006, "volume": 116, "volume_avg": 100, "body": 0.004, "body_avg": 0.005, "bb_mid": 2.712, "bb_upper": 2.728},
                {"open": 2.700, "close": 2.696, "high": 2.702, "low": 2.694, "ema20": 2.709, "ema50": 2.710, "macd": -0.012, "macd_signal": -0.007, "volume": 120, "volume_avg": 100, "body": 0.004, "body_avg": 0.005, "bb_mid": 2.709, "bb_upper": 2.726},
                {"open": 2.696, "close": 2.690, "high": 2.698, "low": 2.688, "ema20": 2.704, "ema50": 2.707, "macd": -0.014, "macd_signal": -0.008, "volume": 148, "volume_avg": 100, "body": 0.006, "body_avg": 0.005, "bb_mid": 2.704, "bb_upper": 2.722},
                {"open": 2.690, "close": 2.695, "high": 2.699, "low": 2.689, "ema20": 2.700, "ema50": 2.704, "macd": -0.011, "macd_signal": -0.009, "volume": 138, "volume_avg": 100, "body": 0.005, "body_avg": 0.005, "bb_mid": 2.700, "bb_upper": 2.718},
                {"open": 2.695, "close": 2.701, "high": 2.703, "low": 2.694, "ema20": 2.699, "ema50": 2.702, "rsi": 47.0, "macd": -0.005, "macd_signal": -0.008, "atr": 0.006, "volume": 150, "volume_avg": 100, "body": 0.006, "body_avg": 0.005, "bb_mid": 2.699, "bb_upper": 2.716},
            ]
        )
        instrument = InstrumentConfig(symbol="NGJ6", figi="FIGI", display_name="Natural Gas")

        signal, reason = evaluate_momentum_breakout(df, self.config, instrument, "SHORT")

        self.assertEqual(signal, "HOLD")
        self.assertIn("Bollinger", reason)

    def test_ngj6_exit_uses_higher_tf_indicator_context(self) -> None:
        lower_df = candle_rows(
            [
                {"close": 2.740, "ema20": 2.735, "macd": 0.004, "macd_signal": 0.002},
                {"close": 2.742, "ema20": 2.736, "macd": 0.005, "macd_signal": 0.002},
                {"close": 2.744, "ema20": 2.737, "macd": 0.006, "macd_signal": 0.003},
            ]
        )
        higher_df = candle_rows(
            [
                {"close": 2.760, "ema20": 2.742, "macd": 0.010, "macd_signal": 0.006},
                {"close": 2.755, "ema20": 2.746, "macd": 0.007, "macd_signal": 0.007},
                {"close": 2.736, "ema20": 2.744, "macd": 0.003, "macd_signal": 0.006},
            ]
        )
        ngj6 = InstrumentConfig(symbol="NGJ6", figi="FIGI", display_name="Natural Gas")
        gnm6 = InstrumentConfig(symbol="GNM6", figi="FIGI", display_name="Gold")
        brk6 = InstrumentConfig(symbol="BRK6", figi="FIGI", display_name="Brent")
        rbm6 = InstrumentConfig(symbol="RBM6", figi="FIGI", display_name="RGBI")
        imoexf = InstrumentConfig(symbol="IMOEXF", figi="FIGI", display_name="IMOEX")
        usdrubf = InstrumentConfig(symbol="USDRUBF", figi="FIGI", display_name="USD/RUB")

        self.assertIs(mod.select_exit_indicator_df(ngj6, lower_df, higher_df), higher_df)
        self.assertIs(mod.select_exit_indicator_df(gnm6, lower_df, higher_df), higher_df)
        self.assertIs(mod.select_exit_indicator_df(rbm6, lower_df, higher_df), higher_df)
        self.assertIs(mod.select_exit_indicator_df(imoexf, lower_df, higher_df), higher_df)
        self.assertIs(mod.select_exit_indicator_df(usdrubf, lower_df, higher_df), higher_df)
        self.assertIs(mod.select_exit_indicator_df(brk6, lower_df, higher_df), lower_df)
        self.assertTrue(mod.macd_crossed_down_with_ema_loss(higher_df))

    def test_imoexf_short_exit_detects_higher_tf_macd_reclaim(self) -> None:
        higher_df = candle_rows(
            [
                {"close": 2741.0, "ema20": 2748.0, "macd": -2.6, "macd_signal": -1.9},
                {"close": 2745.0, "ema20": 2747.0, "macd": -2.1, "macd_signal": -2.0},
                {"close": 2750.0, "ema20": 2746.0, "macd": -1.4, "macd_signal": -1.9},
            ]
        )

        self.assertTrue(mod.macd_crossed_up_with_ema_reclaim(higher_df))

    def test_ngj6_short_exit_detects_higher_tf_macd_reclaim(self) -> None:
        higher_df = candle_rows(
            [
                {"close": 2.730, "ema20": 2.742, "macd": -0.010, "macd_signal": -0.006},
                {"close": 2.735, "ema20": 2.741, "macd": -0.007, "macd_signal": -0.007},
                {"close": 2.748, "ema20": 2.740, "macd": -0.003, "macd_signal": -0.006},
            ]
        )

        self.assertTrue(mod.macd_crossed_up_with_ema_reclaim(higher_df))

    def test_vbm6_short_does_not_exit_only_on_rsi_oversold_without_reclaim(self) -> None:
        lower_df = candle_rows(
            [
                {"close": 9738.0, "ema20": 9788.0, "macd": -5.8, "macd_signal": -4.9, "rsi": 28.0},
                {"close": 9726.0, "ema20": 9782.0, "macd": -6.4, "macd_signal": -5.2, "rsi": 24.0},
                {"close": 9719.0, "ema20": 9777.0, "macd": -7.1, "macd_signal": -5.4, "rsi": 22.0},
            ]
        )
        higher_df = candle_rows(
            [
                {"close": 9795.0, "ema20": 9890.0, "macd": -32.0, "macd_signal": -22.0, "rsi": 33.0},
                {"close": 9760.0, "ema20": 9868.0, "macd": -40.0, "macd_signal": -28.0, "rsi": 28.0},
                {"close": 9719.0, "ema20": 9845.0, "macd": -51.0, "macd_signal": -34.0, "rsi": 22.0},
            ]
        )
        instrument = InstrumentConfig(symbol="VBM6", figi="FIGI", display_name="VTB")
        state = mod.InstrumentState(
            position_side="SHORT",
            position_qty=1,
            entry_price=9801.0,
            min_price=9719.0,
            breakeven_armed=True,
            entry_strategy="range_break_continuation",
            entry_time="2026-04-21T12:07:00+00:00",
        )

        with patch.object(mod, "get_last_price", return_value=9719.0), patch.object(mod, "close_position") as close_mock:
            mod.check_exit(None, self.config, instrument, state, lower_df, "HOLD", higher_tf_df=higher_df)

        close_mock.assert_not_called()

    def test_usdrubf_short_does_not_exit_only_on_rsi_oversold_without_reclaim(self) -> None:
        lower_df = candle_rows(
            [
                {"close": 74.78, "ema20": 74.93, "macd": -0.14, "macd_signal": -0.09, "rsi": 31.0},
                {"close": 74.70, "ema20": 74.89, "macd": -0.17, "macd_signal": -0.10, "rsi": 28.0},
                {"close": 74.64, "ema20": 74.86, "macd": -0.20, "macd_signal": -0.11, "rsi": 26.0},
            ]
        )
        higher_df = candle_rows(
            [
                {"close": 74.86, "ema20": 75.02, "macd": -0.16, "macd_signal": -0.08, "rsi": 35.0},
                {"close": 74.74, "ema20": 74.95, "macd": -0.19, "macd_signal": -0.10, "rsi": 30.0},
                {"close": 74.64, "ema20": 74.88, "macd": -0.22, "macd_signal": -0.11, "rsi": 26.0},
            ]
        )
        instrument = InstrumentConfig(symbol="USDRUBF", figi="FIGI", display_name="USD/RUB")
        state = mod.InstrumentState(
            position_side="SHORT",
            position_qty=1,
            entry_price=74.91,
            min_price=74.64,
            breakeven_armed=True,
            entry_strategy="opening_range_breakout",
            entry_time="2026-04-21T07:01:00+00:00",
        )

        with patch.object(mod, "get_last_price", return_value=74.64), patch.object(mod, "close_position") as close_mock:
            mod.check_exit(None, self.config, instrument, state, lower_df, "HOLD", higher_tf_df=higher_df)

        close_mock.assert_not_called()

    def test_rbm6_sideways_exhaustion_locks_profit_after_impulse(self) -> None:
        df = candle_rows(
            [
                {"close": 12118.0, "high": 12122.0, "low": 12112.0, "ema20": 12108.0, "macd": 1.0, "macd_signal": 0.2, "rsi": 52.0},
                {"close": 12135.0, "high": 12142.0, "low": 12118.0, "ema20": 12115.0, "macd": 2.0, "macd_signal": 0.5, "rsi": 58.0},
                {"close": 12155.0, "high": 12160.0, "low": 12134.0, "ema20": 12125.0, "macd": 3.0, "macd_signal": 0.8, "rsi": 63.0},
                {"close": 12150.0, "high": 12158.0, "low": 12145.0, "ema20": 12132.0, "macd": 3.1, "macd_signal": 1.0, "rsi": 60.0},
                {"close": 12147.0, "high": 12157.0, "low": 12142.0, "ema20": 12136.0, "macd": 3.0, "macd_signal": 1.1, "rsi": 59.0},
                {"close": 12145.0, "high": 12156.0, "low": 12140.0, "ema20": 12139.0, "macd": 2.8, "macd_signal": 1.2, "rsi": 57.0},
                {"close": 12139.0, "high": 12155.0, "low": 12134.0, "ema20": 12140.0, "macd": 2.5, "macd_signal": 1.3, "rsi": 56.0},
                {"close": 12135.0, "high": 12154.0, "low": 12130.0, "ema20": 12141.0, "macd": 2.2, "macd_signal": 1.4, "rsi": 55.0},
            ]
        )
        instrument = InstrumentConfig(symbol="RBM6", figi="FIGI", display_name="RGBI")
        state = mod.InstrumentState(
            position_side="LONG",
            position_qty=1,
            entry_price=12100.0,
            max_price=12160.0,
            entry_commission_rub=5.0,
        )

        reason = mod.rbm6_sideways_exhaustion_exit_reason(instrument, state, df, 12135.0)

        self.assertIn("RBM6 profit-lock", reason)


if __name__ == "__main__":
    unittest.main()
