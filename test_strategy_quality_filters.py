import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import bot_oil_main as mod
import strategy_engine
from bot_oil_main import InstrumentConfig
from instrument_groups import DEFAULT_SYMBOLS, uses_unified_reversal_1h
from strategy_registry import get_primary_strategies, get_secondary_strategies
from strategies.reversal_1h import evaluate_signal as evaluate_reversal_1h
from strategies.reversal_core import get_profile as get_reversal_profile


def make_config() -> SimpleNamespace:
    return SimpleNamespace(
        token="",
        account_id="",
        target="",
        symbols=[],
        dry_run=True,
        allow_orders=False,
        tg_token="",
        tg_chat_id="",
        order_quantity=1,
        max_order_quantity=1,
        risk_per_trade_pct=0.05,
        max_margin_usage_pct=0.85,
        portfolio_usage_pct=0.85,
        capital_reserve_pct=0.05,
        base_trade_allocation_pct=0.25,
        poll_seconds=1,
        startup_retry_seconds=1,
        candle_hours=120,
        candle_interval=None,
        candle_interval_minutes=15,
        higher_tf_interval=None,
        higher_tf_interval_minutes=60,
        max_daily_loss=2500.0,
        max_consecutive_errors=3,
        max_cycles=1,
        min_hold_minutes=15,
        stop_loss_pct=0.006,
        breakeven_profit_pct=0.004,
        trailing_stop_pct=0.005,
        ema_slope_threshold=0.0002,
        near_ema20_pct=0.006,
        volume_factor=1.0,
        atr_min_pct=0.0005,
        long_rsi_min=36.0,
        long_rsi_max=64.0,
        short_rsi_min=35.0,
        short_rsi_max=64.0,
        rsi_exit_long=68.0,
        rsi_exit_short=32.0,
    )


def candle_rows(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "time": pd.Timestamp("2026-05-01T10:00:00Z"),
        "is_complete": True,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "ema20": 100.0,
        "ema50": 100.0,
        "rsi": 50.0,
        "macd": 0.0,
        "macd_signal": 0.0,
        "atr": 0.2,
        "volume": 100.0,
        "volume_avg": 100.0,
        "body": 0.5,
        "body_avg": 0.5,
        "bb_upper": 102.0,
        "bb_lower": 98.0,
        "stoch_k": 50.0,
        "stoch_d": 50.0,
        "ao": 0.0,
        "chaikin": 0.0,
    }
    normalized = []
    for index, row in enumerate(rows):
        item = dict(defaults)
        item["time"] = defaults["time"] + pd.Timedelta(minutes=15 * index)
        item.update(row)
        normalized.append(item)
    return pd.DataFrame(normalized)


class StrategyQualityFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = make_config()

    def test_all_default_symbols_use_unified_reversal_only(self) -> None:
        for symbol in DEFAULT_SYMBOLS.split(","):
            self.assertTrue(uses_unified_reversal_1h(symbol), symbol)
            self.assertEqual(get_primary_strategies(symbol), ["reversal_1h"])
            self.assertEqual(get_secondary_strategies(symbol), [])

    def test_legacy_strategy_name_is_not_evaluated_live(self) -> None:
        instrument = InstrumentConfig(symbol="GNM6", figi="FIGI", display_name="Gold")
        df = candle_rows([{}, {}, {}, {}, {}, {}, {}, {}])

        with patch("strategy_engine.get_primary_strategies", return_value=["trend_pullback"]):
            signal, reason, strategy_name = strategy_engine.evaluate_primary_signal_bundle(df, self.config, instrument, "")

        self.assertEqual(signal, "HOLD")
        self.assertEqual(strategy_name, "trend_pullback")
        self.assertIn("больше не поддерживается", reason)

    def test_unified_reversal_symbols_use_longer_bootstrap_lookback(self) -> None:
        self.assertGreaterEqual(mod.get_lower_tf_lookback_hours(self.config, "BMM6", interval_minutes=60), 240)
        self.assertGreaterEqual(mod.get_lower_tf_lookback_hours(self.config, "USDRUBF", interval_minutes=60), 240)
        self.assertGreaterEqual(mod.get_lower_tf_lookback_hours(self.config, "NGK6", interval_minutes=60), 240)
        self.assertGreaterEqual(mod.get_lower_tf_lookback_hours(self.config, "GNM6", interval_minutes=60), 240)

    def test_add_indicators_can_skip_ema200_for_unified_reversal(self) -> None:
        rows = []
        price = 10.0
        for index in range(80):
            price += 0.01
            rows.append(
                {
                    "time": pd.Timestamp("2026-05-01T00:00:00Z") + pd.Timedelta(minutes=15 * index),
                    "is_complete": True,
                    "open": price - 0.02,
                    "high": price + 0.03,
                    "low": price - 0.03,
                    "close": price,
                    "volume": 100 + index,
                }
            )

        result = mod.add_indicators(pd.DataFrame(rows), include_ema200=False)

        self.assertIn("ema20", result.columns)
        self.assertIn("ema50", result.columns)
        self.assertNotIn("ema200", result.columns)

    def test_unified_reversal_allows_short_when_macd_rsi_stoch_and_volume_align(self) -> None:
        df = candle_rows(
            [
                {"close": 100.5, "ema20": 100.4, "ema50": 100.2, "rsi": 58.0, "macd": 0.08, "macd_signal": 0.03, "stoch_k": 70.0, "stoch_d": 65.0, "ao": 0.04},
                {"close": 100.4, "ema20": 100.4, "ema50": 100.2, "rsi": 56.0, "macd": 0.07, "macd_signal": 0.04, "stoch_k": 66.0, "stoch_d": 63.0, "ao": 0.03},
                {"close": 100.3, "ema20": 100.35, "ema50": 100.25, "rsi": 54.0, "macd": 0.05, "macd_signal": 0.04, "stoch_k": 61.0, "stoch_d": 60.0, "ao": 0.02},
                {"close": 100.1, "ema20": 100.30, "ema50": 100.24, "rsi": 51.0, "macd": 0.02, "macd_signal": 0.03, "stoch_k": 53.0, "stoch_d": 56.0, "ao": -0.01},
                {"close": 99.9, "ema20": 100.20, "ema50": 100.22, "rsi": 48.0, "macd": -0.01, "macd_signal": 0.02, "stoch_k": 44.0, "stoch_d": 50.0, "ao": -0.03},
                {"close": 99.6, "ema20": 100.05, "ema50": 100.18, "rsi": 44.0, "macd": -0.04, "macd_signal": 0.00, "stoch_k": 34.0, "stoch_d": 43.0, "ao": -0.06},
                {"close": 99.3, "ema20": 99.90, "ema50": 100.10, "rsi": 40.0, "macd": -0.07, "macd_signal": -0.02, "stoch_k": 25.0, "stoch_d": 35.0, "ao": -0.09},
                {
                    "open": 99.3,
                    "close": 98.8,
                    "high": 99.35,
                    "low": 98.7,
                    "ema20": 99.70,
                    "ema50": 100.00,
                    "rsi": 37.0,
                    "macd": -0.10,
                    "macd_signal": -0.04,
                    "stoch_k": 18.0,
                    "stoch_d": 28.0,
                    "volume": 155.0,
                    "volume_avg": 100.0,
                    "body": 0.50,
                    "body_avg": 0.35,
                    "atr": 0.35,
                    "bb_upper": 101.0,
                    "bb_lower": 98.6,
                    "ao": -0.12,
                    "chaikin": -5.0,
                },
            ]
        )
        instrument = InstrumentConfig(symbol="CNYRUBF", figi="FIGI", display_name="CNY/RUB")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "SHORT")
        self.assertIn("reversal_1h", reason)

    def test_unified_reversal_1h_allows_short_when_macd_ao_and_volume_align(self) -> None:
        df = candle_rows(
            [
                {"close": 100.5, "ema20": 100.4, "ema50": 100.2, "rsi": 58.0, "macd": 0.08, "macd_signal": 0.03, "ao": 0.04},
                {"close": 100.4, "ema20": 100.4, "ema50": 100.2, "rsi": 56.0, "macd": 0.07, "macd_signal": 0.04, "ao": 0.03},
                {"close": 100.3, "ema20": 100.35, "ema50": 100.25, "rsi": 54.0, "macd": 0.05, "macd_signal": 0.04, "ao": 0.02},
                {"close": 100.1, "ema20": 100.30, "ema50": 100.24, "rsi": 51.0, "macd": 0.02, "macd_signal": 0.03, "ao": -0.01},
                {"close": 99.9, "ema20": 100.20, "ema50": 100.22, "rsi": 48.0, "macd": -0.01, "macd_signal": 0.02, "ao": -0.03},
                {"close": 99.6, "ema20": 100.05, "ema50": 100.18, "rsi": 44.0, "macd": -0.04, "macd_signal": 0.00, "ao": -0.06},
                {"close": 99.3, "ema20": 99.90, "ema50": 100.10, "rsi": 40.0, "macd": -0.07, "macd_signal": -0.02, "ao": -0.09},
                {"open": 99.3, "close": 98.8, "high": 99.35, "low": 98.7, "ema20": 99.70, "ema50": 100.00, "rsi": 37.0, "macd": -0.10, "macd_signal": -0.04, "volume": 155.0, "volume_avg": 100.0, "body": 0.50, "body_avg": 0.35, "atr": 0.35, "bb_upper": 101.0, "bb_lower": 98.6, "ao": -0.12, "chaikin": -5.0},
            ]
        )
        instrument = InstrumentConfig(symbol="IMOEXF", figi="FIGI", display_name="MOEX")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "SHORT")
        self.assertIn("reversal_1h", reason)

    def test_unified_reversal_blocks_new_short_when_oscillators_are_exhausted(self) -> None:
        df = candle_rows(
            [
                {"close": 100.8, "ema20": 100.6, "ema50": 100.35, "rsi": 60.0, "macd": 0.08, "macd_signal": 0.05, "ao": 0.06, "stoch_k": 74.0, "stoch_d": 68.0},
                {"close": 100.7, "ema20": 100.55, "ema50": 100.33, "rsi": 59.0, "macd": 0.07, "macd_signal": 0.05, "ao": 0.05, "stoch_k": 72.0, "stoch_d": 66.0},
                {"close": 100.6, "ema20": 100.5, "ema50": 100.3, "rsi": 58.0, "macd": 0.06, "macd_signal": 0.04, "ao": 0.05, "stoch_k": 70.0, "stoch_d": 64.0},
                {"close": 100.4, "ema20": 100.45, "ema50": 100.3, "rsi": 54.0, "macd": 0.03, "macd_signal": 0.04, "ao": 0.01, "stoch_k": 58.0, "stoch_d": 60.0},
                {"close": 100.1, "ema20": 100.35, "ema50": 100.28, "rsi": 46.0, "macd": -0.01, "macd_signal": 0.02, "ao": -0.04, "stoch_k": 42.0, "stoch_d": 50.0},
                {"close": 99.8, "ema20": 100.18, "ema50": 100.20, "rsi": 34.0, "macd": -0.05, "macd_signal": 0.00, "ao": -0.08, "stoch_k": 18.0, "stoch_d": 34.0},
                {"close": 99.4, "ema20": 99.95, "ema50": 100.08, "rsi": 26.0, "macd": -0.09, "macd_signal": -0.03, "ao": -0.13, "stoch_k": 8.0, "stoch_d": 20.0},
                {"close": 99.0, "ema20": 99.65, "ema50": 99.92, "rsi": 24.0, "macd": -0.13, "macd_signal": -0.06, "ao": -0.18, "stoch_k": 5.0, "stoch_d": 12.0, "volume": 160.0, "volume_avg": 100.0, "body": 0.50, "body_avg": 0.30, "atr": 0.35, "bb_upper": 101.0, "bb_lower": 98.5},
            ]
        )
        instrument = InstrumentConfig(symbol="VBM6", figi="FIGI", display_name="VTB")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "HOLD")
        self.assertIn("вход перегрет", reason)

    def test_unified_reversal_allows_long_with_high_stochastic_when_macd_and_ao_confirm(self) -> None:
        df = candle_rows(
            [
                {"close": 99.8, "ema20": 100.2, "ema50": 100.35, "rsi": 35.0, "macd": -0.12, "macd_signal": -0.08, "ao": -0.15, "stoch_k": 16.0, "stoch_d": 24.0},
                {"close": 99.9, "ema20": 100.15, "ema50": 100.32, "rsi": 37.0, "macd": -0.11, "macd_signal": -0.08, "ao": -0.13, "stoch_k": 18.0, "stoch_d": 22.0},
                {"close": 100.0, "ema20": 100.1, "ema50": 100.3, "rsi": 39.0, "macd": -0.10, "macd_signal": -0.06, "ao": -0.12, "stoch_k": 20.0, "stoch_d": 28.0},
                {"close": 100.1, "ema20": 100.05, "ema50": 100.25, "rsi": 42.0, "macd": -0.07, "macd_signal": -0.06, "ao": -0.10, "stoch_k": 36.0, "stoch_d": 30.0},
                {"close": 100.3, "ema20": 100.08, "ema50": 100.20, "rsi": 46.0, "macd": -0.02, "macd_signal": -0.04, "ao": -0.05, "stoch_k": 58.0, "stoch_d": 42.0},
                {"close": 100.6, "ema20": 100.18, "ema50": 100.18, "rsi": 52.0, "macd": 0.03, "macd_signal": -0.01, "ao": 0.01, "stoch_k": 76.0, "stoch_d": 58.0},
                {"close": 100.9, "ema20": 100.34, "ema50": 100.20, "rsi": 56.0, "macd": 0.07, "macd_signal": 0.01, "ao": 0.08, "stoch_k": 88.0, "stoch_d": 70.0},
                {"close": 101.2, "ema20": 100.55, "ema50": 100.28, "rsi": 59.0, "macd": 0.11, "macd_signal": 0.04, "ao": 0.14, "stoch_k": 92.0, "stoch_d": 82.0, "volume": 140.0, "volume_avg": 100.0, "body": 0.45, "body_avg": 0.30, "atr": 0.30, "bb_upper": 101.5, "bb_lower": 99.0},
            ]
        )
        instrument = InstrumentConfig(symbol="USDRUBF", figi="FIGI", display_name="USD/RUB")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "LONG")
        self.assertIn("Stochastic", reason)

        df.loc[df.index[-1], "rsi"] = 75.0
        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "HOLD")
        self.assertIn("вход перегрет", reason)

    def test_unified_reversal_blocks_early_long_with_weak_volume_and_impulse(self) -> None:
        df = candle_rows(
            [
                {"close": 99.8, "ema20": 100.0, "ema50": 100.15, "rsi": 40.8, "macd": -0.12, "macd_signal": -0.08, "ao": -0.14},
                {"close": 99.9, "ema20": 99.98, "ema50": 100.12, "rsi": 41.2, "macd": -0.11, "macd_signal": -0.08, "ao": -0.12},
                {"close": 100.0, "ema20": 99.97, "ema50": 100.08, "rsi": 42.3, "macd": -0.08, "macd_signal": -0.07, "ao": -0.09},
                {"close": 100.1, "ema20": 99.98, "ema50": 100.05, "rsi": 44.8, "macd": -0.03, "macd_signal": -0.05, "ao": -0.03},
                {"close": 100.25, "ema20": 100.02, "ema50": 100.03, "rsi": 46.5, "macd": 0.01, "macd_signal": -0.03, "ao": 0.01},
                {"close": 100.4, "ema20": 100.10, "ema50": 100.02, "rsi": 48.1, "macd": 0.04, "macd_signal": -0.01, "ao": 0.05},
                {"close": 100.55, "ema20": 100.20, "ema50": 100.04, "rsi": 49.7, "macd": 0.06, "macd_signal": 0.01, "ao": 0.08},
                {"close": 100.8, "ema20": 100.35, "ema50": 100.10, "rsi": 51.2, "macd": 0.09, "macd_signal": 0.03, "ao": 0.12, "volume": 62.0, "volume_avg": 100.0, "body": 0.42, "body_avg": 0.48, "atr": 0.28, "bb_upper": 101.2, "bb_lower": 99.3, "stoch_k": 72.0, "stoch_d": 58.0},
            ]
        )
        instrument = InstrumentConfig(symbol="USDRUBF", figi="FIGI", display_name="USD/RUB")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "HOLD")
        self.assertIn("объём слишком слабый", reason)

    def test_unified_reversal_blocks_weak_trend_continuation_without_fresh_cross(self) -> None:
        df = candle_rows(
            [
                {"close": 100.0, "ema20": 99.6, "ema50": 99.1, "rsi": 51.0, "macd": 0.08, "macd_signal": 0.02, "ao": 0.16},
                {"close": 100.3, "ema20": 99.8, "ema50": 99.2, "rsi": 53.0, "macd": 0.10, "macd_signal": 0.03, "ao": 0.18},
                {"close": 100.7, "ema20": 100.0, "ema50": 99.35, "rsi": 55.0, "macd": 0.13, "macd_signal": 0.05, "ao": 0.20},
                {"close": 101.1, "ema20": 100.25, "ema50": 99.55, "rsi": 58.0, "macd": 0.16, "macd_signal": 0.07, "ao": 0.23},
                {"close": 101.5, "ema20": 100.55, "ema50": 99.80, "rsi": 61.0, "macd": 0.20, "macd_signal": 0.10, "ao": 0.26},
                {"close": 101.9, "ema20": 100.90, "ema50": 100.10, "rsi": 64.0, "macd": 0.24, "macd_signal": 0.13, "ao": 0.30},
                {"close": 102.4, "ema20": 101.25, "ema50": 100.40, "rsi": 67.0, "macd": 0.29, "macd_signal": 0.17, "ao": 0.34},
                {
                    "open": 102.3,
                    "close": 102.8,
                    "high": 102.9,
                    "low": 102.1,
                    "ema20": 101.60,
                    "ema50": 100.75,
                    "rsi": 69.0,
                    "macd": 0.31,
                    "macd_signal": 0.20,
                    "ao": 0.27,
                    "volume": 95.0,
                    "volume_avg": 100.0,
                    "body": 0.50,
                    "body_avg": 0.60,
                    "atr": 0.30,
                    "bb_upper": 103.2,
                    "bb_lower": 99.7,
                    "stoch_k": 87.0,
                    "stoch_d": 82.0,
                },
            ]
        )
        instrument = InstrumentConfig(symbol="RNM6", figi="FIGI", display_name="Rosneft")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "HOLD")
        self.assertIn("long не подтверждён", reason)

    def test_hold_diagnostic_marks_clear_unfinished_long_pressure(self) -> None:
        df = candle_rows(
            [
                {"close": 100.0, "ema20": 99.8, "rsi": 52.0, "macd": 0.10, "macd_signal": 0.06, "ao": 0.12},
                {"close": 100.8, "ema20": 100.0, "rsi": 56.0, "macd": 0.16, "macd_signal": 0.08, "ao": 0.20},
            ]
        )

        direction, score = mod.infer_hold_diagnostic_direction(df)

        self.assertEqual(direction, "LONG")
        self.assertGreaterEqual(score, 0.83)

    def test_hold_diagnostic_keeps_mixed_picture_neutral(self) -> None:
        df = candle_rows(
            [
                {"close": 100.0, "ema20": 100.0, "rsi": 50.0, "macd": 0.01, "macd_signal": 0.01, "ao": 0.0},
                {"close": 99.9, "ema20": 100.0, "rsi": 51.0, "macd": 0.02, "macd_signal": 0.01, "ao": -0.01},
            ]
        )

        direction, score = mod.infer_hold_diagnostic_direction(df)

        self.assertEqual(direction, "")
        self.assertEqual(score, 0.0)

    def test_unified_reversal_allows_strong_trend_continuation_without_recent_cross(self) -> None:
        df = candle_rows(
            [
                {"close": 100.0, "ema20": 99.6, "ema50": 99.1, "rsi": 51.0, "macd": 0.08, "macd_signal": 0.02, "ao": 0.16},
                {"close": 100.3, "ema20": 99.8, "ema50": 99.2, "rsi": 53.0, "macd": 0.10, "macd_signal": 0.03, "ao": 0.18},
                {"close": 100.7, "ema20": 100.0, "ema50": 99.35, "rsi": 55.0, "macd": 0.13, "macd_signal": 0.05, "ao": 0.20},
                {"close": 101.1, "ema20": 100.25, "ema50": 99.55, "rsi": 57.0, "macd": 0.16, "macd_signal": 0.07, "ao": 0.23},
                {"close": 101.5, "ema20": 100.55, "ema50": 99.80, "rsi": 59.0, "macd": 0.20, "macd_signal": 0.10, "ao": 0.26},
                {"close": 101.9, "ema20": 100.90, "ema50": 100.10, "rsi": 61.0, "macd": 0.24, "macd_signal": 0.13, "ao": 0.30},
                {"close": 102.2, "ema20": 101.35, "ema50": 100.40, "rsi": 63.0, "macd": 0.28, "macd_signal": 0.17, "ao": 0.34},
                {
                    "open": 102.0,
                    "close": 102.8,
                    "high": 102.9,
                    "low": 101.9,
                    "ema20": 102.32,
                    "ema50": 100.75,
                    "rsi": 65.0,
                    "macd": 0.32,
                    "macd_signal": 0.21,
                    "ao": 0.40,
                    "volume": 135.0,
                    "volume_avg": 100.0,
                    "body": 0.80,
                    "body_avg": 0.60,
                    "atr": 0.30,
                    "bb_upper": 103.2,
                    "bb_lower": 99.7,
                    "stoch_k": 84.0,
                    "stoch_d": 78.0,
                },
            ]
        )
        instrument = InstrumentConfig(symbol="ONU6", figi="FIGI", display_name="Ozon")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "LONG")
        self.assertIn("MACD cross вверх: нет", reason)

    def test_unified_reversal_rejects_low_volume_trend_continuation_without_fresh_cross(self) -> None:
        df = candle_rows(
            [
                {"close": 100.0, "ema20": 100.4, "ema50": 100.9, "rsi": 49.0, "macd": -0.08, "macd_signal": -0.02, "ao": -0.16},
                {"close": 99.7, "ema20": 100.2, "ema50": 100.8, "rsi": 47.0, "macd": -0.10, "macd_signal": -0.03, "ao": -0.18},
                {"close": 99.3, "ema20": 100.0, "ema50": 100.65, "rsi": 45.0, "macd": -0.13, "macd_signal": -0.05, "ao": -0.20},
                {"close": 98.9, "ema20": 99.75, "ema50": 100.45, "rsi": 42.0, "macd": -0.16, "macd_signal": -0.07, "ao": -0.23},
                {"close": 98.5, "ema20": 99.45, "ema50": 100.20, "rsi": 39.0, "macd": -0.20, "macd_signal": -0.10, "ao": -0.26},
                {"close": 98.1, "ema20": 99.10, "ema50": 99.90, "rsi": 36.0, "macd": -0.24, "macd_signal": -0.13, "ao": -0.30},
                {"close": 97.7, "ema20": 98.75, "ema50": 99.55, "rsi": 33.0, "macd": -0.29, "macd_signal": -0.17, "ao": -0.34},
                {
                    "open": 97.8,
                    "close": 97.3,
                    "high": 98.0,
                    "low": 97.2,
                    "ema20": 98.40,
                    "ema50": 99.20,
                    "rsi": 31.0,
                    "macd": -0.31,
                    "macd_signal": -0.20,
                    "ao": -0.38,
                    "volume": 105.0,
                    "volume_avg": 100.0,
                    "body": 0.60,
                    "body_avg": 0.60,
                    "atr": 0.30,
                    "bb_upper": 100.3,
                    "bb_lower": 96.8,
                    "stoch_k": 13.0,
                    "stoch_d": 18.0,
                },
            ]
        )
        instrument = InstrumentConfig(symbol="IMOEXF", figi="FIGI", display_name="MOEX")

        signal, reason = evaluate_reversal_1h(df, self.config, instrument, "")

        self.assertEqual(signal, "HOLD")
        self.assertIn("нет свежего MACD cross вниз", reason)

    def test_unified_reversal_entry_edge_is_not_crushed_by_early_compression(self) -> None:
        state = mod.InstrumentState(
            last_setup_quality_label="medium",
            last_market_regime="compression",
            last_market_regime_confidence=0.891,
        )

        with patch.object(mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")), patch.object(
            mod,
            "get_strategy_regime_health_score",
            return_value=(1.0, "режим compression нейтрален"),
        ):
            edge, label, reason = mod.get_entry_edge_profile(state, "VBM6", "reversal_1h", "LONG")

        self.assertGreaterEqual(edge, 0.60)
        self.assertEqual(label, "confirmed")
        self.assertIn("локальный 1ч", reason)

    def test_reversal_core_profile_is_shared_for_gold_without_special_strategy(self) -> None:
        self.assertEqual(get_reversal_profile("GNM6", 60), get_reversal_profile("RNM6", 60))

    def test_unified_reversal_short_does_not_exit_on_rsi_oversold_while_ao_confirms_trend(self) -> None:
        df = candle_rows(
            [
                {"close": 100.0, "ema20": 100.5, "macd": -0.10, "macd_signal": -0.05, "ao": -0.12, "rsi": 34.0},
                {"close": 99.5, "ema20": 100.2, "macd": -0.12, "macd_signal": -0.07, "ao": -0.14, "rsi": 31.0},
            ]
        )

        self.assertTrue(mod.unified_reversal_pressure_intact(df, "SHORT"))

    def test_unified_reversal_compression_breakout_does_not_crash(self) -> None:
        rows = [
            {"open": 99.96, "high": 100.05, "low": 99.90, "close": 100.00, "ema20": 100.00, "ema50": 100.00, "macd": -0.03, "macd_signal": -0.01, "ao": -0.02},
            {"open": 99.98, "high": 100.06, "low": 99.92, "close": 100.01, "ema20": 100.00, "ema50": 100.00, "macd": -0.02, "macd_signal": -0.01, "ao": -0.01},
            {"open": 100.00, "high": 100.07, "low": 99.94, "close": 100.02, "ema20": 100.00, "ema50": 100.00, "macd": -0.01, "macd_signal": -0.01, "ao": -0.005},
            {"open": 100.01, "high": 100.08, "low": 99.95, "close": 100.03, "ema20": 100.00, "ema50": 100.00, "macd": -0.005, "macd_signal": 0.0, "ao": -0.002},
            {"open": 100.02, "high": 100.09, "low": 99.96, "close": 100.04, "ema20": 100.01, "ema50": 100.00, "macd": 0.002, "macd_signal": 0.0, "ao": 0.002},
            {"open": 100.03, "high": 100.10, "low": 99.97, "close": 100.05, "ema20": 100.02, "ema50": 100.00, "rsi": 50.0, "macd": 0.006, "macd_signal": 0.001, "ao": 0.005},
            {"open": 100.04, "high": 100.11, "low": 99.98, "close": 100.06, "ema20": 100.03, "ema50": 100.01, "rsi": 52.0, "macd": 0.010, "macd_signal": 0.003, "ao": 0.008},
            {"open": 100.05, "high": 100.35, "low": 100.02, "close": 100.30, "ema20": 100.05, "ema50": 100.01, "rsi": 54.0, "macd": 0.018, "macd_signal": 0.006, "ao": 0.015, "atr": 0.10, "bb_upper": 100.55, "bb_lower": 99.55, "volume": 130.0, "volume_avg": 100.0, "body": 0.25, "body_avg": 0.20},
        ]
        instrument = InstrumentConfig(symbol="USDRUBF", figi="FIGI", display_name="USD/RUB")

        signal, reason = evaluate_reversal_1h(candle_rows(rows), self.config, instrument, "")

        self.assertEqual(signal, "LONG")
        self.assertIn("reversal_1h", reason)

    def test_unified_trend_score_distinguishes_live_trend_from_reversal(self) -> None:
        alive = candle_rows(
            [
                {"close": 100.5, "ema20": 100.0, "rsi": 53.0, "macd": 0.20, "macd_signal": 0.12, "ao": 0.10, "chaikin": 1.0, "volume": 90.0},
                {"close": 101.0, "ema20": 100.2, "rsi": 56.0, "macd": 0.25, "macd_signal": 0.14, "ao": 0.14, "chaikin": 2.0, "volume": 105.0},
                {"close": 101.6, "ema20": 100.5, "rsi": 59.0, "macd": 0.31, "macd_signal": 0.17, "ao": 0.19, "chaikin": 3.0, "volume": 120.0},
            ]
        )
        reversed_df = candle_rows(
            [
                {"close": 101.0, "ema20": 100.4, "rsi": 58.0, "macd": 0.25, "macd_signal": 0.14, "ao": 0.15, "chaikin": 2.0},
                {"close": 100.2, "ema20": 100.5, "rsi": 49.0, "macd": 0.10, "macd_signal": 0.13, "ao": 0.04, "chaikin": 0.5},
                {"close": 99.6, "ema20": 100.3, "rsi": 42.0, "macd": -0.04, "macd_signal": 0.08, "ao": -0.06, "chaikin": -1.0},
            ]
        )

        alive_score, _ = mod.assess_unified_trend_alive(alive, "LONG")
        reversed_score, _ = mod.assess_unified_trend_alive(reversed_df, "LONG")

        self.assertGreaterEqual(alive_score, 0.65)
        self.assertLess(reversed_score, 0.65)

    def test_reversal_turnover_gate_requires_edge_to_cover_costs(self) -> None:
        instrument = InstrumentConfig(
            symbol="USDRUBF",
            figi="FIGI",
            display_name="USD/RUB",
            min_price_increment=0.0025,
            min_price_increment_amount=2.5,
        )
        weak = mod.InstrumentState(
            position_side="LONG",
            position_qty=2,
            entry_commission_rub=12.0,
            last_entry_edge_score=0.54,
            last_atr_pct=0.001,
        )
        strong = mod.InstrumentState(
            position_side="LONG",
            position_qty=2,
            entry_commission_rub=12.0,
            last_entry_edge_score=0.82,
            last_atr_pct=0.02,
        )

        weak_allowed, weak_reason = mod.reversal_turnover_gate(instrument, weak, 80.0, "SHORT")
        strong_allowed, _ = mod.reversal_turnover_gate(instrument, strong, 80.0, "SHORT")

        self.assertFalse(weak_allowed)
        self.assertIn("оборот", weak_reason)
        self.assertTrue(strong_allowed)

    def test_unified_trailing_exit_requires_completed_candle_reversal(self) -> None:
        intact = candle_rows(
            [
                {"close": 102.0, "ema20": 100.0, "macd": 0.30, "macd_signal": 0.15, "ao": 0.20},
                {"close": 101.5, "ema20": 100.5, "macd": 0.24, "macd_signal": 0.14, "ao": 0.16},
            ]
        )
        reversed_df = candle_rows(
            [
                {"close": 102.0, "ema20": 100.0, "macd": 0.30, "macd_signal": 0.15, "ao": 0.20},
                {"close": 99.8, "ema20": 100.5, "macd": 0.08, "macd_signal": 0.13, "ao": 0.04},
            ]
        )

        self.assertFalse(mod.unified_trailing_reversal_confirmed(intact, "LONG"))
        self.assertTrue(mod.unified_trailing_reversal_confirmed(reversed_df, "LONG"))

    def test_recovery_mode_allows_only_unified_reversal(self) -> None:
        state = mod.InstrumentState(last_setup_quality_label="strong", last_market_regime="trend_expansion")

        with patch.object(mod, "get_recovery_mode_status", return_value={"active": True, "reason": "recovery"}):
            old_reason = mod.recovery_mode_block_reason(state, "SRM6", "trend_pullback", "LONG")
            unified_reason = mod.recovery_mode_block_reason(state, "SRM6", "reversal_1h", "LONG")

        self.assertIn("Разрешены только", old_reason)
        self.assertEqual(unified_reason, "")

    def test_regime_entry_blocks_removed_legacy_strategy(self) -> None:
        reason = mod.regime_entry_block_reason("SRM6", "trend_pullback", "LONG", "trend_expansion", {})

        self.assertIn("больше не используется", reason)


if __name__ == "__main__":
    unittest.main()
