import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import bot_oil_main as bot
from strategies.reversal_exit_protection import (
    EXIT_MODEL_VERSION,
    advance_reversal_protection,
    new_reversal_protection_state,
)
from test_strategy_quality_filters import candle_rows, make_config


def completed_30m(*, reversal: bool = False) -> pd.DataFrame:
    origin = pd.Timestamp("2026-10-22T08:00:00Z")
    rows = []
    for index in range(5):
        open_price = 102.0 + index * 0.10
        close_price = open_price + 0.10
        rows.append(
            {
                "time": origin + pd.Timedelta(minutes=30 * index),
                "closed_at": origin + pd.Timedelta(minutes=30 * (index + 1)),
                "open": open_price,
                "high": close_price + 0.10,
                "low": open_price - 0.10,
                "close": close_price,
                "body": abs(close_price - open_price),
                "ao_delta": 0.05,
                "macd_hist": 0.10 + index * 0.01,
            }
        )
    if reversal:
        rows[-2].update(
            open=103.0,
            high=103.1,
            low=102.5,
            close=102.6,
            body=0.4,
            ao_delta=-0.08,
            macd_hist=-0.10,
        )
        rows[-1].update(
            open=102.6,
            high=102.7,
            low=102.1,
            close=102.2,
            body=0.4,
            ao_delta=-0.10,
            macd_hist=-0.20,
        )
    return pd.DataFrame(rows)


class ReversalExitProtectionTests(unittest.TestCase):
    def test_long_moves_to_cost_aware_breakeven_at_one_r(self) -> None:
        state = new_reversal_protection_state("LONG", 100.0, 98.0, entry_atr=1.0)

        updated = advance_reversal_protection(
            state,
            current_price=102.0,
            completed_30m=completed_30m(),
            tick_size=0.1,
        )

        self.assertEqual(updated.phase, "BREAKEVEN")
        self.assertTrue(updated.breakeven_armed)
        self.assertGreater(updated.current_stop, 100.0)
        self.assertEqual(updated.exit_reason, "")

    def test_profit_trail_starts_at_one_point_two_five_r_and_never_loosens(self) -> None:
        state = new_reversal_protection_state("LONG", 100.0, 98.0, entry_atr=1.0)
        first = advance_reversal_protection(
            state,
            current_price=103.0,
            completed_30m=completed_30m(),
            tick_size=0.1,
        )
        self.assertEqual(first.phase, "PROFIT")
        self.assertTrue(first.profit_trail_armed)
        self.assertEqual(first.current_stop, 101.8)

        unchanged = advance_reversal_protection(
            first,
            current_price=102.8,
            completed_30m=completed_30m(),
            tick_size=0.1,
        )
        self.assertGreaterEqual(unchanged.current_stop, first.current_stop)

    def test_short_uses_symmetric_profit_protection(self) -> None:
        state = new_reversal_protection_state("SHORT", 100.0, 102.0, entry_atr=1.0)

        updated = advance_reversal_protection(
            state,
            current_price=97.0,
            completed_30m=completed_30m(),
            tick_size=0.1,
        )

        self.assertEqual(updated.phase, "PROFIT")
        self.assertLess(updated.current_stop, 100.0)
        self.assertEqual(updated.current_stop, 98.2)

    def test_completed_30m_reversal_produces_exit_reason(self) -> None:
        state = new_reversal_protection_state("LONG", 100.0, 98.0, entry_atr=1.0)

        updated = advance_reversal_protection(
            state,
            current_price=102.2,
            completed_30m=completed_30m(reversal=True),
            tick_size=0.1,
        )

        self.assertEqual(updated.exit_reason, "CONFIRMED_REVERSAL")

    def test_old_position_is_not_migrated_when_state_is_reloaded(self) -> None:
        state = bot.InstrumentState(
            entry_strategy="reversal_1h",
            entry_price=100.0,
            entry_stop_price=98.0,
            position_side="LONG",
            position_qty=1,
        )

        with TemporaryDirectory() as directory:
            original = bot.STATE_DIR
            bot.STATE_DIR = Path(directory)
            try:
                bot.save_state("TEST", state)
                restored = bot.load_state("TEST")
            finally:
                bot.STATE_DIR = original

        self.assertEqual(restored.exit_model_version, "")
        self.assertFalse(bot.has_reversal_exit_protection(restored))

    def test_future_state_field_does_not_reset_known_position_data(self) -> None:
        with TemporaryDirectory() as directory:
            original = bot.STATE_DIR
            bot.STATE_DIR = Path(directory)
            try:
                bot.STATE_DIR.mkdir(parents=True, exist_ok=True)
                bot.state_path_for("TEST").write_text(
                    json.dumps(
                        {
                            "entry_strategy": "reversal_1h",
                            "entry_price": 100.0,
                            "position_side": "LONG",
                            "position_qty": 2,
                            "future_exit_field": "newer-version",
                        }
                    ),
                    encoding="utf-8",
                )
                restored = bot.load_state("TEST")
            finally:
                bot.STATE_DIR = original

        self.assertEqual(restored.entry_strategy, "reversal_1h")
        self.assertEqual(restored.entry_price, 100.0)
        self.assertEqual(restored.position_qty, 2)

    def test_new_reversal_entry_records_versioned_protection_and_clear_resets_it(self) -> None:
        state = bot.InstrumentState(
            entry_strategy="reversal_1h",
            position_side="LONG",
            entry_stop_price=98.0,
        )
        entry_df = pd.DataFrame([{"atr": 1.5}])
        stop_plan = {"working_stop_price": 98.0, "emergency_stop_price": 96.0, "distance": 2.0}

        bot.initialize_reversal_exit_protection(
            state,
            strategy_name="reversal_1h",
            entry_price=100.0,
            stop_plan=stop_plan,
            entry_df=entry_df,
        )
        bot.finalize_reversal_exit_protection_entry(state, 100.2)

        self.assertEqual(state.exit_model_version, EXIT_MODEL_VERSION)
        self.assertEqual(state.exit_phase, "RISK")
        self.assertAlmostEqual(state.exit_initial_risk_distance, 2.2)
        self.assertEqual(state.exit_entry_atr, 1.5)
        self.assertTrue(bot.has_reversal_exit_protection(state))

        bot.clear_entry_protection(state)

        self.assertEqual(state.exit_model_version, "")
        self.assertEqual(state.exit_phase, "")
        self.assertIsNone(state.exit_current_stop_price)

    def test_versioned_breakeven_stop_closes_without_legacy_trailing_conditions(self) -> None:
        config = make_config()
        instrument = bot.InstrumentConfig(
            symbol="TEST",
            figi="FIGI",
            display_name="Test",
            min_price_increment=0.1,
        )
        state = bot.InstrumentState(
            entry_strategy="reversal_1h",
            entry_price=100.0,
            entry_stop_price=98.0,
            entry_emergency_stop_price=96.0,
            position_side="LONG",
            position_qty=1,
            max_price=102.0,
            min_price=100.0,
            breakeven_armed=True,
            exit_model_version=EXIT_MODEL_VERSION,
            exit_phase="BREAKEVEN",
            exit_current_stop_price=100.1,
            exit_initial_risk_distance=2.0,
            exit_entry_atr=1.0,
            exit_best_move=2.0,
        )
        frame = candle_rows([{} for _ in range(8)])

        with patch.object(bot, "get_last_price", return_value=100.0), patch.object(bot, "close_position") as close:
            bot.check_exit(
                None,
                config,
                instrument,
                state,
                frame,
                "HOLD",
                reversal_exit_30m_df=pd.DataFrame(),
            )

        close.assert_called_once()
        self.assertIn("Защита безубыточности", close.call_args.args[-1])

    def test_versioned_risk_stop_still_waits_for_completed_hourly_candle(self) -> None:
        config = make_config()
        instrument = bot.InstrumentConfig(
            symbol="TEST",
            figi="FIGI",
            display_name="Test",
            min_price_increment=0.1,
        )
        state = bot.InstrumentState(
            entry_strategy="reversal_1h",
            entry_price=100.0,
            entry_stop_price=98.0,
            entry_emergency_stop_price=95.0,
            position_side="LONG",
            position_qty=1,
            max_price=100.0,
            min_price=97.5,
            exit_model_version=EXIT_MODEL_VERSION,
            exit_phase="RISK",
            exit_current_stop_price=98.0,
            exit_initial_risk_distance=2.0,
            exit_entry_atr=1.0,
        )
        frame = candle_rows([{} for _ in range(8)])
        frame.loc[frame.index[-1], "is_complete"] = False
        frame.loc[frame.index[-1], "close"] = 97.5

        with patch.object(bot, "get_last_price", return_value=97.5), patch.object(bot, "close_position") as close:
            bot.check_exit(
                None,
                config,
                instrument,
                state,
                frame,
                "HOLD",
                reversal_exit_30m_df=pd.DataFrame(),
            )

        close.assert_not_called()

    def test_versioned_phase_survives_restart(self) -> None:
        state = bot.InstrumentState(
            entry_strategy="reversal_1h",
            entry_price=100.0,
            entry_stop_price=98.0,
            position_side="LONG",
            position_qty=1,
            exit_model_version=EXIT_MODEL_VERSION,
            exit_phase="PROFIT",
            exit_current_stop_price=101.8,
            exit_initial_risk_distance=2.0,
            exit_entry_atr=1.0,
            exit_best_move=3.0,
            exit_profit_trail_armed=True,
            exit_last_trail_30m="2026-10-22T10:00:00+00:00",
        )

        with TemporaryDirectory() as directory:
            original = bot.STATE_DIR
            bot.STATE_DIR = Path(directory)
            try:
                bot.save_state("TEST", state)
                restored = bot.load_state("TEST")
            finally:
                bot.STATE_DIR = original

        self.assertTrue(bot.has_reversal_exit_protection(restored))
        self.assertEqual(restored.exit_phase, "PROFIT")
        self.assertEqual(restored.exit_current_stop_price, 101.8)
        self.assertTrue(restored.exit_profit_trail_armed)

    def test_30m_reversal_from_before_entry_cannot_close_new_position(self) -> None:
        state = bot.InstrumentState(
            entry_strategy="reversal_1h",
            entry_price=100.0,
            entry_stop_price=98.0,
            position_side="LONG",
            position_qty=1,
            max_price=102.2,
            min_price=100.0,
            entry_time="2026-10-22T12:00:00+00:00",
            exit_model_version=EXIT_MODEL_VERSION,
            exit_phase="BREAKEVEN",
            exit_current_stop_price=100.1,
            exit_initial_risk_distance=2.0,
            exit_entry_atr=1.0,
            exit_best_move=2.2,
            breakeven_armed=True,
        )

        exit_reason = bot.advance_state_reversal_exit_protection(
            state,
            current_price=102.2,
            completed_30m=completed_30m(reversal=True),
            tick_size=0.1,
        )

        self.assertEqual(exit_reason, "")

    def test_dry_run_reversal_open_activates_new_model_at_public_entry_seam(self) -> None:
        state = bot.InstrumentState()
        config = SimpleNamespace(
            dry_run=True,
            cash_manager_enabled=False,
            ao_canary_enabled=False,
        )
        instrument = bot.InstrumentConfig(
            symbol="TEST",
            figi="FIGI",
            display_name="Test",
            min_price_increment=0.1,
        )
        entry_df = pd.DataFrame([{"atr": 1.0}])
        stop_plan = {
            "working_stop_price": 98.0,
            "emergency_stop_price": 96.0,
            "distance": 2.0,
            "description": "проверочный план",
        }
        sizing = {
            "quantity": 1,
            "money_risk_per_contract_rub": 200.0,
            "risk_budget_rub": 1_000.0,
        }

        with patch.object(bot, "get_market_session", return_value="MAIN"), patch.object(
            bot, "session_allows_new_entries", return_value=True
        ), patch.object(bot, "get_global_daily_loss_block_reason", return_value=""), patch.object(
            bot, "get_daily_loss_recovery_entry_reason", return_value=""
        ), patch.object(bot, "recent_strategy_performance_block_reason", return_value=""), patch.object(
            bot, "get_last_price", return_value=100.0
        ), patch.object(bot, "build_strategy_entry_stop_plan", return_value=stop_plan), patch.object(
            bot, "calculate_position_sizing_context", return_value=sizing
        ), patch.object(bot, "build_position_sizing_lines", return_value=[]), patch.object(
            bot, "build_allocator_summary_text", return_value="размер рассчитан"
        ), patch.object(bot, "save_state"), patch.object(bot, "append_trade_journal"), patch.object(
            bot, "maybe_notify_trade_execution"
        ):
            bot.open_position(
                None,
                config,
                instrument,
                state,
                "LONG",
                "reversal_1h",
                "проверочный вход",
                entry_df=entry_df,
            )

        self.assertTrue(bot.has_reversal_exit_protection(state))
        self.assertEqual(state.exit_phase, "RISK")
        self.assertEqual(state.exit_current_stop_price, 98.0)
        context = bot.build_trade_event_context(state)
        self.assertEqual(context["exit_protection"]["model_version"], EXIT_MODEL_VERSION)


if __name__ == "__main__":
    unittest.main()
