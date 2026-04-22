import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as mod


class PositionSizingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = mod.InstrumentConfig(
            symbol="CNYRUBF",
            figi="FIGI",
            display_name="FX",
            initial_margin_on_buy=1000.0,
            initial_margin_on_sell=1000.0,
        )
        self.state = mod.InstrumentState(last_higher_tf_bias="SHORT", last_news_bias="NEUTRAL")
        self.config = SimpleNamespace(
            account_id="acc",
            max_margin_usage_pct=0.35,
            portfolio_usage_pct=0.85,
            capital_reserve_pct=0.35,
            base_trade_allocation_pct=0.22,
            risk_per_trade_pct=0.0,
            stop_loss_pct=0.008,
            max_order_quantity=3,
        )

    def test_strong_setup_increases_target_trade_margin(self) -> None:
        self.state.last_setup_quality_label = "strong"
        self.state.last_market_regime = "trend_expansion"
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.08, "связка сильна 3 дня")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.05, "режим trend_expansion рабочий")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "range_break_continuation"
            )

        self.assertGreater(sizing["adaptive_size_multiplier"], 1.0)
        self.assertGreater(sizing["target_trade_margin_rub"], 3740.0)

    def test_weak_setup_and_negative_day_reduce_size(self) -> None:
        self.state.last_setup_quality_label = "weak"
        self.state.last_market_regime = "chop"
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(0.82, "связка слаба 3 дня")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(0.80, "режим chop токсичен")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "range_break_continuation"
            )

        self.assertLess(sizing["adaptive_size_multiplier"], 1.0)
        self.assertLess(sizing["target_trade_margin_rub"], 3740.0)

    def test_sizing_uses_margin_headroom_as_primary_budget(self) -> None:
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.0, "режим mixed нейтрален")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "range_break_continuation"
            )

        self.assertEqual(sizing["working_margin_budget_rub"], 20000.0)
        self.assertEqual(sizing["reserve_rub"], 7000.0)
        self.assertEqual(sizing["allocatable_margin_rub"], 13000.0)
        self.assertEqual(sizing["target_trade_margin_rub"], 3740.0)
        self.assertEqual(sizing["qty_by_headroom"], 20)

    def test_quantity_is_capped_by_allocatable_margin_and_config(self) -> None:
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.4
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.35)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.0, "режим trend_expansion нейтрален")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "opening_range_breakout"
            )

        self.assertGreaterEqual(sizing["qty_by_target"], 3)
        self.assertEqual(sizing["quantity"], 3)

    def test_srm6_short_can_use_single_lot_when_working_budget_is_almost_enough(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="SRM6",
            figi="FIGI",
            display_name="SBER",
            initial_margin_on_buy=6164.1,
            initial_margin_on_sell=6164.1,
        )
        state = mod.InstrumentState(last_higher_tf_bias="SHORT", last_news_bias="NEUTRAL")
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=5972.04
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.20
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("средний", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.0, "режим mixed нейтрален")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, instrument, state, 33000.0, "SHORT", "range_break_continuation"
            )

        self.assertEqual(sizing["qty_by_working"], 0)
        self.assertEqual(sizing["qty_by_headroom"], 0)
        self.assertEqual(sizing["quantity"], 1)

    def test_recovery_mode_further_reduces_size_even_for_strong_setup(self) -> None:
        self.state.last_setup_quality_label = "strong"
        self.state.last_market_regime = "trend_expansion"
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.0, "режим trend_expansion нейтрален")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": True}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "trend_pullback"
            )

        self.assertLess(sizing["adaptive_size_multiplier"], 1.0)
        self.assertTrue(sizing["recovery_mode_active"])

    def test_health_score_can_boost_size_for_consistent_combo(self) -> None:
        self.state.last_setup_quality_label = "medium"
        self.state.last_market_regime = "mixed"
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.12, "связка сильна 3 дня")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.10, "режим mixed рабочий")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "failed_breakout"
            )

        self.assertGreater(sizing["strategy_health_score"], 1.0)
        self.assertIn("сильна 3 дня", sizing["strategy_health_reason"])

    def test_toxic_strategy_regime_reduces_participation(self) -> None:
        self.state.last_setup_quality_label = "strong"
        self.state.last_market_regime = "chop"
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(0.78, "режим chop токсичен")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "failed_breakout"
            )

        self.assertLess(sizing["adaptive_size_multiplier"], 1.0)
        self.assertLess(sizing["strategy_regime_health_score"], 1.0)

    def test_confirmed_edge_boosts_entry_multiplier(self) -> None:
        self.state.last_setup_quality_label = "strong"
        self.state.last_market_regime = "trend_expansion"
        self.state.last_market_regime_confidence = 0.84
        self.state.last_entry_edge_score = 0.83
        self.state.last_entry_edge_label = "high"
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.0, "режим trend_expansion нейтрален")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "range_break_continuation"
            )

        self.assertGreater(sizing["adaptive_size_multiplier"], 1.15)
        self.assertEqual(sizing["entry_edge_label"], "high")

    def test_fragile_edge_reduces_entry_multiplier(self) -> None:
        self.state.last_setup_quality_label = "medium"
        self.state.last_market_regime = "mixed"
        self.state.last_market_regime_confidence = 0.41
        self.state.last_entry_edge_score = 0.34
        self.state.last_entry_edge_label = "fragile"
        snapshot = mod.AccountSnapshot(total_portfolio=25000.0, free_rub=8000.0, blocked_guarantee_rub=9000.0)
        with patch.object(mod, "get_account_snapshot", return_value=snapshot), patch.object(
            mod, "get_margin_headroom_rub", return_value=20000.0
        ), patch.object(
            mod, "get_signal_conviction_weight", return_value=1.0
        ), patch.object(
            mod, "get_session_position_multiplier", return_value=1.0
        ), patch.object(
            mod, "get_instrument_allocation_weight", return_value=("лёгкий", 1.0)
        ), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "нейтральная форма связки")
        ), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.0, "режим mixed нейтрален")
        ), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            sizing = mod.calculate_position_sizing_context(
                None, self.config, self.instrument, self.state, 11.3, "SHORT", "failed_breakout"
            )

        self.assertLess(sizing["adaptive_size_multiplier"], 0.8)
        self.assertEqual(sizing["entry_edge_label"], "fragile")


if __name__ == "__main__":
    unittest.main()
