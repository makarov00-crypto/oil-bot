import unittest
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as mod


class DailyRiskLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = mod.InstrumentConfig(symbol="TEST", figi="FIGI", display_name="Test")
        self.config = SimpleNamespace(
            account_id="acc",
            max_daily_loss=5.0,
            dry_run=False,
        )

    def test_daily_loss_limit_uses_global_closed_net_pnl(self) -> None:
        rows = [
            {"event": "CLOSE", "symbol": "BRK6", "net_pnl_rub": -1800.0},
            {"event": "CLOSE", "symbol": "NGJ6", "net_pnl_rub": -1190.58},
            {"event": "CLOSE", "symbol": "SRM6", "net_pnl_rub": 100.0},
            {"event": "OPEN", "symbol": "CNYRUBF", "net_pnl_rub": -5.0},
        ]
        snapshot = mod.AccountSnapshot(total_portfolio=50000.0, free_rub=10000.0, blocked_guarantee_rub=0.0)

        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows), patch.object(
            mod, "get_account_snapshot", return_value=snapshot
        ):
            status = mod.get_daily_loss_limit_status(None, self.config)

        self.assertTrue(status["allowed"])
        self.assertEqual(status["net_pnl_rub"], -2890.58)
        self.assertEqual(status["limit_rub"], 2500.0)
        self.assertEqual(status["mode"], "recovery")

    def test_open_position_is_blocked_after_global_daily_loss_limit(self) -> None:
        state = mod.InstrumentState(last_signal_summary=["old"])
        snapshot = mod.AccountSnapshot(total_portfolio=50000.0, free_rub=10000.0, blocked_guarantee_rub=0.0)
        rows = [
            {"event": "CLOSE", "symbol": "BRK6", "net_pnl_rub": -2600.0},
        ]

        with TemporaryDirectory() as temp_dir, patch.object(
            mod, "STATE_DIR", mod.Path(temp_dir)
        ), patch.object(
            mod, "get_today_trade_journal_rows", return_value=rows
        ), patch.object(
            mod, "get_account_snapshot", return_value=snapshot
        ), patch.object(
            mod, "get_market_session", return_value="DAY"
        ), patch.object(
            mod, "get_last_price", side_effect=AssertionError("price should not be requested")
        ):
            mod.open_position(None, self.config, self.instrument, state, "LONG", "test_strategy", "entry")

        self.assertEqual(state.position_side, "FLAT")
        self.assertIn("мягкий дневной стоп", state.last_error)
        self.assertEqual(state.last_allocator_quantity, 0)

    def test_hard_daily_loss_limit_still_blocks_all_entries(self) -> None:
        state = mod.InstrumentState(last_signal_summary=["old"])
        snapshot = mod.AccountSnapshot(total_portfolio=50000.0, free_rub=10000.0, blocked_guarantee_rub=0.0)
        rows = [{"event": "CLOSE", "symbol": "BRK6", "net_pnl_rub": -3600.0}]

        with TemporaryDirectory() as temp_dir, patch.object(
            mod, "STATE_DIR", mod.Path(temp_dir)
        ), patch.object(
            mod, "get_today_trade_journal_rows", return_value=rows
        ), patch.object(
            mod, "get_account_snapshot", return_value=snapshot
        ), patch.object(
            mod, "get_market_session", return_value="DAY"
        ), patch.object(
            mod, "get_last_price", side_effect=AssertionError("price should not be requested")
        ):
            mod.open_position(None, self.config, self.instrument, state, "LONG", "test_strategy", "entry")

        self.assertEqual(state.position_side, "FLAT")
        self.assertEqual(state.last_risk_stop_day, mod.datetime.now(mod.MOSCOW_TZ).date().isoformat())
        self.assertIn("глобальный дневной стоп", state.last_error)

    def test_daily_loss_recovery_allows_only_high_quality_entries(self) -> None:
        snapshot = mod.AccountSnapshot(total_portfolio=50000.0, free_rub=10000.0, blocked_guarantee_rub=0.0)
        rows = [{"event": "CLOSE", "symbol": "BRK6", "net_pnl_rub": -2600.0}]
        weak_state = mod.InstrumentState(
            last_entry_edge_score=0.62,
            last_market_regime_confidence=0.74,
            last_higher_tf_bias="LONG",
        )
        strong_state = mod.InstrumentState(
            last_entry_edge_score=0.84,
            last_market_regime_confidence=0.74,
            last_higher_tf_bias="LONG",
        )

        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows), patch.object(
            mod, "get_account_snapshot", return_value=snapshot
        ):
            weak_reason = mod.get_daily_loss_recovery_entry_reason(None, self.config, weak_state, "LONG")
            strong_reason = mod.get_daily_loss_recovery_entry_reason(None, self.config, strong_state, "LONG")

        self.assertIn("только входы высокого качества", weak_reason)
        self.assertEqual(strong_reason, "")

    def test_daily_loss_recovery_reduces_position_size(self) -> None:
        instrument = mod.InstrumentConfig(symbol="CNYRUBF", figi="FIGI", display_name="FX", initial_margin_on_buy=1000.0, initial_margin_on_sell=1000.0)
        state = mod.InstrumentState(
            last_signal="LONG",
            last_setup_quality_label="strong",
            last_market_regime="trend_expansion",
            last_market_regime_confidence=0.81,
            last_entry_edge_score=0.84,
            last_entry_edge_label="high",
            last_higher_tf_bias="LONG",
        )
        config = SimpleNamespace(
            account_id="acc",
            max_daily_loss=5.0,
            dry_run=False,
            max_margin_usage_pct=0.35,
            portfolio_usage_pct=0.85,
            capital_reserve_pct=0.35,
            base_trade_allocation_pct=0.22,
            risk_per_trade_pct=0.0,
            stop_loss_pct=0.008,
            max_order_quantity=3,
        )
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
        ), patch.object(
            mod, "get_today_closed_net_pnl_rub", return_value=-1300.0
        ):
            sizing = mod.calculate_position_sizing_context(None, config, instrument, state, 11.3, "LONG", "trend_pullback")

        self.assertTrue(sizing["daily_loss_recovery_active"])
        self.assertLess(sizing["target_trade_margin_rub"], 3000.0)

    def test_weekend_blocks_currency_but_allows_other_symbols_before_cutoff(self) -> None:
        self.assertTrue(mod.session_allows_new_entries("WEEKEND", "BRK6"))
        self.assertFalse(mod.session_allows_new_entries("WEEKEND", "CNYRUBF"))

    def test_weekend_session_closes_after_1900_moscow(self) -> None:
        saturday_before_cutoff = mod.datetime(2026, 4, 18, 18, 59, tzinfo=mod.MOSCOW_TZ)
        saturday_at_cutoff = mod.datetime(2026, 4, 18, 19, 0, tzinfo=mod.MOSCOW_TZ)
        sunday_before_cutoff = mod.datetime(2026, 4, 19, 18, 59, tzinfo=mod.MOSCOW_TZ)
        sunday_after_cutoff = mod.datetime(2026, 4, 19, 19, 1, tzinfo=mod.MOSCOW_TZ)

        self.assertEqual(mod.get_market_session(saturday_before_cutoff), "WEEKEND")
        self.assertEqual(mod.get_market_session(saturday_at_cutoff), "CLOSED")
        self.assertEqual(mod.get_market_session(sunday_before_cutoff), "WEEKEND")
        self.assertEqual(mod.get_market_session(sunday_after_cutoff), "CLOSED")

    def test_recent_strategy_performance_blocks_toxic_combo(self) -> None:
        today = mod.datetime.now(mod.MOSCOW_TZ).date().isoformat()
        rows = [
            {"time": f"{today}T10:10:00+03:00", "event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -120.0},
            {"time": f"{today}T10:40:00+03:00", "event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -80.0},
            {"time": f"{today}T11:15:00+03:00", "event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -75.0},
            {"time": f"{today}T11:45:00+03:00", "event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -60.0},
            {"time": f"{today}T12:20:00+03:00", "event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": 20.0},
            {"time": f"{today}T12:35:00+03:00", "event": "CLOSE", "symbol": "CNYRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -500.0},
        ]

        stats = mod.calculate_recent_strategy_performance(
            "USDRUBF",
            "opening_range_breakout",
            rows=rows,
        )

        self.assertEqual(stats["closed_count"], 5)
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["net_pnl_rub"], -315.0)
        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows):
            reason = mod.recent_strategy_performance_block_reason("USDRUBF", "opening_range_breakout")

        self.assertIn("daily performance guard", reason)
        self.assertIn("USDRUBF", reason)

    def test_recent_strategy_performance_allows_healthy_combo(self) -> None:
        rows = [
            {"event": "CLOSE", "symbol": "RBM6", "strategy": "range_break_continuation", "net_pnl_rub": 20.0},
            {"event": "CLOSE", "symbol": "RBM6", "strategy": "range_break_continuation", "net_pnl_rub": 15.0},
            {"event": "CLOSE", "symbol": "RBM6", "strategy": "range_break_continuation", "net_pnl_rub": -5.0},
        ]

        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows):
            reason = mod.recent_strategy_performance_block_reason("RBM6", "range_break_continuation")

        self.assertEqual(reason, "")

    def test_recent_strategy_performance_ignores_previous_days(self) -> None:
        rows = [
            {"time": "2026-04-17T10:10:00+03:00", "event": "CLOSE", "symbol": "NGJ6", "strategy": "momentum_breakout", "net_pnl_rub": -220.0},
            {"time": "2026-04-17T11:10:00+03:00", "event": "CLOSE", "symbol": "NGJ6", "strategy": "momentum_breakout", "net_pnl_rub": -180.0},
            {"time": "2026-04-17T12:10:00+03:00", "event": "CLOSE", "symbol": "NGJ6", "strategy": "momentum_breakout", "net_pnl_rub": -160.0},
        ]

        with patch.object(mod, "get_today_trade_journal_rows", return_value=[]), patch.object(
            mod, "get_trade_journal_rows_since", return_value=rows
        ):
            reason = mod.recent_strategy_performance_block_reason("NGJ6", "momentum_breakout")

        self.assertEqual(reason, "")

    def test_strategy_performance_groups_broker_multi_lot_close_as_one_trade(self) -> None:
        today = mod.datetime.now(mod.MOSCOW_TZ).date().isoformat()
        rows = [
            {
                "time": f"{today}T12:15:00+03:00",
                "event": "CLOSE",
                "symbol": "CNYRUBF",
                "side": "LONG",
                "strategy": "opening_range_breakout",
                "qty_lots": 1,
                "net_pnl_rub": -71.77,
                "broker_op_id": "op-1",
                "broker_op_unit": 0,
            },
            {
                "time": f"{today}T12:15:00+03:00",
                "event": "CLOSE",
                "symbol": "CNYRUBF",
                "side": "LONG",
                "strategy": "opening_range_breakout",
                "qty_lots": 1,
                "net_pnl_rub": -71.77,
                "broker_op_id": "op-1",
                "broker_op_unit": 1,
            },
            {
                "time": f"{today}T12:15:00+03:00",
                "event": "CLOSE",
                "symbol": "CNYRUBF",
                "side": "LONG",
                "strategy": "opening_range_breakout",
                "qty_lots": 1,
                "net_pnl_rub": -71.77,
                "broker_op_id": "op-1",
                "broker_op_unit": 2,
            },
        ]

        stats = mod.calculate_recent_strategy_performance(
            "CNYRUBF",
            "opening_range_breakout",
            rows=rows,
        )

        self.assertEqual(stats["closed_count"], 1)
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["net_pnl_rub"], -215.31)
        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows):
            reason = mod.recent_strategy_performance_block_reason("CNYRUBF", "opening_range_breakout")

        self.assertEqual(reason, "")

    def test_intraday_chop_guard_blocks_same_day_losing_combo(self) -> None:
        today = mod.datetime.now(mod.MOSCOW_TZ).date().isoformat()
        rows = [
            {
                "time": f"{today}T10:15:00+03:00",
                "event": "CLOSE",
                "symbol": "SRM6",
                "strategy": "range_break_continuation",
                "net_pnl_rub": -42.0,
            },
            {
                "time": f"{today}T11:20:00+03:00",
                "event": "CLOSE",
                "symbol": "SRM6",
                "strategy": "range_break_continuation",
                "net_pnl_rub": -35.0,
            },
            {
                "time": f"{today}T11:45:00+03:00",
                "event": "CLOSE",
                "symbol": "BRK6",
                "strategy": "range_break_continuation",
                "net_pnl_rub": -500.0,
            },
        ]

        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows):
            reason = mod.recent_strategy_performance_block_reason("SRM6", "range_break_continuation")

        self.assertIn("anti-chop guard", reason)
        self.assertIn("SRM6", reason)

    def test_intraday_chop_guard_allows_combo_after_same_day_win(self) -> None:
        today = mod.datetime.now(mod.MOSCOW_TZ).date().isoformat()
        rows = [
            {
                "time": f"{today}T10:15:00+03:00",
                "event": "CLOSE",
                "symbol": "SRM6",
                "strategy": "range_break_continuation",
                "net_pnl_rub": -42.0,
            },
            {
                "time": f"{today}T11:20:00+03:00",
                "event": "CLOSE",
                "symbol": "SRM6",
                "strategy": "range_break_continuation",
                "net_pnl_rub": 12.0,
            },
        ]

        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows):
            reason = mod.recent_strategy_performance_block_reason("SRM6", "range_break_continuation")

        self.assertEqual(reason, "")

    def test_profit_lock_triggers_after_giving_back_commission_covered_move(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="TEST",
            figi="FIGI",
            display_name="Test",
            min_price_increment=1.0,
            min_price_increment_amount=1.0,
        )
        state = mod.InstrumentState(
            entry_price=100.0,
            max_price=180.0,
            position_qty=1,
            position_side="LONG",
            entry_commission_rub=5.0,
        )

        reason = mod.build_profit_lock_exit_reason(instrument, state, 108.0)

        self.assertIn("Profit-lock", reason)
        self.assertIn("позиция уже давала 80.00 RUB", reason)

    def test_profit_lock_waits_until_move_covers_commission(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="TEST",
            figi="FIGI",
            display_name="Test",
            min_price_increment=1.0,
            min_price_increment_amount=1.0,
        )
        state = mod.InstrumentState(
            entry_price=100.0,
            max_price=130.0,
            position_qty=1,
            position_side="LONG",
            entry_commission_rub=5.0,
        )

        reason = mod.build_profit_lock_exit_reason(instrument, state, 108.0)

        self.assertEqual(reason, "")

    def test_recovery_mode_blocks_non_defensive_strategy_after_losing_series(self) -> None:
        today = mod.datetime.now(mod.MOSCOW_TZ).date().isoformat()
        rows = [
            {"time": f"{today}T10:00:00+03:00", "event": "CLOSE", "symbol": "NGJ6", "strategy": "momentum_breakout", "net_pnl_rub": -80.0},
            {"time": f"{today}T11:00:00+03:00", "event": "CLOSE", "symbol": "NGJ6", "strategy": "momentum_breakout", "net_pnl_rub": -70.0},
        ]
        state = mod.InstrumentState(
            last_setup_quality_label="strong",
            last_market_regime="trend_expansion",
            last_higher_tf_bias="LONG",
        )

        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows):
            reason = mod.recovery_mode_block_reason(state, "NGJ6", "momentum_breakout", "LONG")

        self.assertIn("recovery mode", reason)
        self.assertIn("точечные стратегии", reason)

    def test_recovery_mode_allows_strong_pullback_with_higher_tf_alignment(self) -> None:
        today = mod.datetime.now(mod.MOSCOW_TZ).date().isoformat()
        rows = [
            {"time": f"{today}T10:00:00+03:00", "event": "CLOSE", "symbol": "GNM6", "strategy": "trend_pullback", "net_pnl_rub": -80.0},
            {"time": f"{today}T11:00:00+03:00", "event": "CLOSE", "symbol": "GNM6", "strategy": "trend_pullback", "net_pnl_rub": -70.0},
        ]
        state = mod.InstrumentState(
            last_setup_quality_label="strong",
            last_market_regime="trend_pullback",
            last_higher_tf_bias="LONG",
        )

        with patch.object(mod, "get_today_trade_journal_rows", return_value=rows):
            reason = mod.recovery_mode_block_reason(state, "GNM6", "trend_pullback", "LONG")

        self.assertEqual(reason, "")

    def test_strategy_health_score_penalizes_weak_three_day_combo(self) -> None:
        with patch.object(
            mod,
            "calculate_today_strategy_performance",
            return_value={"closed_count": 2, "net_pnl_rub": -130.0, "win_rate": 0.0},
        ), patch.object(
            mod,
            "calculate_recent_strategy_performance",
            return_value={"closed_count": 5, "net_pnl_rub": -310.0, "win_rate": 20.0},
        ):
            score, reason = mod.get_strategy_health_score("NGJ6", "momentum_breakout")

        self.assertLess(score, 1.0)
        self.assertIn("3 дня", reason)
        self.assertIn("просадка", reason)

    def test_strategy_health_score_rewards_stable_positive_combo(self) -> None:
        with patch.object(
            mod,
            "calculate_today_strategy_performance",
            return_value={"closed_count": 3, "net_pnl_rub": 120.0, "win_rate": 66.7},
        ), patch.object(
            mod,
            "calculate_recent_strategy_performance",
            return_value={"closed_count": 6, "net_pnl_rub": 260.0, "win_rate": 66.7},
        ):
            score, reason = mod.get_strategy_health_score("RBM6", "range_break_continuation")

        self.assertGreater(score, 1.0)
        self.assertIn("сильна 3 дня", reason)
        self.assertIn("edge", reason)

    def test_recent_strategy_regime_performance_uses_context_regime(self) -> None:
        today = mod.datetime.now(mod.MOSCOW_TZ).date().isoformat()
        rows = [
            {
                "time": f"{today}T10:00:00+03:00",
                "event": "CLOSE",
                "symbol": "NGJ6",
                "strategy": "trend_pullback",
                "net_pnl_rub": 90.0,
                "context": {"market_regime": "trend_pullback"},
            },
            {
                "time": f"{today}T11:00:00+03:00",
                "event": "CLOSE",
                "symbol": "NGJ6",
                "strategy": "trend_pullback",
                "net_pnl_rub": -40.0,
                "context": {"market_regime": "chop"},
            },
        ]

        stats = mod.calculate_recent_strategy_regime_performance(
            "NGJ6",
            "trend_pullback",
            "trend_pullback",
            rows=rows,
        )

        self.assertEqual(stats["closed_count"], 1)
        self.assertEqual(stats["net_pnl_rub"], 90.0)

    def test_strategy_regime_health_score_penalizes_toxic_regime(self) -> None:
        with patch.object(
            mod,
            "calculate_recent_strategy_regime_performance",
            return_value={"closed_count": 3, "net_pnl_rub": -160.0, "win_rate": 0.0},
        ):
            score, reason = mod.get_strategy_regime_health_score("NGJ6", "trend_pullback", "chop")

        self.assertLess(score, 1.0)
        self.assertIn("токсичен", reason)

    def test_strategy_regime_health_score_rewards_working_regime(self) -> None:
        with patch.object(
            mod,
            "calculate_recent_strategy_regime_performance",
            return_value={"closed_count": 4, "net_pnl_rub": 180.0, "win_rate": 75.0},
        ):
            score, reason = mod.get_strategy_regime_health_score("RBM6", "failed_breakout", "trend_expansion")

        self.assertGreater(score, 1.0)
        self.assertIn("рабочий", reason)

    def test_strategy_regime_guard_blocks_toxic_combination(self) -> None:
        with patch.object(
            mod,
            "calculate_recent_strategy_regime_performance",
            return_value={
                "symbol": "NGJ6",
                "strategy": "trend_pullback",
                "closed_count": 3,
                "wins": 0,
                "losses": 3,
                "net_pnl_rub": -220.0,
            },
        ):
            reason = mod.strategy_regime_block_reason("NGJ6", "trend_pullback", "chop")

        self.assertIn("strategy-regime guard", reason)
        self.assertIn("в режиме chop", reason)

    def test_strategy_regime_guard_allows_small_sample(self) -> None:
        with patch.object(
            mod,
            "calculate_recent_strategy_regime_performance",
            return_value={
                "symbol": "NGJ6",
                "strategy": "trend_pullback",
                "closed_count": 2,
                "wins": 0,
                "losses": 2,
                "net_pnl_rub": -220.0,
            },
        ):
            reason = mod.strategy_regime_block_reason("NGJ6", "trend_pullback", "chop")

        self.assertEqual(reason, "")

    def test_entry_priority_score_rewards_confirmed_candidate(self) -> None:
        state = mod.InstrumentState(
            last_signal="LONG",
            last_setup_quality_label="strong",
            last_market_regime="trend_expansion",
            last_market_regime_confidence=0.83,
            last_entry_edge_score=0.82,
            last_higher_tf_bias="LONG",
        )

        with patch.object(mod, "get_strategy_health_score", return_value=(1.08, "связка сильна 3 дня")), patch.object(
            mod, "get_strategy_regime_health_score", return_value=(1.07, "режим рабочий")
        ), patch.object(mod, "get_recovery_mode_status", return_value={"active": False}):
            score, reason = mod.calculate_entry_priority_score(state, "BRK6", "range_break_continuation")

        self.assertGreater(score, 0.75)
        self.assertIn("высокое качество входа", reason)
        self.assertIn("режим подтверждён", reason)

    def test_entry_priority_score_penalizes_bad_signal_learning_combo(self) -> None:
        state = mod.InstrumentState(
            last_signal="LONG",
            last_setup_quality_label="strong",
            last_market_regime="trend_expansion",
            last_market_regime_confidence=0.83,
            last_entry_edge_score=0.82,
            last_entry_edge_label="high",
        )

        with TemporaryDirectory() as temp_dir, patch.object(mod, "TRADE_DB_PATH", mod.Path(temp_dir) / "trades.sqlite3"), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "")
        ), patch.object(mod, "get_strategy_regime_health_score", return_value=(1.0, "")), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            for index in range(10):
                mod.append_signal_observation(
                    mod.TRADE_DB_PATH,
                    {
                        "observed_at": f"2026-04-24T10:{index:02d}:00+03:00",
                        "evaluated_at": f"2026-04-24T10:{index:02d}:15+03:00",
                        "symbol": "BRK6",
                        "signal": "LONG",
                        "strategy": "trend_rollover",
                        "decision": "selected",
                        "market_regime": "trend_expansion",
                        "setup_quality": "strong",
                        "move_pct": -1.2 - index * 0.01,
                        "favorable": False,
                        "context": {"entry_edge_label": "high", "candle_time": f"2026-04-24 10:{index:02d}"},
                    },
                )
            score, reason = mod.calculate_entry_priority_score(state, "BRK6", "trend_rollover")

        self.assertLess(score, 0.72)
        self.assertIn("обучение связки: штраф", reason)
        self.assertIn("0% подтверждений", reason)

    def test_entry_priority_score_boosts_good_signal_learning_combo(self) -> None:
        state = mod.InstrumentState(
            last_signal="SHORT",
            last_setup_quality_label="strong",
            last_market_regime="trend_expansion",
            last_market_regime_confidence=0.76,
            last_entry_edge_score=0.79,
            last_entry_edge_label="confirmed",
        )

        with TemporaryDirectory() as temp_dir, patch.object(mod, "TRADE_DB_PATH", mod.Path(temp_dir) / "trades.sqlite3"), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "")
        ), patch.object(mod, "get_strategy_regime_health_score", return_value=(1.0, "")), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            for index in range(8):
                mod.append_signal_observation(
                    mod.TRADE_DB_PATH,
                    {
                        "observed_at": f"2026-04-24T10:{index:02d}:00+03:00",
                        "evaluated_at": f"2026-04-24T10:{index:02d}:15+03:00",
                        "symbol": "UCM6",
                        "signal": "SHORT",
                        "strategy": "opening_range_breakout",
                        "decision": "selected",
                        "market_regime": "trend_expansion",
                        "setup_quality": "strong",
                        "move_pct": 0.9,
                        "favorable": True,
                        "context": {"entry_edge_label": "confirmed", "candle_time": f"2026-04-24 10:{index:02d}"},
                    },
                )
            score, reason = mod.calculate_entry_priority_score(state, "UCM6", "opening_range_breakout")

        self.assertGreater(score, 0.76)
        self.assertIn("обучение связки: бонус", reason)
        self.assertIn("100% подтверждений", reason)

    def test_entry_priority_score_ignores_small_signal_learning_sample(self) -> None:
        state = mod.InstrumentState(
            last_signal="LONG",
            last_setup_quality_label="strong",
            last_market_regime="trend_expansion",
            last_market_regime_confidence=0.83,
            last_entry_edge_score=0.82,
            last_entry_edge_label="high",
        )

        with TemporaryDirectory() as temp_dir, patch.object(mod, "TRADE_DB_PATH", mod.Path(temp_dir) / "trades.sqlite3"), patch.object(
            mod, "get_strategy_health_score", return_value=(1.0, "")
        ), patch.object(mod, "get_strategy_regime_health_score", return_value=(1.0, "")), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            for index in range(2):
                mod.append_signal_observation(
                    mod.TRADE_DB_PATH,
                    {
                        "observed_at": f"2026-04-24T10:{index:02d}:00+03:00",
                        "evaluated_at": f"2026-04-24T10:{index:02d}:15+03:00",
                        "symbol": "BRK6",
                        "signal": "LONG",
                        "strategy": "trend_rollover",
                        "decision": "selected",
                        "market_regime": "trend_expansion",
                        "setup_quality": "strong",
                        "move_pct": -2.0,
                        "favorable": False,
                        "context": {"entry_edge_label": "high", "candle_time": f"2026-04-24 10:{index:02d}"},
                    },
                )
            score, reason = mod.calculate_entry_priority_score(state, "BRK6", "trend_rollover")

        self.assertGreater(score, 0.72)
        self.assertIn("мало данных", reason)

    def test_rank_cycle_entry_candidates_keeps_only_top_priority_set(self) -> None:
        candidates = [
            {"symbol": "BRK6", "priority_score": 0.86, "entry_edge_score": 0.82, "regime_confidence": 0.81, "allocator_quantity": 1},
            {"symbol": "NGJ6", "priority_score": 0.74, "entry_edge_score": 0.71, "regime_confidence": 0.76, "allocator_quantity": 1},
            {"symbol": "IMOEXF", "priority_score": 0.39, "entry_edge_score": 0.34, "regime_confidence": 0.42, "allocator_quantity": 1},
        ]

        selected, deferred = mod.rank_cycle_entry_candidates(candidates, max_entries=2, min_priority_score=0.45)

        self.assertEqual([item["symbol"] for item in selected], ["BRK6", "NGJ6"])
        self.assertEqual([item["symbol"] for item in deferred], ["IMOEXF"])

    def test_rank_cycle_entry_candidates_respects_cycle_budget_and_class_balance(self) -> None:
        candidates = [
            {
                "symbol": "NGJ6",
                "priority_score": 0.83,
                "entry_edge_score": 0.81,
                "regime_confidence": 0.78,
                "allocator_quantity": 1,
                "instrument_class": "тяжёлый",
                "requested_margin_rub": 5800.0,
                "allocatable_margin_rub": 6000.0,
            },
            {
                "symbol": "BRK6",
                "priority_score": 0.79,
                "entry_edge_score": 0.80,
                "regime_confidence": 0.77,
                "allocator_quantity": 2,
                "instrument_class": "базовый",
                "requested_margin_rub": 1800.0,
                "allocatable_margin_rub": 6000.0,
            },
            {
                "symbol": "VBM6",
                "priority_score": 0.76,
                "entry_edge_score": 0.79,
                "regime_confidence": 0.74,
                "allocator_quantity": 2,
                "instrument_class": "базовый",
                "requested_margin_rub": 1900.0,
                "allocatable_margin_rub": 6000.0,
            },
        ]

        selected, deferred = mod.rank_cycle_entry_candidates(candidates, max_entries=2, min_priority_score=0.45)

        self.assertEqual([item["symbol"] for item in selected], ["NGJ6"])
        self.assertEqual([item["symbol"] for item in deferred], ["BRK6", "VBM6"])
        self.assertIn("не хватает свободного бюджета ГО", deferred[0]["defer_reason"])

    def test_rank_cycle_entry_candidates_defers_similar_market_idea(self) -> None:
        candidates = [
            {
                "symbol": "BRK6",
                "signal": "LONG",
                "priority_score": 0.84,
                "entry_edge_score": 0.82,
                "regime_confidence": 0.80,
                "allocator_quantity": 1,
                "instrument_class": "базовый",
                "requested_margin_rub": 1800.0,
                "allocatable_margin_rub": 7000.0,
            },
            {
                "symbol": "NGJ6",
                "signal": "LONG",
                "priority_score": 0.78,
                "entry_edge_score": 0.80,
                "regime_confidence": 0.76,
                "allocator_quantity": 1,
                "instrument_class": "тяжёлый",
                "requested_margin_rub": 5600.0,
                "allocatable_margin_rub": 7000.0,
            },
            {
                "symbol": "GNM6",
                "signal": "SHORT",
                "priority_score": 0.76,
                "entry_edge_score": 0.79,
                "regime_confidence": 0.75,
                "allocator_quantity": 1,
                "instrument_class": "средний",
                "requested_margin_rub": 3200.0,
                "allocatable_margin_rub": 7000.0,
            },
        ]

        selected, deferred = mod.rank_cycle_entry_candidates(candidates, max_entries=3, min_priority_score=0.45)

        self.assertEqual([item["symbol"] for item in selected], ["BRK6", "GNM6"])
        self.assertEqual([item["symbol"] for item in deferred], ["NGJ6"])
        self.assertIn("похожая рыночная идея", deferred[0]["defer_reason"])

    def test_mark_cycle_deferred_candidate_updates_allocator_summary(self) -> None:
        symbol = "TESTRANK"
        state = mod.InstrumentState(last_signal_summary=["старый сигнал"])
        with patch.object(mod, "load_state", return_value=state), patch.object(
            mod, "save_state"
        ) as save_state, patch.object(mod, "append_allocator_decision") as append_decision:
            mod.mark_cycle_deferred_candidate(
                {"symbol": symbol, "signal": "LONG", "priority_score": 0.72},
                "кандидат отложен: есть более сильный сигнал.",
            )

        self.assertEqual(state.last_allocator_quantity, 0)
        self.assertIn("кандидат отложен", state.last_allocator_summary)
        self.assertEqual(state.last_signal_summary[0], "кандидат отложен: есть более сильный сигнал.")
        save_state.assert_called_once()
        append_decision.assert_called_once()

    def test_append_signal_observation_decision_persists_candidate(self) -> None:
        candidate = {
            "symbol": "BRK6",
            "signal": "LONG",
            "strategy_name": "range_break_continuation",
            "observed_at": "2026-04-24T10:00:00+03:00",
            "candle_time": "2026-04-24 10:00",
            "observed_price": 80.0,
            "priority_score": 0.83,
            "entry_edge_score": 0.86,
            "market_regime": "trend_expansion",
            "regime_confidence": 0.8,
            "setup_quality_label": "strong",
            "priority_reason": "сильный пробой",
        }

        with TemporaryDirectory() as temp_dir, patch.object(mod, "TRADE_DB_PATH", mod.Path(temp_dir) / "trade.sqlite3"):
            uid = mod.append_signal_observation_decision(
                candidate,
                decision="selected",
                decision_reason="выбран для входа",
            )
            rows = mod.load_signal_observations(mod.TRADE_DB_PATH)

        self.assertTrue(uid)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["decision"], "selected")
        self.assertEqual(rows[0]["observed_price"], 80.0)
        self.assertEqual(rows[0]["context"]["observation_key"], "2026-04-24 10:00")

    def test_append_signal_observation_decision_dedupes_same_candle(self) -> None:
        candidate = {
            "symbol": "BRK6",
            "signal": "LONG",
            "strategy_name": "range_break_continuation",
            "observed_price": 80.0,
            "priority_score": 0.83,
            "entry_edge_score": 0.86,
            "candle_time": "2026-04-24 10:00",
        }

        with TemporaryDirectory() as temp_dir, patch.object(mod, "TRADE_DB_PATH", mod.Path(temp_dir) / "trade.sqlite3"):
            first_uid = mod.append_signal_observation_decision(
                {**candidate, "observed_at": "2026-04-24T10:00:05+03:00"},
                decision="selected",
                decision_reason="первый проход",
            )
            second_uid = mod.append_signal_observation_decision(
                {**candidate, "observed_at": "2026-04-24T10:00:15+03:00", "observed_price": 80.2},
                decision="selected",
                decision_reason="второй проход",
            )
            rows = mod.load_signal_observations(mod.TRADE_DB_PATH)

        self.assertEqual(first_uid, second_uid)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["decision_reason"], "второй проход")
        self.assertEqual(rows[0]["observed_price"], 80.2)

    def test_mark_cycle_deferred_candidate_persists_learning_reason(self) -> None:
        with TemporaryDirectory() as temp_dir, patch.object(mod, "ALLOCATOR_DECISIONS_PATH", mod.Path(temp_dir) / "allocator_decisions.jsonl"), patch.object(
            mod, "load_state", return_value=mod.InstrumentState()
        ), patch.object(mod, "save_state"):
            mod.mark_cycle_deferred_candidate(
                {
                    "symbol": "BRK6",
                    "signal": "LONG",
                    "priority_score": 0.64,
                    "entry_edge_score": 0.82,
                    "learning_adjustment": -0.08,
                    "learning_reason": "обучение связки: штраф -0.08, 0% подтверждений, 10 наблюд.",
                },
                "не хватило ГО",
            )
            rows = [mod.json.loads(line) for line in mod.ALLOCATOR_DECISIONS_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["learning_adjustment"], -0.08)
        self.assertIn("штраф -0.08", rows[0]["learning_reason"])

    def test_update_signal_observation_outcomes_marks_favorable_short(self) -> None:
        instrument = mod.InstrumentConfig(symbol="UCM6", figi="FIGI", display_name="USD/CNY")
        old_time = (mod.datetime.now(mod.UTC) - mod.timedelta(minutes=20)).astimezone(mod.MOSCOW_TZ).isoformat()

        with TemporaryDirectory() as temp_dir, patch.object(mod, "TRADE_DB_PATH", mod.Path(temp_dir) / "trade.sqlite3"):
            mod.append_signal_observation_decision(
                {
                    "symbol": "UCM6",
                    "signal": "SHORT",
                    "strategy_name": "breakdown_continuation",
                    "observed_at": old_time,
                    "observed_price": 7.20,
                    "priority_score": 0.8,
                    "entry_edge_score": 0.82,
                },
                decision="deferred",
                decision_reason="не хватило ГО",
                horizon_minutes=15,
            )
            with patch.object(
                mod,
                "get_price_near_observation_horizon",
                return_value=(7.10, mod.datetime(2026, 4, 24, 10, 15, tzinfo=mod.MOSCOW_TZ)),
            ), patch.object(mod, "get_last_price", side_effect=AssertionError("live price should not be used")):
                updated = mod.update_signal_observation_outcomes(None, self.config, [instrument], horizon_minutes=15)
            rows = mod.load_signal_observations(mod.TRADE_DB_PATH)

        self.assertEqual(updated, 1)
        self.assertTrue(rows[0]["favorable"])
        self.assertGreater(rows[0]["move_pct"], 0)
        self.assertEqual(rows[0]["evaluated_at"], "2026-04-24T10:15:00+03:00")

    def test_update_signal_observation_outcomes_waits_for_horizon_snapshot(self) -> None:
        instrument = mod.InstrumentConfig(symbol="UCM6", figi="FIGI", display_name="USD/CNY")
        old_time = (mod.datetime.now(mod.UTC) - mod.timedelta(minutes=20)).astimezone(mod.MOSCOW_TZ).isoformat()

        with TemporaryDirectory() as temp_dir, patch.object(mod, "TRADE_DB_PATH", mod.Path(temp_dir) / "trade.sqlite3"):
            mod.append_signal_observation_decision(
                {
                    "symbol": "UCM6",
                    "signal": "SHORT",
                    "strategy_name": "breakdown_continuation",
                    "observed_at": old_time,
                    "observed_price": 7.20,
                    "priority_score": 0.8,
                    "entry_edge_score": 0.82,
                    "candle_time": "2026-04-24 10:00",
                },
                decision="deferred",
                decision_reason="не хватило ГО",
                horizon_minutes=15,
            )
            with patch.object(mod, "get_price_near_observation_horizon", return_value=None), patch.object(
                mod, "get_last_price", side_effect=AssertionError("live price should not be used")
            ):
                updated = mod.update_signal_observation_outcomes(None, self.config, [instrument], horizon_minutes=15)
            rows = mod.load_signal_observations(mod.TRADE_DB_PATH)

        self.assertEqual(updated, 0)
        self.assertEqual(rows[0]["evaluated_at"], "")
        self.assertIsNone(rows[0]["current_price"])

    def test_capital_rotation_selects_strong_margin_blocked_candidate_over_weak_open_position(self) -> None:
        watchlist = [
            mod.InstrumentConfig(symbol="BRK6", figi="1", display_name="Brent"),
            mod.InstrumentConfig(symbol="VBM6", figi="2", display_name="Bonds"),
        ]
        candidate = {
            "symbol": "BRK6",
            "signal": "LONG",
            "priority_score": 0.88,
            "priority_reason": "высокое качество входа, режим подтверждён",
            "entry_edge_score": 0.84,
            "allocator_quantity": 0,
        }
        weak_open_state = mod.InstrumentState(
            position_qty=2,
            position_side="SHORT",
            entry_strategy="range_break_continuation",
            entry_time=(mod.datetime.now(mod.UTC) - mod.timedelta(minutes=25)).isoformat(),
            last_signal="HOLD",
            last_market_regime="chop",
            last_market_regime_confidence=0.44,
            last_entry_edge_score=0.22,
            position_variation_margin_rub=-18.0,
            entry_commission_rub=4.8,
        )
        flat_state = mod.InstrumentState()

        def fake_load_state(symbol: str) -> mod.InstrumentState:
            return {"VBM6": weak_open_state, "BRK6": flat_state}[symbol]

        with patch.object(mod, "load_state", side_effect=fake_load_state), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            plan = mod.select_capital_rotation_plan(watchlist, [candidate])

        self.assertIsNotNone(plan)
        self.assertEqual(plan["candidate"]["symbol"], "BRK6")
        self.assertEqual(plan["position"]["instrument"].symbol, "VBM6")

    def test_capital_rotation_does_not_replace_strong_profitable_position(self) -> None:
        watchlist = [
            mod.InstrumentConfig(symbol="BRK6", figi="1", display_name="Brent"),
            mod.InstrumentConfig(symbol="GNM6", figi="2", display_name="Gold"),
        ]
        candidate = {
            "symbol": "BRK6",
            "signal": "LONG",
            "priority_score": 0.86,
            "priority_reason": "высокое качество входа, режим подтверждён",
            "entry_edge_score": 0.82,
            "allocator_quantity": 0,
        }
        strong_open_state = mod.InstrumentState(
            position_qty=1,
            position_side="SHORT",
            entry_strategy="trend_pullback",
            entry_time=(mod.datetime.now(mod.UTC) - mod.timedelta(minutes=40)).isoformat(),
            last_signal="SHORT",
            last_market_regime="trend_pullback",
            last_market_regime_confidence=0.78,
            last_entry_edge_score=0.84,
            position_variation_margin_rub=320.0,
            entry_commission_rub=8.8,
        )
        flat_state = mod.InstrumentState()

        def fake_load_state(symbol: str) -> mod.InstrumentState:
            return {"GNM6": strong_open_state, "BRK6": flat_state}[symbol]

        with patch.object(mod, "load_state", side_effect=fake_load_state), patch.object(
            mod, "get_recovery_mode_status", return_value={"active": False}
        ):
            plan = mod.select_capital_rotation_plan(watchlist, [candidate])

        self.assertIsNone(plan)

    def test_adaptive_exit_profile_tightens_in_chop_recovery_mode(self) -> None:
        config = SimpleNamespace(
            min_hold_minutes=30,
            breakeven_profit_pct=0.008,
            trailing_stop_pct=0.006,
        )
        instrument = mod.InstrumentConfig(symbol="NGJ6", figi="FIGI", display_name="Gas")
        state = mod.InstrumentState(
            entry_strategy="trend_pullback",
            last_market_regime="chop",
            last_setup_quality_label="medium",
        )
        base_profile = mod.ExitProfile(min_hold_minutes=30, breakeven_profit_pct=0.008, trailing_stop_pct=0.006)

        with patch.object(mod, "get_recovery_mode_status", return_value={"active": True}):
            adapted, reason = mod.get_adaptive_exit_profile(config, instrument, state, base_profile)

        self.assertLessEqual(adapted.min_hold_minutes, 20)
        self.assertLessEqual(adapted.trailing_stop_pct, 0.0040)
        self.assertIn("recovery mode", reason)

    def test_adaptive_exit_profile_allows_strong_trend_more_room(self) -> None:
        config = SimpleNamespace(
            min_hold_minutes=20,
            breakeven_profit_pct=0.004,
            trailing_stop_pct=0.0045,
        )
        instrument = mod.InstrumentConfig(symbol="BRK6", figi="FIGI", display_name="Brent")
        state = mod.InstrumentState(
            entry_strategy="range_break_continuation",
            last_market_regime="trend_expansion",
            last_setup_quality_label="strong",
        )
        base_profile = mod.ExitProfile(min_hold_minutes=25, breakeven_profit_pct=0.005, trailing_stop_pct=0.006)

        with patch.object(mod, "get_recovery_mode_status", return_value={"active": False}):
            adapted, reason = mod.get_adaptive_exit_profile(config, instrument, state, base_profile)

        self.assertGreaterEqual(adapted.min_hold_minutes, 30)
        self.assertGreaterEqual(adapted.trailing_stop_pct, 0.0075)
        self.assertIn("strong setup", reason)

    def test_adaptive_exit_profile_tightens_for_fragile_edge(self) -> None:
        config = SimpleNamespace(
            min_hold_minutes=24,
            breakeven_profit_pct=0.0055,
            trailing_stop_pct=0.0055,
        )
        instrument = mod.InstrumentConfig(symbol="IMOEXF", figi="FIGI", display_name="Index")
        state = mod.InstrumentState(
            entry_strategy="failed_breakout",
            last_market_regime="mixed",
            last_setup_quality_label="medium",
            last_entry_edge_score=0.34,
            last_entry_edge_label="fragile",
        )
        base_profile = mod.ExitProfile(min_hold_minutes=24, breakeven_profit_pct=0.0055, trailing_stop_pct=0.0055)

        with patch.object(mod, "get_recovery_mode_status", return_value={"active": False}):
            adapted, reason = mod.get_adaptive_exit_profile(config, instrument, state, base_profile)

        self.assertLessEqual(adapted.min_hold_minutes, 18)
        self.assertLessEqual(adapted.trailing_stop_pct, 0.0038)
        self.assertIn("edge fragile", reason)

    def test_adaptive_exit_profile_extends_room_for_high_edge(self) -> None:
        config = SimpleNamespace(
            min_hold_minutes=22,
            breakeven_profit_pct=0.0045,
            trailing_stop_pct=0.0050,
        )
        instrument = mod.InstrumentConfig(symbol="BRK6", figi="FIGI", display_name="Brent")
        state = mod.InstrumentState(
            entry_strategy="range_break_continuation",
            last_market_regime="trend_expansion",
            last_setup_quality_label="strong",
            last_entry_edge_score=0.84,
            last_entry_edge_label="high",
        )
        base_profile = mod.ExitProfile(min_hold_minutes=25, breakeven_profit_pct=0.0050, trailing_stop_pct=0.0060)

        with patch.object(mod, "get_recovery_mode_status", return_value={"active": False}):
            adapted, reason = mod.get_adaptive_exit_profile(config, instrument, state, base_profile)

        self.assertGreaterEqual(adapted.min_hold_minutes, 35)
        self.assertGreaterEqual(adapted.trailing_stop_pct, 0.0080)
        self.assertIn("edge high", reason)


if __name__ == "__main__":
    unittest.main()
