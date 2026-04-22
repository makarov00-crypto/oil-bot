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

        self.assertFalse(status["allowed"])
        self.assertEqual(status["net_pnl_rub"], -2890.58)
        self.assertEqual(status["limit_rub"], 2500.0)

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
        self.assertEqual(state.last_risk_stop_day, mod.datetime.now(mod.MOSCOW_TZ).date().isoformat())
        self.assertIn("глобальный дневной стоп", state.last_error)
        self.assertEqual(state.last_allocator_quantity, 0)

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

    def test_rank_cycle_entry_candidates_keeps_only_top_priority_set(self) -> None:
        candidates = [
            {"symbol": "BRK6", "priority_score": 0.86, "entry_edge_score": 0.82, "regime_confidence": 0.81, "allocator_quantity": 1},
            {"symbol": "NGJ6", "priority_score": 0.74, "entry_edge_score": 0.71, "regime_confidence": 0.76, "allocator_quantity": 1},
            {"symbol": "IMOEXF", "priority_score": 0.39, "entry_edge_score": 0.34, "regime_confidence": 0.42, "allocator_quantity": 1},
        ]

        selected, deferred = mod.rank_cycle_entry_candidates(candidates, max_entries=2, min_priority_score=0.45)

        self.assertEqual([item["symbol"] for item in selected], ["BRK6", "NGJ6"])
        self.assertEqual([item["symbol"] for item in deferred], ["IMOEXF"])

    def test_mark_cycle_deferred_candidate_updates_allocator_summary(self) -> None:
        symbol = "TESTRANK"
        state = mod.InstrumentState(last_signal_summary=["старый сигнал"])
        with patch.object(mod, "load_state", return_value=state), patch.object(mod, "save_state") as save_state:
            mod.mark_cycle_deferred_candidate(
                {"symbol": symbol},
                "кандидат отложен: есть более сильный сигнал.",
            )

        self.assertEqual(state.last_allocator_quantity, 0)
        self.assertIn("кандидат отложен", state.last_allocator_summary)
        self.assertEqual(state.last_signal_summary[0], "кандидат отложен: есть более сильный сигнал.")
        save_state.assert_called_once()

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
