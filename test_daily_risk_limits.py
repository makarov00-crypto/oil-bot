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
        rows = [
            {"event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -120.0},
            {"event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -80.0},
            {"event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -75.0},
            {"event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -60.0},
            {"event": "CLOSE", "symbol": "USDRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": 20.0},
            {"event": "CLOSE", "symbol": "CNYRUBF", "strategy": "opening_range_breakout", "net_pnl_rub": -500.0},
        ]

        stats = mod.calculate_recent_strategy_performance(
            "USDRUBF",
            "opening_range_breakout",
            rows=rows,
        )

        self.assertEqual(stats["closed_count"], 5)
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["net_pnl_rub"], -315.0)
        with patch.object(mod, "get_trade_journal_rows_since", return_value=rows):
            reason = mod.recent_strategy_performance_block_reason("USDRUBF", "opening_range_breakout")

        self.assertIn("performance guard", reason)
        self.assertIn("USDRUBF", reason)

    def test_recent_strategy_performance_allows_healthy_combo(self) -> None:
        rows = [
            {"event": "CLOSE", "symbol": "RBM6", "strategy": "range_break_continuation", "net_pnl_rub": 20.0},
            {"event": "CLOSE", "symbol": "RBM6", "strategy": "range_break_continuation", "net_pnl_rub": 15.0},
            {"event": "CLOSE", "symbol": "RBM6", "strategy": "range_break_continuation", "net_pnl_rub": -5.0},
        ]

        with patch.object(mod, "get_trade_journal_rows_since", return_value=rows):
            reason = mod.recent_strategy_performance_block_reason("RBM6", "range_break_continuation")

        self.assertEqual(reason, "")


if __name__ == "__main__":
    unittest.main()
