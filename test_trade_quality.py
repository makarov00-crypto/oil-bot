import unittest
from datetime import datetime, timezone

from trade_quality import (
    add_trade_counterfactuals,
    build_trade_quality_overview,
    calculate_post_exit_move,
    calculate_trade_excursion,
    is_material_early_exit,
    pair_closed_trades,
    summarize_trade_dimension,
    summarize_trade_quality,
)


class TradeQualityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.entry = datetime(2026, 8, 10, 10, 15, tzinfo=timezone.utc)
        self.exit = datetime(2026, 8, 10, 13, 45, tzinfo=timezone.utc)

    def test_pairs_open_and_close_with_broker_numbers_unchanged(self) -> None:
        rows = [
            {"_dt": self.entry, "symbol": "BRU6", "side": "LONG", "event": "OPEN", "qty_lots": 2, "price": 100.0},
            {
                "_dt": self.exit,
                "symbol": "BRU6",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 2,
                "price": 103.0,
                "pnl_rub": 250.0,
                "commission_rub": 14.0,
            },
        ]

        pairs = pair_closed_trades(rows)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["pnl_rub"], 250.0)
        self.assertEqual(pairs[0]["commission_rub"], 14.0)
        self.assertEqual(pairs[0]["qty_lots"], 2)

    def test_pair_preserves_entry_ai_decision(self) -> None:
        rows = [
            {"_dt": self.entry, "symbol": "BRU6", "side": "LONG", "event": "OPEN", "qty_lots": 1, "price": 100.0, "context": {"shadow_ai": {"action": "ENTER", "confidence": 0.81, "reason": "тренд подтверждён"}}},
            {"_dt": self.exit, "symbol": "BRU6", "side": "LONG", "event": "CLOSE", "qty_lots": 1, "price": 102.0, "pnl_rub": 190.0, "commission_rub": 10.0},
        ]

        trade = pair_closed_trades(rows)[0]

        self.assertEqual(trade["shadow_ai_action"], "ENTER")
        self.assertEqual(trade["shadow_ai_confidence"], 0.81)

    def test_counterfactuals_convert_hold_result_to_money(self) -> None:
        trade = {
            "side": "LONG",
            "pnl_rub": 190.0,
            "commission_rub": 10.0,
            "realized_price_pct": 2.0,
            "mfe_pct": 4.0,
            "post_exit_1h_pct": 1.0,
            "post_exit_2h_pct": -1.0,
        }

        result = add_trade_counterfactuals(trade)

        self.assertAlmostEqual(result["hold_1h_net_rub"], 292.0)
        self.assertAlmostEqual(result["hold_1h_delta_rub"], 102.0)
        self.assertAlmostEqual(result["hold_2h_net_rub"], 88.0)
        self.assertAlmostEqual(result["max_possible_net_rub"], 390.0)
        self.assertAlmostEqual(result["missed_profit_rub"], 200.0)

    def test_calculates_hourly_excursion_and_boundary_prices(self) -> None:
        trade = {
            "symbol": "BRU6",
            "side": "LONG",
            "entry_time": self.entry.isoformat(),
            "exit_time": self.exit.isoformat(),
            "entry_price": 100.0,
            "exit_price": 102.0,
        }
        hourly = [
            {"time": datetime(2026, 8, 10, 11, 0, tzinfo=timezone.utc), "high": 104.0, "low": 99.0},
            {"time": datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc), "high": 103.0, "low": 100.0},
        ]
        boundaries = [
            {"time": datetime(2026, 8, 10, 10, 20, tzinfo=timezone.utc), "high": 101.0, "low": 99.5},
            {"time": datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc), "high": 102.5, "low": 101.0},
        ]

        quality = calculate_trade_excursion(trade, hourly, boundaries)

        self.assertEqual(quality["mfe_pct"], 4.0)
        self.assertEqual(quality["mae_pct"], 1.0)
        self.assertEqual(quality["realized_price_pct"], 2.0)

    def test_summarizes_early_exit_only_when_direction_continued(self) -> None:
        trade = {
            "symbol": "BRU6",
            "side": "LONG",
            "entry_time": self.entry.isoformat(),
            "exit_time": self.exit.isoformat(),
            "pnl_rub": 100.0,
            "commission_rub": 10.0,
            "mfe_pct": 1.2,
            "mae_pct": 0.4,
            "post_exit_4h_pct": calculate_post_exit_move({"side": "LONG", "exit_price": 100.0}, 102.0),
        }

        summary = summarize_trade_quality([trade])

        self.assertEqual(summary[0]["early_exit_count"], 1)
        self.assertEqual(summary[0]["average_post_exit_4h_pct"], 2.0)

    def test_overview_uses_journal_pnl_as_net_result_without_second_commission_deduction(self) -> None:
        trades = [
            {"pnl_rub": 100.0, "commission_rub": 10.0, "mfe_pct": 2.0, "realized_price_pct": 1.0},
            {"pnl_rub": -40.0, "commission_rub": 8.0, "mfe_pct": 0.2, "realized_price_pct": -0.4},
        ]

        overview = build_trade_quality_overview(trades, [])

        self.assertEqual(overview["net_pnl_rub"], 60.0)
        self.assertEqual(overview["gross_pnl_rub"], 78.0)
        self.assertEqual(overview["commission_rub"], 18.0)
        self.assertEqual(overview["profit_capture_pct"], 45.5)

    def test_early_exit_requires_meaningful_move_relative_to_atr_or_floor(self) -> None:
        self.assertFalse(is_material_early_exit({"post_exit_4h_pct": 0.2, "entry_atr_pct": 0.001}))
        self.assertFalse(is_material_early_exit({"post_exit_4h_pct": 0.7, "entry_atr_pct": 0.01}))
        self.assertTrue(is_material_early_exit({"post_exit_4h_pct": 1.1, "entry_atr_pct": 0.01}))

    def test_summarizes_quality_by_entry_dimension(self) -> None:
        result = summarize_trade_dimension(
            [
                {"market_regime": "trend", "pnl_rub": 50.0, "commission_rub": 5.0},
                {"market_regime": "trend", "pnl_rub": -10.0, "commission_rub": 2.0},
            ],
            "market_regime",
        )

        self.assertEqual(result[0]["label"], "trend")
        self.assertEqual(result[0]["net_pnl_rub"], 40.0)
        self.assertEqual(result[0]["win_rate_pct"], 50.0)


if __name__ == "__main__":
    unittest.main()
