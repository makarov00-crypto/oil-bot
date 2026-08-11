import unittest
from datetime import datetime, timezone

from trade_quality import calculate_post_exit_move, calculate_trade_excursion, pair_closed_trades, summarize_trade_quality


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


if __name__ == "__main__":
    unittest.main()
