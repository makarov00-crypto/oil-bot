import unittest
from datetime import datetime, timezone

try:
    import web_dashboard as dashboard
except ModuleNotFoundError as error:
    dashboard = None
    IMPORT_ERROR = error
else:
    IMPORT_ERROR = None


class DashboardTradeReviewTests(unittest.TestCase):
    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_instrument_catalog_has_labels_for_all_dashboard_symbols(self) -> None:
        catalog = dashboard.build_instrument_catalog()

        for symbol in ["BRK6", "NGJ6", "RBM6", "SRM6", "UCM6", "USDRUBF", "CNYRUBF", "VBM6", "IMOEXF"]:
            with self.subTest(symbol=symbol):
                self.assertIn(symbol, catalog)
                self.assertNotEqual(catalog[symbol], symbol)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_trade_review_pairs_multi_lot_open_with_unit_closes(self) -> None:
        rows = [
            {
                "_dt": datetime(2026, 4, 13, 6, 25, 33, tzinfo=timezone.utc),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 3,
                "price": 11.17,
                "strategy": "opening_range_breakout",
                "reason": "open",
            },
            {
                "_dt": datetime(2026, 4, 13, 12, 15, 22, tzinfo=timezone.utc),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 11.146,
                "pnl_rub": 83.4,
                "net_pnl_rub": 83.4,
                "strategy": "opening_range_breakout",
                "reason": "close",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "_dt": datetime(2026, 4, 13, 12, 15, 22, tzinfo=timezone.utc),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 11.146,
                "pnl_rub": 18.42,
                "net_pnl_rub": 18.42,
                "strategy": "opening_range_breakout",
                "reason": "close",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "_dt": datetime(2026, 4, 13, 12, 15, 22, tzinfo=timezone.utc),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 11.146,
                "pnl_rub": 18.42,
                "net_pnl_rub": 18.42,
                "strategy": "opening_range_breakout",
                "reason": "close",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
        ]

        review = dashboard.build_trade_review(rows)

        self.assertEqual(review["closed_count"], 3)
        self.assertEqual(review["closed_total_pnl_rub"], 120.24)
        self.assertEqual([row["entry_time"] for row in review["closed_reviews"]], ["13.04 06:25:33"] * 3)
        self.assertEqual(review["best_regime"]["regime"], "режим trend_expansion | сетап strong")
        self.assertEqual(review["best_edge"]["label"], "high")
        self.assertEqual(
            review["best_strategy_regime"]["label"],
            "opening_range_breakout @ режим trend_expansion | сетап strong",
        )
        focus = dashboard.summarize_strategy_regime_focus(rows)
        self.assertEqual(focus["strongest"][0]["label"], "opening_range_breakout @ режим trend_expansion | сетап strong")
        edge_focus = dashboard.summarize_edge_focus(rows)
        self.assertEqual(edge_focus["strongest"][0]["label"], "high")

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_trade_review_keeps_remaining_multi_lot_open_qty(self) -> None:
        rows = [
            {
                "_dt": datetime(2026, 4, 13, 6, 25, 33, tzinfo=timezone.utc),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 3,
                "price": 11.17,
                "strategy": "opening_range_breakout",
                "reason": "open",
            },
            {
                "_dt": datetime(2026, 4, 13, 12, 15, 22, tzinfo=timezone.utc),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 11.146,
                "pnl_rub": 83.4,
                "net_pnl_rub": 83.4,
                "strategy": "opening_range_breakout",
                "reason": "close",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "confirmed"},
            },
        ]

        review = dashboard.build_trade_review(rows)

        self.assertEqual(review["closed_count"], 1)
        self.assertEqual(len(review["current_open"]), 1)
        self.assertEqual(review["current_open"][0]["qty_lots"], 2)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_trade_review_falls_back_to_open_context_when_close_context_missing(self) -> None:
        rows = [
            {
                "_dt": datetime(2026, 4, 14, 8, 0, 0, tzinfo=timezone.utc),
                "symbol": "BRK6",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 80.0,
                "strategy": "range_break_continuation",
                "reason": "open",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "_dt": datetime(2026, 4, 14, 9, 0, 0, tzinfo=timezone.utc),
                "symbol": "BRK6",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 81.0,
                "pnl_rub": 120.0,
                "net_pnl_rub": 120.0,
                "strategy": "range_break_continuation",
                "reason": "close",
            },
        ]

        review = dashboard.build_trade_review(rows)

        self.assertEqual(review["best_regime"]["regime"], "режим trend_expansion | сетап strong")
        self.assertEqual(review["best_edge"]["label"], "high")
        self.assertEqual(review["closed_reviews"][0]["exit_context_display"], "режим trend_expansion | сетап strong")
        focus = dashboard.summarize_strategy_regime_focus_from_reviews(review["closed_reviews"])
        self.assertEqual(focus["strongest"][0]["label"], "range_break_continuation @ режим trend_expansion | сетап strong")


if __name__ == "__main__":
    unittest.main()
