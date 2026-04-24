import unittest
import json
from datetime import date, datetime, timezone
from tempfile import TemporaryDirectory
from unittest.mock import patch

from trade_storage import append_signal_observation

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
        self.assertEqual(review["best_regime"]["regime"], "режим trend_expansion | сетап strong | качество входа высокое")
        self.assertEqual(review["best_edge"]["label"], "high")
        self.assertEqual(
            review["best_strategy_regime"]["label"],
            "opening_range_breakout @ режим trend_expansion | сетап strong | качество входа высокое",
        )
        focus = dashboard.summarize_strategy_regime_focus(rows)
        self.assertEqual(
            focus["strongest"][0]["label"],
            "opening_range_breakout @ режим trend_expansion | сетап strong | качество входа высокое",
        )
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

        self.assertEqual(review["best_regime"]["regime"], "режим trend_expansion | сетап strong | качество входа высокое")
        self.assertEqual(review["best_edge"]["label"], "high")
        self.assertEqual(
            review["closed_reviews"][0]["exit_context_display"],
            "режим trend_expansion | сетап strong | качество входа высокое",
        )
        focus = dashboard.summarize_strategy_regime_focus_from_reviews(review["closed_reviews"])
        self.assertEqual(
            focus["strongest"][0]["label"],
            "range_break_continuation @ режим trend_expansion | сетап strong | качество входа высокое",
        )

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_allocator_decisions_for_day_filters_and_labels_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            decisions_path = dashboard.Path(temp_dir) / "allocator_decisions.jsonl"
            rows = [
                {
                    "time": "2026-04-24T10:15:00+03:00",
                    "decision": "deferred",
                    "symbol": "BRK6",
                    "signal": "LONG",
                    "reason": "недостаточно ГО",
                    "priority_score": 0.84,
                    "learning_adjustment": -0.08,
                    "learning_reason": "обучение связки: штраф -0.08, 0% подтверждений, 10 наблюд., среднее движение -1.61%",
                },
                {
                    "time": "2026-04-23T10:15:00+03:00",
                    "decision": "rotation",
                    "symbol": "NGJ6",
                    "signal": "SHORT",
                },
            ]
            decisions_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows), encoding="utf-8")

            with patch.object(dashboard, "ALLOCATOR_DECISIONS_PATH", decisions_path):
                loaded = dashboard.load_allocator_decisions_for_day(date(2026, 4, 24))

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["symbol"], "BRK6")
        self.assertEqual(loaded[0]["decision_display"], "отложен")
        self.assertEqual(loaded[0]["time_display"], "10:15:00")
        self.assertEqual(loaded[0]["learning_adjustment"], -0.08)
        self.assertIn("штраф -0.08", loaded[0]["learning_reason"])

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_signal_observation_summary_for_day_counts_learning_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = dashboard.Path(temp_dir) / "trade_analytics.sqlite3"
            append_signal_observation(
                db_path,
                {
                    "observed_at": "2026-04-24T10:15:00+03:00",
                    "evaluated_at": "2026-04-24T10:30:00+03:00",
                    "symbol": "BRK6",
                    "signal": "LONG",
                    "strategy": "momentum_breakout",
                    "decision": "deferred",
                    "decision_reason": "не хватило ГО",
                    "priority_score": 0.91,
                    "market_regime": "trend_expansion",
                    "setup_quality": "strong",
                    "observed_price": 80.0,
                    "current_price": 80.8,
                    "move_pct": 1.0,
                    "favorable": True,
                    "context": {
                        "entry_edge_label": "confirmed",
                        "learning_adjustment": 0.05,
                        "learning_reason": "обучение связки: бонус +0.05, 67% подтверждений, 6 наблюд.",
                    },
                },
            )
            append_signal_observation(
                db_path,
                {
                    "observed_at": "2026-04-24T11:15:00+03:00",
                    "evaluated_at": "2026-04-24T11:30:00+03:00",
                    "symbol": "UCM6",
                    "signal": "SHORT",
                    "strategy": "opening_range_breakout",
                    "decision": "selected",
                    "decision_reason": "вошёл по приоритету",
                    "priority_score": 0.82,
                    "market_regime": "range_chop",
                    "setup_quality": "fragile",
                    "observed_price": 7.20,
                    "current_price": 7.25,
                    "move_pct": -0.69,
                    "favorable": False,
                    "context": {
                        "entry_edge_label": "fragile",
                        "learning_adjustment": -0.07,
                        "learning_reason": "обучение связки: штраф -0.07, 20% подтверждений, 5 наблюд.",
                    },
                },
            )

            with patch.object(dashboard, "TRADE_DB_PATH", db_path):
                summary = dashboard.load_signal_observation_summary_for_day(date(2026, 4, 24))

        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["evaluated"], 2)
        self.assertEqual(summary["favorable"], 1)
        self.assertEqual(summary["favorable_rate"], 50.0)
        self.assertEqual(summary["deferred_favorable"], 1)
        self.assertEqual(summary["selected_unfavorable"], 1)
        self.assertEqual(summary["learning_bonus_count"], 1)
        self.assertEqual(summary["learning_penalty_count"], 1)
        self.assertEqual(summary["items"][0]["decision_display"], "выбран")
        self.assertEqual(summary["items"][0]["learning_adjustment"], -0.07)
        self.assertIn("штраф -0.07", summary["items"][0]["learning_reason"])
        self.assertEqual(summary["learning_combos"]["strongest"][0]["bonus_count"], 1)
        self.assertEqual(summary["learning_combos"]["strongest"][0]["count"], 1)
        self.assertEqual(summary["learning_combos"]["weakest"][0]["penalty_count"], 1)
        self.assertEqual(summary["learning_combos"]["weakest"][0]["count"], 1)
        self.assertEqual(summary["combos"]["strongest"][0]["symbol"], "BRK6")
        self.assertEqual(summary["combos"]["strongest"][0]["confirmation_rate"], 100.0)
        self.assertEqual(summary["combos"]["weakest"][0]["symbol"], "UCM6")
        self.assertEqual(summary["combos"]["weakest"][0]["confirmation_rate"], 0.0)
        self.assertTrue(summary["combos"]["weakest"][0]["sample_warning"])


if __name__ == "__main__":
    unittest.main()
