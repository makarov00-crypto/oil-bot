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
    def test_load_trade_review_for_day_keeps_pairing_when_raw_rows_exceed_limit(self) -> None:
        rows = []
        base_dt = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)
        rows.append(
            {
                "_dt": base_dt,
                "_date": "2026-04-24",
                "time": "2026-04-24T12:00:00+03:00",
                "symbol": "BRK6",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 80.0,
                "strategy": "trend_rollover",
                "reason": "open brk6",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            }
        )
        for idx in range(205):
            dt = base_dt.replace(minute=0) + dashboard.timedelta(minutes=idx + 1)
            rows.append(
                {
                    "_dt": dt,
                    "_date": "2026-04-24",
                    "time": dt.astimezone(timezone.utc).isoformat(),
                    "symbol": "TMP",
                    "side": "LONG",
                    "event": "OPEN" if idx % 2 == 0 else "CLOSE",
                    "qty_lots": 1,
                    "price": 100.0 + idx,
                    "strategy": "tmp",
                    "reason": "tmp",
                }
            )
        rows.append(
            {
                "_dt": base_dt + dashboard.timedelta(hours=5),
                "_date": "2026-04-24",
                "time": "2026-04-24T17:00:00+03:00",
                "symbol": "BRK6",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 81.0,
                "pnl_rub": 100.0,
                "net_pnl_rub": 100.0,
                "strategy": "trend_rollover",
                "reason": "close brk6",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            }
        )

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows):
            review = dashboard.load_trade_review_for_day(date(2026, 4, 24), limit=20)

        brk6 = next(item for item in review["closed_reviews"] if item["symbol"] == "BRK6")
        self.assertEqual(brk6["entry_time"], "24.04 12:00:00")

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_trade_review_for_day_uses_full_3day_window_for_focus(self) -> None:
        rows = []
        for idx in range(25):
            dt = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc) + dashboard.timedelta(minutes=idx)
            rows.append(
                {
                    "_dt": dt,
                    "_date": "2026-04-24",
                    "time": dt.isoformat(),
                    "symbol": f"S{idx}",
                    "side": "LONG",
                    "event": "OPEN",
                    "qty_lots": 1,
                    "price": 100.0,
                    "strategy": "trend_rollover",
                    "reason": "open",
                    "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
                }
            )
            rows.append(
                {
                    "_dt": dt + dashboard.timedelta(minutes=1),
                    "_date": "2026-04-24",
                    "time": (dt + dashboard.timedelta(minutes=1)).isoformat(),
                    "symbol": f"S{idx}",
                    "side": "LONG",
                    "event": "CLOSE",
                    "qty_lots": 1,
                    "price": 101.0,
                    "pnl_rub": 10.0,
                    "net_pnl_rub": 10.0,
                    "strategy": "trend_rollover",
                    "reason": "close",
                    "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
                }
            )

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows):
            review = dashboard.load_trade_review_for_day(date(2026, 4, 24), limit=5)

        self.assertEqual(review["focus_3d"]["strongest"][0]["count"], 25)
        self.assertEqual(len(review["closed_reviews"]), 5)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_strategy_regime_summary_avoids_duplicate_watch_labels(self) -> None:
        summary = dashboard.build_strategy_regime_summary(
            focus_today={
                "strongest": [{"label": "trend_pullback @ режим A", "pnl_rub": 40.0, "count": 2}],
                "toxic": [{"label": "trend_rollover @ режим B", "pnl_rub": -30.0, "count": 2}],
            },
            focus_3d={
                "strongest": [
                    {"label": "trend_pullback @ режим A", "pnl_rub": 140.0, "count": 6},
                    {"label": "opening_range_breakout @ режим C", "pnl_rub": 80.0, "count": 3},
                ],
                "toxic": [{"label": "trend_rollover @ режим B", "pnl_rub": -90.0, "count": 5}],
            },
        )

        self.assertEqual(summary["working"], "trend_pullback @ режим A")
        self.assertEqual(summary["toxic"], "trend_rollover @ режим B")
        self.assertEqual(summary["watch"], "opening_range_breakout @ режим C")

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_trade_review_uses_latest_open_and_entry_context_for_summary(self) -> None:
        rows = [
            {
                "_dt": datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T12:00:00+03:00",
                "symbol": "GNM6",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 105.0,
                "strategy": "momentum_breakout",
                "reason": "older open",
                "context": {"market_regime": "old_regime", "setup_quality_label": "weak", "entry_edge_label": "fragile"},
            },
            {
                "_dt": datetime(2026, 4, 24, 9, 5, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T12:05:00+03:00",
                "symbol": "GNM6",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 100.0,
                "strategy": "trend_pullback",
                "reason": "newer open",
                "context": {"market_regime": "entry_regime", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "_dt": datetime(2026, 4, 24, 9, 10, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T12:10:00+03:00",
                "symbol": "GNM6",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 99.0,
                "pnl_rub": 10.0,
                "net_pnl_rub": 10.0,
                "strategy": "trend_pullback",
                "reason": "close",
                "context": {"market_regime": "exit_regime", "setup_quality_label": "medium", "entry_edge_label": "confirmed"},
            },
        ]

        review = dashboard.build_trade_review(rows)

        trade = review["closed_reviews"][0]
        self.assertEqual(trade["entry_time"], "24.04 12:05:00")
        self.assertEqual(trade["entry_context_display"], "режим entry_regime | сетап strong | качество входа высокое")
        self.assertEqual(review["best_regime"]["regime"], "entry_regime")
        self.assertEqual(review["best_setup_quality"]["label"], "strong")
        self.assertEqual(review["best_edge"]["label"], "high")
        self.assertEqual(review["best_strategy_regime"]["label"], "trend_pullback @ режим entry_regime | сетап strong | качество входа высокое")

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_trade_review_consumes_full_close_qty_from_open_stack(self) -> None:
        rows = [
            {
                "_dt": datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T12:00:00+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 3,
                "price": 10.95,
                "strategy": "opening_range_breakout",
                "reason": "open stack",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "_dt": datetime(2026, 4, 24, 9, 30, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T12:30:00+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 3,
                "price": 10.93,
                "pnl_rub": 30.0,
                "net_pnl_rub": 30.0,
                "strategy": "opening_range_breakout",
                "reason": "close stack",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "_dt": datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T13:00:00+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 10.92,
                "strategy": "opening_range_breakout",
                "reason": "new open",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "_dt": datetime(2026, 4, 24, 10, 30, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T13:30:00+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 10.91,
                "pnl_rub": 10.0,
                "net_pnl_rub": 10.0,
                "strategy": "opening_range_breakout",
                "reason": "close new",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
        ]

        review = dashboard.build_trade_review(rows)

        self.assertEqual(review["closed_count"], 2)
        self.assertEqual(review["current_open"], [])
        latest_trade = review["closed_reviews"][0]
        self.assertEqual(latest_trade["entry_time"], "24.04 13:00:00")

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_trade_review_keeps_pairing_when_global_rows_exceed_limit(self) -> None:
        rows = []
        base_dt = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)
        rows.append(
            {
                "_dt": base_dt,
                "_date": "2026-04-24",
                "time": "2026-04-24T12:00:00+03:00",
                "symbol": "BRK6",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 80.0,
                "strategy": "trend_rollover",
                "reason": "open brk6",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            }
        )
        for idx in range(205):
            dt = base_dt.replace(minute=0) + dashboard.timedelta(minutes=idx + 1)
            rows.append(
                {
                    "_dt": dt,
                    "_date": "2026-04-24",
                    "time": dt.astimezone(timezone.utc).isoformat(),
                    "symbol": "TMP",
                    "side": "LONG",
                    "event": "OPEN" if idx % 2 == 0 else "CLOSE",
                    "qty_lots": 1,
                    "price": 100.0 + idx,
                    "strategy": "tmp",
                    "reason": "tmp",
                }
            )
        rows.append(
            {
                "_dt": base_dt + dashboard.timedelta(hours=5),
                "_date": "2026-04-24",
                "time": "2026-04-24T17:00:00+03:00",
                "symbol": "BRK6",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 81.0,
                "pnl_rub": 100.0,
                "net_pnl_rub": 100.0,
                "strategy": "trend_rollover",
                "reason": "close brk6",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            }
        )

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows):
            review = dashboard.load_trade_review(limit=20)

        brk6 = next(item for item in review["closed_reviews"] if item["symbol"] == "BRK6")
        self.assertEqual(brk6["entry_time"], "24.04 12:00:00")
        self.assertEqual(len(review["closed_reviews"]), 20)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_daily_performance_omits_historical_pct_without_day_portfolio(self) -> None:
        rows = [
            {"_date": "2026-04-23", "event": "CLOSE", "pnl_rub": 100.0},
            {"_date": "2026-04-24", "event": "CLOSE", "pnl_rub": 50.0},
        ]

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows), patch.object(
            dashboard, "datetime"
        ) as fake_datetime:
            fake_datetime.now.return_value = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
            result = dashboard.build_daily_performance({"total_portfolio_rub": 1000.0}, date(2026, 4, 24), {})

        by_date = {item["date"]: item for item in result["series"]}
        self.assertIsNone(by_date["2026-04-23"]["pnl_pct"])
        self.assertIsNone(by_date["2026-04-23"]["cumulative_pnl_pct"])
        self.assertEqual(by_date["2026-04-24"]["pnl_pct"], 5.0)
        self.assertEqual(by_date["2026-04-24"]["cumulative_pnl_pct"], 5.0)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_daily_performance_uses_stable_base_for_cumulative_pct(self) -> None:
        rows = [
            {"_date": "2026-04-23", "event": "CLOSE", "pnl_rub": 100.0},
            {"_date": "2026-04-24", "event": "CLOSE", "pnl_rub": 50.0},
        ]
        accounting_history = {
            "2026-04-23": {"total_portfolio_rub": 1000.0},
            "2026-04-24": {"total_portfolio_rub": 2000.0},
        }

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows), patch.object(
            dashboard, "datetime"
        ) as fake_datetime:
            fake_datetime.now.return_value = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
            result = dashboard.build_daily_performance({"total_portfolio_rub": 2000.0}, date(2026, 4, 24), accounting_history)

        by_date = {item["date"]: item for item in result["series"]}
        self.assertEqual(by_date["2026-04-23"]["pnl_pct"], 10.0)
        self.assertEqual(by_date["2026-04-23"]["cumulative_pnl_pct"], 10.0)
        self.assertEqual(by_date["2026-04-24"]["pnl_pct"], 2.5)
        self.assertEqual(by_date["2026-04-24"]["cumulative_pnl_pct"], 15.0)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_daily_performance_does_not_backfill_cumulative_pct_before_first_base(self) -> None:
        rows = [
            {"_date": "2026-04-22", "event": "CLOSE", "pnl_rub": 40.0},
            {"_date": "2026-04-23", "event": "CLOSE", "pnl_rub": 60.0},
            {"_date": "2026-04-24", "event": "CLOSE", "pnl_rub": 50.0},
        ]
        accounting_history = {
            "2026-04-23": {"total_portfolio_rub": 1000.0},
            "2026-04-24": {"total_portfolio_rub": 1200.0},
        }

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows), patch.object(
            dashboard, "datetime"
        ) as fake_datetime:
            fake_datetime.now.return_value = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
            result = dashboard.build_daily_performance({"total_portfolio_rub": 1200.0}, date(2026, 4, 24), accounting_history)

        by_date = {item["date"]: item for item in result["series"]}
        self.assertIsNone(by_date["2026-04-22"]["cumulative_pnl_pct"])
        self.assertEqual(by_date["2026-04-23"]["cumulative_pnl_pct"], 6.0)
        self.assertEqual(by_date["2026-04-24"]["cumulative_pnl_pct"], 11.0)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_portfolio_view_for_historical_day_ignores_current_live_positions(self) -> None:
        portfolio = {
            "bot_actual_varmargin_rub": 50.0,
            "bot_actual_fee_rub": 5.0,
            "bot_actual_cash_effect_rub": 45.0,
            "bot_estimated_variation_margin_rub": 220.0,
            "open_positions_count": 2,
            "broker_open_positions": [
                {"symbol": "BRK6", "expected_yield_rub": 120.0, "variation_margin_rub": 120.0},
                {"symbol": "NGJ6", "expected_yield_rub": -20.0, "variation_margin_rub": -20.0},
            ],
        }
        rows = [
            {
                "_dt": datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
                "_date": "2026-04-24",
                "time": "2026-04-24T13:00:00+03:00",
                "symbol": "BRK6",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 1,
                "gross_pnl_rub": 100.0,
                "commission_rub": 10.0,
                "net_pnl_rub": 90.0,
                "pnl_rub": 90.0,
            }
        ]

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows):
            view = dashboard.build_portfolio_view_for_day(portfolio, date(2026, 4, 24), {})

        self.assertFalse(view["selected_is_today"])
        self.assertEqual(view["bot_broker_day_pnl_rub"], 0.0)
        self.assertEqual(view["bot_open_positions_live_pnl_rub"], 0.0)
        self.assertEqual(view["open_positions_count"], 0)
        self.assertEqual(view["bot_total_varmargin_rub"], 100.0)
        self.assertEqual(view["bot_total_pnl_rub"], 100.0)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_build_portfolio_view_for_day_uses_full_day_rows_for_closed_totals(self) -> None:
        rows = []
        base_dt = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)
        for idx in range(305):
            rows.append(
                {
                    "_dt": base_dt + dashboard.timedelta(minutes=idx),
                    "_date": "2026-04-24",
                    "time": (base_dt + dashboard.timedelta(minutes=idx)).isoformat(),
                    "symbol": f"S{idx}",
                    "side": "LONG",
                    "event": "CLOSE",
                    "qty_lots": 1,
                    "gross_pnl_rub": 10.0,
                    "commission_rub": 1.0,
                    "net_pnl_rub": 9.0,
                    "pnl_rub": 9.0,
                }
            )

        with patch.object(dashboard, "load_all_trade_rows", return_value=rows):
            view = dashboard.build_portfolio_view_for_day({}, date(2026, 4, 24), {})

        self.assertEqual(view["bot_realized_gross_pnl_rub"], 3050.0)
        self.assertEqual(view["bot_realized_commission_rub"], 305.0)
        self.assertEqual(view["bot_realized_pnl_rub"], 2745.0)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_ai_review_does_not_fallback_to_latest_for_historical_day(self) -> None:
        with TemporaryDirectory() as temp_dir:
            review_dir = dashboard.Path(temp_dir)
            (review_dir / "latest_review.md").write_text("# latest", encoding="utf-8")

            with patch.object(dashboard, "AI_REVIEW_DIR", review_dir):
                payload = dashboard.load_ai_review(date(2026, 4, 24))

        self.assertFalse(payload["available"])
        self.assertEqual(payload["status"], "missing")

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_ai_review_uses_latest_for_today_when_dated_missing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            review_dir = dashboard.Path(temp_dir)
            (review_dir / "latest_review.md").write_text("# latest", encoding="utf-8")

            with patch.object(dashboard, "AI_REVIEW_DIR", review_dir), patch.object(
                dashboard, "datetime"
            ) as fake_datetime:
                fake_datetime.now.return_value = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
                fake_datetime.fromtimestamp.side_effect = datetime.fromtimestamp
                payload = dashboard.load_ai_review(date(2026, 4, 24))

        self.assertTrue(payload["available"])
        self.assertEqual(payload["source"], "latest_review.md")

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_api_dashboard_loads_full_day_trades_before_annotation(self) -> None:
        with patch.object(dashboard, "load_states", return_value={}), patch.object(
            dashboard, "load_portfolio_snapshot", return_value={}
        ), patch.object(dashboard, "load_accounting_history", return_value={}), patch.object(
            dashboard, "load_runtime_status", return_value={}
        ), patch.object(dashboard, "build_portfolio_view_for_day", return_value={}), patch.object(
            dashboard, "annotate_trade_rows", return_value=[]
        ) as annotate_mock, patch.object(dashboard, "load_trade_rows_for_day", return_value=[]) as rows_mock, patch.object(
            dashboard, "build_instrument_catalog", return_value={}
        ), patch.object(dashboard, "get_bot_service_status", return_value={}), patch.object(
            dashboard, "build_health_payload", return_value={}
        ), patch.object(dashboard, "build_capital_alert", return_value={}), patch.object(
            dashboard, "load_news_snapshot", return_value={}
        ), patch.object(dashboard, "load_trade_review_for_day", return_value={}), patch.object(
            dashboard, "load_allocator_decisions_for_day", return_value=[]
        ), patch.object(dashboard, "load_signal_observation_summary_for_day", return_value={}), patch.object(
            dashboard, "summarize_states", return_value={}
        ), patch.object(dashboard, "load_meta", return_value={}), patch.object(
            dashboard, "build_manual_instruments_payload", return_value=[]
        ), patch.object(dashboard, "build_daily_performance", return_value={}), patch.object(
            dashboard, "load_ai_review", return_value={}
        ):
            dashboard.api_dashboard("2026-04-24")

        rows_mock.assert_called_once_with(date(2026, 4, 24), None)
        annotate_mock.assert_called_once()

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
        self.assertEqual([row["entry_time"] for row in review["closed_reviews"]], ["13.04 09:25:33"] * 3)
        self.assertEqual(review["best_regime"]["regime"], "trend_expansion")
        self.assertEqual(review["best_setup_quality"]["label"], "strong")
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

        self.assertEqual(review["best_regime"]["regime"], "trend_expansion")
        self.assertEqual(review["best_setup_quality"]["label"], "strong")
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
    def test_build_trade_review_formats_iso_time_and_long_reason(self) -> None:
        long_reason = (
            "Сигнал SHORT (range_break_continuation): старший ТФ=SHORT; пробой вниз диапазона 11.0130: да; "
            "мягкий пробой вниз: да; продолжение вниз после слома: да; цена ниже EMA20 и EMA50: да; "
            "RSI=42.10 в рабочей зоне 28-58; объём достаточный; импульс свечи есть; MACD поддерживает снижение."
        )
        rows = [
            {
                "time": "2026-04-25T00:00:05.411333+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 11.01,
                "strategy": "range_break_continuation",
                "reason": long_reason,
            },
            {
                "time": "2026-04-25T00:15:05.411333+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 10.99,
                "pnl_rub": 10.0,
                "net_pnl_rub": 10.0,
                "strategy": "range_break_continuation",
                "reason": long_reason,
            },
        ]

        review = dashboard.build_trade_review(rows)

        self.assertEqual(review["closed_reviews"][0]["entry_time"], "25.04 00:00:05")
        self.assertEqual(review["closed_reviews"][0]["exit_time"], "25.04 00:15:05")
        self.assertIn("Сигнал SHORT (range_break_continuation):", review["closed_reviews"][0]["exit_reason"])
        self.assertIn("пробой вниз диапазона 11.0130: да", review["closed_reviews"][0]["exit_reason"])
        self.assertTrue(review["closed_reviews"][0]["exit_reason"].endswith("..."))

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_annotate_trade_rows_uses_latest_open_and_skips_duplicate_carry(self) -> None:
        rows = [
            {
                "time": "2026-04-24T16:05:21+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 3,
                "price": 11.011,
                "source": "portfolio_confirmation",
                "strategy": "range_break_continuation",
            },
            {
                "time": "2026-04-24T16:06:00+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 3,
                "price": 11.011,
                "source": "portfolio_recovery",
                "strategy": "range_break_continuation",
            },
            {
                "time": "2026-04-24T17:00:00+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 11.005,
                "source": "portfolio_confirmation",
                "strategy": "range_break_continuation",
            },
            {
                "time": "2026-04-24T18:00:00+03:00",
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 10.995,
                "source": "broker_ops_auto_recovery",
                "strategy": "range_break_continuation",
            },
        ]

        annotated = dashboard.annotate_trade_rows(
            rows,
            states={"CNYRUBF": {"position_side": "SHORT", "position_qty": 3}},
        )

        statuses = [item["event_status"] for item in annotated]
        self.assertEqual(statuses, ["active", "history", "closed", "closed"])

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
                        "execution_status": "confirmed_open",
                    },
                },
            )
            append_signal_observation(
                db_path,
                {
                    "observed_at": "2026-04-24T12:15:00+03:00",
                    "evaluated_at": "2026-04-24T12:30:00+03:00",
                    "symbol": "BRK6",
                    "signal": "LONG",
                    "strategy": "momentum_breakout",
                    "decision": "deferred",
                    "decision_reason": "ещё один хороший сигнал",
                    "priority_score": 0.88,
                    "market_regime": "trend_expansion",
                    "setup_quality": "strong",
                    "observed_price": 80.2,
                    "current_price": 80.9,
                    "move_pct": 0.87,
                    "favorable": True,
                    "context": {
                        "entry_edge_label": "confirmed",
                        "learning_adjustment": 0.06,
                        "learning_reason": "обучение связки: бонус +0.06, 70% подтверждений, 7 наблюд.",
                    },
                },
            )
            append_signal_observation(
                db_path,
                {
                    "observed_at": "2026-04-24T12:45:00+03:00",
                    "evaluated_at": "2026-04-24T13:00:00+03:00",
                    "symbol": "UCM6",
                    "signal": "SHORT",
                    "strategy": "opening_range_breakout",
                    "decision": "selected",
                    "decision_reason": "ещё один слабый вход",
                    "priority_score": 0.80,
                    "market_regime": "range_chop",
                    "setup_quality": "fragile",
                    "observed_price": 7.18,
                    "current_price": 7.24,
                    "move_pct": -0.84,
                    "favorable": False,
                    "context": {
                        "entry_edge_label": "fragile",
                        "learning_adjustment": -0.08,
                        "learning_reason": "обучение связки: штраф -0.08, 18% подтверждений, 6 наблюд.",
                        "execution_status": "confirmed_open",
                    },
                },
            )

            with patch.object(dashboard, "TRADE_DB_PATH", db_path):
                summary = dashboard.load_signal_observation_summary_for_day(date(2026, 4, 24))

        self.assertEqual(summary["total"], 4)
        self.assertEqual(summary["evaluated"], 4)
        self.assertEqual(summary["favorable"], 2)
        self.assertEqual(summary["favorable_rate"], 50.0)
        self.assertEqual(summary["deferred_favorable"], 2)
        self.assertEqual(summary["selected_unfavorable"], 2)
        self.assertEqual(summary["learning_bonus_count"], 2)
        self.assertEqual(summary["learning_penalty_count"], 2)
        self.assertEqual(summary["items"][0]["decision_display"], "выбран")
        self.assertEqual(summary["items"][0]["learning_adjustment"], -0.08)
        self.assertIn("штраф -0.08", summary["items"][0]["learning_reason"])
        self.assertEqual(summary["learning_combos"]["strongest"][0]["bonus_count"], 2)
        self.assertEqual(summary["learning_combos"]["strongest"][0]["count"], 2)
        self.assertEqual(summary["learning_combos"]["weakest"][0]["penalty_count"], 2)
        self.assertEqual(summary["learning_combos"]["weakest"][0]["count"], 2)
        self.assertTrue(any("Снижать приоритет связки" in item for item in summary["actions"]))
        self.assertTrue(any("Быстрее пропускать связку" in item for item in summary["actions"]))
        self.assertEqual(summary["combos"]["strongest"][0]["symbol"], "BRK6")
        self.assertEqual(summary["combos"]["strongest"][0]["confirmation_rate"], 100.0)
        self.assertEqual(summary["combos"]["weakest"][0]["symbol"], "UCM6")
        self.assertEqual(summary["combos"]["weakest"][0]["confirmation_rate"], 0.0)
        self.assertTrue(summary["combos"]["weakest"][0]["sample_warning"])

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_signal_observation_summary_ignores_unexecuted_selected_losses(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = dashboard.Path(temp_dir) / "trade_analytics.sqlite3"
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
                        "execution_status": "rejected",
                        "execution_note": "заявка отклонена",
                    },
                },
            )

            with patch.object(dashboard, "TRADE_DB_PATH", db_path):
                summary = dashboard.load_signal_observation_summary_for_day(date(2026, 4, 24))

        self.assertEqual(summary["selected"], 1)
        self.assertEqual(summary["selected_unfavorable"], 0)

    @unittest.skipIf(dashboard is None, f"web_dashboard dependencies are unavailable: {IMPORT_ERROR}")
    def test_load_signal_observation_summary_ignores_pending_learning_for_actions(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = dashboard.Path(temp_dir) / "trade_analytics.sqlite3"
            for observed_at, adjustment in [
                ("2026-04-24T10:15:00+03:00", -0.07),
                ("2026-04-24T10:30:00+03:00", -0.08),
            ]:
                append_signal_observation(
                    db_path,
                    {
                        "observed_at": observed_at,
                        "symbol": "UCM6",
                        "signal": "SHORT",
                        "strategy": "opening_range_breakout",
                        "decision": "selected",
                        "decision_reason": "pending learning row",
                        "priority_score": 0.80,
                        "market_regime": "range_chop",
                        "setup_quality": "fragile",
                        "observed_price": 7.18,
                        "horizon_minutes": 15,
                        "context": {
                            "entry_edge_label": "fragile",
                            "learning_adjustment": adjustment,
                            "learning_reason": f"обучение связки: штраф {adjustment:+.2f}",
                        },
                    },
                )

            with patch.object(dashboard, "TRADE_DB_PATH", db_path):
                summary = dashboard.load_signal_observation_summary_for_day(date(2026, 4, 24))

        self.assertEqual(summary["pending"], 2)
        self.assertEqual(summary["learning_penalty_count"], 2)
        self.assertEqual(summary["learning_combos"]["weakest"], [])
        self.assertEqual(summary["actions"], [])


if __name__ == "__main__":
    unittest.main()
