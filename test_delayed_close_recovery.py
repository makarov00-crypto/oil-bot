import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as mod
import strategy_registry


class DelayedCloseRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = mod.InstrumentConfig(symbol="TEST", figi="FIGI", display_name="Test")

    def test_defer_close_recovery_appends_queue_instead_of_overwriting(self) -> None:
        state = mod.InstrumentState()
        first_time = datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc)
        second_time = datetime(2026, 4, 8, 11, 0, tzinfo=timezone.utc)

        with patch.object(mod, "save_state", lambda *args, **kwargs: None):
            mod.defer_close_recovery_to_broker_ops(
                self.instrument,
                state,
                previous_side="LONG",
                previous_qty=1,
                previous_entry_price=100.0,
                previous_entry_commission=1.0,
                previous_strategy="s1",
                previous_exit_reason="r1",
                previous_entry_time=first_time,
                pending_submitted_at=first_time,
                grace_seconds=None,
            )
            mod.defer_close_recovery_to_broker_ops(
                self.instrument,
                state,
                previous_side="SHORT",
                previous_qty=2,
                previous_entry_price=200.0,
                previous_entry_commission=2.0,
                previous_strategy="s2",
                previous_exit_reason="r2",
                previous_entry_time=second_time,
                pending_submitted_at=second_time,
                grace_seconds=None,
            )

        queue = mod.ensure_delayed_close_queue(state)
        self.assertEqual(len(queue), 2)
        self.assertEqual(queue[0]["side"], "LONG")
        self.assertEqual(queue[1]["side"], "SHORT")
        self.assertTrue(state.delayed_close_recovery_needed)
        self.assertEqual(state.delayed_close_side, "LONG")

    def test_reconcile_delayed_close_recovers_oldest_and_keeps_next(self) -> None:
        state = mod.InstrumentState(
            delayed_close_queue=[
                {
                    "side": "LONG",
                    "qty": 1,
                    "entry_price": 100.0,
                    "entry_commission_rub": 1.0,
                    "strategy": "s1",
                    "reason": "r1",
                    "entry_time": datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc).isoformat(),
                    "submitted_at": datetime(2026, 4, 8, 10, 5, tzinfo=timezone.utc).isoformat(),
                },
                {
                    "side": "SHORT",
                    "qty": 1,
                    "entry_price": 200.0,
                    "entry_commission_rub": 2.0,
                    "strategy": "s2",
                    "reason": "r2",
                    "entry_time": datetime(2026, 4, 8, 11, 0, tzinfo=timezone.utc).isoformat(),
                    "submitted_at": datetime(2026, 4, 8, 11, 5, tzinfo=timezone.utc).isoformat(),
                },
            ]
        )
        mod.sync_legacy_delayed_close_fields(state)
        calls = []

        def fake_confirm(*args, **kwargs):
            calls.append(kwargs["previous_side"])
            return kwargs["previous_side"] == "LONG"

        with patch.object(mod, "confirm_pending_close_from_broker", fake_confirm), patch.object(
            mod, "save_state", lambda *args, **kwargs: None
        ):
            recovered = mod.reconcile_delayed_close_from_broker(None, None, self.instrument, state)

        self.assertTrue(recovered)
        self.assertEqual(calls, ["LONG"])
        queue = mod.ensure_delayed_close_queue(state)
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["side"], "SHORT")
        self.assertEqual(state.delayed_close_side, "SHORT")

    def test_legacy_delayed_close_state_is_normalized_into_queue(self) -> None:
        state = mod.InstrumentState(
            delayed_close_recovery_needed=True,
            delayed_close_side="LONG",
            delayed_close_qty=1,
            delayed_close_entry_price=123.0,
            delayed_close_entry_commission_rub=4.5,
            delayed_close_strategy="legacy",
            delayed_close_reason="legacy_reason",
            delayed_close_entry_time="2026-04-08T10:00:00+00:00",
            delayed_close_submitted_at="2026-04-08T10:05:00+00:00",
        )
        queue = mod.ensure_delayed_close_queue(state)
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["side"], "LONG")
        self.assertEqual(queue[0]["strategy"], "legacy")

    def test_confirm_pending_open_appends_missing_open_entry(self) -> None:
        state = mod.InstrumentState(
            position_side="SHORT",
            position_qty=1,
            pending_order_side="SHORT",
            pending_order_qty=1,
            pending_order_action="OPEN",
            pending_order_id="oid",
            pending_entry_reason="Сигнал SHORT (momentum_breakout): старший ТФ=SHORT; цена ниже EMA20 и EMA50: да.",
            entry_price=2.772,
            entry_strategy="momentum_breakout",
        )
        config = SimpleNamespace(dry_run=False)
        recorded: list[dict] = []

        def fake_append(*args, **kwargs):
            recorded.append(kwargs)

        with patch.object(
            mod,
            "find_recent_live_open_details",
            return_value=(datetime(2026, 4, 8, 19, 35, tzinfo=timezone.utc), 5.5),
        ), patch.object(mod, "has_journal_event_since", return_value=False), patch.object(
            mod, "append_trade_journal", side_effect=fake_append
        ), patch.object(
            mod, "update_latest_unclosed_open_journal_entry", return_value=True
        ), patch.object(
            mod, "save_state", lambda *args, **kwargs: None
        ):
            confirmed = mod.confirm_pending_open_from_broker(
                None,
                config,
                self.instrument,
                state,
                not_before=datetime(2026, 4, 8, 19, 34, tzinfo=timezone.utc),
            )

        self.assertTrue(confirmed)
        self.assertEqual(len(recorded), 1)
        self.assertEqual(recorded[0]["reason"], state.entry_reason)
        self.assertEqual(recorded[0]["source"], "portfolio_confirmation")
        self.assertEqual(recorded[0]["strategy"], "momentum_breakout")

    def test_reconcile_delayed_close_clears_item_when_close_already_in_journal(self) -> None:
        state = mod.InstrumentState(
            delayed_close_queue=[
                {
                    "side": "SHORT",
                    "qty": 1,
                    "entry_price": 2.771,
                    "entry_commission_rub": 5.5,
                    "strategy": "momentum_breakout",
                    "reason": "Трейлинг-стоп",
                    "entry_time": datetime(2026, 4, 8, 16, 47, tzinfo=timezone.utc).isoformat(),
                    "submitted_at": datetime(2026, 4, 8, 17, 26, tzinfo=timezone.utc).isoformat(),
                }
            ]
        )
        mod.sync_legacy_delayed_close_fields(state)

        with patch.object(mod, "has_journal_event_since", return_value=True), patch.object(
            mod, "save_state", lambda *args, **kwargs: None
        ):
            recovered = mod.reconcile_delayed_close_from_broker(None, None, self.instrument, state)

        self.assertFalse(recovered)
        self.assertEqual(mod.ensure_delayed_close_queue(state), [])
        self.assertFalse(state.delayed_close_recovery_needed)

    def test_reconcile_delayed_close_allows_small_broker_clock_skew(self) -> None:
        submitted_at = datetime(2026, 4, 13, 12, 55, 4, 142354, tzinfo=timezone.utc)
        close_at = datetime(2026, 4, 13, 12, 55, 4, 13004, tzinfo=timezone.utc)
        state = mod.InstrumentState(
            delayed_close_queue=[
                {
                    "side": "SHORT",
                    "qty": 1,
                    "entry_price": 4757.2,
                    "entry_commission_rub": 9.15,
                    "strategy": "trend_rollover",
                    "reason": "MACD развернулся вверх",
                    "entry_time": datetime(2026, 4, 13, 9, 10, tzinfo=timezone.utc).isoformat(),
                    "submitted_at": submitted_at.isoformat(),
                }
            ]
        )
        mod.sync_legacy_delayed_close_fields(state)
        rows = [
            {
                "time": close_at.astimezone(mod.MOSCOW_TZ).isoformat(),
                "symbol": "TEST",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
            }
        ]

        with patch.object(mod, "load_trade_journal", return_value=rows), patch.object(
            mod, "save_state", lambda *args, **kwargs: None
        ):
            recovered = mod.reconcile_delayed_close_from_broker(None, None, self.instrument, state)

        self.assertFalse(recovered)
        self.assertEqual(mod.ensure_delayed_close_queue(state), [])
        self.assertFalse(state.delayed_close_recovery_needed)

    def test_confirm_pending_open_does_not_duplicate_existing_active_open(self) -> None:
        state = mod.InstrumentState(
            position_side="SHORT",
            position_qty=1,
            pending_order_side="SHORT",
            pending_order_qty=1,
            pending_order_action="OPEN",
            pending_order_id="oid",
            pending_entry_reason="short setup",
            entry_price=100.0,
            entry_strategy="range_break_continuation",
        )
        config = SimpleNamespace(dry_run=False)
        recorded: list[tuple[tuple, dict]] = []

        with patch.object(
            mod,
            "find_recent_live_open_details",
            return_value=(datetime(2026, 4, 9, 9, 5, 4, tzinfo=timezone.utc), 6.0),
        ), patch.object(
            mod, "has_journal_event_since", return_value=False
        ), patch.object(
            mod, "get_active_journal_lots", return_value=1
        ), patch.object(
            mod, "append_trade_journal", side_effect=lambda *args, **kwargs: recorded.append((args, kwargs))
        ), patch.object(
            mod, "update_latest_unclosed_open_journal_entry", return_value=True
        ), patch.object(
            mod, "save_state", lambda *args, **kwargs: None
        ):
            confirmed = mod.confirm_pending_open_from_broker(
                None,
                config,
                self.instrument,
                state,
                not_before=datetime(2026, 4, 9, 9, 5, 25, tzinfo=timezone.utc),
            )

        self.assertTrue(confirmed)
        self.assertEqual(recorded, [])

    def test_confirm_pending_open_appends_only_missing_lots_for_scale_in(self) -> None:
        state = mod.InstrumentState(
            position_side="SHORT",
            position_qty=2,
            pending_order_side="SHORT",
            pending_order_qty=1,
            pending_order_action="OPEN",
            pending_order_id="oid",
            pending_entry_reason="short setup",
            entry_price=100.0,
            entry_strategy="range_break_continuation",
        )
        config = SimpleNamespace(dry_run=False)
        recorded: list[tuple[tuple, dict]] = []

        with patch.object(
            mod,
            "find_recent_live_open_details",
            return_value=(datetime(2026, 4, 9, 15, 5, 0, tzinfo=timezone.utc), 6.0),
        ), patch.object(
            mod, "has_journal_event_since", return_value=False
        ), patch.object(
            mod, "get_active_journal_lots", return_value=1
        ), patch.object(
            mod, "append_trade_journal", side_effect=lambda *args, **kwargs: recorded.append((args, kwargs))
        ), patch.object(
            mod, "update_latest_unclosed_open_journal_entry", return_value=True
        ), patch.object(
            mod, "save_state", lambda *args, **kwargs: None
        ):
            confirmed = mod.confirm_pending_open_from_broker(
                None,
                config,
                self.instrument,
                state,
                not_before=datetime(2026, 4, 9, 15, 4, 59, tzinfo=timezone.utc),
            )

        self.assertTrue(confirmed)
        self.assertEqual(len(recorded), 1)
        self.assertEqual(recorded[0][0][3], 1)

    def test_auto_recovery_splits_combined_close_qty_across_two_unmatched_opens(self) -> None:
        rows = [
            {
                "time": "2026-04-09T16:13:15.749718+03:00",
                "symbol": "TEST",
                "display_name": "Test",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "lot_size": 1,
                "price": 101.0,
                "commission_rub": 6.0,
                "strategy": "range_break_continuation",
                "mode": "LIVE",
                "session": "DAY",
            },
            {
                "time": "2026-04-09T18:04:59.756160+03:00",
                "symbol": "TEST",
                "display_name": "Test",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "lot_size": 1,
                "price": 100.0,
                "commission_rub": 6.0,
                "strategy": "range_break_continuation",
                "mode": "LIVE",
                "session": "DAY",
            },
        ]
        saved = {}
        config = SimpleNamespace(account_id="acc", dry_run=False)
        fee_by_parent = {"close-op": 10.0}
        trade_ops = [
            mod.BrokerTradeOp(
                symbol="TEST",
                display_name="Test",
                figi="FIGI",
                op_id="close-op",
                parent_id="parent",
                op_type=mod.OperationType.OPERATION_TYPE_BUY,
                side="LONG",
                qty=2,
                price=95.0,
                dt=datetime(2026, 4, 9, 19, 25, 12, tzinfo=timezone.utc),
            )
        ]

        def fake_save(new_rows):
            saved["rows"] = new_rows

        with patch.object(mod, "load_trade_journal", return_value=list(rows)), patch.object(
            mod, "save_trade_journal", side_effect=fake_save
        ), patch.object(
            mod, "fetch_trade_operations_for_day", return_value=(trade_ops, fee_by_parent)
        ), patch.object(
            mod, "infer_close_reason_for_recovery", return_value="test reason"
        ):
            recovered = mod.reconcile_missing_trade_closes_from_broker(
                None,
                config,
                [self.instrument],
                target_day=datetime(2026, 4, 9, tzinfo=timezone.utc).date(),
            )

        self.assertEqual(recovered, 2)
        closes = [row for row in saved["rows"] if row.get("event") == "CLOSE"]
        self.assertEqual(len(closes), 2)
        self.assertTrue(all(row["qty_lots"] == 1 for row in closes))
        self.assertTrue(all(row["commission_rub"] == 11.0 for row in closes))
        self.assertEqual([row["broker_op_unit"] for row in closes], [0, 1])

    def test_auto_recovery_matches_previous_day_open_closed_after_midnight(self) -> None:
        rows = [
            {
                "time": "2026-04-09T23:58:00+03:00",
                "symbol": "TEST",
                "display_name": "Test",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "lot_size": 1,
                "price": 101.0,
                "commission_rub": 6.0,
                "strategy": "range_break_continuation",
                "mode": "LIVE",
                "session": "NIGHT",
            },
        ]
        saved = {}
        config = SimpleNamespace(account_id="acc", dry_run=False)
        fee_by_parent = {"close-op": 5.0}
        trade_ops = [
            mod.BrokerTradeOp(
                symbol="TEST",
                display_name="Test",
                figi="FIGI",
                op_id="close-op",
                parent_id="parent",
                op_type=mod.OperationType.OPERATION_TYPE_BUY,
                side="LONG",
                qty=1,
                price=99.0,
                dt=datetime(2026, 4, 9, 21, 5, tzinfo=timezone.utc),
            )
        ]

        def fake_save(new_rows):
            saved["rows"] = new_rows

        with patch.object(mod, "load_trade_journal", return_value=list(rows)), patch.object(
            mod, "save_trade_journal", side_effect=fake_save
        ), patch.object(
            mod, "fetch_trade_operations_for_day", return_value=(trade_ops, fee_by_parent)
        ), patch.object(
            mod, "infer_close_reason_for_recovery", return_value="overnight close"
        ):
            recovered = mod.reconcile_missing_trade_closes_from_broker(
                None,
                config,
                [self.instrument],
                target_day=datetime(2026, 4, 10, tzinfo=timezone.utc).date(),
            )

        self.assertEqual(recovered, 1)
        closes = [row for row in saved["rows"] if row.get("event") == "CLOSE"]
        self.assertEqual(len(closes), 1)
        self.assertEqual(closes[0]["reason"], "overnight close")

    def test_auto_recovery_matches_newest_same_side_open_first(self) -> None:
        rows = [
            {
                "time": "2026-04-28T09:00:00+03:00",
                "symbol": "TEST",
                "display_name": "Test",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "lot_size": 1,
                "price": 120.0,
                "commission_rub": 5.0,
                "strategy": "momentum_breakout",
                "mode": "LIVE",
                "session": "DAY",
            },
            {
                "time": "2026-04-28T10:00:00+03:00",
                "symbol": "TEST",
                "display_name": "Test",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "lot_size": 1,
                "price": 100.0,
                "commission_rub": 5.0,
                "strategy": "momentum_breakout",
                "mode": "LIVE",
                "session": "DAY",
            },
        ]
        saved = {}
        config = SimpleNamespace(account_id="acc", dry_run=False)
        fee_by_parent = {"close-op": 6.0}
        trade_ops = [
            mod.BrokerTradeOp(
                symbol="TEST",
                display_name="Test",
                figi="FIGI",
                op_id="close-op",
                parent_id="parent",
                op_type=mod.OperationType.OPERATION_TYPE_BUY,
                side="LONG",
                qty=1,
                price=99.0,
                dt=datetime(2026, 4, 28, 8, 5, tzinfo=timezone.utc),
            )
        ]

        def fake_save(new_rows):
            saved["rows"] = new_rows

        with patch.object(mod, "load_trade_journal", return_value=list(rows)), patch.object(
            mod, "save_trade_journal", side_effect=fake_save
        ), patch.object(
            mod, "fetch_trade_operations_for_day", return_value=(trade_ops, fee_by_parent)
        ), patch.object(
            mod, "infer_close_reason_for_recovery", return_value="same-side recovery"
        ), patch.object(
            mod, "calculate_futures_pnl_rub", side_effect=lambda instrument, entry_price, exit_price, qty, side: round((entry_price - exit_price) * qty, 2)
        ):
            recovered = mod.reconcile_missing_trade_closes_from_broker(
                None,
                config,
                [self.instrument],
                target_day=datetime(2026, 4, 28, tzinfo=timezone.utc).date(),
            )

        self.assertEqual(recovered, 1)
        closes = [row for row in saved["rows"] if row.get("event") == "CLOSE"]
        self.assertEqual(len(closes), 1)
        self.assertEqual(closes[0]["gross_pnl_rub"], 1.0)
        self.assertEqual(closes[0]["net_pnl_rub"], -10.0)

    def test_auto_recovery_does_not_consume_broker_open_claim_from_same_day(self) -> None:
        rows = [
            {
                "time": "2026-04-23T15:55:34+03:00",
                "symbol": "TEST",
                "display_name": "Test",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "lot_size": 1,
                "price": 110.0,
                "commission_rub": 5.0,
                "strategy": "range_break_continuation",
                "source": "portfolio_confirmation",
                "mode": "LIVE",
                "session": "DAY",
            },
            {
                "time": "2026-04-29T10:20:52+03:00",
                "symbol": "TEST",
                "display_name": "Test",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 2,
                "lot_size": 1,
                "price": 100.0,
                "commission_rub": 10.0,
                "strategy": "range_break_continuation",
                "source": "portfolio_confirmation",
                "mode": "LIVE",
                "session": "DAY",
            },
        ]
        saved = {}
        config = SimpleNamespace(account_id="acc", dry_run=False)
        fee_by_parent = {"sell-op": 6.0, "buy-op": 6.0}
        trade_ops = [
            mod.BrokerTradeOp(
                symbol="TEST",
                display_name="Test",
                figi="FIGI",
                op_id="sell-op",
                parent_id="parent",
                op_type=mod.OperationType.OPERATION_TYPE_SELL,
                side="SHORT",
                qty=2,
                price=100.0,
                dt=datetime(2026, 4, 29, 7, 20, 32, tzinfo=timezone.utc),
            ),
            mod.BrokerTradeOp(
                symbol="TEST",
                display_name="Test",
                figi="FIGI",
                op_id="buy-op",
                parent_id="parent",
                op_type=mod.OperationType.OPERATION_TYPE_BUY,
                side="LONG",
                qty=2,
                price=101.0,
                dt=datetime(2026, 4, 29, 7, 45, 38, tzinfo=timezone.utc),
            ),
        ]

        def fake_save(new_rows):
            saved["rows"] = new_rows

        with patch.object(mod, "load_trade_journal", return_value=list(rows)), patch.object(
            mod, "save_trade_journal", side_effect=fake_save
        ), patch.object(
            mod, "fetch_trade_operations_for_day", return_value=(trade_ops, fee_by_parent)
        ), patch.object(
            mod, "infer_close_reason_for_recovery", return_value="test reason"
        ), patch.object(
            mod,
            "calculate_futures_pnl_rub",
            side_effect=lambda instrument, entry_price, exit_price, qty, side: round((entry_price - exit_price) * qty, 2)
            if side == "SHORT"
            else round((exit_price - entry_price) * qty, 2),
        ):
            recovered = mod.reconcile_missing_trade_closes_from_broker(
                None,
                config,
                [self.instrument],
                target_day=datetime(2026, 4, 29, tzinfo=timezone.utc).date(),
            )

        self.assertEqual(recovered, 2)
        closes = [row for row in saved["rows"] if row.get("event") == "CLOSE"]
        self.assertEqual(len(closes), 2)
        self.assertTrue(all(row["side"] == "SHORT" for row in closes))

    def test_build_today_journal_integrity_alert_reports_broker_issue(self) -> None:
        fake_audit = SimpleNamespace(
            broker_alignment_issues=[{"symbol": "IMOEXF", "type": "broker_op_mismatch"}],
            orphan_closes=[],
        )

        with patch("scripts.audit_trade_journal_integrity.classify_journal", return_value=fake_audit), patch.object(
            mod, "load_trade_journal", return_value=[]
        ), patch.object(mod, "current_moscow_time", return_value=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc)):
            alert = mod.build_today_journal_integrity_alert()

        self.assertIn("journal integrity alert", alert)
        self.assertIn("IMOEXF", alert)
        self.assertIn("broker_op_mismatch", alert)

    def test_infer_close_reason_for_recovery_matches_closest_delayed_queue_item(self) -> None:
        state = mod.InstrumentState(
            delayed_close_queue=[
                {
                    "side": "SHORT",
                    "qty": 1,
                    "entry_price": 101.0,
                    "entry_commission_rub": 5.0,
                    "strategy": "s1",
                    "reason": "Старый reason",
                    "entry_time": datetime(2026, 4, 10, 9, 0, tzinfo=timezone.utc).isoformat(),
                    "submitted_at": datetime(2026, 4, 10, 10, 0, tzinfo=timezone.utc).isoformat(),
                },
                {
                    "side": "SHORT",
                    "qty": 1,
                    "entry_price": 99.0,
                    "entry_commission_rub": 5.0,
                    "strategy": "s2",
                    "reason": "Нужный reason",
                    "entry_time": datetime(2026, 4, 10, 11, 0, tzinfo=timezone.utc).isoformat(),
                    "submitted_at": datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc).isoformat(),
                },
            ]
        )
        mod.sync_legacy_delayed_close_fields(state)

        with patch.object(mod, "load_state", return_value=state):
            reason = mod.infer_close_reason_for_recovery(
                "TEST",
                "SHORT",
                1,
                datetime(2026, 4, 10, 12, 3, tzinfo=timezone.utc),
            )

        self.assertEqual(reason, "Нужный reason")

    def test_append_trade_journal_skips_semantic_duplicate_open_from_recovery(self) -> None:
        instrument = mod.InstrumentConfig(symbol="GNM6", figi="FIGI", display_name="Gold")
        with TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            journal_path = log_dir / "trade_journal.jsonl"
            trade_db_path = log_dir / "trade_analytics.sqlite3"
            with patch.object(mod, "LOG_DIR", log_dir), patch.object(mod, "TRADE_JOURNAL_PATH", journal_path), patch.object(
                mod, "TRADE_DB_PATH", trade_db_path
            ):
                event_time = datetime(2026, 4, 9, 14, 55, 4, 212691, tzinfo=timezone.utc)
                mod.append_trade_journal(
                    instrument,
                    "OPEN",
                    "LONG",
                    1,
                    4805.4,
                    event_time=event_time,
                    commission_rub=9.41,
                    net_pnl_rub=-9.41,
                    reason="from confirmation",
                    source="portfolio_confirmation",
                    strategy="trend_rollover",
                    dry_run=False,
                )
                mod.append_trade_journal(
                    instrument,
                    "OPEN",
                    "LONG",
                    1,
                    4805.4,
                    event_time=event_time,
                    commission_rub=9.41,
                    net_pnl_rub=-9.41,
                    reason="from recovery",
                    source="portfolio_recovery",
                    strategy="trend_rollover",
                    dry_run=False,
                )

                rows = mod.load_trade_journal()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["source"], "portfolio_confirmation")

    def test_active_journal_lots_ignores_orphan_close_before_new_open(self) -> None:
        rows = [
            {
                "time": "2026-04-10T09:10:14+03:00",
                "symbol": "USDRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
            },
            {
                "time": "2026-04-10T10:05:08+03:00",
                "symbol": "USDRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
            },
        ]
        self.assertEqual(mod.get_active_journal_lots("USDRUBF", "SHORT", rows), 1)

    def test_old_reentry_exit_from_previous_trading_day_does_not_block_brk6(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="BRK6",
            figi="FIGI",
            display_name="Brent",
            min_price_increment=0.01,
        )
        state = mod.InstrumentState(
            trading_day="2026-04-13",
            last_exit_time=datetime(2026, 4, 4, 15, 45, tzinfo=timezone.utc).isoformat(),
            last_exit_side="LONG",
            last_exit_pnl_rub=-188.99,
            last_exit_price=114.07,
        )

        allowed, reason = mod.position_reentry_allowed(state, instrument, "LONG", 98.82)

        self.assertTrue(allowed)
        self.assertEqual(reason, "")

    def test_fx_reentry_after_macd_exit_requires_new_extreme(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="UCM6",
            figi="FIGI",
            display_name="USD/CNY",
            min_price_increment=0.001,
        )
        state = mod.InstrumentState(
            trading_day="2026-04-13",
            last_exit_time=datetime(2026, 4, 13, 15, 0, tzinfo=timezone.utc).isoformat(),
            last_exit_side="SHORT",
            last_exit_pnl_rub=-72.06,
            last_exit_price=6.818,
            last_exit_reason="MACD подтверждённо развернулся вверх и цена вернулась выше EMA20",
        )

        allowed, reason = mod.position_reentry_allowed(state, instrument, "SHORT", 6.817)

        self.assertFalse(allowed)
        self.assertIn("нового экстремума", reason)

    def test_fx_reentry_after_macd_exit_allows_new_extreme(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="UCM6",
            figi="FIGI",
            display_name="USD/CNY",
            min_price_increment=0.001,
        )
        state = mod.InstrumentState(
            trading_day="2026-04-13",
            last_exit_time=datetime(2026, 4, 13, 15, 0, tzinfo=timezone.utc).isoformat(),
            last_exit_side="SHORT",
            last_exit_pnl_rub=-72.06,
            last_exit_price=6.818,
            last_exit_reason="MACD подтверждённо развернулся вверх и цена вернулась выше EMA20",
        )

        allowed, reason = mod.position_reentry_allowed(state, instrument, "SHORT", 6.815)

        self.assertTrue(allowed)
        self.assertEqual(reason, "")

    def test_ngj6_rsi_profit_reentry_requires_new_extreme(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="NGJ6",
            figi="FIGI",
            display_name="Natural Gas",
            min_price_increment=0.001,
        )
        state = mod.InstrumentState(
            trading_day="2026-04-13",
            last_exit_time=datetime(2026, 4, 13, 11, 48, tzinfo=timezone.utc).isoformat(),
            last_exit_side="LONG",
            last_exit_pnl_rub=174.16,
            last_exit_price=2.759,
            last_exit_reason="RSI вышел в зону перегрева: 66.41 >= 65.00",
        )

        allowed, reason = mod.position_reentry_allowed(state, instrument, "LONG", 2.760)

        self.assertFalse(allowed)
        self.assertIn("RSI-фиксации", reason)

    def test_equity_future_reentry_after_loss_requires_new_extreme(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="VBM6",
            figi="FIGI",
            display_name="VTB",
            min_price_increment=1.0,
        )
        state = mod.InstrumentState(
            trading_day="2026-04-13",
            last_exit_time=datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc).isoformat(),
            last_exit_side="LONG",
            last_exit_pnl_rub=-18.7,
            last_exit_price=9386.0,
            last_exit_reason="MACD подтверждённо развернулся вниз и цена потеряла EMA20",
        )

        allowed, reason = mod.position_reentry_allowed(state, instrument, "LONG", 9387.0)

        self.assertFalse(allowed)
        self.assertIn("нового экстремума", reason)

    def test_imoexf_uses_failed_breakout_primary_strategy(self) -> None:
        strategies = strategy_registry.get_primary_strategies("IMOEXF")

        self.assertIn("failed_breakout", strategies)
        self.assertEqual(strategies, ["range_break_continuation", "failed_breakout", "trend_pullback"])

    def test_srm6_uses_failed_breakout_primary_strategy(self) -> None:
        strategies = strategy_registry.get_primary_strategies("SRM6")

        self.assertIn("failed_breakout", strategies)
        self.assertEqual(strategies, ["range_break_continuation", "failed_breakout", "trend_pullback"])

    def test_rbm6_uses_reversal_first_primary_strategy(self) -> None:
        strategies = strategy_registry.get_primary_strategies("RBM6")

        self.assertEqual(strategies, ["failed_breakout", "range_break_continuation", "trend_pullback"])

    def test_gnm6_uses_pullback_first_primary_strategy(self) -> None:
        strategies = strategy_registry.get_primary_strategies("GNM6")

        self.assertEqual(strategies, ["trend_pullback", "momentum_breakout", "trend_rollover"])

    def test_update_latest_unclosed_open_respects_not_before(self) -> None:
        rows = [
            {
                "time": "2026-04-08T18:00:00+00:00",
                "symbol": "TEST",
                "side": "SHORT",
                "event": "OPEN",
                "commission_rub": None,
                "net_pnl_rub": None,
            },
            {
                "time": "2026-04-08T19:00:00+00:00",
                "symbol": "TEST",
                "side": "SHORT",
                "event": "OPEN",
                "commission_rub": None,
                "net_pnl_rub": None,
            },
        ]
        saved = {}

        def fake_load():
            return [dict(row) for row in rows]

        def fake_save(new_rows):
            saved["rows"] = new_rows

        with patch.object(mod, "load_trade_journal", side_effect=fake_load), patch.object(
            mod, "save_trade_journal", side_effect=fake_save
        ):
            changed = mod.update_latest_unclosed_open_journal_entry(
                "TEST",
                "SHORT",
                not_before=datetime(2026, 4, 8, 18, 30, tzinfo=timezone.utc),
                commission_rub=7.5,
                net_pnl_rub=-7.5,
            )

        self.assertTrue(changed)
        self.assertEqual(saved["rows"][0]["commission_rub"], None)
        self.assertEqual(saved["rows"][1]["commission_rub"], 7.5)

    def test_pair_trade_journal_rows_keeps_sides_separate(self) -> None:
        rows = [
            {
                "time": "2026-04-08T10:00:00+00:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "OPEN",
                "price": 100.0,
                "qty_lots": 1,
                "reason": "long open",
                "strategy": "s1",
            },
            {
                "time": "2026-04-08T10:05:00+00:00",
                "symbol": "TEST",
                "side": "SHORT",
                "event": "OPEN",
                "price": 99.0,
                "qty_lots": 1,
                "reason": "short open",
                "strategy": "s2",
            },
            {
                "time": "2026-04-08T10:10:00+00:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "CLOSE",
                "price": 101.0,
                "qty_lots": 1,
                "reason": "long close",
                "strategy": "s1",
            },
        ]
        closed_reviews, current_open = mod.pair_trade_journal_rows(rows)
        self.assertEqual(len(closed_reviews), 1)
        self.assertEqual(closed_reviews[0]["side"], "LONG")
        self.assertEqual(closed_reviews[0]["entry_reason"], "long open")
        self.assertIn("TEST", current_open)
        self.assertEqual(current_open["TEST"]["side"], "SHORT")

    def test_auto_recovery_appends_missing_close_from_broker_ops(self) -> None:
        open_time = datetime(2026, 4, 9, 10, 0, tzinfo=timezone.utc)
        close_time = datetime(2026, 4, 9, 10, 30, tzinfo=timezone.utc)
        rows = [
            {
                "time": open_time.isoformat(),
                "symbol": "TEST",
                "display_name": "Test",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "lot_size": 1,
                "price": 100.0,
                "commission_rub": 2.0,
                "strategy": "range_break_continuation",
                "mode": "LIVE",
                "session": "DAY",
            }
        ]
        saved = {}
        config = SimpleNamespace(account_id="acc", dry_run=False)
        fee_by_parent = {"close-op": 3.0}
        trade_ops = [
            mod.BrokerTradeOp(
                symbol="TEST",
                display_name="Test",
                figi="FIGI",
                op_id="close-op",
                parent_id="parent",
                op_type=mod.OperationType.OPERATION_TYPE_BUY,
                side="LONG",
                qty=1,
                price=95.0,
                dt=close_time,
            )
        ]

        def fake_save(new_rows):
            saved["rows"] = new_rows

        with patch.object(mod, "load_trade_journal", return_value=list(rows)), patch.object(
            mod, "save_trade_journal", side_effect=fake_save
        ), patch.object(
            mod, "fetch_trade_operations_for_day", return_value=(trade_ops, fee_by_parent)
        ), patch.object(
            mod, "infer_close_reason_for_recovery", return_value="test reason"
        ):
            recovered = mod.reconcile_missing_trade_closes_from_broker(
                None,
                config,
                [self.instrument],
                target_day=datetime(2026, 4, 9, tzinfo=timezone.utc).date(),
            )

        self.assertEqual(recovered, 1)
        self.assertEqual(len(saved["rows"]), 2)
        close_row = saved["rows"][1]
        self.assertEqual(close_row["event"], "CLOSE")
        self.assertEqual(close_row["symbol"], "TEST")
        self.assertEqual(close_row["reason"], "test reason")
        self.assertEqual(close_row["commission_rub"], 5.0)

    def test_trade_journal_queue_splits_open_fees_and_counts_close_units(self) -> None:
        rows = [
            {
                "time": "2026-04-13T09:25:10+03:00",
                "symbol": "TEST",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 3,
                "price": 11.17,
                "commission_rub": 8.37,
                "net_pnl_rub": -8.37,
            },
            {
                "time": "2026-04-13T15:15:22+03:00",
                "symbol": "TEST",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
            },
            {
                "time": "2026-04-13T15:15:22+03:00",
                "symbol": "TEST",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
            },
        ]

        unmatched, close_signatures = mod.build_trade_journal_queues_for_day(
            rows,
            datetime(2026, 4, 13, tzinfo=timezone.utc).date(),
        )

        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched[0]["qty_lots"], 1)
        self.assertEqual(unmatched[0]["commission_rub"], 2.79)
        self.assertIn("TEST:SHORT:ts:1776082522:0", close_signatures)
        self.assertIn("TEST:SHORT:ts:1776082522:1", close_signatures)

    def test_trade_journal_queue_ignores_duplicate_carry_open_and_matches_latest_open(self) -> None:
        rows = [
            {
                "time": "2026-04-16T23:14:10+03:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 100.0,
                "strategy": "momentum_breakout",
                "source": "portfolio_confirmation",
            },
            {
                "time": "2026-04-17T08:50:19+03:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 100.0,
                "strategy": "momentum_breakout",
                "source": "portfolio_confirmation",
            },
            {
                "time": "2026-04-17T09:00:31+03:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 99.5,
                "source": "broker_ops_auto_recovery",
            },
        ]

        unmatched, _close_signatures = mod.build_trade_journal_queues_for_day(
            rows,
            datetime(2026, 4, 17, tzinfo=timezone.utc).date(),
        )

        self.assertEqual(unmatched, [])

    def test_pair_trade_journal_rows_ignores_duplicate_carry_open(self) -> None:
        rows = [
            {
                "time": "2026-04-16T23:14:10+03:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 100.0,
                "reason": "carry open",
                "strategy": "momentum_breakout",
                "source": "portfolio_confirmation",
            },
            {
                "time": "2026-04-17T08:50:19+03:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 100.0,
                "reason": "duplicate carry open",
                "strategy": "momentum_breakout",
                "source": "portfolio_confirmation",
            },
            {
                "time": "2026-04-17T09:00:31+03:00",
                "symbol": "TEST",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 99.5,
                "reason": "close",
                "strategy": "momentum_breakout",
                "source": "broker_ops_auto_recovery",
            },
        ]

        closed_reviews, current_open = mod.pair_trade_journal_rows(rows)

        self.assertEqual(len(closed_reviews), 1)
        self.assertEqual(closed_reviews[0]["entry_time"], "2026-04-16T23:14:10+03:00")
        self.assertEqual(current_open, {})

    def test_duplicate_carry_open_allows_weekend_portfolio_recovery_dedupe(self) -> None:
        previous = {
            "time": "2026-04-17T22:20:06+03:00",
            "symbol": "TEST",
            "side": "LONG",
            "event": "OPEN",
            "price": 11.136,
            "strategy": "range_break_continuation",
            "source": "portfolio_confirmation",
        }
        candidate = {
            "time": "2026-04-19T16:40:12+03:00",
            "symbol": "TEST",
            "side": "LONG",
            "event": "OPEN",
            "price": 11.136,
            "strategy": "range_break_continuation",
            "source": "portfolio_recovery",
        }

        self.assertTrue(mod.is_duplicate_carry_open(previous, candidate))


if __name__ == "__main__":
    unittest.main()
