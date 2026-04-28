import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import scripts.audit_trade_journal_integrity as audit_mod


class TradeJournalIntegrityAuditTests(unittest.TestCase):
    def test_classify_journal_flags_safe_legacy_rows(self) -> None:
        rows = [
            {
                "time": "2026-04-07T10:06:26+03:00",
                "symbol": "CNYRUBF",
                "event": "OPEN",
                "side": "SHORT",
                "price": 11.466,
                "qty_lots": 1,
                "strategy": "opening_range_breakout",
                "source": "broker_ops_rebuild",
                "reason": "status=stale_pending_cleared reason=Подвисшая заявка очищена, открытой позиции нет.",
            },
            {
                "time": "2026-04-17T22:20:06+03:00",
                "symbol": "TEST",
                "event": "OPEN",
                "side": "LONG",
                "price": 11.136,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "portfolio_confirmation",
                "reason": "carry",
            },
            {
                "time": "2026-04-19T16:40:12+03:00",
                "symbol": "TEST",
                "event": "OPEN",
                "side": "LONG",
                "price": 11.136,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "portfolio_recovery",
                "reason": "carry",
            },
            {
                "time": "2026-04-20T10:00:00+03:00",
                "symbol": "TEST",
                "event": "CLOSE",
                "side": "SHORT",
                "price": 11.101,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "delayed_broker_ops_recovery",
                "reason": "orphan",
            },
            {
                "time": "2026-04-20T10:01:00+03:00",
                "symbol": "TEST",
                "event": "CLOSE",
                "side": "SHORT",
                "price": 11.102,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "delayed_broker_ops_recovery",
                "reason": "orphan",
            },
        ]

        with patch.object(audit_mod, "load_rows", return_value=[dict(row) for row in rows]), patch.object(
            audit_mod, "load_live_position_map", return_value={}
        ):
            audit = audit_mod.classify_journal(audit_mod.load_rows())

        self.assertEqual(len(audit.bogus_rebuild_opens), 1)
        self.assertEqual(len(audit.duplicate_portfolio_recovery_opens), 1)
        self.assertEqual(len(audit.orphan_closes), 1)
        self.assertEqual(len(audit.duplicate_orphan_closes), 1)

    def test_cleanup_safe_rows_removes_only_safe_categories(self) -> None:
        rows = [
            {
                "time": "2026-04-07T10:06:26+03:00",
                "symbol": "CNYRUBF",
                "event": "OPEN",
                "side": "SHORT",
                "price": 11.466,
                "qty_lots": 1,
                "strategy": "opening_range_breakout",
                "source": "broker_ops_rebuild",
                "reason": "status=stale_pending_cleared reason=Подвисшая заявка очищена, открытой позиции нет.",
            },
            {
                "time": "2026-04-17T22:20:06+03:00",
                "symbol": "TEST",
                "event": "OPEN",
                "side": "LONG",
                "price": 11.136,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "portfolio_confirmation",
                "reason": "carry",
            },
            {
                "time": "2026-04-19T16:40:12+03:00",
                "symbol": "TEST",
                "event": "OPEN",
                "side": "LONG",
                "price": 11.136,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "portfolio_recovery",
                "reason": "carry",
            },
            {
                "time": "2026-04-20T10:00:00+03:00",
                "symbol": "TEST",
                "event": "CLOSE",
                "side": "SHORT",
                "price": 11.101,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "delayed_broker_ops_recovery",
                "reason": "orphan",
            },
            {
                "time": "2026-04-20T10:01:00+03:00",
                "symbol": "TEST",
                "event": "CLOSE",
                "side": "SHORT",
                "price": 11.102,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "delayed_broker_ops_recovery",
                "reason": "orphan",
            },
        ]
        enriched = []
        for row in rows:
            item = dict(row)
            item["_dt"] = audit_mod.parse_state_datetime(item["time"])
            enriched.append(item)
        audit = audit_mod.classify_journal(enriched)

        cleaned_rows, removed = audit_mod.cleanup_safe_rows(enriched, audit)

        self.assertEqual(removed, 3)
        self.assertEqual(len(cleaned_rows), 2)
        self.assertEqual(cleaned_rows[0]["source"], "portfolio_confirmation")
        self.assertEqual(cleaned_rows[1]["source"], "delayed_broker_ops_recovery")

    def test_classify_journal_separates_live_and_stale_unmatched_opens(self) -> None:
        rows = [
            {
                "time": "2026-04-24T16:05:21+03:00",
                "symbol": "CNYRUBF",
                "event": "OPEN",
                "side": "SHORT",
                "price": 11.011,
                "qty_lots": 3,
                "strategy": "range_break_continuation",
                "source": "portfolio_confirmation",
                "reason": "live position",
            },
            {
                "time": "2026-04-10T22:20:26+03:00",
                "symbol": "SRM6",
                "event": "OPEN",
                "side": "SHORT",
                "price": 32547.0,
                "qty_lots": 1,
                "strategy": "range_break_continuation",
                "source": "portfolio_confirmation",
                "reason": "stale open",
            },
        ]
        enriched = []
        for row in rows:
            item = dict(row)
            item["_dt"] = audit_mod.parse_state_datetime(item["time"])
            enriched.append(item)

        with patch.object(audit_mod, "load_live_position_map", return_value={("CNYRUBF", "SHORT"): 3}):
            audit = audit_mod.classify_journal(enriched)

        self.assertEqual(audit.live_unmatched_open_counts, {"CNYRUBF": 1})
        self.assertEqual(audit.stale_unmatched_open_counts, {"SRM6": 1})

    def test_classify_journal_includes_broker_alignment_issues_when_day_requested(self) -> None:
        rows = [
            {
                "time": "2026-04-28T14:30:57+03:00",
                "symbol": "VBM6",
                "event": "OPEN",
                "side": "SHORT",
                "price": 9488.0,
                "qty_lots": 3,
                "strategy": "range_break_continuation",
                "_dt": audit_mod.parse_state_datetime("2026-04-28T14:30:57+03:00"),
            }
        ]

        with patch.object(audit_mod, "load_broker_alignment_issues", return_value=[{"type": "broker_signature_mismatch"}]):
            audit = audit_mod.classify_journal(rows, broker_day=date(2026, 4, 28))

        self.assertEqual(audit.broker_alignment_issues, [{"type": "broker_signature_mismatch"}])

    def test_load_broker_alignment_issues_flags_broker_op_and_pairing_mismatch(self) -> None:
        rows = [
            {
                "time": "2026-04-28T14:30:57+03:00",
                "symbol": "VBM6",
                "event": "OPEN",
                "side": "SHORT",
                "price": 9488.0,
                "qty_lots": 3,
                "strategy": "range_break_continuation",
                "_dt": audit_mod.parse_state_datetime("2026-04-28T14:30:57+03:00"),
            },
            {
                "time": "2026-04-28T14:37:59+03:00",
                "symbol": "VBM6",
                "event": "CLOSE",
                "side": "SHORT",
                "price": 9493.0,
                "qty_lots": 1,
                "gross_pnl_rub": 77.0,
                "strategy": "range_break_continuation",
                "broker_op_id": "close-1",
                "_dt": audit_mod.parse_state_datetime("2026-04-28T14:37:59+03:00"),
            },
        ]

        broker_ops = [
            SimpleNamespace(
                symbol="VBM6",
                op_id="sell-1",
                side="SHORT",
                qty=3,
                price=9488.0,
                dt=datetime(2026, 4, 28, 11, 30, 57, tzinfo=timezone.utc),
            ),
            SimpleNamespace(
                symbol="VBM6",
                op_id="close-1",
                side="LONG",
                qty=3,
                price=9493.0,
                dt=datetime(2026, 4, 28, 11, 37, 59, tzinfo=timezone.utc),
            ),
        ]

        fake_config = SimpleNamespace(account_id="acc", token="token")
        fake_client = object()
        client_cls = MagicMock()
        client_cls.return_value.__enter__.return_value = fake_client
        client_cls.return_value.__exit__.return_value = False
        with patch.object(audit_mod, "get_broker_audit_dependencies") as deps_mock, patch.object(
            audit_mod, "resolve_instruments_for_audit",
            return_value=[{"symbol": "VBM6", "figi": "FIGI", "display_name": "VTB"}],
        ):
            deps_mock.return_value = (client_cls, lambda: fake_config, lambda *args: (broker_ops, {}))
            issues = audit_mod.load_broker_alignment_issues(date(2026, 4, 28), rows)

        issue_types = {item["type"] for item in issues}
        self.assertIn("broker_op_mismatch", issue_types)
        self.assertIn("same_day_pairing_sign_mismatch", issue_types)
        self.assertIn("broker_signature_mismatch", issue_types)

    def test_load_broker_alignment_issues_ignores_portfolio_recovery_open_and_normalizes_tick(self) -> None:
        rows = [
            {
                "time": "2026-04-27T08:50:18+03:00",
                "symbol": "CNYRUBF",
                "event": "OPEN",
                "side": "SHORT",
                "price": 11.011,
                "qty_lots": 3,
                "strategy": "range_break_continuation",
                "source": "portfolio_recovery",
                "_dt": audit_mod.parse_state_datetime("2026-04-27T08:50:18+03:00"),
            },
            {
                "time": "2026-04-27T18:55:39+03:00",
                "symbol": "GNM6",
                "event": "OPEN",
                "side": "SHORT",
                "price": 4676.8,
                "qty_lots": 2,
                "strategy": "momentum_breakout",
                "source": "portfolio_confirmation",
                "_dt": audit_mod.parse_state_datetime("2026-04-27T18:55:39+03:00"),
            },
        ]

        broker_ops = [
            SimpleNamespace(
                symbol="GNM6",
                op_id="sell-1",
                side="SHORT",
                qty=2,
                price=4676.75,
                dt=datetime(2026, 4, 27, 15, 55, 20, tzinfo=timezone.utc),
            )
        ]

        fake_config = SimpleNamespace(account_id="acc", token="token")
        fake_client = object()
        client_cls = MagicMock()
        client_cls.return_value.__enter__.return_value = fake_client
        client_cls.return_value.__exit__.return_value = False
        with patch.object(audit_mod, "get_broker_audit_dependencies") as deps_mock, patch.object(
            audit_mod, "resolve_instruments_for_audit",
            return_value=[
                {"symbol": "CNYRUBF", "figi": "FIGI1", "display_name": "CNY", "min_price_increment": 0.001},
                {"symbol": "GNM6", "figi": "FIGI2", "display_name": "Gold", "min_price_increment": 0.1},
            ],
        ):
            deps_mock.return_value = (client_cls, lambda: fake_config, lambda *args: (broker_ops, {}))
            issues = audit_mod.load_broker_alignment_issues(date(2026, 4, 27), rows)

        self.assertEqual([item for item in issues if item["symbol"] == "GNM6"], [])
        cny_signature_issues = [item for item in issues if item["symbol"] == "CNYRUBF" and item["type"] == "broker_signature_mismatch"]
        self.assertEqual(cny_signature_issues, [])


if __name__ == "__main__":
    unittest.main()
