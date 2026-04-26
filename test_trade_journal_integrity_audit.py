import unittest
from unittest.mock import patch

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
        ]

        with patch.object(audit_mod, "load_rows", return_value=[dict(row) for row in rows]):
            audit = audit_mod.classify_journal(audit_mod.load_rows())

        self.assertEqual(len(audit.bogus_rebuild_opens), 1)
        self.assertEqual(len(audit.duplicate_portfolio_recovery_opens), 1)
        self.assertEqual(len(audit.orphan_closes), 1)

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
        ]
        enriched = []
        for row in rows:
            item = dict(row)
            item["_dt"] = audit_mod.parse_state_datetime(item["time"])
            enriched.append(item)
        audit = audit_mod.classify_journal(enriched)

        cleaned_rows, removed = audit_mod.cleanup_safe_rows(enriched, audit)

        self.assertEqual(removed, 2)
        self.assertEqual(len(cleaned_rows), 2)
        self.assertEqual(cleaned_rows[0]["source"], "portfolio_confirmation")
        self.assertEqual(cleaned_rows[1]["source"], "delayed_broker_ops_recovery")


if __name__ == "__main__":
    unittest.main()
