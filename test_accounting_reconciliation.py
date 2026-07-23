from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import bot_oil_main as mod


class AccountingReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SimpleNamespace()
        self.watchlist = []

    def test_reconciles_previous_day_once_after_clearing_window(self) -> None:
        meta: dict[str, str] = {}
        now = datetime(2026, 7, 22, 21, 17, tzinfo=timezone.utc)  # 00:17 Moscow
        entry = {"source": "live", "actual_varmargin_rub": -1819.95}

        with patch.object(mod, "update_accounting_history_for_day", return_value=entry) as refresh:
            refreshed = mod.reconcile_previous_accounting_day_if_needed(
                None, self.config, self.watchlist, meta, now=now
            )
            repeated = mod.reconcile_previous_accounting_day_if_needed(
                None, self.config, self.watchlist, meta, now=now
            )

        self.assertTrue(refreshed)
        self.assertFalse(repeated)
        self.assertEqual(meta["accounting_reconciled_through"], "2026-07-22")
        refresh.assert_called_once_with(None, self.config, self.watchlist, date(2026, 7, 22))

    def test_waits_until_broker_posting_window_has_ended(self) -> None:
        meta: dict[str, str] = {}
        now = datetime(2026, 7, 22, 21, 15, tzinfo=timezone.utc)  # 00:15 Moscow

        with patch.object(mod, "update_accounting_history_for_day") as refresh:
            refreshed = mod.reconcile_previous_accounting_day_if_needed(
                None, self.config, self.watchlist, meta, now=now
            )

        self.assertFalse(refreshed)
        refresh.assert_not_called()

    def test_evening_clearing_after_midnight_stays_on_previous_day(self) -> None:
        target_day = date(2026, 7, 22)
        before_midnight = SimpleNamespace(
            type=next(iter(mod.VARMARGIN_OPERATION_TYPES)),
            payment=SimpleNamespace(units=-1819, nano=-950_000_000),
            date=datetime(2026, 7, 22, 20, 59, tzinfo=timezone.utc),  # 23:59 Moscow
            figi="FIGI",
        )
        after_midnight = SimpleNamespace(
            type=next(iter(mod.VARMARGIN_OPERATION_TYPES)),
            payment=SimpleNamespace(units=100, nano=0),
            date=datetime(2026, 7, 22, 21, 5, tzinfo=timezone.utc),  # 00:05 Moscow next day
            figi="FIGI",
        )
        next_evening = SimpleNamespace(
            type=next(iter(mod.VARMARGIN_OPERATION_TYPES)),
            payment=SimpleNamespace(units=500, nano=0),
            date=datetime(2026, 7, 23, 20, 59, tzinfo=timezone.utc),
            figi="FIGI",
        )
        response = SimpleNamespace(
            items=[before_midnight, after_midnight, next_evening],
            has_next=False,
            next_cursor="",
        )
        client = SimpleNamespace(operations=SimpleNamespace(get_operations_by_cursor=lambda _: response))
        config = SimpleNamespace(account_id="account")
        watchlist = [SimpleNamespace(figi="FIGI", symbol="BMQ6")]

        result = mod.get_accounting_snapshot_for_day(client, config, target_day, watchlist)

        self.assertEqual(result["actual_varmargin_rub"], -1719.95)
        self.assertEqual(result["varmargin_by_symbol"], {"BMQ6": -1719.95})

    def test_audit_rewrites_one_day_only_after_live_broker_response(self) -> None:
        meta: dict[str, str] = {"accounting_audit_next_day": "2026-07-13"}
        now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)

        with patch.object(
            mod,
            "update_accounting_history_for_day",
            return_value={"source": "live", "actual_varmargin_rub": 100.0},
        ) as refresh:
            reconciled = mod.reconcile_accounting_history_audit_if_needed(
                None, self.config, self.watchlist, meta, now=now
            )

        self.assertTrue(reconciled)
        self.assertEqual(meta["accounting_audit_next_day"], "2026-07-14")
        refresh.assert_called_once_with(None, self.config, self.watchlist, date(2026, 7, 13))

    def test_audit_retries_later_without_advancing_on_cached_data(self) -> None:
        meta: dict[str, str] = {"accounting_audit_next_day": "2026-07-13"}
        now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)

        with patch.object(
            mod,
            "update_accounting_history_for_day",
            return_value={"source": "history_fallback", "actual_varmargin_rub": 0.0},
        ):
            reconciled = mod.reconcile_accounting_history_audit_if_needed(
                None, self.config, self.watchlist, meta, now=now
            )

        self.assertFalse(reconciled)
        self.assertEqual(meta["accounting_audit_next_day"], "2026-07-13")
        self.assertIn("accounting_audit_retry_after", meta)


if __name__ == "__main__":
    unittest.main()
