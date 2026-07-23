from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import bot_oil_main as mod


class AccountingReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SimpleNamespace()
        self.watchlist = []
        mod.SETTLED_VAR_MARGIN_CACHE.update({"fetched_at": 0.0, "account_id": "", "values": {}})

    def test_settled_variation_margin_uses_broker_fixing_field(self) -> None:
        response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "positions": [
                    {"figi": "BMQ6", "varMarginSettled": {"units": "6669", "nano": 910_000_000}},
                    {"figi": "UCU6", "varMarginSettled": {"units": "57", "nano": 840_000_000}},
                ]
            },
        )
        config = SimpleNamespace(account_id="account", token="token", target=mod.INVEST_GRPC_API)

        with patch.object(mod.requests, "post", return_value=response) as post:
            result = mod.get_settled_variation_margins(config)

        self.assertEqual(result, {"BMQ6": 6669.91, "UCU6": 57.84})
        post.assert_called_once()

    def test_live_position_keeps_broker_income_and_variation_margin_separate(self) -> None:
        quote = lambda value: SimpleNamespace(units=int(value), nano=int(round((value - int(value)) * 1_000_000_000)))
        position = SimpleNamespace(
            figi="FIGI",
            quantity=quote(2),
            average_position_price=quote(100.0),
            current_price=quote(105.0),
            var_margin=quote(123.45),
            expected_yield=quote(210.75),
        )
        client = SimpleNamespace(
            operations=SimpleNamespace(
                get_portfolio=lambda account_id: SimpleNamespace(positions=[position])
            )
        )
        config = SimpleNamespace(account_id="account")
        watchlist = [
            SimpleNamespace(
                figi="FIGI",
                symbol="BMQ6",
                lot=1,
                min_price_increment=0.01,
                min_price_increment_amount=1.0,
            )
        ]

        result = mod.get_live_portfolio_positions(client, config, watchlist)["BMQ6"]

        self.assertEqual(result["income_rub"], 210.75)
        self.assertEqual(result["variation_margin_rub"], 123.45)
        self.assertEqual(result["variation_margin_source"], "broker_var_margin")

    def test_parses_moex_funding_table_with_rates_and_lots(self) -> None:
        article = """
        <pre>Asset    Funding   Lot
USDRUBF  0.00225  1000
CNYRUBF  0.00497  1000
IMOEXF   1.22604    10</pre>
        """

        result = mod.parse_moex_funding_rates(article)

        self.assertEqual(result["CNYRUBF"], {"rate_rub": 0.00497, "lot": 1000})
        self.assertEqual(result["IMOEXF"], {"rate_rub": 1.22604, "lot": 10})

    def test_funding_uses_opening_volume_and_position_direction(self) -> None:
        rows = [
            {
                "time": "2026-07-23T08:50:16+03:00",
                "symbol": "CNYRUBF",
                "event": "OPEN",
                "side": "SHORT",
                "qty_lots": 59,
                "source": "portfolio_recovery",
            },
            {
                "time": "2026-07-23T11:00:41+03:00",
                "symbol": "IMOEXF",
                "event": "OPEN",
                "side": "SHORT",
                "qty_lots": 11,
                "source": "portfolio_confirmation",
            },
            {
                "time": "2026-07-23T13:00:00+03:00",
                "symbol": "CNYRUBF",
                "event": "CLOSE",
                "side": "SHORT",
                "qty_lots": 59,
            },
            {
                "time": "2026-07-23T14:00:00+03:00",
                "symbol": "USDRUBF",
                "event": "OPEN",
                "side": "LONG",
                "qty_lots": 2,
            },
        ]
        funding_entry = {
            "source": "moex_derivatives",
            "rates": {
                "CNYRUBF": {"rate_rub": 0.00497, "lot": 1000},
                "IMOEXF": {"rate_rub": 1.22604, "lot": 10},
                "USDRUBF": {"rate_rub": 0.00225, "lot": 1000},
            },
        }

        result = mod.calculate_daily_perpetual_funding(date(2026, 7, 23), funding_entry, rows)

        self.assertEqual(result["by_symbol"]["CNYRUBF"]["funding_rub"], 293.23)
        self.assertEqual(result["by_symbol"]["IMOEXF"]["funding_rub"], 134.86)
        self.assertEqual(result["by_symbol"]["USDRUBF"]["funding_rub"], -4.5)
        self.assertEqual(result["total_rub"], 423.59)

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
