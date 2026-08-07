import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as mod


class CashManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SimpleNamespace(
            cash_manager_enabled=True,
            cash_manager_reserve_pct=0.20,
            cash_manager_max_portfolio_pct=0.30,
            cash_manager_idle_minutes=30,
            cash_manager_min_order_rub=10_000.0,
            cash_manager_release_buffer_pct=0.15,
            cash_manager_fund_symbol="LQDT",
            dry_run=False,
            allow_orders=True,
            account_id="account",
            order_quantity=1,
            max_order_quantity=2,
        )
        self.fund = mod.CashFundConfig(symbol="LQDT", figi="ETF", display_name="Liquidity", lot=1)

    def test_target_is_capped_at_thirty_percent_and_preserves_reserve(self) -> None:
        snapshot = mod.AccountSnapshot(total_portfolio=100_000.0, free_rub=100_000.0, blocked_guarantee_rub=0.0)
        self.assertEqual(mod.cash_manager_target_value_rub(snapshot, self.config), 30_000.0)

        low_cash = mod.AccountSnapshot(total_portfolio=100_000.0, free_rub=15_000.0, blocked_guarantee_rub=0.0)
        self.assertEqual(mod.cash_manager_target_value_rub(low_cash, self.config), 0.0)

    def test_park_submits_buy_after_idle_period_without_candidates(self) -> None:
        old_state = mod.CashManagerState(flat_since=(datetime.now(timezone.utc) - timedelta(minutes=31)).isoformat())
        snapshot = mod.AccountSnapshot(total_portfolio=100_000.0, free_rub=100_000.0, blocked_guarantee_rub=0.0)
        with (
            patch.object(mod, "load_cash_manager_state", return_value=old_state),
            patch.object(mod, "get_market_session", return_value="MAIN"),
            patch.object(mod, "get_live_portfolio_positions", return_value={}),
            patch.object(mod, "cash_manager_has_pending_futures", return_value=False),
            patch.object(mod, "get_account_snapshot", return_value=snapshot),
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 0, "price_rub": 2.0, "value_rub": 0.0}),
            patch.object(mod, "submit_cash_fund_order", return_value=True) as submit,
        ):
            mod.maybe_park_free_cash_in_fund(None, self.config, [], self.fund, [])

        self.assertEqual(submit.call_args.args[3], 15_000)
        self.assertEqual(submit.call_args.args[5], "BUY")

    def test_entry_releases_only_margin_deficit_from_fund(self) -> None:
        instrument = mod.InstrumentConfig(
            symbol="BRU6",
            figi="FUT",
            display_name="Brent",
            initial_margin_on_buy=10_000.0,
        )
        snapshot = mod.AccountSnapshot(total_portfolio=100_000.0, free_rub=5_000.0, blocked_guarantee_rub=0.0)
        state = mod.CashManagerState()
        with (
            patch.object(mod, "load_cash_manager_state", return_value=state),
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 5_000, "price_rub": 2.0, "value_rub": 10_000.0}),
            patch.object(mod, "get_account_snapshot", return_value=snapshot),
            patch.object(mod, "get_margin_headroom_rub", return_value=2_000.0),
            patch.object(mod, "submit_cash_fund_order", return_value=True) as submit,
        ):
            message = mod.maybe_release_cash_fund_for_entry(None, self.config, self.fund, instrument, "LONG")

        self.assertIn("продано 4600 шт. LQDT", message)
        self.assertEqual(submit.call_args.args[3], 4_600)
        self.assertEqual(submit.call_args.args[5], "SELL")

    def test_cash_manager_does_not_release_fund_for_risk_limited_entry(self) -> None:
        sizing = {
            "broker_limit": 12,
            "margin_per_lot_rub": 10_000.0,
            "qty_by_working": 0,
            "risk_budget_rub": 250.0,
            "qty_by_risk": 0,
            "max_open_risk_budget_rub": 1_000.0,
            "qty_by_open_risk": 1,
        }

        self.assertFalse(mod.sizing_requires_margin_release(sizing))

    def test_cash_manager_releases_fund_only_for_margin_limited_entry(self) -> None:
        sizing = {
            "broker_limit": 12,
            "margin_per_lot_rub": 10_000.0,
            "qty_by_working": 0,
            "risk_budget_rub": 250.0,
            "qty_by_risk": 1,
            "max_open_risk_budget_rub": 1_000.0,
            "qty_by_open_risk": 2,
        }

        self.assertTrue(mod.sizing_requires_margin_release(sizing))


if __name__ == "__main__":
    unittest.main()
