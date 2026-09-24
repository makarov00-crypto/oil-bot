import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from grpc import StatusCode
from tinkoff.invest.exceptions import RequestError

import bot_oil_main as mod


class CashManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SimpleNamespace(
            cash_manager_enabled=True,
            cash_manager_reserve_pct=0.20,
            cash_manager_min_cash_rub=30_000.0,
            cash_manager_stress_notional_pct=0.05,
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
            patch.object(mod, "cash_fund_limit_order_available", return_value=True),
            patch.object(mod, "get_live_portfolio_positions", return_value={}),
            patch.object(mod, "cash_manager_has_pending_futures", return_value=False),
            patch.object(mod, "get_account_snapshot", return_value=snapshot),
            patch.object(mod, "get_cash_manager_available_rub", return_value=100_000.0),
            patch.object(mod, "cash_manager_strict_margin_headroom_rub", return_value=100_000.0),
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
            patch.object(mod, "get_cash_manager_available_rub", return_value=5_000.0),
            patch.object(mod, "get_live_portfolio_positions", return_value={}),
            patch.object(mod, "cash_manager_strict_margin_headroom_rub", return_value=2_000.0),
            patch.object(mod, "cash_fund_limit_order_available", return_value=True),
            patch.object(mod, "submit_cash_fund_order", return_value=True) as submit,
        ):
            message = mod.maybe_release_cash_fund_for_entry(None, self.config, self.fund, instrument, "LONG")

        self.assertIn("заявка на продажу 5000 шт. LQDT", message)
        self.assertEqual(submit.call_args.args[3], 5_000)
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

    def test_margin_limited_signal_can_reach_ranking_when_fund_can_be_sold(self) -> None:
        sizing = {
            "quantity": 0, "broker_limit": 0, "margin_per_lot_rub": 10_000.0,
            "qty_by_working": 0, "risk_budget_rub": 500.0, "qty_by_risk": 1,
            "max_open_risk_budget_rub": 1_000.0, "qty_by_open_risk": 1,
        }
        with patch.object(mod, "get_cash_fund_holding", return_value={"qty": 100}):
            self.assertEqual(mod.cash_manager_rankable_quantity(None, self.config, self.fund, sizing), 1)
        sizing["qty_by_risk"] = 0
        self.assertEqual(mod.cash_manager_rankable_quantity(None, self.config, self.fund, sizing), 0)

    def test_missing_cash_fund_order_is_cleared_only_after_grace_period(self) -> None:
        stale_state = mod.CashManagerState(
            pending_order_id="missing-order",
            pending_submitted_at=(datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat(),
        )
        fresh_state = mod.CashManagerState(
            pending_order_id="missing-order",
            pending_submitted_at=datetime.now(timezone.utc).isoformat(),
        )
        error = RequestError(StatusCode.NOT_FOUND, "50005 NOT_FOUND: Order not found", None)

        self.assertTrue(mod.cash_manager_missing_order_is_stale(stale_state, error))
        self.assertFalse(mod.cash_manager_missing_order_is_stale(fresh_state, error))

    def test_entry_cancels_pending_cash_fund_buy_instead_of_waiting(self) -> None:
        state = mod.CashManagerState(pending_order_id="buy-order", pending_action="BUY", pending_qty=100)
        instrument = mod.InstrumentConfig(symbol="BRU6", figi="FUT", display_name="Brent", initial_margin_on_buy=10_000.0)
        client = SimpleNamespace(orders=SimpleNamespace(cancel_order=lambda **_: None))
        with (
            patch.object(mod, "load_cash_manager_state", return_value=state),
            patch.object(mod, "save_cash_manager_state") as save,
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 0}),
        ):
            message = mod.maybe_release_cash_fund_for_entry(client, self.config, self.fund, instrument, "LONG")

        self.assertIn("после подтверждения брокера", message)
        self.assertEqual(state.pending_order_id, "buy-order")
        save.assert_not_called()

    def test_dynamic_reserve_counts_open_risk_and_one_new_lot(self) -> None:
        self.config.cash_manager_reserve_pct = 0.05
        snapshot = mod.AccountSnapshot(total_portfolio=560_000.0, free_rub=410_000.0, blocked_guarantee_rub=87_000.0)
        instrument = mod.InstrumentConfig(symbol="GOLD", figi="GOLD", display_name="Gold", initial_margin_on_buy=34_000.0)
        live = {"OZON": {"notional_rub": 280_000.0}}
        with patch.object(mod, "load_state", return_value=SimpleNamespace(entry_risk_rub=4_000.0)):
            reserve = mod.cash_manager_dynamic_reserve_rub(snapshot, self.config, [instrument], live)
        self.assertEqual(reserve, 30_000.0 + 14_000.0 + 34_000.0 * 1.15)

    def test_buy_target_uses_existing_fund_and_cash_above_reserve(self) -> None:
        snapshot = mod.AccountSnapshot(total_portfolio=560_000.0, free_rub=410_000.0, blocked_guarantee_rub=87_000.0)
        self.config.cash_manager_max_portfolio_pct = 0.50
        target = mod.cash_manager_target_value_rub(snapshot, self.config, 147_000.0, 95_000.0, 420_000.0)
        self.assertEqual(target, 280_000.0)

    def test_order_tracks_broker_id_and_request_id_separately(self) -> None:
        state = mod.CashManagerState()
        sent = {}
        def post_order(**kwargs: object) -> SimpleNamespace:
            sent.update(kwargs)
            return SimpleNamespace(order_id="exchange-123")
        client = SimpleNamespace(orders=SimpleNamespace(post_order=post_order))
        with (
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 50}),
            patch.object(mod, "cash_fund_limit_order_available", return_value=True),
            patch.object(mod, "cash_fund_protected_limit_price", return_value=mod.Quotation(units=2, nano=0)),
            patch.object(mod, "save_cash_manager_state"),
        ):
            submitted = mod.submit_cash_fund_order(
                client, self.config, self.fund, 10, mod.OrderDirection.ORDER_DIRECTION_BUY, "BUY", state,
            )
        self.assertTrue(submitted)
        self.assertEqual(state.pending_order_id, "exchange-123")
        self.assertNotEqual(state.pending_request_id, state.pending_order_id)
        self.assertEqual(state.pending_baseline_qty, 50)
        self.assertEqual(sent["order_id"], state.pending_request_id)
        self.assertEqual(sent["order_type"], mod.OrderType.ORDER_TYPE_LIMIT)
        self.assertEqual(sent["time_in_force"], mod.TimeInForceType.TIME_IN_FORCE_FILL_AND_KILL)

    def test_uncertain_order_response_keeps_pending_intent(self) -> None:
        state = mod.CashManagerState()
        def fail(**_: object) -> None:
            raise RequestError(StatusCode.UNAVAILABLE, "connection lost", None)
        client = SimpleNamespace(orders=SimpleNamespace(post_order=fail))
        with (
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 50}),
            patch.object(mod, "cash_fund_limit_order_available", return_value=True),
            patch.object(mod, "cash_fund_protected_limit_price", return_value=mod.Quotation(units=2, nano=0)),
            patch.object(mod, "save_cash_manager_state"),
        ):
            self.assertFalse(mod.submit_cash_fund_order(
                client, self.config, self.fund, 10, mod.OrderDirection.ORDER_DIRECTION_BUY, "BUY", state,
            ))
        self.assertTrue(mod.cash_manager_has_pending_order(state))
        self.assertEqual(state.pending_baseline_qty, 50)

    def test_open_positions_do_not_prevent_fund_purchase(self) -> None:
        self.config.cash_manager_max_portfolio_pct = 0.50
        self.config.cash_manager_reserve_pct = 0.05
        state = mod.CashManagerState(flat_since=(datetime.now(timezone.utc) - timedelta(minutes=31)).isoformat())
        snapshot = mod.AccountSnapshot(total_portfolio=560_000.0, free_rub=410_000.0, blocked_guarantee_rub=87_000.0)
        live = {"OZON": {"notional_rub": 280_000.0}}
        instrument = mod.InstrumentConfig(symbol="GOLD", figi="GOLD", display_name="Gold", initial_margin_on_buy=34_000.0)
        with (
            patch.object(mod, "load_cash_manager_state", return_value=state),
            patch.object(mod, "cash_fund_limit_order_available", return_value=True),
            patch.object(mod, "cash_manager_has_pending_futures", return_value=False),
            patch.object(mod, "get_live_portfolio_positions", return_value=live),
            patch.object(mod, "load_state", return_value=SimpleNamespace(entry_risk_rub=4_000.0)),
            patch.object(mod, "get_account_snapshot", return_value=snapshot),
            patch.object(mod, "get_cash_manager_available_rub", return_value=410_000.0),
            patch.object(mod, "cash_manager_strict_margin_headroom_rub", return_value=420_000.0),
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 70_000, "price_rub": 2.1, "value_rub": 147_000.0}),
            patch.object(mod, "submit_cash_fund_order", return_value=True) as submit,
        ):
            mod.maybe_park_free_cash_in_fund(None, self.config, [instrument], self.fund, [])
        self.assertEqual(submit.call_args.args[5], "BUY")
        self.assertGreater(submit.call_args.args[3], 0)

    def test_cash_reserve_shortage_sells_fund_without_idle_wait(self) -> None:
        state = mod.CashManagerState()
        snapshot = mod.AccountSnapshot(total_portfolio=100_000.0, free_rub=20_000.0, blocked_guarantee_rub=0.0)
        with (
            patch.object(mod, "load_cash_manager_state", return_value=state),
            patch.object(mod, "cash_fund_limit_order_available", return_value=True),
            patch.object(mod, "cash_manager_has_pending_futures", return_value=False),
            patch.object(mod, "get_live_portfolio_positions", return_value={}),
            patch.object(mod, "get_account_snapshot", return_value=snapshot),
            patch.object(mod, "get_cash_manager_available_rub", return_value=20_000.0),
            patch.object(mod, "cash_manager_strict_margin_headroom_rub", return_value=80_000.0),
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 20_000, "price_rub": 2.0, "value_rub": 40_000.0}),
            patch.object(mod, "submit_cash_fund_order", return_value=True) as submit,
            patch.object(mod, "save_cash_manager_state"),
        ):
            mod.maybe_park_free_cash_in_fund(None, self.config, [], self.fund, [])
        self.assertEqual(submit.call_args.args[5], "SELL")

    def test_entry_waits_when_fund_market_is_closed_and_cash_is_short(self) -> None:
        instrument = mod.InstrumentConfig(symbol="GOLD", figi="GOLD", display_name="Gold", initial_margin_on_buy=10_000.0)
        snapshot = mod.AccountSnapshot(total_portfolio=100_000.0, free_rub=5_000.0, blocked_guarantee_rub=0.0)
        with (
            patch.object(mod, "load_cash_manager_state", return_value=mod.CashManagerState()),
            patch.object(mod, "get_account_snapshot", return_value=snapshot),
            patch.object(mod, "get_cash_manager_available_rub", return_value=5_000.0),
            patch.object(mod, "get_live_portfolio_positions", return_value={}),
            patch.object(mod, "cash_manager_strict_margin_headroom_rub", return_value=80_000.0),
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 20_000, "price_rub": 2.0}),
            patch.object(mod, "cash_fund_limit_order_available", return_value=False),
            patch.object(mod, "submit_cash_fund_order") as submit,
        ):
            reason = mod.maybe_release_cash_fund_for_entry(None, self.config, self.fund, instrument, "LONG")
        self.assertIn("торги фондом сейчас недоступны", reason)
        submit.assert_not_called()

    def test_missing_broker_order_confirms_fill_from_position(self) -> None:
        state = mod.CashManagerState(
            pending_order_id="exchange-123", pending_request_id="request-123", pending_action="BUY",
            pending_qty=10, pending_baseline_qty=50,
        )
        error = RequestError(StatusCode.NOT_FOUND, "50005 NOT_FOUND: Order not found", None)
        def not_found(**_: object) -> None:
            raise error
        client = SimpleNamespace(orders=SimpleNamespace(get_order_state=not_found))
        with (
            patch.object(mod, "load_cash_manager_state", return_value=state),
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 60}),
            patch.object(mod, "save_cash_manager_state"),
        ):
            mod.refresh_cash_manager_pending_order(client, self.config, self.fund)
        self.assertFalse(mod.cash_manager_has_pending_order(state))
        self.assertIn("сверено по остатку", state.last_action)

    def test_missing_broker_order_with_partial_fill_blocks_new_orders(self) -> None:
        state = mod.CashManagerState(
            pending_order_id="exchange-123", pending_request_id="request-123", pending_action="BUY",
            pending_qty=10, pending_baseline_qty=50,
            pending_submitted_at=(datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat(),
        )
        def not_found(**_: object) -> None:
            raise RequestError(StatusCode.NOT_FOUND, "50005 NOT_FOUND: Order not found", None)
        client = SimpleNamespace(orders=SimpleNamespace(get_order_state=not_found))
        with (
            patch.object(mod, "load_cash_manager_state", return_value=state),
            patch.object(mod, "get_cash_fund_holding", return_value={"qty": 55}),
            patch.object(mod, "save_cash_manager_state"),
        ):
            mod.refresh_cash_manager_pending_order(client, self.config, self.fund)
        self.assertTrue(mod.cash_manager_has_pending_order(state))
        self.assertIn("заблокированы", state.last_error)

    def test_protected_limit_requires_depth_near_best_price(self) -> None:
        price = lambda n: mod.Quotation(units=2, nano=n)
        shallow_book = SimpleNamespace(
            bids=[SimpleNamespace(price=price(0), quantity=10)],
            asks=[SimpleNamespace(price=price(1_000_000), quantity=5)],
        )
        client = SimpleNamespace(market_data=SimpleNamespace(get_order_book=lambda **_: shallow_book))
        self.assertIsNone(mod.cash_fund_protected_limit_price(client, self.fund, mod.OrderDirection.ORDER_DIRECTION_BUY, 10))
        self.assertIsNotNone(mod.cash_fund_protected_limit_price(client, self.fund, mod.OrderDirection.ORDER_DIRECTION_BUY, 5))


if __name__ == "__main__":
    unittest.main()
