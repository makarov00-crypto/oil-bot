"""Migration contracts: position ownership, real stop risk, and exit isolation."""

import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import bot_oil_main as mod
from strategies.reversal_1h import evaluate_signal as evaluate_legacy_signal
from test_strategy_quality_filters import candle_rows, make_config


AO = "ao_chaikin_1h"
LEGACY = "reversal_1h"


class AoLiveIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.config = make_config()
        self.instrument = mod.InstrumentConfig(
            symbol="CNYRUBF", figi="TEST", display_name="Test",
            initial_margin_on_buy=100.0, initial_margin_on_sell=100.0,
            min_price_increment=0.01, min_price_increment_amount=1.0,
        )
        self.frame = candle_rows([{} for _ in range(8)])

    def test_restarted_legacy_position_keeps_owner_and_original_signal(self):
        state = mod.InstrumentState(
            position_side="LONG", position_qty=3, entry_price=100.0,
            entry_strategy=LEGACY, last_strategy_name=AO,
            entry_stop_price=97.0, entry_emergency_stop_price=95.0,
            entry_peak_ao_abs=0.75, entry_risk_rub=900.0,
        )
        with TemporaryDirectory() as directory, patch.object(mod, "STATE_DIR", Path(directory)):
            mod.save_state(self.instrument.symbol, state)
            restored = mod.load_state(self.instrument.symbol)
            actual = mod.evaluate_position_owned_signal(
                self.frame, self.config, self.instrument, "", restored,
            )
        expected_signal, expected_reason = evaluate_legacy_signal(
            self.frame, self.config, self.instrument, "",
        )
        self.assertEqual(actual, (expected_signal, expected_reason, LEGACY))
        self.assertEqual(restored.entry_strategy, LEGACY)
        self.assertEqual(restored.entry_stop_price, 97.0)
        self.assertEqual(restored.entry_emergency_stop_price, 95.0)
        self.assertEqual(restored.entry_peak_ao_abs, 0.75)
        self.assertEqual(restored.entry_risk_rub, 900.0)

    def test_pending_legacy_open_retains_owner_across_restart(self):
        state = mod.InstrumentState(
            entry_strategy=LEGACY, last_strategy_name=AO,
            pending_order_id="existing-open", pending_order_action="OPEN",
            pending_order_side="SHORT", pending_order_qty=2,
            pending_submitted_at="2026-08-01T10:00:00+00:00",
        )
        with TemporaryDirectory() as directory, patch.object(mod, "STATE_DIR", Path(directory)):
            mod.save_state(self.instrument.symbol, state)
            restored = mod.load_state(self.instrument.symbol)
            actual = mod.evaluate_position_owned_signal(
                self.frame, self.config, self.instrument, "", restored,
            )
        self.assertEqual(actual[2], LEGACY)
        self.assertEqual(restored.pending_order_id, "existing-open")
        self.assertEqual(restored.pending_order_side, "SHORT")
        self.assertEqual(restored.pending_order_qty, 2)

    def test_flat_position_uses_ao_even_with_stale_legacy_owner(self):
        state = mod.InstrumentState(entry_strategy=LEGACY, last_strategy_name=LEGACY)
        actual = mod.evaluate_position_owned_signal(
            self.frame, self.config, self.instrument, "", state,
        )
        self.assertEqual(actual[2], AO)

    def test_missing_owner_persists_legacy_and_uses_closed_candle_stop(self):
        state = mod.InstrumentState(
            position_side="LONG", position_qty=1, entry_price=100.0,
            entry_time="2026-04-01T10:00:00+00:00",
            entry_stop_price=99.0, entry_emergency_stop_price=95.0,
            max_price=100.0, min_price=98.0,
        )
        frame = self.frame.copy()
        frame.loc[frame.index[-1], "is_complete"] = False
        with TemporaryDirectory() as directory, patch.object(mod, "STATE_DIR", Path(directory)), \
                patch.object(mod, "load_trade_journal", return_value=[]):
            result = mod.evaluate_position_owned_signal(frame, self.config, self.instrument, "", state)
            mod.save_state(self.instrument.symbol, state)
            restored = mod.load_state(self.instrument.symbol)
            self.assertEqual(result[2], LEGACY)
            self.assertEqual(restored.entry_strategy, LEGACY)
            # An intrabar drop through the working stop must not take the
            # generic stop/trailing path; the legacy hourly stop waits.
            with patch.object(mod, "get_last_price", return_value=98.0), patch.object(mod, "close_position") as close:
                mod.check_exit(None, self.config, self.instrument, restored, frame, "HOLD")
            close.assert_not_called()

    def test_pending_open_reserves_full_risk_without_double_counting_partial_fill(self):
        for side, qty in (("FLAT", 0), ("LONG", 1)):
            with self.subTest(side=side), TemporaryDirectory() as directory, \
                    patch.object(mod, "STATE_DIR", Path(directory)):
                mod.save_state("PENDING", mod.InstrumentState(
                    entry_strategy=AO, position_side=side, position_qty=qty,
                    pending_order_id="pending", pending_order_action="OPEN",
                    pending_order_side="LONG", pending_order_qty=2, entry_risk_rub=3000.0,
                ))
                reserved, count = mod.calculate_reserved_open_risk_rub(
                    SimpleNamespace(symbols=["NEW", "PENDING"]), "NEW", 2800.0,
                )
                self.assertEqual(reserved, 3000.0)
                self.assertEqual(count, 1)

    def test_rotation_cannot_fund_ao_or_close_ao_position(self):
        # The legacy/legacy control proves this is otherwise a viable rotation.
        for position_strategy, candidate_strategy, expected_plan in (
            (LEGACY, LEGACY, True), (LEGACY, AO, False), (AO, LEGACY, False),
        ):
            with self.subTest(position=position_strategy, candidate=candidate_strategy), \
                    TemporaryDirectory() as directory, patch.object(mod, "STATE_DIR", Path(directory)), \
                    patch.object(mod, "load_trade_journal", return_value=[]):
                mod.save_state(self.instrument.symbol, mod.InstrumentState(
                    position_side="LONG", position_qty=1, entry_strategy=position_strategy,
                    entry_price=100.0, entry_time="2026-04-01T10:00:00+00:00",
                    position_variation_margin_rub=-100.0, last_signal="HOLD",
                    last_entry_edge_score=0.1, last_market_regime="chop",
                ))
                plan = mod.select_capital_rotation_plan([self.instrument], [{
                    "symbol": "NEW", "strategy_name": candidate_strategy,
                    "signal": "LONG", "allocator_quantity": 0,
                    "priority_score": 0.95, "entry_edge_score": 0.95,
                }])
                self.assertEqual(plan is not None, expected_plan)

    def test_broker_close_recovery_keeps_ao_trade_held_two_days(self):
        rows = [{
            "time": "2026-04-07T10:00:00+03:00", "symbol": self.instrument.symbol,
            "display_name": "Test", "side": "SHORT", "event": "OPEN",
            "qty_lots": 1, "lot_size": 1, "price": 120.0,
            "commission_rub": 5.0, "strategy": AO, "mode": "LIVE", "session": "MAIN",
        }]
        operation = mod.BrokerTradeOp(
            symbol=self.instrument.symbol, display_name="Test", figi=self.instrument.figi,
            op_id="close-op", parent_id="parent", op_type=mod.OperationType.OPERATION_TYPE_BUY,
            side="LONG", qty=1, price=119.0, dt=datetime(2026, 4, 9, 10, 5, tzinfo=timezone.utc),
        )
        with TemporaryDirectory() as directory, patch.object(mod, "STATE_DIR", Path(directory)), \
                patch.object(mod, "load_trade_journal", return_value=rows), \
                patch.object(mod, "save_trade_journal") as save, \
                patch.object(mod, "fetch_trade_operations_for_day", return_value=([operation], {"close-op": 5.0})):
            recovered = mod.reconcile_missing_trade_closes_from_broker(
                None, SimpleNamespace(account_id="test", dry_run=False), [self.instrument],
                target_day=datetime(2026, 4, 9, tzinfo=timezone.utc).date(),
            )
        self.assertEqual(recovered, 1)
        closes = [row for row in save.call_args.args[0] if row.get("event") == "CLOSE"]
        self.assertEqual(len(closes), 1)
        self.assertEqual(closes[0]["strategy"], AO)
        self.assertEqual(closes[0]["broker_op_id"], "close-op")
        self.assertEqual(closes[0]["net_pnl_rub"], 90.0)

    def _sizing(self, *, stop_distance=1.0, old_positions=(), session="MAIN", broker_lots=1000,
                margin_headroom=100000.0):
        config = make_config()
        config.risk_per_trade_pct = 0.02
        config.max_open_risk_pct = 0.08
        config.max_order_quantity = 1000
        config.max_daily_loss = 0.0
        config.symbols = [self.instrument.symbol, *[symbol for symbol, _ in old_positions]]
        state = mod.InstrumentState(
            last_signal="LONG", last_strategy_name=AO,
            last_entry_edge_score=0.95, last_entry_edge_label="high",
            last_setup_quality_label="strong", last_market_regime="trend_expansion",
            last_market_regime_confidence=0.90,
        )
        snapshot = mod.AccountSnapshot(
            total_portfolio=100000.0, free_rub=100000.0, blocked_guarantee_rub=0.0,
        )
        client = SimpleNamespace(users=SimpleNamespace(
            get_margin_attributes=lambda **kwargs: SimpleNamespace(
                amount_of_missing_funds=SimpleNamespace(units=-int(margin_headroom), nano=0),
            ),
        ))
        with TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(mod, "STATE_DIR", Path(directory)))
            stack.enter_context(patch.object(mod, "load_trade_journal", return_value=[]))
            stack.enter_context(patch.object(mod, "get_account_snapshot", return_value=snapshot))
            stack.enter_context(patch.object(mod, "get_broker_max_lots", return_value=broker_lots))
            stack.enter_context(patch.object(mod, "get_market_session", return_value=session))
            for symbol, risk in old_positions:
                mod.save_state(symbol, mod.InstrumentState(
                    position_side="LONG", position_qty=1, entry_strategy=LEGACY,
                    entry_price=100.0, entry_risk_rub=risk,
                ))
            return mod.calculate_position_sizing_context(
                client, config, self.instrument, state, 100.0, "LONG", AO,
                stop_distance_price=stop_distance,
            )

    def test_high_quality_signal_cannot_raise_half_risk_budget(self):
        sizing = self._sizing()
        self.assertAlmostEqual(sizing["risk_budget_rub"], 1000.0)
        self.assertAlmostEqual(sizing["max_open_risk_budget_rub"], 4000.0)
        self.assertEqual(sizing["quantity"], 10)
        self.assertLessEqual(sizing["quantity"] * sizing["money_risk_per_contract_rub"], 1000.0)

    def test_evening_session_reduces_half_risk_further(self):
        sizing = self._sizing(session="EVENING")
        self.assertAlmostEqual(sizing["risk_budget_rub"], 500.0)
        self.assertEqual(sizing["quantity"], 5)

    def test_user_authorized_single_contract_can_exceed_trade_budget(self):
        sizing = self._sizing(stop_distance=20.0)
        self.assertAlmostEqual(sizing["risk_budget_rub"], 1000.0)
        self.assertAlmostEqual(sizing["money_risk_per_contract_rub"], 2000.0)
        self.assertEqual(sizing["quantity"], 1)
        self.assertTrue(sizing["risk_min_lot_override"])

    def test_single_contract_cannot_exceed_remaining_portfolio_risk(self):
        sizing = self._sizing(stop_distance=20.0, old_positions=(("OLD", 3000.0),))
        self.assertAlmostEqual(sizing["reserved_open_risk_rub"], 3000.0)
        self.assertAlmostEqual(sizing["available_open_risk_rub"], 1000.0)
        self.assertEqual(sizing["quantity"], 0)

    def test_missing_old_risk_uses_full_previous_budget(self):
        sizing = self._sizing(old_positions=(("OLD1", 1200.0), ("OLD2", 0.0)))
        # Unknown old risk includes the legacy 1.4 high-quality multiplier.
        self.assertAlmostEqual(sizing["reserved_open_risk_rub"], 4000.0)
        self.assertAlmostEqual(sizing["available_open_risk_rub"], 0.0)
        self.assertEqual(sizing["quantity"], 0)

    def test_known_old_risk_is_reserved_in_full_without_rescaling(self):
        sizing = self._sizing(old_positions=(("OLD", 3200.0),))
        self.assertAlmostEqual(sizing["reserved_open_risk_rub"], 3200.0)
        self.assertAlmostEqual(sizing["available_open_risk_rub"], 800.0)
        self.assertEqual(sizing["quantity"], 8)

    def test_single_contract_requires_broker_capacity_and_margin(self):
        for kwargs in ({"broker_lots": 0}, {"margin_headroom": 50.0}):
            with self.subTest(**kwargs):
                self.assertEqual(self._sizing(stop_distance=20.0, **kwargs)["quantity"], 0)

    def test_ao_reentry_waits_for_new_closed_candle_without_macd_gate(self):
        state = mod.InstrumentState(
            last_strategy_name=AO, last_exit_time="2026-08-01T11:05:00+00:00",
            last_exit_side="LONG", last_exit_reason="MACD потерял EMA20",
            last_exit_pnl_rub=-100.0, last_exit_price=100.0,
        )
        frame = candle_rows([
            {"time": pd.Timestamp("2026-08-01T10:00:00Z"), "macd": -10.0, "macd_signal": 10.0},
            {"time": pd.Timestamp("2026-08-01T11:00:00Z"), "is_complete": False,
             "macd": -10.0, "macd_signal": 10.0},
        ])
        allowed, _ = mod.position_reentry_allowed(state, self.instrument, "LONG", 100.0, frame)
        self.assertFalse(allowed)
        frame.loc[1, "is_complete"] = True
        allowed, _ = mod.position_reentry_allowed(state, self.instrument, "LONG", 100.0, frame)
        self.assertTrue(allowed)

    def _ao_exit_fixture(self):
        frame = candle_rows([
            {"time": pd.Timestamp("2026-08-01T00:00:00Z") + pd.Timedelta(hours=index),
             "open": 100.0 + index * 0.5, "close": 100.0 + index * 0.5,
             "high": 101.0 + index * 0.5, "low": 99.0 + index * 0.5,
             "macd": -10.0, "macd_signal": 10.0, "rsi": 90.0}
            for index in range(40)
        ])
        state = mod.InstrumentState(
            position_side="LONG", position_qty=1, entry_strategy=AO,
            entry_price=110.0, entry_time="2026-08-02T10:00:00+00:00",
            entry_stop_price=90.0, entry_emergency_stop_price=85.0,
            max_price=150.0, min_price=110.0, entry_peak_ao_abs=7.25,
        )
        return frame, state

    def test_ao_position_ignores_old_trailing_rsi_and_opposite_signal(self):
        frame, state = self._ao_exit_fixture()
        with patch.object(mod, "get_last_price", return_value=119.5), patch.object(mod, "close_position") as close:
            mod.check_exit(None, self.config, self.instrument, state, frame, "SHORT")
        close.assert_not_called()

    def test_ao_emergency_stop_remains_active_between_candle_closes(self):
        frame, state = self._ao_exit_fixture()
        frame.loc[frame.index[-1], "is_complete"] = False
        with patch.object(mod, "get_last_price", return_value=84.0), patch.object(mod, "close_position") as close:
            mod.check_exit(None, self.config, self.instrument, state, frame, "HOLD")
        close.assert_called_once()
        self.assertIn("стоп", close.call_args.args[-1].lower())

    def test_ao_working_stop_acts_on_closed_candle(self):
        frame, state = self._ao_exit_fixture()
        frame.loc[frame.index[-1], ["close", "low"]] = [89.0, 88.0]
        with patch.object(mod, "get_last_price", return_value=89.0), patch.object(mod, "close_position") as close:
            mod.check_exit(None, self.config, self.instrument, state, frame, "HOLD")
        close.assert_called_once()
        self.assertIn("стоп", close.call_args.args[-1].lower())


if __name__ == "__main__":
    unittest.main()
