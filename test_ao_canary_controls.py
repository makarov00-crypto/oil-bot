import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as mod
from trade_quality import pair_closed_trades


AO = "ao_chaikin_1h"


def config(**overrides):
    values = {
        "symbols": ["CNYRUBF", "IMOEXF", "GDZ6"],
        "ao_canary_enabled": True,
        "ao_canary_max_lots": 1,
        "ao_canary_max_open_positions": 1,
        "ao_canary_daily_loss_pct": 0.5,
        "ao_canary_weekly_loss_pct": 1.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class AoCanaryControlsTests(unittest.TestCase):
    def test_caps_candidate_to_one_lot(self):
        candidate = {
            "symbol": "GDZ6",
            "strategy_name": AO,
            "allocator_quantity": 4,
            "requested_margin_rub": 40000.0,
        }
        with patch.object(mod, "load_trade_journal", return_value=[]), patch.object(
            mod, "get_account_snapshot", return_value=mod.AccountSnapshot(500000.0, 400000.0, 0.0)
        ), patch.object(mod, "load_state", return_value=mod.InstrumentState()):
            eligible, deferred = mod.filter_ao_canary_candidates(None, config(), [candidate])
        self.assertEqual(deferred, [])
        self.assertEqual(eligible[0]["allocator_quantity"], 1)
        self.assertEqual(eligible[0]["requested_margin_rub"], 10000.0)
        self.assertEqual(eligible[0]["strategy_version"], mod.AO_CANARY_STRATEGY_VERSION)

    def test_blocks_new_entry_while_any_ao_position_is_open(self):
        candidate = {"symbol": "GDZ6", "strategy_name": AO, "allocator_quantity": 1}

        def load_state(symbol):
            if symbol == "CNYRUBF":
                return mod.InstrumentState(
                    position_side="LONG",
                    position_qty=1,
                    entry_strategy=AO,
                    entry_strategy_version=mod.AO_CANARY_STRATEGY_VERSION,
                )
            return mod.InstrumentState()

        with patch.object(mod, "load_trade_journal", return_value=[]), patch.object(
            mod, "get_account_snapshot", return_value=mod.AccountSnapshot(500000.0, 400000.0, 0.0)
        ), patch.object(mod, "load_state", side_effect=load_state):
            eligible, deferred = mod.filter_ao_canary_candidates(None, config(), [candidate])
        self.assertEqual(eligible, [])
        self.assertEqual(deferred[0]["defer_kind"], "ao_canary_open_position_limit")

    def test_legacy_ao_positions_do_not_consume_canary_slot(self):
        candidate = {"symbol": "GDZ6", "strategy_name": AO, "allocator_quantity": 1}

        def load_state(symbol):
            if symbol in {"CNYRUBF", "IMOEXF"}:
                return mod.InstrumentState(
                    position_side="LONG",
                    position_qty=9 if symbol == "CNYRUBF" else 1,
                    entry_strategy=AO,
                    entry_strategy_version="4",
                )
            return mod.InstrumentState()

        with patch.object(mod, "load_trade_journal", return_value=[]), patch.object(
            mod, "get_account_snapshot", return_value=mod.AccountSnapshot(500000.0, 400000.0, 0.0)
        ), patch.object(mod, "load_state", side_effect=load_state):
            eligible, deferred = mod.filter_ao_canary_candidates(None, config(), [candidate])
        self.assertEqual(deferred, [])
        self.assertEqual(len(eligible), 1)

    def test_blocks_at_daily_or_weekly_loss_limit(self):
        rows = [
            {
                "time": "2026-10-03T10:00:00+03:00",
                "event": "CLOSE",
                "strategy": AO,
                "strategy_version": mod.AO_CANARY_STRATEGY_VERSION,
                "net_pnl_rub": -3000.0,
            }
        ]
        candidate = {"symbol": "GDZ6", "strategy_name": AO, "allocator_quantity": 1}
        with patch.object(mod, "load_trade_journal", return_value=rows), patch.object(
            mod, "get_account_snapshot", return_value=mod.AccountSnapshot(500000.0, 400000.0, 0.0)
        ), patch.object(mod, "load_state", return_value=mod.InstrumentState()):
            eligible, deferred = mod.filter_ao_canary_candidates(
                None,
                config(),
                [candidate],
                now=datetime.fromisoformat("2026-10-03T15:00:00+03:00"),
            )
        self.assertEqual(eligible, [])
        self.assertEqual(deferred[0]["defer_kind"], "ao_canary_daily_loss")

        rows[0]["time"] = "2026-10-01T10:00:00+03:00"
        rows[0]["net_pnl_rub"] = -5500.0
        with patch.object(mod, "load_trade_journal", return_value=rows), patch.object(
            mod, "get_account_snapshot", return_value=mod.AccountSnapshot(500000.0, 400000.0, 0.0)
        ), patch.object(mod, "load_state", return_value=mod.InstrumentState()):
            eligible, deferred = mod.filter_ao_canary_candidates(
                None,
                config(),
                [candidate],
                now=datetime.fromisoformat("2026-10-03T15:00:00+03:00"),
            )
        self.assertEqual(eligible, [])
        self.assertEqual(deferred[0]["defer_kind"], "ao_canary_weekly_loss")

    def test_legacy_ao_losses_do_not_consume_canary_loss_budget(self):
        rows = [
            {
                "time": "2026-10-03T10:00:00+03:00",
                "event": "CLOSE",
                "strategy": AO,
                "strategy_version": "4",
                "net_pnl_rub": -10000.0,
            }
        ]
        candidate = {"symbol": "GDZ6", "strategy_name": AO, "allocator_quantity": 1}
        with patch.object(mod, "load_trade_journal", return_value=rows), patch.object(
            mod, "get_account_snapshot", return_value=mod.AccountSnapshot(500000.0, 400000.0, 0.0)
        ), patch.object(mod, "load_state", return_value=mod.InstrumentState()):
            eligible, deferred = mod.filter_ao_canary_candidates(
                None,
                config(),
                [candidate],
                now=datetime.fromisoformat("2026-10-03T15:00:00+03:00"),
            )
        self.assertEqual(deferred, [])
        self.assertEqual(len(eligible), 1)

    def test_disabled_canary_does_not_change_candidates(self):
        candidate = {"symbol": "GDZ6", "strategy_name": AO, "allocator_quantity": 4}
        eligible, deferred = mod.filter_ao_canary_candidates(None, config(ao_canary_enabled=False), [candidate])
        self.assertEqual(eligible, [candidate])
        self.assertEqual(deferred, [])


class JournalPositionAnchorTests(unittest.TestCase):
    def test_position_reconciliation_resets_both_sides_without_rewriting_history(self):
        rows = [
            {"symbol": "CNYRUBF", "event": "OPEN", "side": "SHORT", "qty_lots": 8},
            {"symbol": "CNYRUBF", "event": "OPEN", "side": "LONG", "qty_lots": 7},
            {
                "symbol": "CNYRUBF",
                "event": mod.POSITION_RECONCILE_EVENT,
                "side": "LONG",
                "qty_lots": 9,
            },
        ]
        self.assertEqual(mod.get_active_journal_lots("CNYRUBF", "LONG", rows), 9)
        self.assertEqual(mod.get_active_journal_lots("CNYRUBF", "SHORT", rows), 0)

    def test_trade_event_context_contains_strategy_version(self):
        state = mod.InstrumentState(entry_strategy=AO, entry_strategy_version="4-canary-fast-only")
        context = mod.build_trade_event_context(state)
        self.assertEqual(context["strategy_version"], "4-canary-fast-only")

    def test_quality_pairing_uses_reconciliation_as_new_position_cycle(self):
        rows = [
            {
                "_dt": datetime.fromisoformat("2026-09-20T10:00:00+03:00"),
                "symbol": "CNYRUBF", "event": "OPEN", "side": "SHORT",
                "qty_lots": 8, "price": 12.0, "strategy": AO,
            },
            {
                "_dt": datetime.fromisoformat("2026-10-03T15:00:00+03:00"),
                "symbol": "CNYRUBF", "event": mod.POSITION_RECONCILE_EVENT, "side": "LONG",
                "qty_lots": 9, "price": 12.453, "strategy": AO,
                "strategy_version": "4",
                "context": {"entry_time": "2026-10-03T10:00:00+03:00", "strategy_version": "4"},
            },
            {
                "_dt": datetime.fromisoformat("2026-10-03T16:00:00+03:00"),
                "symbol": "CNYRUBF", "event": "CLOSE", "side": "LONG",
                "qty_lots": 9, "price": 12.5, "strategy": AO, "net_pnl_rub": 400.0,
            },
        ]
        pairs = pair_closed_trades(rows)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["side"], "LONG")
        self.assertEqual(pairs[0]["qty_lots"], 9)
        self.assertEqual(pairs[0]["entry_time"], "2026-10-03T10:00:00+03:00")
        self.assertEqual(pairs[0]["strategy_version"], "4")

    def test_dashboard_pairing_uses_reconciliation_as_new_position_cycle(self):
        rows = [
            {
                "time": "2026-09-20T10:00:00+03:00",
                "symbol": "CNYRUBF", "event": "OPEN", "side": "SHORT",
                "qty_lots": 8, "price": 12.0, "strategy": AO,
            },
            {
                "time": "2026-10-03T15:00:00+03:00",
                "symbol": "CNYRUBF", "event": mod.POSITION_RECONCILE_EVENT, "side": "LONG",
                "qty_lots": 9, "price": 12.453, "strategy": AO,
                "strategy_version": "4",
                "context": {"entry_time": "2026-10-03T10:00:00+03:00", "strategy_version": "4"},
            },
            {
                "time": "2026-10-03T16:00:00+03:00",
                "symbol": "CNYRUBF", "event": "CLOSE", "side": "LONG",
                "qty_lots": 9, "price": 12.5, "strategy": AO, "net_pnl_rub": 400.0,
            },
        ]
        pairs, current_open = mod.pair_trade_journal_rows(rows)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(current_open, {})
        self.assertEqual(pairs[0]["side"], "LONG")
        self.assertEqual(pairs[0]["qty_lots"], 9)
        self.assertEqual(pairs[0]["entry_time"], "2026-10-03T10:00:00+03:00")
        self.assertEqual(pairs[0]["strategy_version"], "4")


if __name__ == "__main__":
    unittest.main()
