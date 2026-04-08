import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import bot_oil_main as mod


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


if __name__ == "__main__":
    unittest.main()
