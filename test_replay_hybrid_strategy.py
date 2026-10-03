import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd


SPEC = importlib.util.spec_from_file_location("replay_hybrid_strategy", Path(__file__).parent / "scripts" / "replay_hybrid_strategy.py")
replay = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = replay
SPEC.loader.exec_module(replay)


class ReplayHybridStrategyTests(unittest.TestCase):
    def candidate(self) -> replay.Candidate:
        return replay.Candidate(
            symbol="BRX6", direction="LONG", signal_time="2026-10-02T12:00:00+00:00",
            signal_price=100.0, atr=1.0, ao=0.5, ao_strength_atr=0.5,
            bars_since_cross=1, wave_time="2026-10-02T11:00:00+00:00",
            entry_path="ZERO_CROSS",
            candle_body_atr_sum=0.8, distance_from_wave_atr=0.4,
        )

    def test_ai_gate_blocks_confident_chop(self) -> None:
        candidate = self.candidate()
        candidate.regime_review = {"texture": "CHOP", "chop_probability_pct": 82, "data_quality": "COMPLETE"}
        candidate.entry_review = {"decision": "ENTER", "entry_score_pct": 78, "data_quality": "COMPLETE"}
        replay.apply_ai_gate(candidate)
        self.assertFalse(candidate.ai_allowed)
        self.assertIn("пила", candidate.ai_gate_reason)

    def test_ai_gate_allows_complete_aligned_candidate(self) -> None:
        candidate = self.candidate()
        candidate.regime_review = {"texture": "TREND", "chop_probability_pct": 8, "data_quality": "COMPLETE"}
        candidate.entry_review = {"decision": "ENTER", "entry_score_pct": 68, "data_quality": "COMPLETE"}
        replay.apply_ai_gate(candidate)
        self.assertTrue(candidate.ai_allowed)
        self.assertIn("canary", candidate.ai_gate_reason)

    def test_metrics_include_commission_and_drawdown(self) -> None:
        trades = [
            replay.SimulatedTrade("A", "LONG", "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00", 1, 2, 100, 10, 90, "EXIT", False, 1, 0, "", None, ""),
            replay.SimulatedTrade("A", "LONG", "2026-01-01T02:00:00+00:00", "2026-01-01T03:00:00+00:00", 2, 1, -50, 10, -60, "STOP", False, 0, 1, "", None, ""),
        ]
        result = replay.metrics(trades)
        self.assertEqual(result["net_pnl_rub"], 30)
        self.assertEqual(result["commission_rub"], 20)
        self.assertEqual(result["max_drawdown_rub"], 60)

    def test_true_breakeven_covers_both_commissions(self) -> None:
        for direction in ("LONG", "SHORT"):
            entry = 100.0
            exit_price = replay.true_breakeven_price(entry, direction, 0.01)
            sign = 1 if direction == "LONG" else -1
            gross = (exit_price - entry) * sign
            commission = (entry + exit_price) * replay.COMMISSION_RATE
            self.assertGreaterEqual(gross - commission, -1e-9)


if __name__ == "__main__":
    unittest.main()
