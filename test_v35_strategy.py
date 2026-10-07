import hashlib
import unittest
from dataclasses import replace
from pathlib import Path

import pandas as pd

from strategies.v35_hierarchy import hierarchical_gate
from strategies.v35_risk import (
    advance_on_15m_bar,
    initial_stop_for_candidate,
    new_protection_state,
    position_size_for_risk,
)
from strategies.v35_signals import V35Candidate, enrich_candles, short_entry_allowed


ROOT = Path(__file__).resolve().parent


def candidate(direction: str = "LONG", atr: float = 10.0) -> V35Candidate:
    return V35Candidate(
        symbol="TEST",
        direction=direction,
        signal_time="2026-10-01T12:00:00+00:00",
        signal_price=100.0,
        atr=atr,
        entry_pattern="AO_ZERO_CROSS",
        directional_bars=2,
        body_sum_atr=1.0,
        largest_body_atr=0.6,
        path_efficiency_6=0.7,
        range_atr_6=2.0,
        ao=1.0,
        ao_delta=0.5,
        ao_zero_distance_atr=0.1,
        macd_hist=0.2,
        macd_hist_delta=0.1,
        chaikin=10.0,
        execution_score=5,
    )


class V35StrategyTests(unittest.TestCase):
    def test_frozen_pattern_and_regime_sources_have_expected_hashes(self) -> None:
        expected = {
            "strategies/v35_pattern_library.py": "e3757e4b1a0794529db156e8e2ca86c69c780916db4e38cc804e22340ee5f38d",
            "strategies/v35_market_regime.py": "c4efdf16653a06f605e9c7db345a2468a5c6b2a671755ae27916616960204b1b",
        }
        for relative, wanted in expected.items():
            actual = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
            self.assertEqual(actual, wanted)

    def test_hierarchical_gate_matches_frozen_v35_rules(self) -> None:
        allowed_cases = (
            ({"group_regime": "BULL", "direction": "LONG", "local_stage": "TREND_EARLY"}, "BULL_LONG_EARLY_TREND"),
            ({"group_regime": "BEAR", "direction": "SHORT", "local_stage": "RANGE"}, "BEAR_SHORT_RANGE"),
            ({"group_regime": "BEAR", "direction": "SHORT", "local_stage": "TRANSITION"}, "BEAR_SHORT_TRANSITION"),
            ({"group_regime": "BEAR", "direction": "SHORT", "local_stage": "BREAKOUT_ATTEMPT"}, "BEAR_SHORT_BREAKOUT_ATTEMPT"),
        )
        for context, reason in allowed_cases:
            self.assertEqual(hierarchical_gate(context), (True, reason))
        self.assertEqual(
            hierarchical_gate({"group_regime": "SIDEWAYS", "direction": "LONG", "local_stage": "TREND_EARLY"}),
            (False, "GROUP_SIDEWAYS"),
        )

    def test_v10_short_filter_requires_complete_execution_or_large_hourly_impulse(self) -> None:
        weak = candidate("SHORT")
        self.assertFalse(short_entry_allowed(weak))
        self.assertTrue(short_entry_allowed(replace(weak, execution_score=6)))
        self.assertTrue(short_entry_allowed(replace(weak, body_sum_atr=2.25)))
        self.assertTrue(short_entry_allowed(candidate("LONG")))

    def test_initial_stop_uses_structure_and_minimum_atr_distance(self) -> None:
        hourly = pd.DataFrame({
            "closed_at": pd.date_range("2026-10-01T10:00:00Z", periods=3, freq="h"),
            "low": [98.0, 97.0, 96.0],
            "high": [102.0, 103.0, 104.0],
        })
        long_stop = initial_stop_for_candidate(candidate("LONG", 10.0), hourly, 105.0, 1.0)
        short_stop = initial_stop_for_candidate(candidate("SHORT", 10.0), hourly, 95.0, 1.0)
        self.assertEqual(long_stop, 95.0)
        self.assertEqual(short_stop, 105.0)

    def test_risk_budget_includes_round_trip_commission(self) -> None:
        quantity, per_lot = position_size_for_risk(100.0, 95.0, 10.0)
        self.assertGreater(per_lot, 50.0)
        self.assertEqual(quantity, int(1000.0 // per_lot))

    def test_stop_is_checked_before_same_bar_profit(self) -> None:
        item = candidate("LONG", 10.0)
        state = new_protection_state("LONG", 100.0, 95.0)
        bar = pd.Series({"open": 94.0, "low": 93.0, "high": 110.0})
        actual = advance_on_15m_bar(state, item, bar, pd.DataFrame(), 1.0)
        self.assertEqual(actual.exit_reason, "RISK_STOP")
        self.assertEqual(actual.exit_price, 94.0)
        self.assertFalse(actual.breakeven_armed)

    def test_four_phases_move_stop_only_toward_profit(self) -> None:
        item = candidate("LONG", 10.0)
        state = new_protection_state("LONG", 100.0, 95.0)
        thirty = pd.DataFrame({
            "closed_at": pd.date_range("2026-10-01T10:30:00Z", periods=3, freq="30min"),
            "low": [99.0, 102.0, 104.0],
            "high": [103.0, 106.0, 108.0],
        })
        state = advance_on_15m_bar(
            state,
            item,
            pd.Series({"open": 100.0, "low": 99.0, "high": 105.0}),
            thirty.iloc[:2],
            1.0,
        )
        self.assertEqual(state.phase, "BREAKEVEN")
        self.assertGreaterEqual(state.current_stop, 101.0)
        stop_after_breakeven = state.current_stop
        state = advance_on_15m_bar(
            state,
            item,
            pd.Series({"open": 105.0, "low": 103.0, "high": 107.0}),
            thirty,
            1.0,
        )
        self.assertEqual(state.phase, "PROFIT")
        self.assertGreaterEqual(state.current_stop, stop_after_breakeven)

    def test_enrichment_ignores_incomplete_candles(self) -> None:
        raw = pd.DataFrame({
            "time": pd.date_range("2026-01-01", periods=41, freq="h", tz="UTC"),
            "open": range(41),
            "high": [value + 2 for value in range(41)],
            "low": [value - 1 for value in range(41)],
            "close": [value + 1 for value in range(41)],
            "volume": [100.0] * 41,
            "is_complete": [True] * 40 + [False],
        })
        enriched = enrich_candles(raw, 60)
        self.assertEqual(len(enriched), 40)
        self.assertEqual(enriched.iloc[-1]["closed_at"], raw.iloc[-2]["time"] + pd.Timedelta(hours=1))


if __name__ == "__main__":
    unittest.main()
