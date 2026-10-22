import hashlib
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from strategies.v35_hierarchy import aggregate_directional_regime, hierarchical_gate
from strategies.v35_pattern_library import PatternDecision, PatternMatch
from strategies.v35_risk import (
    advance_on_15m_bar,
    initial_stop_for_candidate,
    new_protection_state,
    position_size_for_risk,
)
from strategies.v35_signals import (
    V35Candidate,
    enrich_candles,
    find_trend_continuation_candidates,
    short_entry_allowed,
)


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


def trend_frames(direction: str) -> dict[int, pd.DataFrame]:
    sign = 1.0 if direction == "LONG" else -1.0
    origin = pd.Timestamp("2026-10-01T00:00:00Z")

    hourly_rows = []
    for index in range(80):
        close = 100.0 + sign * index * 0.35
        hourly_rows.append({
            "time": origin + pd.Timedelta(hours=index),
            "closed_at": origin + pd.Timedelta(hours=index + 1),
            "open": close - sign * 0.18,
            "high": close + 0.30,
            "low": close - 0.30,
            "close": close,
            "volume": 100.0,
            "body": 0.18,
            "body_atr": 0.18,
            "atr": 1.0,
            "ao": sign * (0.5 + index * 0.01),
            "ao_delta": sign * 0.01,
            "macd_hist": sign * 0.20,
            "chaikin": sign * 10.0,
            "ema20": close - sign * 0.50,
            "ema50": close - sign * 1.00,
        })

    half_hour_rows = []
    for index in range(160):
        close = 100.0 + sign * index * 0.17
        half_hour_rows.append({
            "time": origin + pd.Timedelta(minutes=30 * index),
            "closed_at": origin + pd.Timedelta(minutes=30 * (index + 1)),
            "open": close - sign * 0.10,
            "high": close + 0.15,
            "low": close - 0.15,
            "close": close,
            "body": 0.10,
            "ao": sign * (0.4 + index * 0.002),
            "ao_delta": sign * 0.002,
            "macd_hist": sign * (0.10 + index * 0.001),
        })

    quarter_hour_rows = []
    for index in range(320):
        close = 100.0 + sign * index * 0.08
        quarter_hour_rows.append({
            "time": origin + pd.Timedelta(minutes=15 * index),
            "closed_at": origin + pd.Timedelta(minutes=15 * (index + 1)),
            "open": close - sign * 0.05,
            "high": close + 0.08,
            "low": close - 0.08,
            "close": close,
            "body": 0.05,
            "ao": sign * (0.3 + index * 0.001),
            "ao_delta": sign * 0.001,
            "macd_hist": sign * (0.08 + index * 0.0005),
        })

    # A compact pause followed by a fresh 15-minute structure break. There is
    # deliberately no named hourly pattern: the macro trend supplies context.
    for index in range(312, 319):
        base = 125.0 if direction == "LONG" else 75.0
        close = base + sign * ((index - 312) % 2) * 0.03
        quarter_hour_rows[index].update({
            "open": close + sign * 0.01,
            "high": close + 0.05,
            "low": close - 0.05,
            "close": close,
            "body": 0.01,
            "ao_delta": -sign * 0.001,
            "macd_hist": sign * 0.04,
        })
    trigger = quarter_hour_rows[319]
    prior = quarter_hour_rows[315:319]
    trigger_close = (
        max(row["high"] for row in prior) + 0.18
        if direction == "LONG"
        else min(row["low"] for row in prior) - 0.18
    )
    trigger_open = trigger_close - sign * 0.12
    trigger.update({
        "open": trigger_open,
        "high": max(trigger_open, trigger_close) + 0.04,
        "low": min(trigger_open, trigger_close) - 0.04,
        "close": trigger_close,
        "body": 0.12,
        "ao_delta": sign * 0.03,
        "macd_hist": sign * 0.09,
    })
    return {
        60: pd.DataFrame(hourly_rows),
        30: pd.DataFrame(half_hour_rows),
        15: pd.DataFrame(quarter_hour_rows),
    }


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

    def test_hierarchical_gate_allows_strong_bull_transition_directly(self) -> None:
        context = {"group_regime": "BULL", "direction": "LONG", "local_stage": "TRANSITION"}
        strong = replace(
            candidate("LONG"),
            classic_pattern="MOMENTUM_BREAKOUT",
            pattern_confidence=82,
            body_sum_atr=1.50,
            execution_score=5,
        )
        self.assertEqual(
            hierarchical_gate(context, strong),
            (True, "BULL_LONG_STRONG_TRANSITION"),
        )
        self.assertEqual(
            hierarchical_gate(context, replace(strong, body_sum_atr=1.49)),
            (False, "STAGE_TRANSITION_NOT_ALLOWED_IN_BULL"),
        )

    def test_hierarchical_gate_allows_only_strong_countertrend_reversals(self) -> None:
        bull_context = {"group_regime": "BULL", "direction": "SHORT", "local_stage": "TREND_MATURE"}
        bear_context = {"group_regime": "BEAR", "direction": "LONG", "local_stage": "TRANSITION"}
        reversal = replace(
            candidate("SHORT"),
            classic_pattern="FAILED_BREAKOUT_REVERSAL",
            pattern_confidence=80,
            largest_body_atr=0.75,
            execution_score=5,
        )
        self.assertEqual(
            hierarchical_gate(bull_context, reversal),
            (True, "BULL_SHORT_STRONG_REVERSAL"),
        )
        self.assertEqual(
            hierarchical_gate(bear_context, replace(reversal, direction="LONG", classic_pattern="FALLING_WEDGE_BREAKOUT")),
            (True, "BEAR_LONG_STRONG_REVERSAL"),
        )
        self.assertEqual(
            hierarchical_gate(bull_context, replace(reversal, classic_pattern="MOMENTUM_BREAKOUT")),
            (False, "BULL_DIRECTION_MISMATCH"),
        )

    def test_hierarchical_gate_keeps_sideways_and_shock_as_hard_blocks(self) -> None:
        strong = replace(
            candidate("LONG"),
            classic_pattern="FALLING_WEDGE_BREAKOUT",
            pattern_confidence=95,
            largest_body_atr=2.0,
            body_sum_atr=3.0,
            execution_score=6,
        )
        for regime in ("SIDEWAYS", "SHOCK"):
            with self.subTest(regime=regime):
                self.assertEqual(
                    hierarchical_gate(
                        {"group_regime": regime, "direction": "LONG", "local_stage": "TRANSITION"},
                        strong,
                    ),
                    (False, f"GROUP_{regime}"),
                )

    def test_trend_continuation_gate_is_symmetric_for_long_and_short(self) -> None:
        for regime, direction in (("BULL", "LONG"), ("BEAR", "SHORT")):
            for stage in ("TREND_EARLY", "PULLBACK", "BREAKOUT_ATTEMPT", "TREND_MATURE"):
                with self.subTest(regime=regime, direction=direction, stage=stage):
                    item = replace(
                        candidate(direction),
                        entry_pattern="LOWER_TF_TREND_CONTINUATION",
                        execution_score=5,
                        classic_pattern=(
                            "BULL_FLAG_BREAKOUT" if direction == "LONG" else "BEAR_FLAG_BREAKDOWN"
                        ),
                    )
                    self.assertEqual(
                        hierarchical_gate(
                            {"group_regime": regime, "direction": direction, "local_stage": stage},
                            item,
                        ),
                        (True, f"{regime}_{direction}_{stage}"),
                    )

    def test_single_local_shock_does_not_turn_whole_group_into_shock(self) -> None:
        rows = [
            {"stage": "SHOCK", "move_atr": -2.0},
            {"stage": "TREND_MATURE", "move_atr": -1.4},
        ]
        regime = aggregate_directional_regime(rows, rows[0])
        self.assertEqual(regime["regime"], "BEAR")

    def test_trend_continuation_gate_rejects_synthetic_macro_pattern(self) -> None:
        item = replace(
            candidate("LONG"),
            entry_pattern="LOWER_TF_TREND_CONTINUATION",
            execution_score=5,
            classic_pattern="MACRO_TREND_CONTINUATION",
        )
        self.assertEqual(
            hierarchical_gate(
                {"group_regime": "BULL", "direction": "LONG", "local_stage": "TREND_EARLY"},
                item,
            ),
            (False, "CONTINUATION_REQUIRES_NAMED_PATTERN"),
        )

    def test_lower_timeframe_continuation_requires_named_pattern(self) -> None:
        for direction in ("LONG", "SHORT"):
            frames = trend_frames(direction)
            end = pd.Timestamp(frames[15].iloc[-1]["closed_at"])
            no_pattern = PatternDecision(False, None, (), (), ())
            with patch("strategies.v35_signals.classify_entry_pattern", return_value=no_pattern):
                result = find_trend_continuation_candidates(
                    "TEST",
                    frames,
                    end - pd.Timedelta(hours=2),
                    end,
                )
            self.assertEqual(result, [], direction)

            pattern_name = "BULL_FLAG_BREAKOUT" if direction == "LONG" else "BEAR_FLAG_BREAKDOWN"
            named_pattern = PatternMatch(
                pattern_name,
                "CONTINUATION",
                direction,
                85,
                ("test",),
            )
            decision = PatternDecision(True, named_pattern, (named_pattern,), (), (named_pattern,))
            with patch("strategies.v35_signals.classify_entry_pattern", return_value=decision):
                result = find_trend_continuation_candidates(
                    "TEST",
                    frames,
                    end - pd.Timedelta(hours=2),
                    end,
                )
            self.assertTrue(result, direction)
            self.assertEqual(result[-1].direction, direction)
            self.assertEqual(result[-1].entry_pattern, "LOWER_TF_TREND_CONTINUATION")
            self.assertEqual(result[-1].classic_pattern, pattern_name)
            self.assertGreaterEqual(result[-1].execution_score, 5)

    def test_v10_short_filter_requires_complete_execution_or_large_hourly_impulse(self) -> None:
        weak = candidate("SHORT")
        self.assertFalse(short_entry_allowed(weak))
        self.assertTrue(short_entry_allowed(replace(weak, execution_score=6)))
        self.assertTrue(short_entry_allowed(replace(weak, body_sum_atr=2.25)))
        self.assertTrue(short_entry_allowed(replace(
            weak,
            entry_pattern="LOWER_TF_TREND_CONTINUATION",
            execution_score=5,
        )))
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
