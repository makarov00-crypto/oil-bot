import math
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from ao_chaikin_shadow import (
    DECISION_ENTRY, DECISION_EXIT, DIRECTION_LONG, DIRECTION_SHORT,
    evaluate_shadow_candle, prepare_shadow_indicators,
)
from strategies.ao_chaikin_1h import (
    RISK_MULTIPLIER, entry_ao_peak, evaluate_position, evaluate_signal,
    is_ao_chaikin_strategy,
)


def candles(count=120):
    close = [100 + 10 * math.sin(i * math.pi / 14) for i in range(count)]
    return pd.DataFrame({
        "time": pd.date_range("2026-01-01", periods=count, freq="h", tz="UTC"),
        "high": [c + .5 for c in close], "low": [c - .5 for c in close],
        "close": close, "volume": [1000.0] * count, "is_complete": True,
    })


def prepared(ao, prices=None):
    close = prices if prices is not None else [100.0] * len(ao)
    return pd.DataFrame({
        "time": pd.date_range("2026-01-01", periods=len(ao), freq="h", tz="UTC"),
        "candle_closed_at": pd.date_range("2026-01-01T01:00Z", periods=len(ao), freq="h"),
        "high": [c + 1 for c in close], "low": [c - 1 for c in close],
        "close": close, "volume": [1000.0] * len(ao),
        "shadow_ao": ao, "shadow_atr": [1.0] * len(ao),
        "shadow_chaikin": list(range(len(ao))),
    })


class AoChaikinLiveTests(unittest.TestCase):
    def test_identity_and_half_risk(self):
        self.assertTrue(is_ao_chaikin_strategy("ao_chaikin_1h"))
        self.assertFalse(is_ao_chaikin_strategy("reversal_1h"))
        self.assertEqual(RISK_MULTIPLIER, .5)

    def test_real_ohlcv_entry_matches_shadow_both_directions(self):
        raw = candles()
        seen = set()
        for count in range(36, len(raw) + 1):
            df = raw.iloc[:count]
            frame = prepare_shadow_indicators(df)
            expected = evaluate_shadow_candle(frame, len(frame) - 1, None, symbol="TEST", point_value=1)
            signal, reason = evaluate_signal(df, None, SimpleNamespace(symbol="TEST"), "opposite")
            wanted = "HOLD"
            if expected["decision"] == DECISION_ENTRY:
                wanted = "LONG" if expected["direction"] == DIRECTION_LONG else "SHORT"
            self.assertEqual(signal, wanted)
            self.assertEqual(reason, expected["reason"])
            seen.add(signal)
        self.assertEqual(seen, {"LONG", "SHORT", "HOLD"})

    def test_real_ohlcv_position_matches_shadow_both_directions(self):
        raw = candles()
        for side, direction in (("LONG", DIRECTION_LONG), ("SHORT", DIRECTION_SHORT)):
            for count in (38, 43, 49, 57):
                with self.subTest(side=side, count=count):
                    df = raw.iloc[:count]
                    frame = prepare_shadow_indicators(df)
                    entered = frame.iloc[0]["candle_closed_at"]
                    peak = 50.0
                    previous = {"position_after": direction, "entry_time": entered.isoformat(),
                                "entry_price": 100, "best_price": 100, "worst_price": 100,
                                "peak_ao_magnitude": peak}
                    expected = evaluate_shadow_candle(frame, len(frame) - 1, previous, symbol="", point_value=1)
                    actual = evaluate_position(df, side, 100, entered, peak)
                    for field in ("decision", "reason", "peak_ao_magnitude", "opposite_ao_bars", "price_confirms_exit"):
                        self.assertEqual(actual[field], expected[field])
                    self.assertEqual(actual["should_exit"], expected["decision"] == DECISION_EXIT)

    def test_cross_and_exact_strength_boundary_both_directions(self):
        for sign, expected in ((1, "LONG"), (-1, "SHORT")):
            for ao, wanted in (([-.1, .2, .6], expected), ([-.1, .2, .59999], "HOLD"),
                               ([.1, .2, .8], "HOLD"), ([-.1, .8], "HOLD")):
                with self.subTest(sign=sign, ao=ao):
                    frame = prepared([a * sign for a in ao])
                    with patch("strategies.ao_chaikin_1h._closed_indicators", return_value=frame):
                        actual, _ = evaluate_signal(None, None, None, "")
                    self.assertEqual(actual, wanted)

    def test_zero_and_momentum_exit_parity_both_directions(self):
        for sign, side, direction in ((1, "LONG", DIRECTION_LONG), (-1, "SHORT", DIRECTION_SHORT)):
            for ao, prices, expected_exit in (([10, 9, 8, 6], [100, 105, 104, 103], True),
                                             ([10, 9.5, 9, 8.5], [100, 105, 104, 103], False),
                                             ([10, 9, 8, 6], [100, 101, 102, 103], False),
                                             ([10, 9, 0], [100, 101, 102], True),
                                             ([10, 9, -1], [100, 101, 102], True)):
                with self.subTest(side=side, ao=ao, prices=prices):
                    frame = prepared([sign * a for a in ao], [100 + sign * (p - 100) for p in prices])
                    entered = frame.iloc[0]["candle_closed_at"]
                    previous = {"position_after": direction, "entry_time": entered.isoformat(),
                                "entry_price": 100, "peak_ao_magnitude": 10}
                    expected = evaluate_shadow_candle(frame, len(frame) - 1, previous, symbol="", point_value=1)
                    with patch("strategies.ao_chaikin_1h._closed_indicators", return_value=frame):
                        actual = evaluate_position(None, side, 100, entered, 10)
                    self.assertEqual(actual["decision"], expected["decision"])
                    self.assertEqual(actual["reason"], expected["reason"])
                    self.assertEqual(actual["should_exit"], expected_exit)

    def test_recovers_missed_peak_since_fill_and_preserves_saved_peak(self):
        frame = prepared([100, 5, 10, 9, 8, 6], [100, 100, 103, 102, 101, 100])
        entered = frame.iloc[1]["candle_closed_at"]
        with patch("strategies.ao_chaikin_1h._closed_indicators", return_value=frame):
            result = evaluate_position(None, "LONG", 100, entered, 5)
            self.assertEqual(result["peak_ao_magnitude"], 10)
            self.assertTrue(result["should_exit"])
            result = evaluate_position(None, "LONG", 100, entered, 12.123456789)
            self.assertEqual(result["peak_ao_magnitude"], 12.123456789)

    def test_never_exits_using_candle_closed_before_or_at_fill(self):
        frame = prepared([10, 5, -1])
        with patch("strategies.ao_chaikin_1h._closed_indicators", return_value=frame):
            for delta in (pd.Timedelta(0), pd.Timedelta(seconds=1)):
                result = evaluate_position(None, "LONG", 100, frame.iloc[-1]["candle_closed_at"] + delta, 10)
                self.assertFalse(result["should_exit"])
                self.assertEqual(result["peak_ao_magnitude"], 10)

    def test_entry_peak_uses_favorable_signal_candle(self):
        for side, ao in (("LONG", .8), ("SHORT", -.8)):
            with patch("strategies.ao_chaikin_1h._closed_indicators", return_value=prepared([0, ao])):
                self.assertEqual(entry_ao_peak(None, side), .8)
                self.assertEqual(entry_ao_peak(None, "invalid"), 0)

    def test_incomplete_and_future_candles_are_ignored_before_indicators(self):
        base = candles(60)
        expected = evaluate_signal(base, None, None, "")
        expected_position = evaluate_position(base, "LONG", 100, base.iloc[40]["time"], 10)
        for incomplete_flag, future in ((False, False), (True, True)):
            extra = base.iloc[-1:].copy()
            extra["time"] = (pd.Timestamp.now(tz="UTC").floor("h") if future
                             else base.iloc[-1]["time"] + pd.Timedelta(hours=1))
            extra["is_complete"] = incomplete_flag
            extra[["high", "low", "close"]] = 10000
            df = pd.concat([base, extra], ignore_index=True)
            self.assertEqual(evaluate_signal(df, None, None, ""), expected)
            actual = evaluate_position(df, "LONG", 100, base.iloc[40]["time"], 10)
            for field in ("decision", "reason", "peak_ao_magnitude"):
                self.assertEqual(actual[field], expected_position[field])

    def test_invalid_and_insufficient_data_hold_safely(self):
        invalid_frames = [None, pd.DataFrame(), candles(34)]
        for column, bad in (("close", float("nan")), ("high", float("inf")),
                            ("volume", "bad"), ("time", "bad"), ("close", -1)):
            df = candles().astype({column: object})
            df.loc[len(df) - 1, column] = bad
            invalid_frames.append(df)
        for df in invalid_frames:
            signal, _ = evaluate_signal(df, None, None, "")
            self.assertEqual(signal, "HOLD")
            result = evaluate_position(df, "LONG", 100, "2026-01-01T01:00Z", 10)
            self.assertFalse(result["should_exit"])
            self.assertEqual(result["peak_ao_magnitude"], 10)
        for side, price, entered in (("INVALID", 100, "2026-01-01"), ("LONG", 0, "2026-01-01"),
                                      ("LONG", 100, None), ("LONG", float("nan"), "2026-01-01")):
            self.assertFalse(evaluate_position(candles(), side, price, entered, 10)["should_exit"])


if __name__ == "__main__":
    unittest.main()
