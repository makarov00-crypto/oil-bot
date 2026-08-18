import json
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import pandas as pd

from ao_chaikin_shadow import (
    AoChaikinShadowJournal,
    CHAIKIN_CONFIRMS,
    DECISION_ENTRY,
    DECISION_EXIT,
    DECISION_NO_ENTRY,
    DIRECTION_LONG,
    POSITION_FLAT,
    build_shadow_strategy_payload,
    evaluate_shadow_candle,
)


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def prepared_frame(
    ao_values: list[float],
    prices: list[float] | None = None,
    chaikin_values: list[float] | None = None,
) -> pd.DataFrame:
    closes = prices or [100.0] * len(ao_values)
    return pd.DataFrame(
        {
            "time": pd.date_range("2026-08-14T08:00:00Z", periods=len(ao_values), freq="h"),
            "candle_closed_at": pd.date_range("2026-08-14T09:00:00Z", periods=len(ao_values), freq="h"),
            "open": closes,
            "high": [value + 1.0 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": closes,
            "volume": [1000.0] * len(ao_values),
            "shadow_ao": ao_values,
            "shadow_atr": [value * 0.01 for value in closes],
            "shadow_chaikin": chaikin_values or [10.0 + index * 2.0 for index in range(len(ao_values))],
        }
    )


class AoChaikinShadowTests(unittest.TestCase):
    def test_enters_on_ao_zero_cross_with_atr_relative_strength(self) -> None:
        frame = prepared_frame([-0.1, 0.8])

        result = evaluate_shadow_candle(
            frame,
            1,
            None,
            symbol="VBU6",
            point_value=1.0,
            minimum_strength_atr_ratio=0.7,
        )

        self.assertEqual(result["decision"], DECISION_ENTRY)
        self.assertEqual(result["direction"], DIRECTION_LONG)
        self.assertEqual(result["chaikin_status"], CHAIKIN_CONFIRMS)
        self.assertIn("AO недавно пересёк ноль", result["reason"])

    def test_atr_relative_strength_has_same_meaning_at_different_price_scales(self) -> None:
        small = evaluate_shadow_candle(
            prepared_frame([-0.6, 0.8], [100.0, 100.0]),
            1,
            None,
            symbol="VBU6",
            point_value=1.0,
        )
        large = evaluate_shadow_candle(
            prepared_frame([-60.0, 80.0], [10000.0, 10000.0]),
            1,
            None,
            symbol="LKU6",
            point_value=1.0,
        )

        self.assertEqual(small["decision"], DECISION_ENTRY)
        self.assertEqual(large["decision"], DECISION_ENTRY)
        self.assertEqual(small["ao_strength_atr_ratio"], large["ao_strength_atr_ratio"])

    def test_rejects_direction_when_relative_strength_is_too_low(self) -> None:
        result = evaluate_shadow_candle(
            prepared_frame([-0.1, 0.3]),
            1,
            None,
            symbol="VBU6",
            point_value=1.0,
        )

        self.assertEqual(result["decision"], DECISION_NO_ENTRY)
        self.assertIn("ниже порога", result["reason"])

    def test_allows_one_strengthening_candle_after_ao_zero_cross(self) -> None:
        result = evaluate_shadow_candle(
            prepared_frame([-0.1, 0.4, 0.6]),
            2,
            None,
            symbol="VBU6",
            point_value=1.0,
        )

        self.assertEqual(result["decision"], DECISION_ENTRY)

    def test_rejects_ao_acceleration_without_recent_zero_cross(self) -> None:
        result = evaluate_shadow_candle(
            prepared_frame([0.4, 0.6]),
            1,
            None,
            symbol="VBU6",
            point_value=1.0,
        )

        self.assertEqual(result["decision"], DECISION_NO_ENTRY)
        self.assertIn("двух последовательных", result["reason"])

    def test_exits_after_three_opposite_ao_bars_and_calculates_one_lot_result(self) -> None:
        frame = prepared_frame([10.0, 9.0, 8.0, 7.0], [100.0, 102.0, 104.0, 103.0])
        previous = {
            "position_after": DIRECTION_LONG,
            "entry_time": "2026-08-14T09:00:00+03:00",
            "entry_price": 100.0,
            "best_price": 105.0,
            "worst_price": 99.0,
        }

        result = evaluate_shadow_candle(
            frame,
            3,
            previous,
            symbol="VBU6",
            point_value=1.0,
            commission_rate=0.00025,
        )

        self.assertEqual(result["decision"], DECISION_EXIT)
        self.assertEqual(result["position_after"], POSITION_FLAT)
        self.assertEqual(result["opposite_ao_bars"], 3)
        self.assertEqual(result["gross_result_rub_1lot"], 3.0)
        self.assertEqual(result["estimated_net_rub_1lot"], 2.95)
        self.assertEqual(result["capture_pct"], 60.0)

    def test_exits_after_two_opposite_ao_bars_when_chaikin_confirms_reversal(self) -> None:
        frame = prepared_frame([10.0, 9.0, 8.0], [100.0, 102.0, 101.0], [20.0, 16.0, 12.0])
        previous = {
            "position_after": DIRECTION_LONG,
            "entry_time": "2026-08-14T09:00:00+03:00",
            "entry_price": 100.0,
            "best_price": 103.0,
            "worst_price": 99.0,
        }

        result = evaluate_shadow_candle(frame, 2, previous, symbol="VBU6", point_value=1.0)

        self.assertEqual(result["decision"], DECISION_EXIT)
        self.assertEqual(result["opposite_ao_bars"], 2)
        self.assertIn("подтверждены", result["reason"])

    def test_journal_writes_only_one_record_for_the_same_closed_candle(self) -> None:
        candles = pd.DataFrame(
            {
                "time": pd.date_range("2026-08-10T00:00:00Z", periods=40, freq="h"),
                "open": [100.0] * 40,
                "high": [101.0] * 40,
                "low": [99.0] * 40,
                "close": [100.0] * 40,
                "volume": [1000.0] * 40,
            }
        )
        with TemporaryDirectory() as temp_dir:
            journal = AoChaikinShadowJournal(Path(temp_dir) / "shadow.jsonl")
            first = journal.observe(symbol="VBU6", candles=candles, point_value=1.0)
            second = journal.observe(symbol="VBU6", candles=candles, point_value=1.0)

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])

    def test_dashboard_payload_is_sorted_newest_first(self) -> None:
        rows = [
            {
                "symbol": "VBU6",
                "candle_closed_at": "2026-08-14T12:00:00+03:00",
                "recorded_at": "2026-08-14T12:00:01+03:00",
                "decision": DECISION_ENTRY,
                "position_after": DIRECTION_LONG,
                "chaikin_status": CHAIKIN_CONFIRMS,
            },
            {
                "symbol": "VBU6",
                "candle_closed_at": "2026-08-14T15:00:00+03:00",
                "recorded_at": "2026-08-14T15:00:01+03:00",
                "decision": DECISION_EXIT,
                "position_after": POSITION_FLAT,
                "estimated_net_rub_1lot": 25.0,
                "capture_pct": 75.0,
            },
        ]
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "shadow.jsonl"
            path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")
            payload = build_shadow_strategy_payload(
                path,
                enabled=True,
                now=datetime(2026, 8, 15, 0, 0, tzinfo=MOSCOW_TZ),
            )

        self.assertEqual(payload["decisions"][0]["decision"], DECISION_EXIT)
        self.assertEqual(payload["summary"]["entries"], 1)
        self.assertEqual(payload["summary"]["closed_trades"], 1)
        self.assertEqual(payload["summary"]["net_result_rub_1lot"], 25.0)


if __name__ == "__main__":
    unittest.main()
