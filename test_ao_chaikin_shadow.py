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
    STRATEGY_VERSION,
    build_shadow_exit_analytics,
    build_shadow_strategy_comparison,
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
    def test_enters_on_second_closed_ao_bar_after_zero_cross(self) -> None:
        frame = prepared_frame([-0.1, 0.4, 0.8])

        result = evaluate_shadow_candle(
            frame,
            2,
            None,
            symbol="VBU6",
            point_value=1.0,
            minimum_strength_atr_ratio=0.7,
        )

        self.assertEqual(result["decision"], DECISION_ENTRY)
        self.assertEqual(result["direction"], DIRECTION_LONG)
        self.assertEqual(result["chaikin_status"], CHAIKIN_CONFIRMS)
        self.assertIn("Две закрытые свечи AO", result["reason"])

    def test_does_not_enter_on_first_ao_zero_cross_bar(self) -> None:
        result = evaluate_shadow_candle(
            prepared_frame([-0.1, 0.8]),
            1,
            None,
            symbol="VBU6",
            point_value=1.0,
        )

        self.assertEqual(result["decision"], DECISION_NO_ENTRY)
        self.assertIn("второй закрытой", result["reason"])

    def test_atr_relative_strength_has_same_meaning_at_different_price_scales(self) -> None:
        small = evaluate_shadow_candle(
            prepared_frame([-0.6, 0.4, 0.8], [100.0, 100.0, 100.0]),
            2,
            None,
            symbol="VBU6",
            point_value=1.0,
        )
        large = evaluate_shadow_candle(
            prepared_frame([-60.0, 40.0, 80.0], [10000.0, 10000.0, 10000.0]),
            2,
            None,
            symbol="LKU6",
            point_value=1.0,
        )

        self.assertEqual(small["decision"], DECISION_ENTRY)
        self.assertEqual(large["decision"], DECISION_ENTRY)
        self.assertEqual(small["ao_strength_atr_ratio"], large["ao_strength_atr_ratio"])

    def test_rejects_direction_when_relative_strength_is_too_low(self) -> None:
        result = evaluate_shadow_candle(
            prepared_frame([-0.1, 0.2, 0.3]),
            2,
            None,
            symbol="VBU6",
            point_value=1.0,
        )

        self.assertEqual(result["decision"], DECISION_NO_ENTRY)
        self.assertIn("ниже порога", result["reason"])

    def test_requires_strengthening_second_candle_after_ao_zero_cross(self) -> None:
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
            prepared_frame([0.2, 0.4, 0.6]),
            2,
            None,
            symbol="VBU6",
            point_value=1.0,
        )

        self.assertEqual(result["decision"], DECISION_NO_ENTRY)
        self.assertIn("сразу после пересечения", result["reason"])

    def test_exits_after_three_opposite_ao_bars_and_calculates_one_lot_result(self) -> None:
        frame = prepared_frame([10.0, 9.0, 8.0, 6.0], [100.0, 105.0, 104.0, 103.0])
        previous = {
            "position_after": DIRECTION_LONG,
            "entry_time": "2026-08-14T09:00:00+03:00",
            "entry_price": 100.0,
            "best_price": 105.0,
            "worst_price": 99.0,
            "peak_ao_magnitude": 10.0,
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
        self.assertEqual(result["ao_peak_retention_ratio"], 0.6)
        self.assertTrue(result["price_confirms_exit"])

    def test_chaikin_does_not_force_exit_after_two_opposite_ao_bars(self) -> None:
        frame = prepared_frame([10.0, 9.0, 8.0], [100.0, 102.0, 101.0], [20.0, 16.0, 12.0])
        previous = {
            "position_after": DIRECTION_LONG,
            "entry_time": "2026-08-14T09:00:00+03:00",
            "entry_price": 100.0,
            "best_price": 103.0,
            "worst_price": 99.0,
            "peak_ao_magnitude": 10.0,
        }

        result = evaluate_shadow_candle(frame, 2, previous, symbol="VBU6", point_value=1.0)

        self.assertEqual(result["decision"], "УДЕРЖАНИЕ")
        self.assertEqual(result["opposite_ao_bars"], 2)
        self.assertIn("ослаблений AO подряд: 2 из 3", result["reason"])

    def test_does_not_exit_when_three_ao_bars_keep_most_of_peak_impulse(self) -> None:
        frame = prepared_frame([10.0, 9.5, 9.0, 8.5], [100.0, 105.0, 104.0, 103.0])
        previous = {
            "position_after": DIRECTION_LONG,
            "entry_time": "2026-08-14T09:00:00+03:00",
            "entry_price": 100.0,
            "best_price": 105.0,
            "worst_price": 99.0,
            "peak_ao_magnitude": 10.0,
        }

        result = evaluate_shadow_candle(frame, 3, previous, symbol="VBU6", point_value=1.0)

        self.assertEqual(result["decision"], "УДЕРЖАНИЕ")
        self.assertEqual(result["ao_peak_retention_ratio"], 0.85)

    def test_exits_defensively_when_ao_crosses_zero_against_position(self) -> None:
        frame = prepared_frame([10.0, 5.0, -1.0], [100.0, 99.0, 98.0])
        previous = {
            "position_after": DIRECTION_LONG,
            "entry_time": "2026-08-14T09:00:00+03:00",
            "entry_price": 100.0,
            "best_price": 101.0,
            "worst_price": 98.0,
            "peak_ao_magnitude": 10.0,
        }

        result = evaluate_shadow_candle(frame, 2, previous, symbol="VBU6", point_value=1.0)

        self.assertEqual(result["decision"], DECISION_EXIT)
        self.assertIn("пересёк ноль против", result["reason"])

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

    def test_journal_starts_current_rules_from_flat_instead_of_restoring_old_version(self) -> None:
        old_row = {
            "version": STRATEGY_VERSION - 1,
            "key": "VBU6:2026-08-14T10:00:00+03:00",
            "symbol": "VBU6",
            "candle_closed_at": "2026-08-14T10:00:00+03:00",
            "position_after": DIRECTION_LONG,
            "entry_time": "2026-08-14T09:00:00+03:00",
            "entry_price": 100.0,
        }
        candles = pd.DataFrame(
            {
                "time": pd.date_range("2026-08-15T00:00:00Z", periods=40, freq="h"),
                "open": [100.0] * 40,
                "high": [101.0] * 40,
                "low": [99.0] * 40,
                "close": [100.0] * 40,
                "volume": [1000.0] * 40,
            }
        )
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "shadow.jsonl"
            path.write_text(json.dumps(old_row, ensure_ascii=False) + "\n", encoding="utf-8")
            created = AoChaikinShadowJournal(path).observe(
                symbol="VBU6",
                candles=candles,
                point_value=1.0,
            )

        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["version"], STRATEGY_VERSION)
        self.assertEqual(created[0]["position_before"], POSITION_FLAT)

    def test_journal_closes_replaced_contract_instead_of_leaving_stale_position(self) -> None:
        old_row = {
            "version": STRATEGY_VERSION,
            "key": "BRU6:2026-08-27T16:00:00+03:00",
            "recorded_at": "2026-08-27T16:00:01+03:00",
            "candle_closed_at": "2026-08-27T16:00:00+03:00",
            "symbol": "BRU6",
            "decision": "УДЕРЖАНИЕ",
            "direction": DIRECTION_LONG,
            "position_before": DIRECTION_LONG,
            "position_after": DIRECTION_LONG,
            "price": 98.0,
            "entry_time": "2026-08-27T15:00:00+03:00",
            "entry_price": 97.5,
            "best_price": 98.2,
            "worst_price": 97.4,
            "gross_result_rub_1lot": 50.0,
            "estimated_commission_rub_1lot": 10.0,
            "estimated_net_rub_1lot": 40.0,
            "best_result_rub_1lot": 70.0,
        }
        candles = pd.DataFrame(
            {
                "time": pd.date_range("2026-08-28T00:00:00Z", periods=40, freq="h"),
                "open": [100.0] * 40,
                "high": [101.0] * 40,
                "low": [99.0] * 40,
                "close": [100.0] * 40,
                "volume": [1000.0] * 40,
            }
        )
        resolver = lambda symbol: "BRV6" if symbol in {"BRU6", "BRV6"} else symbol

        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "shadow.jsonl"
            path.write_text(json.dumps(old_row, ensure_ascii=False) + "\n", encoding="utf-8")
            journal = AoChaikinShadowJournal(path, history_symbol_resolver=resolver)
            created = journal.observe(symbol="BRV6", candles=candles, point_value=1.0)

        rollover = next(row for row in created if row.get("exit_kind") == "СМЕНА КОНТРАКТА")
        self.assertEqual(rollover["symbol"], "BRU6")
        self.assertEqual(rollover["rollover_to_symbol"], "BRV6")
        self.assertEqual(rollover["position_after"], POSITION_FLAT)
        self.assertEqual(rollover["estimated_net_rub_1lot"], 40.0)

    def test_comparison_uses_closures_in_period_and_normalizes_live_trades_to_one_lot(self) -> None:
        shadow_records = [
            {
                "version": STRATEGY_VERSION,
                "symbol": "VBU6",
                "candle_closed_at": "2026-08-20T10:00:00+03:00",
                "decision": DECISION_ENTRY,
                "position_after": DIRECTION_LONG,
            },
            {
                "version": STRATEGY_VERSION,
                "symbol": "VBU6",
                "candle_closed_at": "2026-08-20T13:00:00+03:00",
                "decision": DECISION_EXIT,
                "position_after": POSITION_FLAT,
                "estimated_net_rub_1lot": 80.0,
                "gross_result_rub_1lot": 100.0,
                "estimated_commission_rub_1lot": 20.0,
                "capture_pct": 50.0,
            },
        ]
        live_trades = [
            {
                "symbol": "VBU6",
                "entry_time": "2026-08-20T11:00:00+03:00",
                "exit_time": "2026-08-20T12:00:00+03:00",
                "pnl_rub": 120.0,
                "commission_rub": 30.0,
                "qty_lots": 3,
                "mfe_pct": 1.0,
                "realized_price_pct": 0.5,
            },
            {
                "symbol": "VBU6",
                "entry_time": "2026-08-19T11:00:00+03:00",
                "exit_time": "2026-08-20T12:30:00+03:00",
                "pnl_rub": 60.0,
                "commission_rub": 12.0,
                "qty_lots": 1,
            },
        ]

        result = build_shadow_strategy_comparison(
            shadow_records,
            live_trades,
            history_symbol_resolver=lambda symbol: symbol,
        )

        self.assertTrue(result["available"])
        self.assertEqual(result["current"]["closed_trades"], 2)
        self.assertEqual(result["current"]["wins"], 2)
        self.assertEqual(result["current"]["net_result_rub_1lot"], 100.0)
        self.assertEqual(result["current"]["commission_rub_1lot"], 22.0)
        self.assertEqual(result["shadow"]["net_result_rub_1lot"], 80.0)
        self.assertEqual(result["by_symbol"][0]["symbol"], "VBU6")

    def test_exit_analytics_compares_next_closed_hourly_candles(self) -> None:
        records = [
            {
                "version": STRATEGY_VERSION,
                "key": "TEST:2026-08-01T10:00:00+03:00",
                "symbol": "TEST",
                "candle_closed_at": "2026-08-01T10:00:00+03:00",
                "decision": DECISION_EXIT,
                "direction": DIRECTION_LONG,
                "entry_time": "2026-08-01T08:00:00+03:00",
                "entry_price": 100.0,
                "price": 110.0,
                "gross_result_rub_1lot": 10.0,
                "estimated_commission_rub_1lot": 2.0,
                "estimated_net_rub_1lot": 8.0,
                "best_result_rub_1lot": 14.0,
                "capture_pct": 71.4,
            },
            {
                "version": STRATEGY_VERSION,
                "key": "TEST:2026-08-01T11:00:00+03:00",
                "symbol": "TEST",
                "candle_closed_at": "2026-08-01T11:00:00+03:00",
                "decision": DECISION_NO_ENTRY,
                "price": 112.0,
            },
            {
                "version": STRATEGY_VERSION,
                "key": "TEST:2026-08-01T12:00:00+03:00",
                "symbol": "TEST",
                "candle_closed_at": "2026-08-01T12:00:00+03:00",
                "decision": DECISION_NO_ENTRY,
                "price": 108.0,
            },
        ]

        analytics = build_shadow_exit_analytics(records)

        self.assertTrue(analytics["available"])
        self.assertEqual(analytics["closed_trades"], 1)
        self.assertEqual(analytics["horizons"][0]["additional_hours"], 1)
        self.assertEqual(analytics["horizons"][0]["held_net_rub_1lot"], 10.0)
        self.assertEqual(analytics["horizons"][0]["delta_rub_1lot"], 2.0)
        self.assertEqual(analytics["horizons"][1]["held_net_rub_1lot"], 6.0)
        self.assertEqual(analytics["horizons"][1]["delta_rub_1lot"], -2.0)

    def test_dashboard_payload_is_sorted_newest_first(self) -> None:
        rows = [
            {
                "version": STRATEGY_VERSION,
                "symbol": "VBU6",
                "candle_closed_at": "2026-08-14T12:00:00+03:00",
                "recorded_at": "2026-08-14T12:00:01+03:00",
                "decision": DECISION_ENTRY,
                "position_after": DIRECTION_LONG,
                "chaikin_status": CHAIKIN_CONFIRMS,
            },
            {
                "version": STRATEGY_VERSION,
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
        self.assertEqual(payload["decisions"][0]["symbol"], "VBZ6")

    def test_dashboard_payload_does_not_mix_previous_strategy_version(self) -> None:
        rows = [
            {
                "version": STRATEGY_VERSION - 1,
                "symbol": "VBU6",
                "candle_closed_at": "2026-08-14T12:00:00+03:00",
                "decision": DECISION_EXIT,
                "position_after": POSITION_FLAT,
                "estimated_net_rub_1lot": 999.0,
            },
            {
                "version": STRATEGY_VERSION,
                "symbol": "VBU6",
                "candle_closed_at": "2026-08-14T13:00:00+03:00",
                "decision": DECISION_NO_ENTRY,
                "position_after": POSITION_FLAT,
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

        self.assertEqual(payload["summary"]["checks"], 1)
        self.assertEqual(payload["summary"]["closed_trades"], 0)
        self.assertEqual(payload["summary"]["net_result_rub_1lot"], 0.0)


if __name__ == "__main__":
    unittest.main()
