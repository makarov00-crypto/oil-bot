import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from strategies.v35_signals import V35Candidate
from v35_shadow import ORDER_SUBMISSION_ENABLED, V35ShadowJournal, read_shadow_records


def candidate() -> V35Candidate:
    return V35Candidate(
        symbol="TEST",
        direction="LONG",
        signal_time="2026-10-01T12:00:00+00:00",
        signal_price=100.0,
        atr=1.0,
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
        execution_score=6,
    )


class V35ShadowTests(unittest.TestCase):
    def test_shadow_is_hard_disabled_for_order_submission(self) -> None:
        self.assertFalse(ORDER_SUBMISSION_ENABLED)

    def test_journal_records_mechanical_gate_once(self) -> None:
        frames = {"TEST": {15: pd.DataFrame(), 30: pd.DataFrame(), 60: pd.DataFrame()}}
        context = {
            "global_regime": "BULL",
            "group_regime": "BULL",
            "local_stage": "TREND_EARLY",
            "direction": "LONG",
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "candidates.jsonl"
            status_path = Path(directory) / "status.json"
            journal = V35ShadowJournal(path, status_path)
            with patch("v35_shadow.find_multitimeframe_candidates", return_value=[candidate()]), patch(
                "v35_shadow.hierarchical_context", return_value=context
            ):
                first = journal.observe(
                    frames,
                    start=pd.Timestamp("2026-10-01T00:00:00Z"),
                    end=pd.Timestamp("2026-10-01T13:00:00Z"),
                )
                second = journal.observe(
                    frames,
                    start=pd.Timestamp("2026-10-01T00:00:00Z"),
                    end=pd.Timestamp("2026-10-01T13:00:00Z"),
                )
            rows = read_shadow_records(path)
            self.assertEqual(first["new_records"], 1)
            self.assertEqual(second["new_records"], 0)
            self.assertEqual(len(rows), 1)
            self.assertTrue(rows[0]["gate_allowed"])
            self.assertEqual(rows[0]["gate_reason"], "BULL_LONG_EARLY_TREND")
            self.assertFalse(rows[0]["order_submission_enabled"])
            status = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertFalse(status["order_submission_enabled"])

    def test_v10_rejection_never_calls_hierarchical_gate(self) -> None:
        weak_short = candidate()
        weak_short.direction = "SHORT"
        weak_short.execution_score = 5
        weak_short.body_sum_atr = 1.0
        frames = {"TEST": {15: pd.DataFrame(), 30: pd.DataFrame(), 60: pd.DataFrame()}}
        with TemporaryDirectory() as directory:
            journal = V35ShadowJournal(
                Path(directory) / "candidates.jsonl",
                Path(directory) / "status.json",
            )
            with patch("v35_shadow.find_multitimeframe_candidates", return_value=[weak_short]), patch(
                "v35_shadow.hierarchical_context"
            ) as hierarchy:
                journal.observe(
                    frames,
                    start=pd.Timestamp("2026-10-01T00:00:00Z"),
                    end=pd.Timestamp("2026-10-01T13:00:00Z"),
                )
            hierarchy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
