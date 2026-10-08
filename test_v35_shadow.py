import json
import unittest
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from strategies.v35_signals import V35Candidate
from v35_shadow import (
    ORDER_SUBMISSION_ENABLED,
    V35ShadowJournal,
    V35ShadowPortfolio,
    append_cycle_audit,
    _market_snapshot,
    read_shadow_records,
)


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

    def test_market_snapshot_covers_symbols_without_entry_candidates(self) -> None:
        start = pd.Timestamp("2026-10-01T00:00:00Z")
        rows = []
        for index in range(80):
            close = 100.0 + index * 0.4
            rows.append({
                "time": start + pd.Timedelta(hours=index),
                "closed_at": start + pd.Timedelta(hours=index + 1),
                "open": close - 0.25,
                "high": close + 0.4,
                "low": close - 0.4,
                "close": close,
                "body": 0.25,
                "atr": 0.8,
                "ao": 1.0 + index * 0.02,
                "ao_delta": 0.02,
                "macd_hist": 0.4,
            })
        frame = pd.DataFrame(rows)
        snapshot = _market_snapshot(
            {"AAA": {60: frame, 30: pd.DataFrame(), 15: pd.DataFrame()}},
            start + pd.Timedelta(hours=81),
        )
        self.assertEqual(set(snapshot), {"AAA"})
        self.assertIn(snapshot["AAA"]["regime"], {"TREND", "PULLBACK", "CHOP", "SHOCK_AFTER_RANGE"})
        self.assertEqual(snapshot["AAA"]["direction"], "LONG")
        self.assertEqual(snapshot["AAA"]["asset_group"], "EQUITY")

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

    def test_v10_rejection_keeps_context_without_calling_hierarchical_gate(self) -> None:
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
            context = {"global_regime": "BEAR", "group_regime": "BEAR", "local_stage": "TREND_MATURE"}
            with patch("v35_shadow.find_multitimeframe_candidates", return_value=[weak_short]), patch(
                "v35_shadow.hierarchical_context", return_value=context
            ) as hierarchy, patch("v35_shadow.hierarchical_gate") as gate:
                journal.observe(
                    frames,
                    start=pd.Timestamp("2026-10-01T00:00:00Z"),
                    end=pd.Timestamp("2026-10-01T13:00:00Z"),
                )
            hierarchy.assert_called_once()
            gate.assert_not_called()
            row = read_shadow_records(Path(directory) / "candidates.jsonl")[0]
            self.assertEqual(row["context"], context)
            self.assertEqual(row["global_regime"], "BEAR")
            self.assertEqual(row["gate_reason"], "V10_SHORT_QUALITY_REJECTED")

    def test_cycle_audit_is_append_only(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "cycles.jsonl"
            append_cycle_audit(path, {"status": "success", "symbols_count": 11})
            append_cycle_audit(path, {"status": "failed", "error_type": "TimeoutError"})
            rows = read_shadow_records(path)
            self.assertEqual([row["status"] for row in rows], ["success", "failed"])
            self.assertEqual(rows[0]["symbols_count"], 11)
            self.assertTrue(all(row.get("recorded_at") for row in rows))

    def test_portfolio_runs_full_tick_lifecycle_without_orders(self) -> None:
        item = candidate()
        row = {
            "candidate_id": "candidate-1",
            "symbol": "TEST",
            "contract_symbol": "TEST",
            "signal_time": item.signal_time,
            "direction": item.direction,
            "gate_allowed": True,
            "candidate": asdict(item),
            "hourly_structure": {"low": 99.0, "high": 101.0},
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            candidates = root / "candidates.jsonl"
            events = root / "events.jsonl"
            state = root / "state.json"
            status = root / "status.json"
            candidates.write_text(json.dumps(row) + "\n", encoding="utf-8")
            portfolio = V35ShadowPortfolio(candidates, events, state, status)
            meta = {"TEST": {"tick_size": 0.1, "point_value": 10.0}}

            opened = portfolio.advance(
                {"TEST": 100.0},
                meta,
                now=pd.Timestamp("2026-10-01T12:05:00Z"),
            )
            self.assertEqual(opened["active_positions"], 1)
            self.assertFalse(opened["order_submission_enabled"])
            position = portfolio.state["positions"]["TEST"]
            distance = position["protection"]["initial_distance"]
            entry = position["protection"]["entry_price"]

            portfolio.advance(
                {"TEST": entry + distance},
                meta,
                now=pd.Timestamp("2026-10-01T12:05:10Z"),
            )
            self.assertEqual(
                portfolio.state["positions"]["TEST"]["protection"]["phase"],
                "BREAKEVEN",
            )
            portfolio.advance(
                {"TEST": entry + distance * 1.30},
                meta,
                now=pd.Timestamp("2026-10-01T12:05:20Z"),
            )
            protected = portfolio.state["positions"]["TEST"]["protection"]
            self.assertEqual(protected["phase"], "PROFIT")
            result = portfolio.advance(
                {"TEST": protected["current_stop"] - 0.1},
                meta,
                now=pd.Timestamp("2026-10-01T12:05:30Z"),
            )
            self.assertEqual(result["active_positions"], 0)
            self.assertEqual(result["closed_trades"], 1)
            recorded = read_shadow_records(events)
            self.assertEqual(recorded[0]["event"], "OPEN")
            self.assertTrue(any(event["event"] == "PHASE" for event in recorded))
            close = next(event for event in recorded if event["event"] == "CLOSE")
            self.assertEqual(close["exit_reason"], "PROFIT_TRAILING_STOP")
            self.assertFalse(close["order_submission_enabled"])

    def test_portfolio_never_opens_stale_candidate(self) -> None:
        item = candidate()
        row = {
            "candidate_id": "candidate-stale",
            "symbol": "TEST",
            "signal_time": item.signal_time,
            "direction": item.direction,
            "gate_allowed": True,
            "candidate": asdict(item),
            "hourly_structure": {"low": 99.0, "high": 101.0},
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            candidates = root / "candidates.jsonl"
            events = root / "events.jsonl"
            candidates.write_text(json.dumps(row) + "\n", encoding="utf-8")
            portfolio = V35ShadowPortfolio(
                candidates,
                events,
                root / "state.json",
                root / "status.json",
            )
            result = portfolio.advance(
                {"TEST": 100.0},
                {"TEST": {"tick_size": 0.1, "point_value": 10.0}},
                now=pd.Timestamp("2026-10-01T13:00:00Z"),
            )
            self.assertEqual(result["active_positions"], 0)
            self.assertEqual(result["skipped_candidates"], 1)
            self.assertEqual(read_shadow_records(events)[0]["reason"], "STALE_CANDIDATE")


if __name__ == "__main__":
    unittest.main()
