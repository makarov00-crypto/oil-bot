import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from v35_dashboard import build_v35_dashboard_html, build_v35_dashboard_payload


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


class V35DashboardTests(unittest.TestCase):
    def test_payload_reconciles_funnel_positions_and_closed_result(self) -> None:
        now = datetime(2026, 10, 8, 18, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            status = root / "status.json"
            portfolio = root / "portfolio.json"
            candidates = root / "candidates.jsonl"
            events = root / "events.jsonl"
            cycles = root / "cycles.jsonl"
            runtime = root / "runtime.json"
            write_json(status, {
                "strategy_version": "v35-test",
                "updated_at": "2026-10-08T17:59:55+00:00",
                "symbols": ["AAA", "BBB", "CCC"],
                "multitimeframe_candidates": 2,
                "v10_allowed": 1,
                "hierarchical_allowed": 1,
                "market_snapshot": {
                    "AAA": {
                        "regime": "TREND",
                        "confidence": 84,
                        "direction": "LONG",
                        "local_stage": "TREND_EARLY",
                        "group_regime": "BULL",
                        "group_breadth": 0.75,
                        "global_regime": "BULL",
                        "closed_at": "2026-10-08T17:00:00+00:00",
                        "features": {
                            "recent_move_atr": 1.8,
                            "recent_path_efficiency": 0.66,
                            "overlap_ratio": 0.32,
                            "small_body_ratio": 0.17,
                        },
                    }
                },
            })
            write_json(portfolio, {
                "strategy_version": "v35-test",
                "updated_at": "2026-10-08T17:59:58+00:00",
                "order_submission_enabled": False,
                "positions": {
                    "AAA": {
                        "candidate_id": "open-1",
                        "symbol": "AAA",
                        "direction": "LONG",
                        "opened_at": "2026-10-08T17:00:00+00:00",
                        "last_observed_at": "2026-10-08T17:59:58+00:00",
                        "quantity": 2,
                        "point_value": 10.0,
                        "last_price": 102.0,
                        "initial_risk_rub": 200.0,
                        "candidate": {
                            "direction": "LONG",
                            "classic_pattern": "BULL_FLAG_BREAKOUT",
                            "market_regime": "TREND",
                        },
                        "protection": {
                            "entry_price": 100.0,
                            "initial_stop": 99.0,
                            "current_stop": 100.5,
                            "initial_distance": 1.0,
                            "best_move": 2.0,
                            "phase": "PROFIT",
                        },
                    }
                },
            })
            write_jsonl(candidates, [
                {
                    "candidate_id": "open-1",
                    "symbol": "AAA",
                    "signal_time": "2026-10-08T17:00:00+00:00",
                    "direction": "LONG",
                    "v10_allowed": True,
                    "gate_allowed": True,
                    "gate_reason": "BULL_LONG_EARLY_TREND",
                    "global_regime": "BULL",
                    "group_regime": "BULL",
                    "local_stage": "TREND_EARLY",
                    "candidate": {
                        "market_regime": "TREND",
                        "regime_confidence": 82,
                        "regime_direction": "LONG",
                        "entry_pattern": "AO_ZERO_CROSS",
                        "classic_pattern": "BULL_FLAG_BREAKOUT",
                        "pattern_confidence": 78,
                        "confirmation_score": 6,
                        "execution_score": 6,
                        "directional_bars": 3,
                        "body_sum_atr": 1.8,
                        "largest_body_atr": 0.8,
                        "path_efficiency_6": 0.7,
                    },
                },
                {
                    "candidate_id": "reject-1",
                    "symbol": "BBB",
                    "signal_time": "2026-10-08T16:00:00+00:00",
                    "direction": "SHORT",
                    "v10_allowed": False,
                    "gate_allowed": False,
                    "gate_reason": "V10_SHORT_QUALITY_REJECTED",
                    "candidate": {
                        "market_regime": "CHOP",
                        "classic_pattern": "HEAD_AND_SHOULDERS_BREAKDOWN",
                    },
                },
            ])
            write_jsonl(events, [
                {
                    "event": "OPEN",
                    "candidate_id": "closed-1",
                    "symbol": "AAA",
                    "event_time": "2026-10-08T13:00:00+00:00",
                },
                {
                    "event": "CLOSE",
                    "candidate_id": "closed-1",
                    "symbol": "AAA",
                    "direction": "LONG",
                    "event_time": "2026-10-08T14:00:00+00:00",
                    "entry_price": 100.0,
                    "exit_price": 103.0,
                    "net_pnl_rub": 600.0,
                    "net_r_multiple": 1.2,
                    "exit_reason": "PROFIT_TRAILING_STOP",
                },
            ])
            write_jsonl(cycles, [{"status": "success", "duration_seconds": 4.2}])
            write_json(runtime, {"session": "MAIN"})

            payload = build_v35_dashboard_payload(
                days=7,
                now=now,
                status_path=status,
                portfolio_path=portfolio,
                candidate_path=candidates,
                event_path=events,
                cycle_path=cycles,
                runtime_path=runtime,
            )

            self.assertTrue(payload["meta"]["fresh"])
            self.assertFalse(payload["meta"]["order_submission_enabled"])
            self.assertEqual(payload["kpis"]["candidates"], 2)
            self.assertEqual(payload["kpis"]["gate_allowed"], 1)
            self.assertEqual(payload["kpis"]["closed_trades"], 1)
            self.assertEqual(payload["kpis"]["realized_net_pnl_rub"], 600.0)
            self.assertEqual(payload["kpis"]["average_r"], 1.2)
            self.assertEqual(payload["positions"][0]["phase"], "PROFIT")
            self.assertTrue(payload["positions"][0]["protected"])
            self.assertEqual(payload["market_map"][0]["regime"], "TREND")
            self.assertEqual(payload["market_map"][0]["path_efficiency"], 66.0)
            self.assertEqual(payload["funnel"][2]["count"], 1)
            self.assertEqual(payload["breakdowns"]["regimes"][0]["key"], "TREND")
            self.assertEqual(payload["meta"]["session"], "MAIN")
            self.assertEqual(payload["data_quality"]["cycle_rows_total"], 1)
            self.assertEqual(payload["data_quality"]["latest_cycle_status"], "success")
            self.assertEqual(payload["meta"]["symbols"], ["AAA", "BBB", "CCC"])

    def test_symbol_filter_applies_to_period_metrics_and_records(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            status = root / "status.json"
            portfolio = root / "portfolio.json"
            candidates = root / "candidates.jsonl"
            events = root / "events.jsonl"
            cycles = root / "cycles.jsonl"
            runtime = root / "runtime.json"
            write_json(status, {"updated_at": "2026-10-08T17:00:00+00:00"})
            write_json(portfolio, {"updated_at": "2026-10-08T17:00:00+00:00", "positions": {}})
            write_jsonl(candidates, [
                {"candidate_id": "1", "symbol": "AAA", "signal_time": "2026-10-08T12:00:00+00:00", "v10_allowed": True, "gate_allowed": True, "candidate": {}},
                {"candidate_id": "2", "symbol": "BBB", "signal_time": "2026-10-08T12:00:00+00:00", "v10_allowed": True, "gate_allowed": True, "candidate": {}},
            ])
            write_jsonl(events, [])
            write_jsonl(cycles, [])
            write_json(runtime, {"session": "CLEARING"})
            payload = build_v35_dashboard_payload(
                days=7,
                symbol="bbb",
                now=datetime(2026, 10, 8, 18, 0, tzinfo=timezone.utc),
                status_path=status,
                portfolio_path=portfolio,
                candidate_path=candidates,
                event_path=events,
                cycle_path=cycles,
                runtime_path=runtime,
            )
            self.assertEqual(payload["kpis"]["candidates"], 1)
            self.assertEqual(payload["candidates"][0]["symbol"], "BBB")
            self.assertEqual(payload["meta"]["selected_symbol"], "BBB")
            self.assertEqual(payload["meta"]["symbols"], ["AAA", "BBB"])
            self.assertEqual(payload["meta"]["session"], "CLEARING")

    def test_page_is_dedicated_to_mechanical_v35_control(self) -> None:
        html = build_v35_dashboard_html()
        self.assertIn("Стратегия v3.5", html)
        self.assertIn("/api/v35", html)
        self.assertIn("Воронка входов", html)
        self.assertIn("Жизненный цикл", html)
        self.assertIn("Голова и плечи", html)
        self.assertIn("отношение прибыли к убытку", html)
        self.assertIn("Гистограмма MACD", html)
        self.assertIn("minmax(300px,1fr)", html)
        self.assertIn("setInterval(refresh,10000)", html)
        self.assertNotIn("profit factor", html)
        self.assertNotIn("MACD Hist", html)
        self.assertNotIn("score ", html)
        self.assertNotIn("RUB", html)
        self.assertNotIn("AI-разбор", html)
        self.assertNotIn("Теневой ИИ", html)


if __name__ == "__main__":
    unittest.main()
