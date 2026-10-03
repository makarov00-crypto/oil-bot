import json
import unittest

from strategy_ai_guard import EntryReview, RegimeReview, build_entry_prompt, build_regime_prompt


class StrategyAiGuardTests(unittest.TestCase):
    def test_regime_prompt_keeps_full_timeframe_history(self) -> None:
        context = {
            "symbol": "BRX6",
            "timeframes": {
                "1h": [{"t": "a", "ao": 1}, {"t": "b", "ao": 2}],
                "4h": [{"t": "a", "ao": -1}],
                "30m": [{"t": "b", "ao": 3}],
            },
        }
        prompt = build_regime_prompt(context)
        payload = json.loads(prompt.split("\n\n", 1)[1])
        self.assertEqual(len(payload["timeframes"]["1h"]), 2)
        self.assertIn("4h", payload["timeframes"])

    def test_entry_prompt_has_independent_regime_and_strategy_context(self) -> None:
        regime = RegimeReview(
            regime="CHOP", structure_4h="MIXED", swing_1h="RANGE", phase_30m="RANGE", texture="CHOP",
            regime_confidence_pct=81, trend_maturity="NONE", chop_probability_pct=88,
            evidence=["частые развороты", "нет продвижения"], data_quality="COMPLETE",
        )
        entry = {"strategy": "ao-candle-mtf-replay-v1", "candidate": {"direction": "LONG"}, "timeframes": {"1h": []}}
        prompt = build_entry_prompt(entry, regime)
        payload = json.loads(prompt.split("\n\n", 1)[1])
        self.assertEqual(payload["regime_review"]["regime"], "CHOP")
        self.assertEqual(payload["entry_context"]["candidate"]["direction"], "LONG")
        self.assertNotIn("entry_edge_score", prompt)

    def test_review_contract_uses_percent_scale(self) -> None:
        review = EntryReview(
            candidate_direction="SHORT", entry_score_pct=73, decision="ENTER",
            expected_outcome="PROFIT", setup_phase="ON_TIME", late_entry_risk_pct=21,
            timeframe_alignment="ALIGNED", evidence=["свежая волна", "свечи расширяются"],
            invalidation=["AO начнёт расти"], data_quality="COMPLETE",
        )
        self.assertEqual(review.as_dict()["entry_score_pct"], 73)
        self.assertEqual(review.as_dict()["decision"], "ENTER")


if __name__ == "__main__":
    unittest.main()
