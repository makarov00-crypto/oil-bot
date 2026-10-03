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
        regime = RegimeReview("CHOP", 81, "NONE", 88, ["частые развороты", "нет продвижения"], "COMPLETE")
        entry = {"strategy": "ao-candle-mtf-replay-v1", "candidate": {"direction": "LONG"}, "timeframes": {"1h": []}}
        prompt = build_entry_prompt(entry, regime)
        payload = json.loads(prompt.split("\n\n", 1)[1])
        self.assertEqual(payload["regime_review"]["regime"], "CHOP")
        self.assertEqual(payload["entry_context"]["candidate"]["direction"], "LONG")
        self.assertNotIn("entry_edge_score", prompt)

    def test_review_contract_uses_percent_scale(self) -> None:
        review = EntryReview("SHORT", 73, "ALLOW", 21, "ALIGNED", ["свежая волна", "свечи расширяются"], ["AO начнёт расти"], "COMPLETE")
        self.assertEqual(review.as_dict()["entry_score_pct"], 73)


if __name__ == "__main__":
    unittest.main()
