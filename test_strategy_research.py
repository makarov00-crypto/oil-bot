import unittest

from strategy_research import build_ai_research, build_ao_execution_research


def observation(strategy, action, favorable, *, decision="selected", status="confirmed_open", time="2026-09-22T23:00:00+03:00"):
    return {
        "symbol": "BRV6", "signal": "LONG", "strategy": strategy,
        "observed_at": time, "decision": decision,
        "context": {
            "candle_time": time[:16], "execution_status": status,
            "shadow_ai": {"action": action},
            "shadow_ai_outcomes": {"4h": {"favorable": favorable, "move_pct": 0.5 if favorable else -0.5}},
        },
    }


class StrategyResearchTests(unittest.TestCase):
    def test_ai_results_are_split_by_strategy_and_selected_candidate_is_not_double_counted(self):
        old = observation("reversal_1h", "ВХОД", False, time="2026-09-21T10:00:00+03:00")
        ao = observation("ao_chaikin_1h", "ВОЗДЕРЖАТЬСЯ", False)
        deferred_copy = {**ao, "decision": "deferred"}
        groups = build_ai_research([old, deferred_copy, ao])["by_strategy"]
        self.assertEqual(groups["all"]["evaluated"], 2)
        self.assertEqual(groups["reversal_1h"]["enter_correct"], 0)
        self.assertEqual(groups["ao_chaikin_1h"]["abstain_correct"], 1)

    def test_execution_counts_only_post_rollout_ao_and_verified_open(self):
        old = observation("reversal_1h", "ВХОД", True)
        before = observation("ao_chaikin_1h", "ВХОД", True, time="2026-09-22T21:00:00+03:00")
        confirmed = observation("ao_chaikin_1h", "ВХОД", True)
        deferred = observation("ao_chaikin_1h", "ВХОД", True, decision="deferred", status="")
        deferred["symbol"] = "NGU6"
        result = build_ao_execution_research([old, before, confirmed, deferred])
        self.assertEqual((result["candidates"], result["selected"], result["confirmed"], result["deferred"]), (2, 1, 1, 1))


if __name__ == "__main__":
    unittest.main()
