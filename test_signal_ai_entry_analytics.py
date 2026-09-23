import unittest

from signal_ai_entry_analytics import build_signal_ai_entry_analytics


class SignalAIEntryAnalyticsTests(unittest.TestCase):
    def test_ao_chain_dedupes_candidates_and_separates_price_from_closed_net(self):
        def row(strategy, symbol, action, decision, candle, execution, favorable=None):
            context = {
                "candle_time": candle,
                "candidate_id": f"{strategy}:{symbol}:LONG:{candle}",
                "shadow_ai_status": "ready",
                "shadow_ai": {"action": action},
                "execution_status": execution,
            }
            if favorable is not None:
                context["shadow_ai_outcomes"] = {"4h": {
                    "favorable": favorable,
                    "evaluated_at": "2026-09-23T08:00:00+03:00",
                }}
            return {
                "strategy": strategy, "symbol": symbol, "signal": "LONG",
                "observed_at": "2026-09-23T02:00:00+03:00", "decision": decision,
                "context": context,
            }

        enter = row("ao_chaikin_1h", "A", "ВХОД", "selected", "2026-09-23 01:00", "confirmed_open", True)
        duplicate = row("ao_chaikin_1h", "A", "ВХОД", "deferred", "2026-09-23 01:00", "", True)
        abstain = row("ao_chaikin_1h", "B", "ВОЗДЕРЖАТЬСЯ", "selected", "2026-09-23 01:00", "confirmed_open", False)
        old = row("reversal_1h", "C", "ВХОД", "selected", "2026-09-23 01:00", "confirmed_open", True)
        closed = [{"strategy": "ao_chaikin_1h", "symbol": "A", "side": "LONG",
                   "entry_time": "2026-09-23T02:05:00+03:00",
                   "candidate_id": enter["context"]["candidate_id"], "net_pnl_rub": -120.0}]
        result = build_signal_ai_entry_analytics([duplicate, enter, abstain, old], closed)
        counts = result["counts"]
        self.assertEqual((counts["candidates"], counts["reviewed"], counts["confirmed"], counts["closed"]), (2, 2, 2, 1))
        self.assertEqual((counts["price_checked_4h"], counts["price_check_late"]), (2, 2))
        self.assertEqual(result["by_action"]["enter"]["net_pnl_rub"], -120.0)
        self.assertEqual(counts["supported_losers"], 1)
        self.assertEqual(result["by_action"]["abstain"]["closed"], 0)
        self.assertEqual(result["by_action"]["abstain"]["net_pnl_rub"], 0.0)

    def test_partial_close_counts_only_after_final_fill_and_uses_full_net(self):
        candidate_id = "ao_chaikin_1h:A:LONG:2026-09-23 01:00"
        observation = {
            "strategy": "ao_chaikin_1h", "symbol": "A", "signal": "LONG",
            "observed_at": "2026-09-23T02:00:00+03:00", "decision": "selected",
            "context": {"candle_time": "2026-09-23 01:00", "candidate_id": candidate_id,
                        "shadow_ai": {"action": "ВХОД"}, "execution_status": "confirmed_open",
                        "risk_per_contract_rub": 100.0},
        }
        parts = [
            {"strategy": "ao_chaikin_1h", "symbol": "A", "side": "LONG",
             "entry_time": "2026-09-23T02:05:00+03:00", "candidate_id": candidate_id,
             "qty_lots": 1, "fully_closed": False, "net_pnl_rub": 50.0},
            {"strategy": "ao_chaikin_1h", "symbol": "A", "side": "LONG",
             "entry_time": "2026-09-23T02:05:00+03:00", "candidate_id": candidate_id,
             "qty_lots": 1, "fully_closed": True, "net_pnl_rub": -250.0},
        ]
        result = build_signal_ai_entry_analytics([observation], parts)
        self.assertEqual(result["counts"]["closed"], 1)
        self.assertEqual(result["by_action"]["enter"]["net_pnl_rub"], -200.0)
        self.assertEqual(result["by_action"]["enter"]["average_r"], -1.0)


if __name__ == "__main__":
    unittest.main()
