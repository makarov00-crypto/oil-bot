import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bot_oil_main as bot
import signal_ai_reviewer as reviewer


class SignalAiReviewerTests(unittest.TestCase):
    def test_build_prompt_keeps_only_structured_candidate_context(self) -> None:
        prompt = reviewer.build_signal_ai_prompt([
            {"symbol": "BRU6", "signal": "LONG", "strategy_name": "reversal_1h", "reason": "MACD вверх", "shadow_ai_context": {"volume_ratio": 1.4}}
        ])
        self.assertIn("BRU6", prompt)
        self.assertIn("volume_ratio", prompt)

    def test_parses_strict_review(self) -> None:
        payload = {"reviews": [{"symbol": "BRU6", "action": "ВХОД", "direction": "ЛОНГ", "confidence": 0.86, "reason": "импульс", "risk_note": "стоп"}]}
        result = reviewer.parse_signal_ai_reviews(payload)
        self.assertEqual(result["BRU6"].action, "ВХОД")
        self.assertAlmostEqual(result["BRU6"].confidence, 0.86)

    @patch.dict(os.environ, {"OIL_AI_API_MODE": "chat_completions", "OIL_AI_MODEL": "test-model"}, clear=False)
    @patch("signal_ai_reviewer.requests.post")
    def test_request_uses_json_schema(self, post) -> None:
        post.return_value.json.return_value = {"choices": [{"message": {"content": json.dumps({"reviews": []})}}]}
        post.return_value.raise_for_status.return_value = None
        result = reviewer.request_signal_ai_reviews("key", [{"symbol": "BRU6", "signal": "LONG"}])
        self.assertEqual(result, {})
        self.assertEqual(post.call_args.kwargs["json"]["response_format"]["type"], "json_schema")

    @patch.dict(os.environ, {"OIL_SIGNAL_AI_SHADOW_ENABLED": "1", "OPENAI_API_KEY": "key"}, clear=False)
    def test_shadow_review_is_logged_without_changing_candidate_decision(self) -> None:
        candidate = {"symbol": "BRU6", "signal": "LONG", "strategy_name": "reversal_1h", "candle_time": "2026-08-07 12:00"}
        review = reviewer.SignalAiReview("BRU6", "ВХОД", "ЛОНГ", 0.8, "импульс", "стоп")
        state = bot.InstrumentState()
        with tempfile.TemporaryDirectory() as directory, patch.object(bot, "SIGNAL_AI_SHADOW_PATH", Path(directory) / "shadow.jsonl"), patch.object(bot, "request_signal_ai_reviews", return_value={"BRU6": review}), patch.object(bot, "load_state", return_value=state), patch.object(bot, "save_state") as save:
            bot.apply_signal_ai_shadow_reviews([candidate])

        self.assertEqual(candidate["shadow_ai"]["action"], "ВХОД")
        self.assertEqual(state.last_shadow_ai_action, "ВХОД")
        save.assert_called_once_with("BRU6", state)


if __name__ == "__main__":
    unittest.main()
