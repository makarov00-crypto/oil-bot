import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as bot
import signal_ai_reviewer as reviewer
from trade_storage import append_signal_observation, load_signal_observations


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
        self.assertEqual(save.call_count, 2)
        save.assert_called_with("BRU6", state)

    @patch.dict(os.environ, {"OIL_SIGNAL_AI_SHADOW_ENABLED": "1", "OPENAI_API_KEY": "key"}, clear=False)
    def test_shadow_timeout_is_recorded_and_does_not_keep_previous_verdict(self) -> None:
        candidate = {"symbol": "GLU6", "signal": "LONG", "strategy_name": "reversal_1h", "candle_time": "2026-08-14 16:00"}
        state = bot.InstrumentState(last_shadow_ai_action="ВОЗДЕРЖАТЬСЯ", last_shadow_ai_reason="старый ответ")
        with tempfile.TemporaryDirectory() as directory, patch.object(bot, "SIGNAL_AI_SHADOW_PATH", Path(directory) / "shadow.jsonl"), patch.object(
            bot, "request_signal_ai_reviews", side_effect=TimeoutError("тайм-аут")
        ), patch.object(bot, "load_state", return_value=state), patch.object(bot, "save_state"):
            bot.apply_signal_ai_shadow_reviews([candidate])
            logged = (Path(directory) / "shadow.jsonl").read_text(encoding="utf-8")

        self.assertEqual(candidate["shadow_ai_status"], "unavailable")
        self.assertEqual(state.last_shadow_ai_action, "")
        self.assertEqual(state.last_shadow_ai_status, "unavailable")
        self.assertIn("тайм-аут", logged)

    def test_shadow_outcomes_are_saved_for_each_due_hour(self) -> None:
        observed_at = "2026-08-01T10:00:00+03:00"
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "trade.sqlite3"
            uid = append_signal_observation(
                db_path,
                {
                    "observed_at": observed_at,
                    "observation_key": "2026-08-01 10:00",
                    "symbol": "BRU6",
                    "signal": "LONG",
                    "strategy": "reversal_1h",
                    "decision": "selected",
                    "observed_price": 100.0,
                    "horizon_minutes": 60,
                    "context": {"shadow_ai": {"action": "ВХОД"}},
                },
            )
            instrument = bot.InstrumentConfig(symbol="BRU6", figi="FIGI", display_name="Brent")
            evaluated_at = bot.datetime.fromisoformat("2026-08-01T18:05:00+03:00")
            with patch.object(bot, "TRADE_DB_PATH", db_path), patch.object(
                bot,
                "get_hourly_horizon_price",
                return_value=(101.0, evaluated_at, "hourly_close"),
            ):
                updated = bot.update_signal_ai_shadow_outcomes(None, SimpleNamespace(), [instrument])

            row = load_signal_observations(db_path)[0]
            outcomes = row["context"]["shadow_ai_outcomes"]
            self.assertEqual(uid, row["observation_uid"])
            self.assertEqual(updated, 1)
            self.assertEqual(set(outcomes), {"1h", "2h", "4h", "8h"})
            self.assertTrue(outcomes["4h"]["favorable"])

    def test_shadow_outcomes_save_hourly_fallback_source(self) -> None:
        observed_at = "2026-08-01T10:00:00+03:00"
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "trade.sqlite3"
            append_signal_observation(
                db_path,
                {
                    "observed_at": observed_at,
                    "observation_key": "2026-08-01 10:00",
                    "symbol": "BRU6",
                    "signal": "LONG",
                    "strategy": "reversal_1h",
                    "decision": "selected",
                    "observed_price": 100.0,
                    "context": {"shadow_ai": {"action": "ВХОД"}},
                },
            )
            instrument = bot.InstrumentConfig(symbol="BRU6", figi="FIGI", display_name="Brent")
            evaluated_at = bot.datetime.fromisoformat("2026-08-01T15:00:00+03:00")
            with patch.object(bot, "TRADE_DB_PATH", db_path), patch.object(
                bot, "get_hourly_horizon_price", return_value=(101.0, evaluated_at, "hourly_close")
            ):
                bot.update_signal_ai_shadow_outcomes(None, SimpleNamespace(), [instrument])

            outcomes = load_signal_observations(db_path)[0]["context"]["shadow_ai_outcomes"]
            self.assertEqual(outcomes["4h"]["price_source"], "hourly_close")

    def test_hourly_horizon_uses_candle_that_ends_at_target(self) -> None:
        target = datetime(2026, 8, 1, 12, 0, 18, tzinfo=timezone.utc)
        candle = SimpleNamespace(
            time=datetime(2026, 8, 1, 11, 0, tzinfo=timezone.utc),
            close=SimpleNamespace(units=101, nano=0),
            is_complete=True,
        )
        client = SimpleNamespace(market_data=SimpleNamespace(get_candles=lambda **kwargs: SimpleNamespace(candles=[candle])))
        instrument = bot.InstrumentConfig(symbol="BRU6", figi="FIGI", display_name="Brent")

        result = bot.get_hourly_horizon_price(client, SimpleNamespace(), instrument, target)

        self.assertIsNotNone(result)
        price, evaluated_at, source = result
        self.assertEqual(price, 101.0)
        self.assertEqual(evaluated_at.astimezone(timezone.utc), target.replace(second=0))
        self.assertEqual(source, "hourly_close")


if __name__ == "__main__":
    unittest.main()
