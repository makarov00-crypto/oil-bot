import json
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import bot_oil_main as bot
from news_ai_analyzer import request_news_ai_signals
from news_bias import NewsBias


UTC = timezone.utc


def sample_bias() -> NewsBias:
    return NewsBias(
        symbol="USDRUBF",
        category="валюта",
        bias="LONG",
        strength="MEDIUM",
        source="marketsnapshot",
        reason="Позитивный контекст: рост доллара.",
        summary="USDRUBF: фон в лонг",
        horizon="INTRADAY",
        actionability="WATCH",
        expires_at=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
        score=3.0,
        message_text="USD/RUB выше, рост доллара и спрос на валюту.",
        source_speed=0.92,
        source_reliability=0.82,
        source_type="telegram",
        source_label="MarketSnapshot",
        confirming_sources=("MarketSnapshot",),
    )


class NewsAiAnalyzerTests(unittest.TestCase):
    def test_request_news_ai_signals_uses_structured_outputs(self) -> None:
        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "output_text": json.dumps(
                        {
                            "signals": [
                                {
                                    "symbol": "USDRUBF",
                                    "direction": "LONG",
                                    "strength": "HIGH",
                                    "confidence": 0.82,
                                    "horizon": "INTRADAY",
                                    "event_type": "валютный импульс",
                                    "reason": "новость подтверждает рост доллара",
                                    "risk": "может быть краткосрочный шум",
                                }
                            ]
                        }
                    )
                }

        with patch("news_ai_analyzer.requests.post", return_value=FakeResponse()) as mocked_post:
            signals = request_news_ai_signals("test-key", "gpt-5-mini", [sample_bias()])

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].symbol, "USDRUBF")
        self.assertEqual(signals[0].direction, "LONG")
        payload = mocked_post.call_args.kwargs["json"]
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")
        self.assertTrue(payload["text"]["format"]["strict"])

    def test_ai_confirmation_boosts_matching_news_bias(self) -> None:
        bias = sample_bias()
        ai_signal = bot.NewsAiSignal(
            symbol="USDRUBF",
            direction="LONG",
            strength="HIGH",
            confidence=0.8,
            horizon="INTRADAY",
            event_type="валютный импульс",
            reason="AI видит подтверждение",
            risk="шум",
        )

        enriched = bot.apply_ai_signal_to_news_bias(bias, ai_signal)

        self.assertEqual(enriched.ai_direction, "LONG")
        self.assertEqual(enriched.strength, "HIGH")
        self.assertEqual(enriched.actionability, "ACTION")
        self.assertGreater(enriched.score, bias.score)
        self.assertIn("AI подтвердил", enriched.reason)

    def test_enrich_news_biases_is_disabled_by_default(self) -> None:
        old_enabled = os.environ.pop("OIL_NEWS_AI_ENABLED", None)
        try:
            active = {"USDRUBF": sample_bias()}
            self.assertIs(bot.enrich_news_biases_with_ai(active), active)
        finally:
            if old_enabled is not None:
                os.environ["OIL_NEWS_AI_ENABLED"] = old_enabled


if __name__ == "__main__":
    unittest.main()
