from datetime import datetime, timezone
import unittest

import bot_oil_main as bot
from news_bias import NewsBias, NewsMessage, detect_news_bias


UTC = timezone.utc


class NewsBiasTests(unittest.TestCase):
    def test_detects_actionable_block_for_moex_risk_news(self) -> None:
        message = NewsMessage(
            channel="moex_derivatives",
            text="Мосбиржа: повышение гарантийного обеспечения по Brent, изменение параметров риска.",
            created_at=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        brent = next(item for item in items if item.symbol == "BMM6")

        self.assertEqual(brent.bias, "BLOCK")
        self.assertEqual(brent.horizon, "NOW")
        self.assertEqual(brent.actionability, "BLOCK")
        self.assertEqual(brent.category, "нефть")

    def test_detects_ucny_rule_and_topics(self) -> None:
        message = NewsMessage(
            channel="marketsnapshot",
            text="USD/CNY выше после давления на CNY и слабого юаня на рынке Китая.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        ucny = next(item for item in items if item.symbol == "UCM6")

        self.assertEqual(ucny.bias, "LONG")
        self.assertEqual(ucny.category, "валюта")
        self.assertTrue(ucny.summary.startswith("UCM6:"))
        self.assertTrue(len(ucny.topics) >= 1)

    def test_weak_background_conflict_does_not_block_signal(self) -> None:
        bias = NewsBias(
            symbol="USDRUBF",
            category="валюта",
            bias="SHORT",
            strength="LOW",
            source="markettwits",
            reason="Фоновая слабость доллара.",
            summary="USDRUBF: фон в шорт",
            horizon="BACKGROUND",
            actionability="BACKGROUND",
            expires_at=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
            score=1,
        )

        signal, reason = bot.apply_news_bias_to_signal("LONG", "Базовый long", bias)

        self.assertEqual(signal, "LONG")
        self.assertIn("это пока лишь фон", reason)


if __name__ == "__main__":
    unittest.main()
