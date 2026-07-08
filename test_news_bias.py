from datetime import datetime, timezone
import unittest
from unittest.mock import patch

import bot_oil_main as bot
from news_bias import NewsBias, NewsMessage, detect_news_bias, select_active_biases
from news_ingest import fetch_web_news_items


UTC = timezone.utc


class NewsBiasTests(unittest.TestCase):
    def test_detects_actionable_block_for_moex_risk_news(self) -> None:
        message = NewsMessage(
            channel="moex_derivatives",
            text="Мосбиржа: повышение гарантийного обеспечения по Brent, изменение параметров риска.",
            created_at=datetime(2026, 5, 12, 9, 0, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        brent = next(item for item in items if item.category == "нефть")

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
        ucny = next(item for item in items if item.symbol == "UCU6")

        self.assertEqual(ucny.bias, "LONG")
        self.assertEqual(ucny.category, "валюта")
        self.assertTrue(ucny.summary.startswith("UCU6:"))
        self.assertTrue(len(ucny.topics) >= 1)
        self.assertGreater(ucny.source_speed, 0.8)
        self.assertGreater(ucny.source_reliability, 0.7)

    def test_fast_telegram_can_make_strong_intraday_news_actionable(self) -> None:
        message = NewsMessage(
            channel="marketsnapshot",
            text="Нефть Brent резко выше, рост нефти, дефицит нефти, сильный спрос.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        brent = next(item for item in items if item.category == "нефть")

        self.assertEqual(brent.bias, "LONG")
        self.assertEqual(brent.strength, "HIGH")
        self.assertEqual(brent.actionability, "ACTION")
        self.assertEqual(brent.source_type, "telegram")

    def test_detects_finam_alert_as_broker_telegram_source(self) -> None:
        message = NewsMessage(
            channel="finamalert",
            text="USD/RUB выше, рост доллара и спрос на валюту усилились.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        usdrub = next(item for item in items if item.symbol == "USDRUBF")

        self.assertEqual(usdrub.bias, "LONG")
        self.assertEqual(usdrub.source_type, "broker_telegram")
        self.assertEqual(usdrub.source_label, "Финам Alert")
        self.assertGreaterEqual(usdrub.source_speed, 0.9)

    def test_detects_finam_invest_as_broker_telegram_source(self) -> None:
        message = NewsMessage(
            channel="finam_invest",
            text="Нефть Brent выше, рост нефти и сильный спрос поддерживают рынок.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        brent = next(item for item in items if item.category == "нефть")

        self.assertEqual(brent.bias, "LONG")
        self.assertEqual(brent.source_type, "broker_telegram")
        self.assertEqual(brent.source_label, "Финам Invest")
        self.assertGreaterEqual(brent.source_reliability, 0.9)

    def test_detects_bcs_express_telegram_as_broker_telegram_source(self) -> None:
        message = NewsMessage(
            channel="bcs_express_tg",
            text="Акции Сбера под давлением, негатив по рынку и банковский сектор снижается.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        sber = next(item for item in items if item.category == "банки")

        self.assertEqual(sber.bias, "SHORT")
        self.assertEqual(sber.source_type, "broker_telegram")
        self.assertEqual(sber.source_label, "БКС Экспресс")
        self.assertGreaterEqual(sber.source_speed, 0.88)

    def test_detects_t_invest_telegram_as_broker_telegram_source(self) -> None:
        message = NewsMessage(
            channel="tb_invest_official",
            text="Индекс Мосбиржи растёт, российский рынок акций получил позитивный импульс.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        index = next(item for item in items if item.category == "индекс")

        self.assertEqual(index.bias, "LONG")
        self.assertEqual(index.source_type, "broker_telegram")
        self.assertEqual(index.source_label, "Т-Инвестиции Official")
        self.assertGreaterEqual(index.source_reliability, 0.88)

    def test_human_readable_market_terms_map_to_traded_symbols(self) -> None:
        cases = [
            ("marketsnapshot", "Баррель нефти выше после сокращения добычи ОПЕК+.", "нефть"),
            ("marketsnapshot", "Ключевая ставка давит на долговой рынок и ОФЗ.", "облигации"),
            ("bcs_express_tg", "Банковский сектор под давлением, акции Сбера снижаются.", "банки"),
            ("finam_invest", "Газовый рынок растёт, запасы газа снижаются.", "газ"),
        ]

        for channel, text, category in cases:
            with self.subTest(category=category):
                items = detect_news_bias(
                    NewsMessage(
                        channel=channel,
                        text=text,
                        created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
                    )
                )
                self.assertTrue(any(item.category == category for item in items))

    def test_same_direction_from_broker_and_telegram_is_merged(self) -> None:
        created_at = datetime(2026, 5, 12, 10, 15, tzinfo=UTC)
        items = []
        for channel in ("marketsnapshot", "finam"):
            items.extend(
                detect_news_bias(
                    NewsMessage(
                        channel=channel,
                        text="USD/RUB выше, рост доллара и спрос на валюту усилились.",
                        created_at=created_at,
                    )
                )
            )

        active = select_active_biases(items, now=datetime(2026, 5, 12, 10, 20, tzinfo=UTC))
        usdrub = active["USDRUBF"]

        self.assertEqual(usdrub.bias, "LONG")
        self.assertGreaterEqual(usdrub.source_count, 2)
        self.assertIn("MarketSnapshot", usdrub.confirming_sources)
        self.assertIn("Финам", usdrub.confirming_sources)
        self.assertIn("Подтверждено источниками", usdrub.reason)

    def test_fetch_web_news_items_extracts_broker_headlines(self) -> None:
        class FakeResponse:
            text = """
                <html><body>
                  <a href="/news/1">Рост доллара усилился после новых валютных комментариев</a>
                  <a href="/news/1">Рост доллара усилился после новых валютных комментариев</a>
                  <a href="/news/2">читать далее</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        with patch("news_ingest.requests.get", return_value=FakeResponse()):
            items = fetch_web_news_items("finam", limit=10)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source, "finam")
        self.assertTrue(items[0].url.endswith("/news/1"))
        self.assertIn("Рост доллара", items[0].title)

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
        self.assertIn("не влияют на сделку", reason)
        self.assertIn("новость только фоновая", reason)

    def test_gazprom_equity_news_does_not_trigger_natural_gas(self) -> None:
        message = NewsMessage(
            channel="finamalert",
            text=(
                "Изменилась фигура тренда #Газпром с нисходящий клин на расходящийся клин "
                "с наклоном вниз. Настроение инвесторов изменилось на бычье."
            ),
            created_at=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
        )

        biases = detect_news_bias(message)

        self.assertFalse(any(item.category == "газ" for item in biases))


if __name__ == "__main__":
    unittest.main()
