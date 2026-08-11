from datetime import datetime, timezone
import unittest
from unittest.mock import patch

import bot_oil_main as bot
from news_bias import NewsBias, NewsMessage, calibrate_news_biases, detect_news_bias, select_active_biases
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

    def test_detects_ozon_rule_and_topics(self) -> None:
        message = NewsMessage(
            channel="marketsnapshot",
            text="Озон представил сильную отчетность: рост оборота, заказов и прибыли.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        ozon = next(item for item in items if item.symbol == "ONU6")

        self.assertEqual(ozon.bias, "LONG")
        self.assertEqual(ozon.category, "ритейл")
        self.assertTrue(ozon.summary.startswith("ONU6:"))
        self.assertTrue(len(ozon.topics) >= 1)
        self.assertGreater(ozon.source_speed, 0.8)
        self.assertGreater(ozon.source_reliability, 0.7)

    def test_detects_lukoil_rule_and_topics(self) -> None:
        message = NewsMessage(
            channel="marketsnapshot",
            text="Лукойл сообщил о сильной отчетности, росте прибыли и дивидендах.",
            created_at=datetime(2026, 5, 12, 10, 15, tzinfo=UTC),
        )

        items = detect_news_bias(message)
        lukoil = next(item for item in items if item.symbol == "LKU6")

        self.assertEqual(lukoil.bias, "LONG")
        self.assertEqual(lukoil.category, "нефть")
        self.assertTrue(lukoil.summary.startswith("LKU6:"))

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
            ("marketsnapshot", "Озон опубликовал сильную отчетность: рост оборота, заказов и прибыли.", "ритейл"),
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

    def test_outcome_calibration_downgrades_weak_single_source_short(self) -> None:
        message = NewsMessage(
            channel="finamalert",
            text="USD/RUB резко ниже, падение доллара и давление на валюту усилились.",
            created_at=datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
        )
        item = next(value for value in detect_news_bias(message) if value.symbol == "USDRUBF")

        calibrated = calibrate_news_biases(
            [item],
            [{"source": "finamalert", "total_count": 26, "favorable_count": 11, "win_rate_pct": 42.3}],
            [{"label": "SHORT", "total_count": 38, "favorable_count": 15, "win_rate_pct": 39.5}],
        )[0]

        self.assertLessEqual(calibrated.calibration_factor, 0.85)
        self.assertEqual(calibrated.actionability, "WATCH")
        self.assertIn("источник 42.3%", calibrated.calibration_reason)
        eligible, reason = bot.news_bias_trade_gate(calibrated)
        self.assertFalse(eligible)
        self.assertIn("точность", reason)

    def test_outcome_calibration_waits_for_sufficient_sample(self) -> None:
        message = NewsMessage(
            channel="finam_invest",
            text="Нефть Brent выше, рост нефти и сильный спрос поддерживают рынок.",
            created_at=datetime(2026, 8, 12, 10, 0, tzinfo=UTC),
        )
        item = next(value for value in detect_news_bias(message) if value.category == "нефть")

        calibrated = calibrate_news_biases(
            [item],
            [{"source": "finam_invest", "total_count": 10, "favorable_count": 6, "win_rate_pct": 60.0}],
            [{"label": "LONG", "total_count": 8, "favorable_count": 5, "win_rate_pct": 62.5}],
        )[0]

        self.assertEqual(calibrated.calibration_factor, 1.0)
        self.assertEqual(calibrated.calibration_reason, "")
        self.assertEqual(calibrated.actionability, item.actionability)


if __name__ == "__main__":
    unittest.main()
