import unittest
from unittest.mock import patch

import bot_oil_main as mod


class TelegramReportsTest(unittest.TestCase):
    def test_hourly_summary_is_compact_and_has_section_totals(self) -> None:
        instrument = mod.InstrumentConfig(symbol="BMQ6", figi="figi1", display_name="Brent")
        portfolio = {
            "generated_at_moscow": "06.07 12:00:00 МСК",
            "bot_realized_pnl_rub": 150.0,
            "bot_estimated_variation_margin_rub": 25.0,
            "bot_total_pnl_rub": 175.0,
            "total_portfolio_rub": 10000.0,
            "free_rub": 5000.0,
            "blocked_guarantee_rub": 1000.0,
            "broker_open_positions": [
                {
                    "symbol": "BMQ6",
                    "side": "LONG",
                    "qty": 2,
                    "entry_price": 71.6,
                    "variation_margin_rub": 25.0,
                }
            ],
        }
        closed_reviews = [
            {
                "symbol": "BMQ6",
                "side": "LONG",
                "strategy": "reversal_1h",
                "net_pnl_rub": 150.0,
                "exit_reason": "MACD развернулся вниз",
                "exit_time": "2026-07-06T11:00:00+03:00",
            }
        ]
        current_open = {"BMQ6": {"strategy": "reversal_1h"}}
        state = mod.InstrumentState(entry_strategy="reversal_1h")

        with (
            patch.object(mod, "build_portfolio_snapshot_payload", return_value=portfolio),
            patch.object(mod, "get_today_trade_journal_rows", return_value=[]),
            patch.object(mod, "pair_trade_journal_rows", return_value=(closed_reviews, current_open)),
            patch.object(mod, "get_market_session", return_value="MAIN"),
            patch.object(mod, "load_state", return_value=state),
        ):
            text = mod.build_hourly_summary_message(client=None, config=object(), watchlist=[instrument])

        self.assertIn("📕 Сделки", text)
        self.assertIn("📂 Открытые позиции", text)
        self.assertIn("📌 Итог сделок", text)
        self.assertIn("📌 Итог открытых", text)
        self.assertIn("• Суммарная ВМ: 🟢 +25.00 RUB", text)
        self.assertIn("🔺 BMQ6 | LONG | 2 лот. | вход 71.6000 | ВМ 🟢 +25.00 RUB", text)
        self.assertIn("💰 NET закрытых: 🟢 +150.00 RUB", text)
        self.assertNotIn("Диагностика", text)

    def test_signal_change_notification_disabled(self) -> None:
        instrument = mod.InstrumentConfig(symbol="BMQ6", figi="figi1", display_name="Brent")
        state = mod.InstrumentState(last_signal="HOLD")
        config = type("Config", (), {"dry_run": False, "tg_token": "x", "tg_chat_id": "y"})()

        with patch.object(mod, "send_msg") as send_msg:
            mod.notify_signal_change(config, instrument, state, "LONG", 71.5, "reason")

        send_msg.assert_not_called()
