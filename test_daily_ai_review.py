import unittest
from datetime import date

import daily_ai_review as review


class DailyAiReviewTests(unittest.TestCase):
    def test_summarize_closed_trades_tracks_regimes_and_setup_quality(self) -> None:
        trades = [
            review.ClosedTrade(
                symbol="NGJ6",
                side="LONG",
                strategy="trend_pullback",
                entry_time="22.04 10:00:00",
                exit_time="22.04 10:30:00",
                entry_price=2.1,
                exit_price=2.2,
                pnl_rub=150.0,
                entry_reason="open",
                exit_reason="close",
                market_regime="trend_expansion",
                setup_quality_label="strong",
                entry_edge_label="high",
            ),
            review.ClosedTrade(
                symbol="NGJ6",
                side="LONG",
                strategy="trend_pullback",
                entry_time="22.04 11:00:00",
                exit_time="22.04 11:20:00",
                entry_price=2.2,
                exit_price=2.18,
                pnl_rub=-50.0,
                entry_reason="open",
                exit_reason="close",
                market_regime="chop",
                setup_quality_label="weak",
                entry_edge_label="fragile",
            ),
        ]

        summary = review.summarize_closed_trades(trades)

        self.assertEqual(summary["by_regime"]["trend_expansion"], 150.0)
        self.assertEqual(summary["by_regime"]["chop"], -50.0)
        self.assertEqual(summary["by_setup_quality"]["strong"], 150.0)
        self.assertEqual(summary["by_setup_quality"]["weak"], -50.0)
        self.assertEqual(summary["by_edge"]["high"], 150.0)
        self.assertEqual(summary["by_edge"]["fragile"], -50.0)
        self.assertEqual(summary["by_strategy_regime"]["trend_pullback @ trend_expansion"], 150.0)
        self.assertEqual(summary["by_strategy_regime"]["trend_pullback @ chop"], -50.0)
        self.assertEqual(summary["best_regime"]["name"], "trend_expansion")
        self.assertEqual(summary["worst_regime"]["name"], "chop")
        self.assertEqual(summary["best_setup_quality"]["name"], "strong")
        self.assertEqual(summary["worst_setup_quality"]["name"], "weak")
        self.assertEqual(summary["best_edge"]["name"], "high")
        self.assertEqual(summary["worst_edge"]["name"], "fragile")
        self.assertEqual(summary["best_strategy_regime"]["name"], "trend_pullback @ trend_expansion")
        self.assertEqual(summary["worst_strategy_regime"]["name"], "trend_pullback @ chop")
        self.assertEqual(summary["top_positive_strategy_regimes"][0]["name"], "trend_pullback @ trend_expansion")
        self.assertEqual(summary["top_negative_strategy_regimes"][0]["name"], "trend_pullback @ chop")

    def test_build_prompt_includes_regime_focus_and_setup_quality_sections(self) -> None:
        trades = [
            review.ClosedTrade(
                symbol="CNYRUBF",
                side="SHORT",
                strategy="opening_range_breakout",
                entry_time="22.04 09:00:00",
                exit_time="22.04 09:15:00",
                entry_price=11.17,
                exit_price=11.14,
                pnl_rub=120.0,
                entry_reason="open",
                exit_reason="profit_lock",
                market_regime="trend_expansion",
                setup_quality_label="strong",
                entry_edge_label="high",
            )
        ]

        prompt = review.build_prompt(
            target_day=date(2026, 4, 22),
            portfolio={
                "bot_realized_pnl_rub": 120.0,
                "bot_estimated_variation_margin_rub": 0.0,
                "bot_total_pnl_rub": 120.0,
                "open_positions_count": 0,
                "total_portfolio_rub": 50000.0,
            },
            news={"active_biases": []},
            states={
                "CNYRUBF": {
                    "position_side": "FLAT",
                    "last_signal": "SELL",
                    "last_strategy_name": "opening_range_breakout",
                    "last_higher_tf_bias": "SHORT",
                    "last_news_bias": "NEUTRAL",
                    "last_signal_summary": ["signal ok"],
                }
            },
            closed_trades=trades,
            recent_closed_trades=trades,
        )

        self.assertIn("Фокусные точки результата:", prompt)
        self.assertIn("- лучший режим: trend_expansion (120.00 RUB)", prompt)
        self.assertIn("- лучшее качество сетапа: strong (120.00 RUB)", prompt)
        self.assertIn("- лучший edge: high (120.00 RUB)", prompt)
        self.assertIn("- лучшая связка стратегия/режим: opening_range_breakout @ trend_expansion (120.00 RUB)", prompt)
        self.assertIn("Итог по качеству сетапов:", prompt)
        self.assertIn("- strong: 120.00 RUB", prompt)
        self.assertIn("Итог по edge:", prompt)
        self.assertIn("- high: 120.00 RUB", prompt)
        self.assertIn("Итог по сочетаниям стратегия/режим:", prompt)
        self.assertIn("- opening_range_breakout @ trend_expansion: 120.00 RUB", prompt)
        self.assertIn("Сильные сочетания за последние 3 дня:", prompt)
        self.assertIn("Токсичные сочетания за последние 3 дня:", prompt)

    def test_build_prompt_includes_signal_observation_learning_sections(self) -> None:
        signal_rows = [
            {
                "observed_at": "2026-04-24T10:15:00+03:00",
                "evaluated_at": "2026-04-24T10:30:00+03:00",
                "symbol": "BRK6",
                "signal": "LONG",
                "strategy": "trend_rollover",
                "decision": "selected",
                "market_regime": "trend_expansion",
                "setup_quality": "strong",
                "move_pct": -1.6,
                "favorable": False,
                "context": {
                    "entry_edge_label": "high",
                    "learning_adjustment": -0.08,
                    "learning_reason": "обучение связки: штраф -0.08, 0% подтверждений, 10 наблюд.",
                },
            },
            {
                "observed_at": "2026-04-24T11:15:00+03:00",
                "evaluated_at": "2026-04-24T11:30:00+03:00",
                "symbol": "UCM6",
                "signal": "SHORT",
                "strategy": "opening_range_breakout",
                "decision": "deferred",
                "market_regime": "trend_expansion",
                "setup_quality": "strong",
                "move_pct": 0.7,
                "favorable": True,
                "context": {
                    "entry_edge_label": "confirmed",
                    "learning_adjustment": 0.05,
                    "learning_reason": "обучение связки: бонус +0.05, 67% подтверждений, 6 наблюд.",
                },
            },
        ]

        prompt = review.build_prompt(
            target_day=date(2026, 4, 24),
            portfolio={
                "bot_realized_pnl_rub": 0.0,
                "bot_estimated_variation_margin_rub": 0.0,
                "bot_total_pnl_rub": 0.0,
                "open_positions_count": 0,
                "total_portfolio_rub": 50000.0,
            },
            news={"active_biases": []},
            states={},
            closed_trades=[],
            recent_closed_trades=[],
            signal_observations=signal_rows,
            recent_signal_observations=signal_rows,
        )

        self.assertIn("Наблюдения сигналов за день:", prompt)
        self.assertIn("- всего наблюдений: 2", prompt)
        self.assertIn("- подтвердились: 1 (50.0%)", prompt)
        self.assertIn("- отложенные, которые подтвердились: 1", prompt)
        self.assertIn("- выбранные, которые не подтвердились: 1", prompt)
        self.assertIn("- learning-бонусов: 1, learning-штрафов: 1", prompt)
        self.assertIn("Лучшие связки сигналов за день:", prompt)
        self.assertIn("UCM6 | SHORT | opening_range_breakout", prompt)
        self.assertIn("Слабые связки сигналов за день:", prompt)
        self.assertIn("BRK6 | LONG | trend_rollover", prompt)
        self.assertIn("Где обучение повышало приоритет за день:", prompt)
        self.assertIn("бонус +0.05", prompt)
        self.assertIn("Где обучение понижало приоритет за день:", prompt)
        self.assertIn("штраф -0.08", prompt)
        self.assertIn("Слабые связки сигналов за последние 3 дня:", prompt)


if __name__ == "__main__":
    unittest.main()
