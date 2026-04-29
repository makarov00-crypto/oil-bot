import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import daily_ai_review as review


class DailyAiReviewTests(unittest.TestCase):
    def test_build_review_prompt_for_historical_day_ignores_current_live_state(self) -> None:
        with TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            (base_dir / "logs").mkdir(parents=True, exist_ok=True)
            (base_dir / "bot_state").mkdir(parents=True, exist_ok=True)
            (base_dir / "logs" / "trade_journal.jsonl").write_text(
                '{"time":"2026-04-24T10:00:00+03:00","symbol":"BRK6","event":"OPEN","side":"LONG","qty_lots":1,"price":80.0,"strategy":"trend_rollover","reason":"open"}\n'
                '{"time":"2026-04-24T10:30:00+03:00","symbol":"BRK6","event":"CLOSE","side":"LONG","qty_lots":1,"price":81.0,"pnl_rub":100.0,"net_pnl_rub":100.0,"strategy":"trend_rollover","reason":"close"}\n',
                encoding="utf-8",
            )
            (base_dir / "bot_state" / "_portfolio_snapshot.json").write_text(
                '{"bot_total_pnl_rub":999.0,"open_positions_count":3,"total_portfolio_rub":70000.0}',
                encoding="utf-8",
            )
            (base_dir / "bot_state" / "_news_snapshot.json").write_text(
                '{"active_biases":[{"symbol":"LIVEONLY","bias":"LONG","strength":"HIGH","source":"test","reason":"current"}]}',
                encoding="utf-8",
            )
            (base_dir / "bot_state" / "LIVEONLY.json").write_text(
                '{"position_side":"LONG","position_qty":2,"last_signal":"BUY","last_strategy_name":"momentum_breakout","last_news_bias":"LONG_HIGH","last_signal_summary":["live state"]}',
                encoding="utf-8",
            )
            (base_dir / "bot_state" / "_accounting_history.json").write_text(
                '{"2026-04-24":{"total_pnl_rub":150.0}}',
                encoding="utf-8",
            )

            prompt = review.build_review_prompt(base_dir, date(2026, 4, 24))

        self.assertIn("- итог: 150.00 RUB", prompt)
        self.assertIn("- Открытых позиций нет.", prompt)
        self.assertIn("- Нет сохранённого сигнального среза для выбранной даты.", prompt)
        self.assertIn("- Активных news bias сейчас нет.", prompt)
        self.assertNotIn("LIVEONLY", prompt)

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

    def test_pair_closed_trades_uses_latest_open_and_entry_context(self) -> None:
        rows = [
            {
                "time": "2026-04-24T10:00:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:00:00+03:00"),
                "symbol": "GNM6",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 105.0,
                "strategy": "momentum_breakout",
                "reason": "older open",
                "context": {"market_regime": "old_regime", "setup_quality_label": "weak", "entry_edge_label": "fragile"},
            },
            {
                "time": "2026-04-24T10:05:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:05:00+03:00"),
                "symbol": "GNM6",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 100.0,
                "strategy": "trend_pullback",
                "reason": "newer open",
                "context": {"market_regime": "entry_regime", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "time": "2026-04-24T10:10:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:10:00+03:00"),
                "symbol": "GNM6",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 99.0,
                "pnl_rub": 10.0,
                "strategy": "trend_pullback",
                "reason": "close",
                "context": {"market_regime": "exit_regime", "setup_quality_label": "medium", "entry_edge_label": "confirmed"},
            },
        ]

        trades = review.pair_closed_trades(rows)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].entry_time, "24.04 10:05:00")
        self.assertEqual(trades[0].strategy, "trend_pullback")
        self.assertEqual(trades[0].market_regime, "entry_regime")
        self.assertEqual(trades[0].setup_quality_label, "strong")
        self.assertEqual(trades[0].entry_edge_label, "high")

    def test_pair_closed_trades_consumes_full_close_qty_from_open_stack(self) -> None:
        rows = [
            {
                "time": "2026-04-24T10:00:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:00:00+03:00"),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 3,
                "price": 10.95,
                "strategy": "opening_range_breakout",
                "reason": "open stack",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "time": "2026-04-24T10:30:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:30:00+03:00"),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 3,
                "price": 10.93,
                "pnl_rub": 30.0,
                "strategy": "opening_range_breakout",
                "reason": "close stack",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "time": "2026-04-24T11:00:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T11:00:00+03:00"),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 10.92,
                "strategy": "trend_pullback",
                "reason": "new open",
                "context": {"market_regime": "pullback", "setup_quality_label": "medium", "entry_edge_label": "confirmed"},
            },
            {
                "time": "2026-04-24T11:20:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T11:20:00+03:00"),
                "symbol": "CNYRUBF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 10.91,
                "pnl_rub": 10.0,
                "strategy": "trend_pullback",
                "reason": "close new",
                "context": {"market_regime": "pullback", "setup_quality_label": "medium", "entry_edge_label": "confirmed"},
            },
        ]

        trades = review.pair_closed_trades(rows)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[-1].entry_time, "24.04 11:00:00")
        self.assertEqual(trades[-1].strategy, "trend_pullback")

    def test_pair_closed_trades_sorts_rows_and_skips_duplicate_orphan_close(self) -> None:
        rows = [
            {
                "time": "2026-04-24T10:10:40+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:10:40+03:00"),
                "symbol": "IMOEXF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 2720.0,
                "pnl_rub": -20.0,
                "strategy": "trend_rollover",
                "reason": "duplicate orphan",
            },
            {
                "time": "2026-04-24T10:00:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:00:00+03:00"),
                "symbol": "BRK6",
                "side": "LONG",
                "event": "OPEN",
                "qty_lots": 1,
                "price": 80.0,
                "strategy": "trend_rollover",
                "reason": "open",
                "context": {"market_regime": "trend_expansion", "setup_quality_label": "strong", "entry_edge_label": "high"},
            },
            {
                "time": "2026-04-24T10:10:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:10:00+03:00"),
                "symbol": "IMOEXF",
                "side": "SHORT",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 2720.0,
                "pnl_rub": -20.0,
                "strategy": "trend_rollover",
                "reason": "orphan",
            },
            {
                "time": "2026-04-24T10:30:00+03:00",
                "_dt": review.datetime.fromisoformat("2026-04-24T10:30:00+03:00"),
                "symbol": "BRK6",
                "side": "LONG",
                "event": "CLOSE",
                "qty_lots": 1,
                "price": 81.0,
                "pnl_rub": 100.0,
                "strategy": "trend_rollover",
                "reason": "close",
            },
        ]

        trades = review.pair_closed_trades(rows)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[1].entry_time, "24.04 10:00:00")

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
                    "execution_status": "confirmed_open",
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
        self.assertIn("Какие связки обучение чаще усиливает за последние 3 дня:", prompt)
        self.assertIn("средняя поправка +0.05", prompt)
        self.assertIn("Какие связки обучение чаще режет за последние 3 дня:", prompt)
        self.assertIn("средняя поправка -0.08", prompt)
        self.assertIn("Операционные выводы по learning-наблюдениям:", prompt)
        self.assertIn("Явных операционных действий по learning-данным пока нет", prompt)
        self.assertIn("Слабые связки сигналов за последние 3 дня:", prompt)

    def test_build_prompt_does_not_emit_learning_actions_from_pending_rows(self) -> None:
        signal_rows = [
            {
                "observed_at": "2026-04-24T10:15:00+03:00",
                "symbol": "UCM6",
                "signal": "SHORT",
                "strategy": "opening_range_breakout",
                "decision": "selected",
                "market_regime": "range_chop",
                "setup_quality": "fragile",
                "context": {
                    "entry_edge_label": "fragile",
                    "learning_adjustment": -0.08,
                    "learning_reason": "обучение связки: штраф -0.08",
                },
            },
            {
                "observed_at": "2026-04-24T10:30:00+03:00",
                "symbol": "UCM6",
                "signal": "SHORT",
                "strategy": "opening_range_breakout",
                "decision": "selected",
                "market_regime": "range_chop",
                "setup_quality": "fragile",
                "context": {
                    "entry_edge_label": "fragile",
                    "learning_adjustment": -0.07,
                    "learning_reason": "обучение связки: штраф -0.07",
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

        self.assertIn("Операционные выводы по learning-наблюдениям:", prompt)
        self.assertIn("Явных операционных действий по learning-данным пока нет", prompt)
        self.assertNotIn("Снижать приоритет связки", prompt)

    def test_build_prompt_respects_empty_recent_period_instead_of_falling_back(self) -> None:
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
            )
        ]
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
                "move_pct": 0.8,
                "favorable": True,
                "context": {
                    "entry_edge_label": "high",
                    "learning_adjustment": 0.05,
                    "learning_reason": "обучение связки: бонус +0.05",
                    "execution_status": "confirmed_open",
                },
            }
        ]

        prompt = review.build_prompt(
            target_day=date(2026, 4, 24),
            portfolio={
                "bot_realized_pnl_rub": 150.0,
                "bot_estimated_variation_margin_rub": 0.0,
                "bot_total_pnl_rub": 150.0,
                "open_positions_count": 0,
                "total_portfolio_rub": 50000.0,
            },
            news={"active_biases": []},
            states={},
            closed_trades=trades,
            recent_closed_trades=[],
            signal_observations=signal_rows,
            recent_signal_observations=[],
        )

        self.assertIn("Сильные сочетания за последние 3 дня:", prompt)
        self.assertIn("- Нет устойчиво сильных сочетаний за период.", prompt)
        self.assertIn("Токсичные сочетания за последние 3 дня:", prompt)
        self.assertIn("- Нет устойчиво токсичных сочетаний за период.", prompt)
        self.assertIn("Лучшие связки сигналов за последние 3 дня:", prompt)
        self.assertIn("- Недостаточно проверенных наблюдений.", prompt)
        self.assertIn("Какие связки обучение чаще усиливает за последние 3 дня:", prompt)
        self.assertIn("- Недостаточно learning-наблюдений.", prompt)

    def test_build_prompt_ignores_unexecuted_selected_losses(self) -> None:
        signal_rows = [
            {
                "observed_at": "2026-04-24T10:15:00+03:00",
                "evaluated_at": "2026-04-24T10:30:00+03:00",
                "symbol": "UCM6",
                "signal": "SHORT",
                "strategy": "opening_range_breakout",
                "decision": "selected",
                "market_regime": "range_chop",
                "setup_quality": "fragile",
                "favorable": False,
                "move_pct": -0.7,
                "context": {
                    "entry_edge_label": "fragile",
                    "execution_status": "rejected",
                    "execution_note": "заявка отклонена",
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

        self.assertIn("- выбранные, которые не подтвердились: 0", prompt)


if __name__ == "__main__":
    unittest.main()
