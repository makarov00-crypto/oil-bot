import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import bot_oil_main as mod
from trade_storage import load_trade_rows


class TradeStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.instrument = mod.InstrumentConfig(symbol="TEST", figi="FIGI", display_name="Test")

    def test_append_trade_journal_persists_context_to_sqlite(self) -> None:
        state = mod.InstrumentState(
            last_higher_tf_bias="LONG",
            last_news_bias="LONG_HIGH",
            last_news_impact="сильный трендовый фон",
            last_market_regime="trend_expansion",
            last_setup_quality_label="strong",
            last_setup_quality_score=5,
            last_volume_ratio=1.23,
            last_body_ratio=0.91,
            last_atr_pct=0.0012,
            last_range_width_pct=0.0055,
            last_allocator_quantity=2,
            last_allocator_summary="Аллокатор одобрил вход.",
            last_entry_allocator_quantity=1,
            last_entry_allocator_summary="Последний вход: 1 лот.",
            last_signal_summary=["Сильный сигнал", "Объём подтверждает"],
            execution_status="confirmed_open",
        )

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            log_dir = temp_path / "logs"
            state_dir = temp_path / "bot_state"
            journal_path = log_dir / "trade_journal.jsonl"
            db_path = state_dir / "trade_analytics.sqlite3"
            with unittest.mock.patch.object(mod, "LOG_DIR", log_dir), unittest.mock.patch.object(
                mod, "TRADE_JOURNAL_PATH", journal_path
            ), unittest.mock.patch.object(mod, "TRADE_DB_PATH", db_path):
                mod.append_trade_journal(
                    self.instrument,
                    "OPEN",
                    "LONG",
                    2,
                    101.25,
                    reason="Тестовый вход",
                    source="dry_run",
                    strategy="opening_range_breakout",
                    dry_run=True,
                    state=state,
                )

                rows = mod.load_trade_journal()
                self.assertEqual(len(rows), 1)
                self.assertIn("context", rows[0])
                self.assertEqual(rows[0]["context"]["higher_tf_bias"], "LONG")
                self.assertEqual(rows[0]["context"]["allocator_quantity"], 2)
                self.assertEqual(rows[0]["context"]["market_regime"], "trend_expansion")
                self.assertEqual(rows[0]["context"]["setup_quality_label"], "strong")

                db_rows = load_trade_rows(journal_path, db_path)
                self.assertEqual(len(db_rows), 1)
                self.assertEqual(db_rows[0]["context"]["news_bias"], "LONG_HIGH")
                self.assertEqual(db_rows[0]["context"]["execution_status"], "confirmed_open")
                self.assertEqual(db_rows[0]["context"]["setup_quality_score"], 5)

    def test_save_trade_journal_resyncs_sqlite_after_update(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            log_dir = temp_path / "logs"
            state_dir = temp_path / "bot_state"
            journal_path = log_dir / "trade_journal.jsonl"
            db_path = state_dir / "trade_analytics.sqlite3"
            with unittest.mock.patch.object(mod, "LOG_DIR", log_dir), unittest.mock.patch.object(
                mod, "TRADE_JOURNAL_PATH", journal_path
            ), unittest.mock.patch.object(mod, "TRADE_DB_PATH", db_path):
                mod.append_trade_journal(
                    self.instrument,
                    "CLOSE",
                    "LONG",
                    1,
                    105.0,
                    pnl_rub=12.0,
                    gross_pnl_rub=15.0,
                    commission_rub=3.0,
                    net_pnl_rub=12.0,
                    reason="Тестовое закрытие",
                    source="dry_run",
                    strategy="momentum_breakout",
                    dry_run=True,
                )
                rows = mod.load_trade_journal()
                rows[0]["net_pnl_rub"] = 9.5
                mod.save_trade_journal(rows)

                db_rows = load_trade_rows(journal_path, db_path)
                self.assertEqual(len(db_rows), 1)
                self.assertEqual(float(db_rows[0]["net_pnl_rub"]), 9.5)


if __name__ == "__main__":
    unittest.main()
