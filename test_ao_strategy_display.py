import unittest
from unittest.mock import patch

import web_dashboard as dashboard


class AoStrategyDisplayTests(unittest.TestCase):
    def test_strategy_docs_explain_restored_reversal_and_position_ownership(self):
        html = dashboard.build_docs_html()
        self.assertIn('Как работает `reversal_1h`', html)
        self.assertIn('Все новые входы текущих инструментов выполняются по `reversal_1h`', html)
        self.assertIn('Основной триггер — новое значимое пересечение MACD', html)
        self.assertIn('Уже открытая позиция `ao_chaikin_1h`', html)
        self.assertIn('сохраняют стратегию-владельца', html)
        self.assertNotIn('Все новые входы текущих инструментов выполняются по `ao_chaikin_1h`', html)

    def test_strategy_map_uses_primary_registry_for_reversal_mode(self):
        with patch.object(dashboard, 'get_primary_strategies', return_value=['reversal_1h']):
            cards, rows, _, _ = dashboard.build_strategy_docs_rows()
        self.assertIn('ao_chaikin_1h', cards)
        self.assertIn('<td>unified 1ч</td>', rows)
        self.assertNotIn('<td>AO / Чайкин 1ч</td>', rows)

    def test_dashboard_labels_new_strategy_and_preserves_old_trade_label(self):
        self.assertEqual(dashboard.humanize_strategy_name('ao_chaikin_1h'), 'AO / Чайкин 1ч')
        self.assertEqual(dashboard.humanize_strategy_name('reversal_1h'), 'часовой разворот')
        html = dashboard.build_dashboard_html()
        self.assertIn("ao_chaikin_1h: 'AO / Чайкин 1ч'", html)
        self.assertIn('Часовой разворот · текущая', html)
        self.assertIn('AO / Чайкин · архив', html)
        self.assertIn('AO / ЧАЙКИН 1Ч', html)
        self.assertIn('ЧАСОВОЙ РАЗВОРОТ', html)


if __name__ == '__main__':
    unittest.main()
