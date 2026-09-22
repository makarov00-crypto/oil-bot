import unittest
from unittest.mock import patch

import web_dashboard as dashboard


class AoStrategyDisplayTests(unittest.TestCase):
    def test_strategy_docs_explain_migration_risk_and_single_lot_exception(self):
        html = dashboard.build_docs_html()
        self.assertIn('Как работает `ao_chaikin_1h`', html)
        self.assertIn('50% базового риска', html)
        self.assertIn('Позиции `reversal_1h` доводятся по старым правилам', html)
        self.assertIn('один лот сверх половинного бюджета', html)
        self.assertIn('трёх последовательных ослаблений AO', html)
        self.assertIn('70% пика', html)
        self.assertNotIn('Главный триггер — новое значимое пересечение `MACD`', html)

    def test_strategy_map_uses_primary_registry_for_ao_mode(self):
        with patch.object(dashboard, 'get_primary_strategies', return_value=['ao_chaikin_1h']):
            cards, rows, _, _ = dashboard.build_strategy_docs_rows()
        self.assertIn('ao_chaikin_1h', cards)
        self.assertIn('<td>AO / Чайкин 1ч</td>', rows)
        self.assertNotIn('<td>unified 1ч</td>', rows)

    def test_dashboard_labels_new_strategy_and_preserves_old_trade_label(self):
        self.assertEqual(dashboard.humanize_strategy_name('ao_chaikin_1h'), 'AO / Чайкин 1ч')
        self.assertEqual(dashboard.humanize_strategy_name('reversal_1h'), 'часовой разворот')
        html = dashboard.build_dashboard_html()
        self.assertIn("ao_chaikin_1h: 'AO / Чайкин 1ч'", html)
        self.assertIn('AO / ЧАЙКИН 1Ч', html)
        self.assertIn('ЧАСОВОЙ РАЗВОРОТ', html)


if __name__ == '__main__':
    unittest.main()
