import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as mod


class WatchlistResolutionTests(unittest.TestCase):
    def test_resolve_instruments_skips_single_missing_symbol_if_others_exist(self) -> None:
        futures = [
            SimpleNamespace(
                ticker="BRK6",
                figi="FIGI_BRK6",
                name="Brent",
                lot=1,
                min_price_increment=None,
                min_price_increment_amount=None,
                initial_margin_on_buy=None,
                initial_margin_on_sell=None,
            ),
            SimpleNamespace(
                ticker="GNM6",
                figi="FIGI_GNM6",
                name="Gold",
                lot=1,
                min_price_increment=None,
                min_price_increment_amount=None,
                initial_margin_on_buy=None,
                initial_margin_on_sell=None,
            ),
        ]
        client = SimpleNamespace(instruments=SimpleNamespace(futures=lambda: SimpleNamespace(instruments=futures)))
        config = SimpleNamespace(symbols=["BRK6", "NJK6", "GNM6"])

        with patch.object(mod.logging, "warning") as warning_mock:
            resolved = mod.resolve_instruments(client, config)

        self.assertEqual([item.symbol for item in resolved], ["BRK6", "GNM6"])
        warning_mock.assert_called_once()
        self.assertIn("NJK6", warning_mock.call_args.args[1])

    def test_resolve_instruments_still_fails_if_nothing_resolved(self) -> None:
        client = SimpleNamespace(instruments=SimpleNamespace(futures=lambda: SimpleNamespace(instruments=[])))
        config = SimpleNamespace(symbols=["NJK6"])

        with self.assertRaises(RuntimeError) as ctx:
            mod.resolve_instruments(client, config)

        self.assertIn("NJK6", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
