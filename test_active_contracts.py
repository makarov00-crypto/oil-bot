import os
import tempfile
import unittest

from active_contracts import (
    get_active_contract_symbol,
    get_active_contract_template,
    replace_with_active_symbols,
    upsert_active_contract,
)
from instrument_groups import get_instrument_group, uses_unified_reversal_1h
from strategy_registry import get_primary_strategies


class ActiveContractsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.active_path = os.path.join(self.temp_dir.name, "active_contracts.json")
        self.prev_active_path = os.environ.get("OIL_ACTIVE_CONTRACTS_PATH")
        os.environ["OIL_ACTIVE_CONTRACTS_PATH"] = self.active_path

    def tearDown(self) -> None:
        if self.prev_active_path is None:
            os.environ.pop("OIL_ACTIVE_CONTRACTS_PATH", None)
        else:
            os.environ["OIL_ACTIVE_CONTRACTS_PATH"] = self.prev_active_path
        self.temp_dir.cleanup()

    def test_replace_with_active_symbols_swaps_templates(self) -> None:
        upsert_active_contract("BMM6", "BMN6")
        upsert_active_contract("NGK6", "NGM6")

        symbols = replace_with_active_symbols(["BMM6", "NGK6", "GNM6"])

        self.assertEqual(symbols, ["BMN6", "NGM6", "GNM6"])

    def test_active_symbol_inherits_template_group_and_strategy(self) -> None:
        upsert_active_contract("BMM6", "BMN6")

        self.assertEqual(get_active_contract_symbol("BMM6"), "BMN6")
        self.assertEqual(get_active_contract_template("BMN6"), "BMM6")
        self.assertEqual(get_instrument_group("BMN6").name, get_instrument_group("BMM6").name)
        self.assertEqual(get_primary_strategies("BMN6"), get_primary_strategies("BMM6"))
        self.assertTrue(uses_unified_reversal_1h("BMN6"))

    def test_disabled_template_is_removed_from_watchlist(self) -> None:
        upsert_active_contract("RBM6", None, disabled=True)

        symbols = replace_with_active_symbols(["RBM6", "GNM6"])

        self.assertEqual(symbols, ["GNM6"])


if __name__ == "__main__":
    unittest.main()
