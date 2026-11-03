import os
import tempfile
import unittest

from active_contracts import upsert_active_contract
from custom_instruments import (
    get_custom_clone_source,
    list_custom_instruments,
    merge_with_custom_symbols,
    upsert_custom_instrument as upsert_custom_instrument_row,
)
from instrument_groups import get_instrument_group
from strategy_registry import get_primary_strategies, get_secondary_strategies


class CustomInstrumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.custom_path = os.path.join(self.temp_dir.name, "custom.json")
        self.active_path = os.path.join(self.temp_dir.name, "active_contracts.json")
        self.prev_path = os.environ.get("OIL_CUSTOM_INSTRUMENTS_PATH")
        self.prev_active_path = os.environ.get("OIL_ACTIVE_CONTRACTS_PATH")
        os.environ["OIL_CUSTOM_INSTRUMENTS_PATH"] = self.custom_path
        os.environ["OIL_ACTIVE_CONTRACTS_PATH"] = self.active_path

    def tearDown(self) -> None:
        if self.prev_path is None:
            os.environ.pop("OIL_CUSTOM_INSTRUMENTS_PATH", None)
        else:
            os.environ["OIL_CUSTOM_INSTRUMENTS_PATH"] = self.prev_path
        if self.prev_active_path is None:
            os.environ.pop("OIL_ACTIVE_CONTRACTS_PATH", None)
        else:
            os.environ["OIL_ACTIVE_CONTRACTS_PATH"] = self.prev_active_path
        self.temp_dir.cleanup()

    def test_custom_symbol_inherits_group_and_strategies(self) -> None:
        result = upsert_custom_instrument_row("VBM6", "SRM6")
        self.assertEqual(result["status"], "added")
        self.assertEqual(get_custom_clone_source("VBM6"), "SRM6")
        self.assertEqual(get_instrument_group("VBM6").name, get_instrument_group("SRM6").name)
        self.assertEqual(get_primary_strategies("VBM6"), get_primary_strategies("SRM6"))
        self.assertEqual(get_secondary_strategies("VBM6"), get_secondary_strategies("SRM6"))

    def test_merge_symbols_appends_custom_without_duplicates(self) -> None:
        upsert_custom_instrument_row("VBM6", "SRM6")
        merged = merge_with_custom_symbols(["BRK6", "SRM6", "VBM6"])
        self.assertEqual(
            merged,
            ["BRK6", "SRM6", "VBM6", "BTCUSDF", "GZZ6", "TBZ6"],
        )

    def test_merge_symbols_rolls_custom_contracts(self) -> None:
        upsert_active_contract("VBM6", "VBU6")
        upsert_custom_instrument_row("VBM6", "SRM6")

        merged = merge_with_custom_symbols(["SRM6"])

        self.assertEqual(merged, ["SRM6", "BTCUSDF", "GZZ6", "TBZ6", "VBU6"])

    def test_list_custom_instruments_returns_saved_entries(self) -> None:
        upsert_custom_instrument_row("VBM6", "SRM6")
        items = list_custom_instruments()
        by_symbol = {item["symbol"]: item for item in items}
        self.assertEqual(len(items), 4)
        self.assertEqual(by_symbol["VBM6"]["clone_from"], "SRM6")
        self.assertEqual(by_symbol["VBM6"]["source"], "runtime")

    def test_builtin_test_instruments_use_standard_strategy_and_limits(self) -> None:
        expected_templates = {
            "TBZ6": "SRM6",
            "GZZ6": "SRM6",
            "BTCUSDF": "IMOEXF",
        }

        for symbol, template in expected_templates.items():
            with self.subTest(symbol=symbol):
                self.assertEqual(get_custom_clone_source(symbol), template)
                self.assertEqual(get_instrument_group(symbol).name, get_instrument_group(template).name)
                self.assertEqual(get_primary_strategies(symbol), get_primary_strategies(template))
                self.assertEqual(get_secondary_strategies(symbol), get_secondary_strategies(template))

    def test_runtime_entry_can_override_builtin_template(self) -> None:
        upsert_custom_instrument_row("BTCUSDF", "SRM6")

        self.assertEqual(get_custom_clone_source("BTCUSDF"), "SRM6")
        item = next(item for item in list_custom_instruments() if item["symbol"] == "BTCUSDF")
        self.assertEqual(item["source"], "runtime")


if __name__ == "__main__":
    unittest.main()
