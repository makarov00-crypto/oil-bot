import os
import tempfile
import unittest

from custom_instruments import (
    get_custom_clone_source,
    list_custom_instruments,
    merge_with_custom_symbols,
    upsert_custom_instrument,
)
from instrument_groups import get_instrument_group
from strategy_registry import get_primary_strategies, get_secondary_strategies


class CustomInstrumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.custom_path = os.path.join(self.temp_dir.name, "custom.json")
        self.prev_path = os.environ.get("OIL_CUSTOM_INSTRUMENTS_PATH")
        os.environ["OIL_CUSTOM_INSTRUMENTS_PATH"] = self.custom_path

    def tearDown(self) -> None:
        if self.prev_path is None:
            os.environ.pop("OIL_CUSTOM_INSTRUMENTS_PATH", None)
        else:
            os.environ["OIL_CUSTOM_INSTRUMENTS_PATH"] = self.prev_path
        self.temp_dir.cleanup()

    def test_custom_symbol_inherits_group_and_strategies(self) -> None:
        result = upsert_custom_instrument("VBM6", "SRM6")
        self.assertEqual(result["status"], "added")
        self.assertEqual(get_custom_clone_source("VBM6"), "SRM6")
        self.assertEqual(get_instrument_group("VBM6").name, get_instrument_group("SRM6").name)
        self.assertEqual(get_primary_strategies("VBM6"), get_primary_strategies("SRM6"))
        self.assertEqual(get_secondary_strategies("VBM6"), get_secondary_strategies("SRM6"))

    def test_merge_symbols_appends_custom_without_duplicates(self) -> None:
        upsert_custom_instrument("VBM6", "SRM6")
        merged = merge_with_custom_symbols(["BRK6", "SRM6", "VBM6"])
        self.assertEqual(merged, ["BRK6", "SRM6", "VBM6"])

    def test_list_custom_instruments_returns_saved_entries(self) -> None:
        upsert_custom_instrument("VBM6", "SRM6")
        items = list_custom_instruments()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["symbol"], "VBM6")
        self.assertEqual(items[0]["clone_from"], "SRM6")


if __name__ == "__main__":
    unittest.main()
