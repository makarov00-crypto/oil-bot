import os
import tempfile
import unittest

from active_contracts import (
    get_active_contract_symbol,
    get_active_contract_template,
    get_instrument_history_symbol,
    list_active_contracts,
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
        upsert_active_contract("BMM6", "BRU6")

        self.assertEqual(get_active_contract_symbol("BMM6"), "BRU6")
        self.assertEqual(get_active_contract_template("BRU6"), "BMM6")
        self.assertEqual(get_instrument_group("BRU6").name, get_instrument_group("BMM6").name)
        self.assertEqual(get_primary_strategies("BRU6"), get_primary_strategies("BMM6"))
        self.assertTrue(uses_unified_reversal_1h("BRU6"))

    def test_full_gold_contract_inherits_template_group_and_strategy(self) -> None:
        upsert_active_contract("GNM6", "GLU6")
        upsert_active_contract("GNU6", "GLU6")
        upsert_active_contract("GNM6", "GDZ6")

        self.assertEqual(get_active_contract_template("GDZ6"), "GNM6")
        self.assertEqual(get_instrument_group("GDZ6").name, get_instrument_group("GNM6").name)
        self.assertEqual(get_primary_strategies("GDZ6"), get_primary_strategies("GNM6"))
        self.assertTrue(uses_unified_reversal_1h("GDZ6"))
        self.assertEqual(replace_with_active_symbols(["GNU6"]), ["GDZ6"])
        self.assertEqual(get_instrument_history_symbol("GLU6"), "GDZ6")
        self.assertEqual(get_active_contract_template("GLU6"), "GNM6")

    def test_disabled_template_is_removed_from_watchlist(self) -> None:
        upsert_active_contract("ONU6", None, disabled=True)

        symbols = replace_with_active_symbols(["ONU6", "GNM6"])

        self.assertEqual(symbols, ["GNM6"])

    def test_rollover_preserves_old_contract_as_history_alias(self) -> None:
        upsert_active_contract("BMM6", "BMQ6")
        upsert_active_contract("BMM6", "BRU6")

        self.assertEqual(get_active_contract_symbol("BMM6"), "BRU6")
        self.assertEqual(get_active_contract_symbol("BMQ6"), "BRU6")
        self.assertEqual(get_instrument_history_symbol("BMQ6"), "BRU6")
        self.assertEqual(get_instrument_history_symbol("BRU6"), "BRU6")

    def test_rollover_moves_all_existing_aliases_to_new_contract(self) -> None:
        upsert_active_contract("BRK6", "BMQ6")
        upsert_active_contract("BMM6", "BMQ6")

        upsert_active_contract("BMM6", "BRU6")

        contracts = {
            item["template_symbol"]: item["active_symbol"]
            for item in list_active_contracts()
        }
        self.assertEqual(contracts["BRK6"], "BRU6")
        self.assertEqual(contracts["BMM6"], "BRU6")
        self.assertEqual(contracts["BMQ6"], "BRU6")

    def test_gas_rollover_moves_every_historical_alias_and_keeps_strategy(self) -> None:
        for template_symbol in ("NGJ6", "NGK6", "NGM6", "NGN6", "NGQ6"):
            upsert_active_contract(template_symbol, "NGU6")

        upsert_active_contract("NGK6", "NGZ6")

        for symbol in ("NGJ6", "NGK6", "NGM6", "NGN6", "NGQ6", "NGU6"):
            self.assertEqual(get_active_contract_symbol(symbol), "NGZ6")
            self.assertEqual(get_instrument_history_symbol(symbol), "NGZ6")
        self.assertEqual(get_active_contract_template("NGZ6"), "NGK6")
        self.assertEqual(get_primary_strategies("NGZ6"), get_primary_strategies("NGK6"))
        self.assertTrue(uses_unified_reversal_1h("NGZ6"))

    def test_brent_rollover_moves_every_historical_alias_and_keeps_strategy(self) -> None:
        for template_symbol in ("BRK6", "BMM6", "BMN6", "BMQ6", "BRU6"):
            upsert_active_contract(template_symbol, "BRV6")

        upsert_active_contract("BRK6", "BRX6")

        for symbol in ("BRK6", "BMM6", "BMN6", "BMQ6", "BRU6", "BRV6"):
            self.assertEqual(get_active_contract_symbol(symbol), "BRX6")
            self.assertEqual(get_instrument_history_symbol(symbol), "BRX6")
        self.assertEqual(get_active_contract_template("BRX6"), "BRK6")
        self.assertEqual(get_primary_strategies("BRX6"), get_primary_strategies("BRK6"))
        self.assertTrue(uses_unified_reversal_1h("BRX6"))

    def test_retired_alias_keeps_the_canonical_template_after_rollover(self) -> None:
        upsert_active_contract("SRM6", "SRU6")
        upsert_active_contract("SRM6", "SRZ6")

        self.assertEqual(get_active_contract_template("SRU6"), "SRM6")
        self.assertEqual(get_active_contract_template("SRZ6"), "SRM6")


if __name__ == "__main__":
    unittest.main()
