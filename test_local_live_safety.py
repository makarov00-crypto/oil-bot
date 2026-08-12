import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import bot_oil_main as mod


class LocalLiveSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.live_config = SimpleNamespace(dry_run=False)

    def test_blocks_live_orders_on_mac_without_confirmation(self) -> None:
        with patch.object(mod.sys, "platform", "darwin"), patch.dict(os.environ, {}, clear=False):
            os.environ.pop(mod.LOCAL_LIVE_CONFIRMATION_ENV, None)
            with self.assertRaisesRegex(RuntimeError, "Локальный LIVE-режим заблокирован"):
                mod.assert_local_live_trading_confirmation(self.live_config)

    def test_allows_live_orders_on_mac_with_explicit_confirmation(self) -> None:
        with patch.object(mod.sys, "platform", "darwin"), patch.dict(
            os.environ,
            {mod.LOCAL_LIVE_CONFIRMATION_ENV: mod.LOCAL_LIVE_CONFIRMATION_VALUE},
            clear=False,
        ):
            mod.assert_local_live_trading_confirmation(self.live_config)

    def test_does_not_add_confirmation_requirement_on_server(self) -> None:
        with patch.object(mod.sys, "platform", "linux"), patch.dict(os.environ, {}, clear=False):
            os.environ.pop(mod.LOCAL_LIVE_CONFIRMATION_ENV, None)
            mod.assert_local_live_trading_confirmation(self.live_config)


if __name__ == "__main__":
    unittest.main()
