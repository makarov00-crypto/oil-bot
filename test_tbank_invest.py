import os
import unittest
from pathlib import Path
from unittest.mock import patch

import tbank_invest as mod


class TbankInvestTest(unittest.TestCase):
    def test_targets_default_to_tbank(self) -> None:
        self.assertEqual(mod.get_target_by_name("PROD"), "invest-public-api.tbank.ru")
        self.assertEqual(mod.get_target_by_name("SANDBOX"), "sandbox-invest-public-api.tbank.ru")

    def test_bundle_exists_by_default(self) -> None:
        self.assertTrue((Path(mod.__file__).resolve().parent / "certs" / "tbank_trust_bundle.pem").exists())

    def test_custom_cert_path_override(self) -> None:
        with patch.dict(os.environ, {"T_INVEST_ROOT_CERT_PATH": "/tmp/missing-cert.pem"}, clear=False):
            self.assertIsNone(mod._load_root_certificates())
