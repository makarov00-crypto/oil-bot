#!/usr/bin/env python3
"""Build the cached trade-quality report without blocking the live trading loop."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from bot_oil_main import (
    APP_NAME,
    TRADE_QUALITY_ANALYTICS_PATH,
    TRADE_QUALITY_LOCK_PATH,
    build_trade_quality_analytics,
    load_config,
    resolve_instruments,
)
from tbank_invest import Client


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config = load_config()
        with Client(config.token, app_name=APP_NAME, target=config.target) as client:
            watchlist = resolve_instruments(client, config)
            payload = build_trade_quality_analytics(client, config, watchlist)
        TRADE_QUALITY_ANALYTICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = TRADE_QUALITY_ANALYTICS_PATH.with_suffix(".tmp")
        temporary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary_path.replace(TRADE_QUALITY_ANALYTICS_PATH)
        logging.info(
            "trade_quality_analytics_updated trades=%s missed_entries=%s",
            len(payload.get("trades") or []),
            len(payload.get("missed_entries") or []),
        )
        return 0
    except Exception:
        logging.exception("trade_quality_analytics_failed")
        return 1
    finally:
        TRADE_QUALITY_LOCK_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
