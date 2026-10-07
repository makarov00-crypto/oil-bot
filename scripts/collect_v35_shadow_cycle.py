#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot_oil_main import (  # noqa: E402
    APP_NAME,
    Client,
    V35_SHADOW_LOCK_PATH,
    load_config,
    maybe_observe_v35_shadow_cycle,
    resolve_instruments,
    setup_logging,
)


def main() -> int:
    setup_logging()
    try:
        config = load_config()
        with Client(config.token, app_name=f"{APP_NAME}-v35-shadow", target=config.target) as client:
            watchlist = resolve_instruments(client, config)
            status = maybe_observe_v35_shadow_cycle(client, config, watchlist)
        if status is None:
            logging.info("v35_shadow_collection_skipped")
        return 0
    except Exception:
        logging.exception("v35_shadow_collection_failed")
        return 1
    finally:
        V35_SHADOW_LOCK_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
