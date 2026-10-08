#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot_oil_main import (  # noqa: E402
    APP_NAME,
    Client,
    V35_SHADOW_CYCLE_PATH,
    V35_SHADOW_LOCK_PATH,
    load_config,
    maybe_observe_v35_shadow_cycle,
    resolve_instruments,
    setup_logging,
)
from v35_shadow import append_cycle_audit  # noqa: E402


def main() -> int:
    setup_logging()
    started = time.monotonic()
    outcome: dict[str, object] = {"status": "failed"}
    try:
        config = load_config()
        with Client(config.token, app_name=f"{APP_NAME}-v35-shadow", target=config.target) as client:
            watchlist = resolve_instruments(client, config)
            status = maybe_observe_v35_shadow_cycle(client, config, watchlist)
        if status is None:
            logging.info("v35_shadow_collection_skipped")
            outcome = {"status": "skipped", "symbols_count": len(watchlist)}
        else:
            outcome = {
                "status": "success",
                "symbols_count": len(status.get("symbols") or []),
                "window_start": status.get("window_start"),
                "window_end": status.get("window_end"),
                "multitimeframe_candidates": status.get("multitimeframe_candidates", 0),
                "v10_allowed": status.get("v10_allowed", 0),
                "hierarchical_allowed": status.get("hierarchical_allowed", 0),
                "new_records": status.get("new_records", 0),
            }
        return 0
    except Exception as error:
        logging.exception("v35_shadow_collection_failed")
        outcome = {
            "status": "failed",
            "error_type": type(error).__name__,
            "error": str(error)[:500],
        }
        return 1
    finally:
        outcome["finished_at"] = datetime.now(timezone.utc).isoformat()
        outcome["duration_seconds"] = round(time.monotonic() - started, 3)
        try:
            append_cycle_audit(V35_SHADOW_CYCLE_PATH, outcome)
        except OSError as error:
            logging.warning("v35_shadow_cycle_audit_unavailable error=%s", error)
        V35_SHADOW_LOCK_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
