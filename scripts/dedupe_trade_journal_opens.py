#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from bot_oil_main import TRADE_JOURNAL_PATH, parse_state_datetime, save_trade_journal
from bot_oil_main import is_duplicate_carry_open


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def load_rows() -> list[dict]:
    if not TRADE_JOURNAL_PATH.exists():
        return []
    rows: list[dict] = []
    for line in TRADE_JOURNAL_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def find_duplicates(rows: list[dict], day_key: str, window_seconds: int) -> tuple[list[dict], list[dict]]:
    kept: list[dict] = []
    removed: list[dict] = []
    last_open: dict[tuple[str, str], tuple[dict, datetime]] = {}

    for row in rows:
        row_time_raw = str(row.get("time") or "")
        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").upper()
        event = str(row.get("event") or "").upper()
        row_dt = parse_state_datetime(row_time_raw)
        key = (symbol, side)

        if event == "CLOSE":
            last_open.pop(key, None)
            kept.append(row)
            continue

        if event != "OPEN":
            kept.append(row)
            continue

        previous = last_open.get(key)
        if row_time_raw.startswith(day_key) and previous is not None and row_dt is not None:
            previous_row, previous_dt = previous
            if is_duplicate_carry_open(previous_row, row):
                removed.append(row)
                continue
            same_strategy = str(previous_row.get("strategy") or "") == str(row.get("strategy") or "")
            same_source = str(previous_row.get("source") or "") == str(row.get("source") or "")
            same_reason = str(previous_row.get("reason") or "") == str(row.get("reason") or "")
            gap = (row_dt - previous_dt).total_seconds()
            if same_strategy and same_source and same_reason and 0 <= gap <= window_seconds:
                removed.append(row)
                continue

        kept.append(row)
        if row_dt is not None:
            last_open[key] = (row, row_dt)

    return kept, removed


def main() -> int:
    args = parse_args()
    rows = load_rows()
    kept, removed = find_duplicates(rows, args.date, args.window_seconds)

    if args.write and removed:
        backup = TRADE_JOURNAL_PATH.with_name(f"{TRADE_JOURNAL_PATH.name}.bak-{args.date}-dedupe-opens")
        shutil.copy2(TRADE_JOURNAL_PATH, backup)
        save_trade_journal(kept)

    payload = {
        "ok": True,
        "date": args.date,
        "removed": len(removed),
        "items": [
            {
                "time": item.get("time"),
                "symbol": item.get("symbol"),
                "side": item.get("side"),
                "event": item.get("event"),
            }
            for item in removed
        ],
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"Удалено дублей OPEN: {len(removed)}")
        for item in payload["items"]:
            print(f"- {item['time']} {item['symbol']} {item['side']} {item['event']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
