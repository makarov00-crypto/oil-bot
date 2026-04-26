#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from bot_oil_main import TRADE_JOURNAL_PATH, is_duplicate_carry_open, parse_state_datetime, save_trade_journal


@dataclass
class AuditResult:
    bogus_rebuild_opens: list[dict[str, Any]]
    duplicate_portfolio_recovery_opens: list[dict[str, Any]]
    orphan_closes: list[dict[str, Any]]
    unmatched_open_counts: dict[str, int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cleanup-safe", action="store_true", help="Удалить только безопасные legacy-дубли.")
    parser.add_argument("--write", action="store_true", help="Применить cleanup к журналу.")
    return parser.parse_args()


def load_rows() -> list[dict[str, Any]]:
    if not TRADE_JOURNAL_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in TRADE_JOURNAL_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        row_dt = parse_state_datetime(str(row.get("time") or ""))
        if row_dt is None:
            continue
        row["_dt"] = row_dt
        rows.append(row)
    rows.sort(key=lambda row: row["_dt"])
    return rows


def is_bogus_rebuild_open(row: dict[str, Any]) -> bool:
    if str(row.get("source") or "") != "broker_ops_rebuild":
        return False
    if str(row.get("event") or "").upper() != "OPEN":
        return False
    reason = str(row.get("reason") or "")
    return (
        "stale_pending_cleared" in reason
        or "Подвисшая заявка очищена, открытой позиции нет" in reason
        or "close_waiting_broker_ops" in reason
    )


def find_duplicate_portfolio_recovery_opens(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    duplicates: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if str(row.get("source") or "") != "portfolio_recovery":
            continue
        if str(row.get("event") or "").upper() != "OPEN":
            continue
        for previous in reversed(rows[:index]):
            if str(previous.get("symbol") or "").upper() != str(row.get("symbol") or "").upper():
                continue
            if str(previous.get("side") or "").upper() != str(row.get("side") or "").upper():
                continue
            if str(previous.get("event") or "").upper() != "OPEN":
                continue
            if is_duplicate_carry_open(previous, row):
                duplicates.append(row)
            break
    return duplicates


def classify_journal(rows: list[dict[str, Any]]) -> AuditResult:
    bogus_rebuild_opens = [row for row in rows if is_bogus_rebuild_open(row)]
    duplicate_portfolio_recovery_opens = find_duplicate_portfolio_recovery_opens(rows)

    queues: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    orphan_closes: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").upper()
        event = str(row.get("event") or "").upper()
        if not symbol or not side:
            continue
        key = (symbol, side)
        if event == "OPEN":
            if str(row.get("source") or "") == "portfolio_recovery" and queues[key]:
                previous = queues[key][-1]
                if is_duplicate_carry_open(previous, row):
                    continue
            queues[key].append(row)
            continue
        if event != "CLOSE":
            continue
        if queues[key]:
            queues[key].pop()
        else:
            orphan_closes.append(row)

    unmatched_open_counts: dict[str, int] = {}
    for (symbol, _side), items in queues.items():
        if items:
            unmatched_open_counts[symbol] = unmatched_open_counts.get(symbol, 0) + len(items)

    return AuditResult(
        bogus_rebuild_opens=bogus_rebuild_opens,
        duplicate_portfolio_recovery_opens=duplicate_portfolio_recovery_opens,
        orphan_closes=orphan_closes,
        unmatched_open_counts=dict(sorted(unmatched_open_counts.items())),
    )


def make_row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("time"),
        row.get("symbol"),
        row.get("event"),
        row.get("side"),
        row.get("price"),
        row.get("qty_lots"),
        row.get("strategy"),
        row.get("source"),
        row.get("reason"),
    )


def cleanup_safe_rows(rows: list[dict[str, Any]], audit: AuditResult) -> tuple[list[dict[str, Any]], int]:
    removable_keys = {
        make_row_key(row)
        for row in [*audit.bogus_rebuild_opens, *audit.duplicate_portfolio_recovery_opens]
    }
    kept: list[dict[str, Any]] = []
    removed = 0
    for row in rows:
        if make_row_key(row) in removable_keys:
            removed += 1
            continue
        clean_row = dict(row)
        clean_row.pop("_dt", None)
        kept.append(clean_row)
    return kept, removed


def payload_from_audit(audit: AuditResult) -> dict[str, Any]:
    return {
        "ok": True,
        "bogus_rebuild_open_count": len(audit.bogus_rebuild_opens),
        "duplicate_portfolio_recovery_open_count": len(audit.duplicate_portfolio_recovery_opens),
        "orphan_close_count": len(audit.orphan_closes),
        "unmatched_open_counts": audit.unmatched_open_counts,
        "bogus_rebuild_opens": [
            {key: row.get(key) for key in ("time", "symbol", "side", "price", "strategy", "reason")}
            for row in audit.bogus_rebuild_opens
        ],
        "duplicate_portfolio_recovery_opens": [
            {key: row.get(key) for key in ("time", "symbol", "side", "price", "strategy", "reason")}
            for row in audit.duplicate_portfolio_recovery_opens
        ],
        "orphan_closes": [
            {key: row.get(key) for key in ("time", "symbol", "side", "price", "strategy", "source", "reason")}
            for row in audit.orphan_closes
        ],
    }


def main() -> int:
    args = parse_args()
    rows = load_rows()
    audit = classify_journal(rows)
    removed = 0
    if args.cleanup_safe:
        cleaned_rows, removed = cleanup_safe_rows(rows, audit)
        if args.write and removed:
            save_trade_journal(cleaned_rows)
            rows = load_rows()
            audit = classify_journal(rows)

    payload = payload_from_audit(audit)
    payload["safe_cleanup_removed"] = removed

    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"Ложных rebuild OPEN: {payload['bogus_rebuild_open_count']}")
        print(f"Дублей portfolio_recovery OPEN: {payload['duplicate_portfolio_recovery_open_count']}")
        print(f"Orphan CLOSE: {payload['orphan_close_count']}")
        print(f"Unmatched OPEN по символам: {payload['unmatched_open_counts']}")
        if args.cleanup_safe:
            print(f"Удалено безопасных legacy-строк: {removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
