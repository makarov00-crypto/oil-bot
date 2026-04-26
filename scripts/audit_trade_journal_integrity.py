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
    duplicate_orphan_closes: list[dict[str, Any]]
    orphan_closes: list[dict[str, Any]]
    stale_unmatched_open_counts: dict[str, int]
    live_unmatched_open_counts: dict[str, int]


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


def load_live_position_map() -> dict[tuple[str, str], int]:
    path = BASE_DIR / "bot_state" / "_portfolio_snapshot.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    result: dict[tuple[str, str], int] = {}
    for item in payload.get("broker_open_positions") or []:
        symbol = str(item.get("symbol") or "").upper()
        side = str(item.get("side") or "").upper()
        qty = int(item.get("qty") or 0)
        if symbol and side and qty > 0:
            result[(symbol, side)] = qty
    return result


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
    close_history: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    orphan_closes: list[dict[str, Any]] = []
    duplicate_orphan_closes: list[dict[str, Any]] = []
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
            close_history[key].append(row)
        else:
            previous_close = close_history[key][-1] if close_history[key] else None
            is_duplicate = False
            if previous_close is not None:
                previous_dt = previous_close.get("_dt")
                current_dt = row.get("_dt")
                if previous_dt is not None and current_dt is not None:
                    delta_seconds = (current_dt - previous_dt).total_seconds()
                    if 0 <= delta_seconds <= 3 * 60 * 60:
                        is_duplicate = True
                if (
                    str(previous_close.get("strategy") or "") == str(row.get("strategy") or "")
                    and str(previous_close.get("reason") or "") == str(row.get("reason") or "")
                ):
                    is_duplicate = True
            if is_duplicate:
                duplicate_orphan_closes.append(row)
            else:
                orphan_closes.append(row)
            close_history[key].append(row)

    live_positions = load_live_position_map()
    stale_unmatched_open_counts: dict[str, int] = {}
    live_unmatched_open_counts: dict[str, int] = {}
    for (symbol, _side), items in queues.items():
        if items:
            live_qty = int(live_positions.get((symbol, _side), 0) or 0)
            unmatched_qty = len(items)
            live_matched = min(live_qty, unmatched_qty)
            stale_qty = max(0, unmatched_qty - live_matched)
            if live_matched:
                live_unmatched_open_counts[symbol] = live_unmatched_open_counts.get(symbol, 0) + live_matched
            if stale_qty:
                stale_unmatched_open_counts[symbol] = stale_unmatched_open_counts.get(symbol, 0) + stale_qty

    return AuditResult(
        bogus_rebuild_opens=bogus_rebuild_opens,
        duplicate_portfolio_recovery_opens=duplicate_portfolio_recovery_opens,
        duplicate_orphan_closes=duplicate_orphan_closes,
        orphan_closes=orphan_closes,
        stale_unmatched_open_counts=dict(sorted(stale_unmatched_open_counts.items())),
        live_unmatched_open_counts=dict(sorted(live_unmatched_open_counts.items())),
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
        for row in [
            *audit.bogus_rebuild_opens,
            *audit.duplicate_portfolio_recovery_opens,
            *audit.duplicate_orphan_closes,
        ]
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
        "duplicate_orphan_close_count": len(audit.duplicate_orphan_closes),
        "orphan_close_count": len(audit.orphan_closes),
        "stale_unmatched_open_counts": audit.stale_unmatched_open_counts,
        "live_unmatched_open_counts": audit.live_unmatched_open_counts,
        "bogus_rebuild_opens": [
            {key: row.get(key) for key in ("time", "symbol", "side", "price", "strategy", "reason")}
            for row in audit.bogus_rebuild_opens
        ],
        "duplicate_portfolio_recovery_opens": [
            {key: row.get(key) for key in ("time", "symbol", "side", "price", "strategy", "reason")}
            for row in audit.duplicate_portfolio_recovery_opens
        ],
        "duplicate_orphan_closes": [
            {key: row.get(key) for key in ("time", "symbol", "side", "price", "strategy", "source", "reason")}
            for row in audit.duplicate_orphan_closes
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
        print(f"Дублей orphan CLOSE: {payload['duplicate_orphan_close_count']}")
        print(f"Orphan CLOSE: {payload['orphan_close_count']}")
        print(f"Stale unmatched OPEN по символам: {payload['stale_unmatched_open_counts']}")
        print(f"Live unmatched OPEN по символам: {payload['live_unmatched_open_counts']}")
        if args.cleanup_safe:
            print(f"Удалено безопасных legacy-строк: {removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
