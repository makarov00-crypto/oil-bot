#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from bot_oil_main import (
    TRADE_JOURNAL_PATH,
    is_duplicate_carry_open,
    parse_state_datetime,
    save_trade_journal,
)


@dataclass
class AuditResult:
    bogus_rebuild_opens: list[dict[str, Any]]
    duplicate_portfolio_recovery_opens: list[dict[str, Any]]
    duplicate_orphan_closes: list[dict[str, Any]]
    orphan_closes: list[dict[str, Any]]
    stale_unmatched_open_counts: dict[str, int]
    live_unmatched_open_counts: dict[str, int]
    broker_alignment_issues: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cleanup-safe", action="store_true", help="Удалить только безопасные legacy-дубли.")
    parser.add_argument("--write", action="store_true", help="Применить cleanup к журналу.")
    parser.add_argument("--date", help="Проверить дополнительно сверку журнала с брокером за YYYY-MM-DD.")
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


def parse_target_day(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    return datetime.strptime(raw_value, "%Y-%m-%d").date()


def journal_trade_date(row: dict[str, Any]) -> date | None:
    row_dt = row.get("_dt")
    if isinstance(row_dt, datetime):
        return row_dt.date()
    return None


def journal_action(row: dict[str, Any]) -> str:
    event = str(row.get("event") or "").upper()
    side = str(row.get("side") or "").upper()
    if event == "OPEN":
        return "BUY" if side == "LONG" else "SELL"
    if event == "CLOSE":
        return "SELL" if side == "LONG" else "BUY"
    return ""


def canonical_price(price: float, tick: float | None) -> float:
    if tick is None or tick <= 0:
        return round(price, 4)
    steps = round(price / tick)
    return round(steps * tick, 6)


def load_broker_alignment_issues(target_day: date, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    client_cls, load_config_fn, fetch_operations_fn = get_broker_audit_dependencies()
    config = load_config_fn()
    issues: list[dict[str, Any]] = []

    with client_cls(config.token) as client:
        instruments = resolve_instruments_for_audit(client, config)
        figi_to_symbol = {item["figi"]: item["symbol"] for item in instruments}
        symbol_to_name = {item["symbol"]: item["display_name"] for item in instruments}
        tick_by_symbol = {item["symbol"]: item.get("min_price_increment") for item in instruments}
        broker_ops, _fee_by_parent = fetch_operations_fn(client, config.account_id, target_day, figi_to_symbol, symbol_to_name)

    broker_by_symbol: dict[str, list[Any]] = defaultdict(list)
    broker_by_id: dict[str, Any] = {}
    for op in broker_ops:
        broker_by_symbol[op.symbol].append(op)
        broker_by_id[op.op_id] = op

    day_rows = [row for row in rows if journal_trade_date(row) == target_day]
    symbol_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in day_rows:
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            symbol_rows[symbol].append(row)

    for symbol, symbol_day_rows in symbol_rows.items():
        broker_symbol_ops = broker_by_symbol.get(symbol, [])
        broker_signature_counts: dict[tuple[str, int, float], int] = defaultdict(int)
        for op in broker_symbol_ops:
            action = "BUY" if str(op.side).upper() == "LONG" else "SELL"
            price_key = canonical_price(float(op.price), tick_by_symbol.get(symbol))
            broker_signature_counts[(action, int(op.qty), price_key)] += 1

        journal_signature_counts: dict[tuple[str, int, float], int] = defaultdict(int)
        queues: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in sorted(symbol_day_rows, key=lambda item: item["_dt"]):
            event = str(row.get("event") or "").upper()
            if event not in {"OPEN", "CLOSE"}:
                continue
            action = journal_action(row)
            qty = int(row.get("qty_lots") or 0)
            price = float(row.get("price") or 0.0)
            price_key = canonical_price(price, tick_by_symbol.get(symbol))
            if not action or qty <= 0:
                continue
            source = str(row.get("source") or "")
            if not (event == "OPEN" and source == "portfolio_recovery"):
                journal_signature_counts[(action, qty, price_key)] += 1

            broker_op_id = str(row.get("broker_op_id") or "").strip()
            if broker_op_id:
                broker_op = broker_by_id.get(broker_op_id)
                if broker_op is None:
                    issues.append(
                        {
                            "type": "unknown_broker_op_id",
                            "symbol": symbol,
                            "time": row.get("time"),
                            "broker_op_id": broker_op_id,
                            "event": event,
                            "side": str(row.get("side") or "").upper(),
                            "qty_lots": qty,
                            "price": price,
                        }
                    )
                else:
                    broker_action = "BUY" if str(broker_op.side).upper() == "LONG" else "SELL"
                    broker_qty = int(broker_op.qty)
                    broker_price = float(broker_op.price)
                    broker_price_key = canonical_price(broker_price, tick_by_symbol.get(symbol))
                    if action != broker_action or qty != broker_qty or abs(price_key - broker_price_key) > 1e-6:
                        issues.append(
                            {
                                "type": "broker_op_mismatch",
                                "symbol": symbol,
                                "time": row.get("time"),
                                "broker_op_id": broker_op_id,
                                "journal_action": action,
                                "broker_action": broker_action,
                                "journal_qty": qty,
                                "broker_qty": broker_qty,
                                "journal_price": round(price, 6),
                                "broker_price": round(broker_price, 6),
                            }
                        )

            side = str(row.get("side") or "").upper()
            if event == "OPEN":
                queues[side].append({"price": float(row.get("price") or 0.0), "qty": qty, "row": row})
                continue

            if not queues[side]:
                continue

            remaining_qty = qty
            exit_price = float(row.get("price") or 0.0)
            actual_gross = row.get("gross_pnl_rub")
            while remaining_qty > 0 and queues[side]:
                open_leg = queues[side][-1]
                matched_qty = min(remaining_qty, int(open_leg["qty"]))
                entry_price = float(open_leg["price"])
                expected_diff = (exit_price - entry_price) if side == "LONG" else (entry_price - exit_price)
                if matched_qty > 0 and actual_gross not in (None, ""):
                    actual_value = float(actual_gross)
                    if abs(expected_diff) > 1e-9 and abs(actual_value) > 1e-9 and expected_diff * actual_value < 0:
                        issues.append(
                            {
                                "type": "same_day_pairing_sign_mismatch",
                                "symbol": symbol,
                                "time": row.get("time"),
                                "side": side,
                                "entry_time": open_leg["row"].get("time"),
                                "entry_price": round(entry_price, 4),
                                "exit_price": round(exit_price, 4),
                                "expected_price_diff_sign": "profit" if expected_diff > 0 else "loss",
                                "actual_gross_pnl_rub": round(actual_value, 2),
                            }
                        )
                        break
                open_leg["qty"] -= matched_qty
                remaining_qty -= matched_qty
                if open_leg["qty"] <= 0:
                    queues[side].pop()

        if broker_signature_counts != journal_signature_counts:
            signature_deltas: list[dict[str, Any]] = []
            signature_keys = set(broker_signature_counts) | set(journal_signature_counts)
            for action, qty, price in sorted(signature_keys):
                broker_count = broker_signature_counts.get((action, qty, price), 0)
                journal_count = journal_signature_counts.get((action, qty, price), 0)
                if broker_count != journal_count:
                    signature_deltas.append(
                        {
                            "action": action,
                            "qty": qty,
                                "price": price_key,
                                "broker_count": broker_count,
                                "journal_count": journal_count,
                            }
                    )
            issues.append(
                {
                    "type": "broker_signature_mismatch",
                    "symbol": symbol,
                    "date": target_day.isoformat(),
                    "signature_deltas": signature_deltas,
                }
            )

    return issues


def resolve_instruments_for_audit(client: Any, config: Any) -> list[dict[str, str]]:
    from bot_oil_main import resolve_instruments

    return [
        {
            "symbol": instrument.symbol,
            "figi": instrument.figi,
            "display_name": instrument.display_name,
            "min_price_increment": float(getattr(instrument, "min_price_increment", 0.0) or 0.0),
        }
        for instrument in resolve_instruments(client, config)
    ]


def get_broker_audit_dependencies() -> tuple[Any, Any, Any]:
    from tinkoff.invest import Client

    from bot_oil_main import load_config
    from scripts.recover_trade_operations import fetch_operations

    return Client, load_config, fetch_operations


def classify_journal(rows: list[dict[str, Any]], *, broker_day: date | None = None) -> AuditResult:
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

    broker_alignment_issues = load_broker_alignment_issues(broker_day, rows) if broker_day is not None else []

    return AuditResult(
        bogus_rebuild_opens=bogus_rebuild_opens,
        duplicate_portfolio_recovery_opens=duplicate_portfolio_recovery_opens,
        duplicate_orphan_closes=duplicate_orphan_closes,
        orphan_closes=orphan_closes,
        stale_unmatched_open_counts=dict(sorted(stale_unmatched_open_counts.items())),
        live_unmatched_open_counts=dict(sorted(live_unmatched_open_counts.items())),
        broker_alignment_issues=broker_alignment_issues,
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
        "broker_alignment_issue_count": len(audit.broker_alignment_issues),
        "broker_alignment_issues": audit.broker_alignment_issues,
    }


def main() -> int:
    args = parse_args()
    rows = load_rows()
    broker_day = parse_target_day(args.date)
    audit = classify_journal(rows, broker_day=broker_day)
    removed = 0
    if args.cleanup_safe:
        cleaned_rows, removed = cleanup_safe_rows(rows, audit)
        if args.write and removed:
            save_trade_journal(cleaned_rows)
            rows = load_rows()
            audit = classify_journal(rows, broker_day=broker_day)

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
        print(f"Расхождений журнал/брокер: {payload['broker_alignment_issue_count']}")
        if args.cleanup_safe:
            print(f"Удалено безопасных legacy-строк: {removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
