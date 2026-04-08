#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from tinkoff.invest import Client
from tinkoff.invest.schemas import GetOperationsByCursorRequest, OperationState, OperationType

from bot_oil_main import (
    FEE_OPERATION_TYPES,
    MOSCOW_TZ,
    UTC,
    calculate_futures_pnl_rub,
    compact_reason,
    load_config,
    load_trade_journal,
    quotation_to_float,
    resolve_instruments,
    save_trade_journal,
)

STATE_DIR = BASE_DIR / "bot_state"


@dataclass
class BrokerTradeOp:
    symbol: str
    display_name: str
    figi: str
    op_id: str
    parent_id: str
    op_type: Any
    side: str
    qty: int
    price: float
    dt: datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def to_utc_bounds(target_day: date) -> tuple[datetime, datetime]:
    start_local = datetime.combine(target_day, time.min, tzinfo=MOSCOW_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def parse_row_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MOSCOW_TZ)
    return parsed


def to_moscow_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(MOSCOW_TZ).isoformat()


def load_state_snapshot(symbol: str) -> dict[str, Any]:
    path = STATE_DIR / f"{symbol}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def fetch_operations(
    client: Client,
    account_id: str,
    target_day: date,
    figi_to_symbol: dict[str, str],
    symbol_to_name: dict[str, str],
) -> tuple[list[BrokerTradeOp], dict[str, float]]:
    from_utc, to_utc = to_utc_bounds(target_day)
    cursor = ""
    fee_by_parent: dict[str, float] = defaultdict(float)
    trade_ops: list[BrokerTradeOp] = []

    while True:
        response = client.operations.get_operations_by_cursor(
            GetOperationsByCursorRequest(
                account_id=account_id,
                from_=from_utc,
                to=to_utc,
                cursor=cursor,
                limit=500,
                state=OperationState.OPERATION_STATE_EXECUTED,
                without_commissions=False,
                without_overnights=False,
                without_trades=False,
            )
        )
        for item in getattr(response, "items", []) or []:
            figi = str(getattr(item, "figi", "") or "")
            symbol = figi_to_symbol.get(figi)
            if not symbol:
                continue
            op_type = getattr(item, "type", None)
            op_id = str(getattr(item, "id", "") or "")
            parent_id = str(getattr(item, "parent_operation_id", "") or "")
            payment = quotation_to_float(getattr(item, "payment", None))

            if op_type in FEE_OPERATION_TYPES and parent_id:
                fee_by_parent[parent_id] += abs(payment)
                continue

            if op_type not in {
                OperationType.OPERATION_TYPE_BUY,
                OperationType.OPERATION_TYPE_SELL,
            }:
                continue

            op_dt = getattr(item, "date", None)
            if not isinstance(op_dt, datetime):
                continue
            if op_dt.tzinfo is None:
                op_dt = op_dt.replace(tzinfo=UTC)

            price = quotation_to_float(getattr(item, "price", None))
            qty = int(getattr(item, "quantity", 0) or 0)
            side = "LONG" if op_type == OperationType.OPERATION_TYPE_BUY else "SHORT"

            trade_ops.append(
                BrokerTradeOp(
                    symbol=symbol,
                    display_name=symbol_to_name.get(symbol, symbol),
                    figi=figi,
                    op_id=op_id,
                    parent_id=parent_id,
                    op_type=op_type,
                    side=side,
                    qty=qty,
                    price=price,
                    dt=op_dt,
                )
            )

        next_cursor = str(getattr(response, "next_cursor", "") or "")
        if not getattr(response, "has_next", False) or not next_cursor:
            break
        cursor = next_cursor

    trade_ops.sort(key=lambda item: item.dt)
    return trade_ops, dict(fee_by_parent)


def build_journal_queues(rows: list[dict[str, Any]], target_day: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    day_prefix = target_day.isoformat()
    target_rows = [row for row in rows if str(row.get("time", "")).startswith(day_prefix)]
    queues: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    existing_close_signatures: set[str] = set()

    sorted_rows = sorted(
        target_rows,
        key=lambda row: parse_row_dt(row.get("time")) or datetime.min.replace(tzinfo=UTC),
    )

    for row in sorted_rows:
        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").upper()
        event = str(row.get("event") or "").upper()
        if not symbol or not side:
            continue
        if event == "OPEN":
            queues[(symbol, side)].append(row)
            continue
        if event != "CLOSE":
            continue
        close_dt = parse_row_dt(row.get("time"))
        if close_dt is not None:
            existing_close_signatures.add(f"{symbol}:{side}:{int(close_dt.timestamp())}")
        queue = queues[(symbol, side)]
        if queue:
            queue.pop(0)

    unmatched = [row for queue in queues.values() for row in queue]
    unmatched.sort(key=lambda row: parse_row_dt(row.get("time")) or datetime.min.replace(tzinfo=UTC))
    return target_rows, unmatched, existing_close_signatures


def infer_open_fee(
    open_row: dict[str, Any],
    instrument,
    trade_ops: list[BrokerTradeOp],
    fee_by_parent: dict[str, float],
) -> float:
    try:
        existing_fee = float(open_row.get("commission_rub") or 0.0)
    except Exception:
        existing_fee = 0.0
    if existing_fee > 0:
        return existing_fee

    open_dt = parse_row_dt(open_row.get("time"))
    open_price = float(open_row.get("price") or 0.0)
    qty = int(open_row.get("qty_lots") or 0)
    side = str(open_row.get("side") or "").upper()
    if open_dt is None or qty <= 0 or side not in {"LONG", "SHORT"}:
        return 0.0

    expected_type = OperationType.OPERATION_TYPE_BUY if side == "LONG" else OperationType.OPERATION_TYPE_SELL
    tolerance = max(float(getattr(instrument, "min_price_increment", 0.0) or 0.0) * 3, 1e-4)

    for op in trade_ops:
        if op.symbol != open_row.get("symbol"):
            continue
        if op.op_type != expected_type:
            continue
        if op.dt < open_dt - timedelta(minutes=2) or op.dt > open_dt + timedelta(minutes=10):
            continue
        if qty > 0 and op.qty not in {0, qty}:
            continue
        if open_price > 0 and op.price > 0 and abs(op.price - open_price) > tolerance:
            continue
        return round(float(fee_by_parent.get(op.op_id, 0.0)), 2)
    return 0.0


def infer_close_reason(symbol: str, side: str, qty: int, close_dt: datetime) -> str:
    state = load_state_snapshot(symbol)
    delayed_reason = compact_reason(str(state.get("delayed_close_reason") or ""))
    delayed_side = str(state.get("delayed_close_side") or "").upper()
    delayed_qty = int(state.get("delayed_close_qty") or 0)
    delayed_at = parse_row_dt(state.get("delayed_close_submitted_at"))
    if delayed_reason and delayed_side == side and delayed_qty == qty and delayed_at is not None:
        if abs((close_dt - delayed_at).total_seconds()) <= 6 * 60 * 60:
            return delayed_reason

    last_reason = compact_reason(str(state.get("last_exit_reason") or ""))
    last_side = str(state.get("last_exit_side") or "").upper()
    last_time = parse_row_dt(state.get("last_exit_time"))
    if last_reason and last_side == side and last_time is not None:
        if abs((close_dt - last_time).total_seconds()) <= 6 * 60 * 60:
            return last_reason

    pending_reason = compact_reason(str(state.get("pending_exit_reason") or ""))
    if pending_reason:
        return pending_reason

    return "Торговая причина выхода не сохранилась, закрытие подтверждено брокерскими операциями."


def find_missing_close_matches(
    unmatched_opens: list[dict[str, Any]],
    trade_ops: list[BrokerTradeOp],
    fee_by_parent: dict[str, float],
    instruments_by_symbol: dict[str, Any],
    existing_close_signatures: set[str],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    used_op_ids: set[str] = set()

    for open_row in unmatched_opens:
        symbol = str(open_row.get("symbol") or "").upper()
        side = str(open_row.get("side") or "").upper()
        qty = int(open_row.get("qty_lots") or 0)
        open_dt = parse_row_dt(open_row.get("time"))
        instrument = instruments_by_symbol.get(symbol)
        if not symbol or side not in {"LONG", "SHORT"} or qty <= 0 or open_dt is None or instrument is None:
            continue

        expected_type = OperationType.OPERATION_TYPE_SELL if side == "LONG" else OperationType.OPERATION_TYPE_BUY
        candidate: BrokerTradeOp | None = None

        for op in trade_ops:
            if op.op_id in used_op_ids:
                continue
            if op.symbol != symbol or op.op_type != expected_type:
                continue
            if op.dt <= open_dt:
                continue
            if qty > 0 and op.qty not in {0, qty}:
                continue
            signature = f"{symbol}:{side}:{int(op.dt.timestamp())}"
            if signature in existing_close_signatures:
                continue
            candidate = op
            break

        if candidate is None:
            continue

        entry_price = float(open_row.get("price") or 0.0)
        open_fee = infer_open_fee(open_row, instrument, trade_ops, fee_by_parent)
        close_fee = round(float(fee_by_parent.get(candidate.op_id, 0.0)), 2)
        gross = calculate_futures_pnl_rub(
            instrument,
            entry_price,
            candidate.price,
            qty,
            side,
        )
        total_commission = round(open_fee + close_fee, 2)
        net = round(gross - total_commission, 2)
        matches.append(
            {
                "open_row": open_row,
                "close_row": {
                    "time": to_moscow_iso(candidate.dt),
                    "symbol": symbol,
                    "display_name": open_row.get("display_name") or candidate.display_name,
                    "event": "CLOSE",
                    "side": side,
                    "qty_lots": qty,
                    "lot_size": open_row.get("lot_size") or getattr(instrument, "lot", 1),
                    "price": candidate.price,
                    "pnl_rub": net,
                    "gross_pnl_rub": round(gross, 2),
                    "commission_rub": total_commission,
                    "net_pnl_rub": net,
                    "reason": infer_close_reason(symbol, side, qty, candidate.dt),
                    "source": "broker_ops_recovery",
                    "strategy": open_row.get("strategy") or "",
                    "mode": open_row.get("mode") or "LIVE",
                    "session": open_row.get("session") or "",
                },
            }
        )
        used_op_ids.add(candidate.op_id)
        existing_close_signatures.add(f"{symbol}:{side}:{int(candidate.dt.timestamp())}")

    return matches


def build_summary(target_day: date, matches: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = [item["close_row"]["symbol"] for item in matches]
    return {
        "ok": True,
        "date": target_day.isoformat(),
        "recovered_closes": len(matches),
        "symbols": symbols,
        "message": (
            f"Восстановлено закрытий: {len(matches)}."
            if matches
            else "Пропавшие закрытия за выбранную дату не найдены."
        ),
        "items": [
            {
                "symbol": item["close_row"]["symbol"],
                "entry_time": item["open_row"].get("time"),
                "exit_time": item["close_row"]["time"],
                "net_pnl_rub": item["close_row"]["net_pnl_rub"],
                "reason": item["close_row"]["reason"],
            }
            for item in matches
        ],
    }


def main() -> int:
    args = parse_args()
    target_day = parse_day(args.date)
    config = load_config()

    with Client(config.token, target=config.target) as client:
        watchlist = resolve_instruments(client, config)
        instruments_by_symbol = {item.symbol: item for item in watchlist}
        figi_to_symbol = {item.figi: item.symbol for item in watchlist if item.figi}
        symbol_to_name = {item.symbol: item.display_name for item in watchlist}
        trade_ops, fee_by_parent = fetch_operations(
            client,
            config.account_id,
            target_day,
            figi_to_symbol,
            symbol_to_name,
        )

    rows = load_trade_journal()
    _, unmatched_opens, existing_close_signatures = build_journal_queues(rows, target_day)
    matches = find_missing_close_matches(
        unmatched_opens,
        trade_ops,
        fee_by_parent,
        instruments_by_symbol,
        existing_close_signatures,
    )

    if args.write and matches:
        for item in matches:
            rows.append(item["close_row"])
        rows.sort(key=lambda row: parse_row_dt(row.get("time")) or datetime.min.replace(tzinfo=UTC))
        save_trade_journal(rows)

    summary = build_summary(target_day, matches)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False))
    else:
        print(summary["message"])
        for item in summary["items"]:
            print(
                f"- {item['symbol']} {item['entry_time']} -> {item['exit_time']} "
                f"net={item['net_pnl_rub']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
