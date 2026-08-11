from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable


def build_trade_key(trade: dict[str, Any]) -> str:
    return "|".join(
        [
            str(trade.get("symbol") or "").upper(),
            str(trade.get("side") or "").upper(),
            str(trade.get("entry_time") or ""),
            str(trade.get("exit_time") or ""),
            str(trade.get("entry_price") or ""),
            str(trade.get("exit_price") or ""),
        ]
    )


def pair_closed_trades(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pair journal OPEN/CLOSE rows without changing broker PnL or commissions."""
    opens: dict[tuple[str, str], list[dict[str, Any]]] = {}
    pairs: list[dict[str, Any]] = []
    ordered = sorted(rows, key=lambda item: item.get("_dt") or datetime.min)
    for row in ordered:
        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").upper()
        event = str(row.get("event") or "").upper()
        if not symbol or side not in {"LONG", "SHORT"}:
            continue
        key = (symbol, side)
        if event == "OPEN":
            try:
                qty = max(1, int(row.get("qty_lots") or 1))
            except (TypeError, ValueError):
                qty = 1
            opens.setdefault(key, []).extend([dict(row)] * qty)
            continue
        if event != "CLOSE" or not opens.get(key):
            continue
        try:
            close_qty = max(1, int(row.get("qty_lots") or 1))
        except (TypeError, ValueError):
            close_qty = 1
        matched = [opens[key].pop() for _ in range(min(close_qty, len(opens[key])))]
        if not matched:
            continue
        entry = matched[-1]
        entry_price = float(entry.get("price") or 0.0)
        exit_price = float(row.get("price") or 0.0)
        if entry_price <= 0.0 or exit_price <= 0.0:
            continue
        entry_time = entry.get("_dt")
        exit_time = row.get("_dt")
        if not isinstance(entry_time, datetime) or not isinstance(exit_time, datetime) or exit_time < entry_time:
            continue
        try:
            pnl_rub = float(row.get("pnl_rub") or 0.0)
        except (TypeError, ValueError):
            pnl_rub = 0.0
        try:
            commission_rub = abs(float(row.get("commission_rub") or 0.0))
        except (TypeError, ValueError):
            commission_rub = 0.0
        pairs.append(
            {
                "symbol": symbol,
                "side": side,
                "strategy": str(row.get("strategy") or entry.get("strategy") or ""),
                "entry_time": entry_time.isoformat(),
                "exit_time": exit_time.isoformat(),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty_lots": len(matched),
                "pnl_rub": pnl_rub,
                "commission_rub": commission_rub,
                "exit_reason": str(row.get("reason") or ""),
            }
        )
    return pairs


def calculate_trade_excursion(
    trade: dict[str, Any],
    hourly_candles: Iterable[dict[str, Any]],
    boundary_candles: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Calculate MFE/MAE from hourly candles and precise boundary candles only."""
    entry_time = datetime.fromisoformat(str(trade["entry_time"]))
    exit_time = datetime.fromisoformat(str(trade["exit_time"]))
    entry_price = float(trade["entry_price"])
    exit_price = float(trade["exit_price"])
    side = str(trade["side"]).upper()
    if entry_time.tzinfo is None or exit_time.tzinfo is None or entry_price <= 0.0:
        return {}

    entry_hour = entry_time.replace(minute=0, second=0, microsecond=0)
    exit_hour = exit_time.replace(minute=0, second=0, microsecond=0)
    hourly_rows = list(hourly_candles)
    boundary_rows = list(boundary_candles)
    candles: list[dict[str, Any]] = []
    for candle in hourly_rows:
        moment = candle.get("time")
        if not isinstance(moment, datetime):
            continue
        if entry_hour < moment < exit_hour:
            candles.append(candle)
    for candle in boundary_rows:
        moment = candle.get("time")
        if not isinstance(moment, datetime):
            continue
        if entry_time <= moment < exit_time:
            candles.append(candle)

    highs = [entry_price, exit_price]
    lows = [entry_price, exit_price]
    for candle in candles:
        try:
            highs.append(float(candle["high"]))
            lows.append(float(candle["low"]))
        except (KeyError, TypeError, ValueError):
            continue
    best_price = max(highs)
    worst_price = min(lows)
    if side == "LONG":
        mfe_pct = (best_price - entry_price) / entry_price * 100.0
        mae_pct = (entry_price - worst_price) / entry_price * 100.0
        realized_pct = (exit_price - entry_price) / entry_price * 100.0
    else:
        mfe_pct = (entry_price - worst_price) / entry_price * 100.0
        mae_pct = (best_price - entry_price) / entry_price * 100.0
        realized_pct = (entry_price - exit_price) / entry_price * 100.0
    return {
        "mfe_pct": round(max(0.0, mfe_pct), 4),
        "mae_pct": round(max(0.0, mae_pct), 4),
        "realized_price_pct": round(realized_pct, 4),
        "best_price": round(best_price, 6),
        "worst_price": round(worst_price, 6),
        "hourly_candles": sum(1 for candle in hourly_rows if entry_hour < candle.get("time", entry_hour) < exit_hour),
        "boundary_candles": len(candles) - sum(1 for candle in hourly_rows if entry_hour < candle.get("time", entry_hour) < exit_hour),
    }


def calculate_post_exit_move(
    trade: dict[str, Any],
    horizon_price: float,
) -> float:
    exit_price = float(trade.get("exit_price") or 0.0)
    if exit_price <= 0.0 or horizon_price <= 0.0:
        return 0.0
    if str(trade.get("side") or "").upper() == "LONG":
        return round((horizon_price - exit_price) / exit_price * 100.0, 4)
    return round((exit_price - horizon_price) / exit_price * 100.0, 4)


def summarize_trade_quality(trades: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        grouped.setdefault(str(trade.get("symbol") or ""), []).append(trade)
    result: list[dict[str, Any]] = []
    for symbol, rows in grouped.items():
        if not symbol:
            continue
        pnl = sum(float(row.get("pnl_rub") or 0.0) for row in rows)
        commission = sum(float(row.get("commission_rub") or 0.0) for row in rows)
        holds = []
        mfes = []
        maes = []
        early_moves = []
        for row in rows:
            try:
                held = datetime.fromisoformat(str(row["exit_time"])) - datetime.fromisoformat(str(row["entry_time"]))
                holds.append(max(0.0, held.total_seconds() / 60.0))
            except (KeyError, TypeError, ValueError):
                pass
            if row.get("mfe_pct") is not None:
                mfes.append(float(row["mfe_pct"]))
            if row.get("mae_pct") is not None:
                maes.append(float(row["mae_pct"]))
            if row.get("post_exit_4h_pct") is not None:
                early_moves.append(float(row["post_exit_4h_pct"]))
        result.append(
            {
                "symbol": symbol,
                "trades": len(rows),
                "net_pnl_rub": round(pnl, 2),
                "commission_rub": round(commission, 2),
                "average_hold_minutes": round(sum(holds) / len(holds)) if holds else 0,
                "average_mfe_pct": round(sum(mfes) / len(mfes), 3) if mfes else None,
                "average_mae_pct": round(sum(maes) / len(maes), 3) if maes else None,
                "early_exit_count": sum(1 for item in early_moves if item > 0.0),
                "average_post_exit_4h_pct": round(sum(early_moves) / len(early_moves), 3) if early_moves else None,
            }
        )
    return sorted(result, key=lambda item: (item["net_pnl_rub"], item["symbol"]))
