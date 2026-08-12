from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable

from active_contracts import get_instrument_history_symbol


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
        entry_context = entry.get("context") if isinstance(entry.get("context"), dict) else {}
        shadow_ai = entry_context.get("shadow_ai") if isinstance(entry_context.get("shadow_ai"), dict) else {}
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
                "market_regime": str(entry_context.get("market_regime") or ""),
                "entry_edge_label": str(entry_context.get("entry_edge_label") or ""),
                "setup_quality_label": str(entry_context.get("setup_quality_label") or ""),
                "entry_atr_pct": _as_float(entry_context.get("atr_pct")),
                "shadow_ai_action": str(shadow_ai.get("action") or ""),
                "shadow_ai_direction": str(shadow_ai.get("direction") or ""),
                "shadow_ai_confidence": _as_float(shadow_ai.get("confidence")),
                "shadow_ai_reason": str(shadow_ai.get("reason") or ""),
                "shadow_ai_risk_note": str(shadow_ai.get("risk_note") or ""),
                "shadow_ai_source": "trade_entry" if shadow_ai.get("action") else "",
            }
        )
    return pairs


def restore_shadow_ai_from_signal_observations(
    trade: dict[str, Any],
    observations: Iterable[dict[str, Any]],
    *,
    max_gap: timedelta = timedelta(minutes=20),
) -> dict[str, Any]:
    """Restore an entry AI review from the matching executed signal observation."""
    if str(trade.get("shadow_ai_action") or "").strip():
        return dict(trade)
    try:
        entry_time = datetime.fromisoformat(str(trade.get("entry_time") or ""))
    except (TypeError, ValueError):
        return dict(trade)
    if entry_time.tzinfo is None:
        return dict(trade)

    symbol = str(trade.get("symbol") or "").upper()
    side = str(trade.get("side") or "").upper()
    best_match: tuple[float, dict[str, Any]] | None = None
    for observation in observations:
        if str(observation.get("symbol") or "").upper() != symbol:
            continue
        if str(observation.get("signal") or "").upper() != side:
            continue
        if str(observation.get("decision") or "").lower() != "selected":
            continue
        context = observation.get("context") if isinstance(observation.get("context"), dict) else {}
        if str(context.get("execution_status") or "").lower() not in {
            "confirmed_open",
            "recovered_open",
            "submitted_open",
        }:
            continue
        shadow_ai = context.get("shadow_ai") if isinstance(context.get("shadow_ai"), dict) else {}
        if not str(shadow_ai.get("action") or "").strip():
            continue
        try:
            observed_at = datetime.fromisoformat(str(observation.get("observed_at") or ""))
        except (TypeError, ValueError):
            continue
        if observed_at.tzinfo is None:
            continue
        seconds_after_signal = (entry_time - observed_at).total_seconds()
        if seconds_after_signal < -120 or seconds_after_signal > max_gap.total_seconds():
            continue
        distance = abs(seconds_after_signal)
        if best_match is None or distance < best_match[0]:
            best_match = (distance, shadow_ai)

    if best_match is None:
        return dict(trade)
    shadow_ai = best_match[1]
    return {
        **trade,
        "shadow_ai_action": str(shadow_ai.get("action") or ""),
        "shadow_ai_direction": str(shadow_ai.get("direction") or ""),
        "shadow_ai_confidence": _as_float(shadow_ai.get("confidence")),
        "shadow_ai_reason": str(shadow_ai.get("reason") or ""),
        "shadow_ai_risk_note": str(shadow_ai.get("risk_note") or ""),
        "shadow_ai_source": "signal_observation",
    }


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


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return default


def add_trade_counterfactuals(trade: dict[str, Any]) -> dict[str, Any]:
    """Estimate alternative hold results in RUB using the trade's observed sensitivity."""
    result = dict(trade)
    realized_pct = _as_float(trade.get("realized_price_pct"))
    net_rub = _as_float(trade.get("pnl_rub"))
    commission_rub = abs(_as_float(trade.get("commission_rub")))
    gross_rub = net_rub + commission_rub
    if abs(realized_pct) < 1e-9 or gross_rub * realized_pct <= 0.0:
        return result

    rub_per_pct = abs(gross_rub / realized_pct)
    side = str(trade.get("side") or "").upper()
    result["estimated_rub_per_pct"] = round(rub_per_pct, 4)
    for hours in (1, 2, 4, 8):
        key = f"post_exit_{hours}h_pct"
        if trade.get(key) is None:
            continue
        post_exit_pct = _as_float(trade.get(key))
        realized_fraction = realized_pct / 100.0
        continuation_fraction = post_exit_pct / 100.0
        if side == "SHORT":
            hold_pct = (realized_fraction + continuation_fraction - realized_fraction * continuation_fraction) * 100.0
        else:
            hold_pct = ((1.0 + realized_fraction) * (1.0 + continuation_fraction) - 1.0) * 100.0
        hold_net_rub = hold_pct * rub_per_pct - commission_rub
        result[f"hold_{hours}h_price_pct"] = round(hold_pct, 4)
        result[f"hold_{hours}h_net_rub"] = round(hold_net_rub, 2)
        result[f"hold_{hours}h_delta_rub"] = round(hold_net_rub - net_rub, 2)

    if trade.get("mfe_pct") is not None:
        max_possible_net_rub = _as_float(trade.get("mfe_pct")) * rub_per_pct - commission_rub
        result["max_possible_net_rub"] = round(max_possible_net_rub, 2)
        result["missed_profit_rub"] = round(max(0.0, max_possible_net_rub - net_rub), 2)
    return result


def early_exit_threshold_pct(trade: dict[str, Any]) -> float:
    """Ignore harmless post-exit noise; use entry ATR when it is available."""
    atr_pct = _as_float(trade.get("entry_atr_pct")) * 100.0
    return round(max(0.35, atr_pct), 3)


def is_material_early_exit(trade: dict[str, Any]) -> bool:
    move = _as_float(trade.get("post_exit_4h_pct"))
    return move >= early_exit_threshold_pct(trade)


def summarize_trade_quality(trades: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        symbol = get_instrument_history_symbol(str(trade.get("symbol") or ""))
        grouped.setdefault(symbol, []).append(trade)
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
        wins = 0
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
            if _as_float(row.get("pnl_rub")) > 0:
                wins += 1
        gross = pnl + commission
        positive_mfe = sum(max(0.0, _as_float(row.get("mfe_pct"))) for row in rows)
        captured = sum(max(0.0, _as_float(row.get("realized_price_pct"))) for row in rows)
        material_early_moves = [
            _as_float(row.get("post_exit_4h_pct"))
            for row in rows
            if is_material_early_exit(row)
        ]
        result.append(
            {
                "symbol": symbol,
                "trades": len(rows),
                "net_pnl_rub": round(pnl, 2),
                "gross_pnl_rub": round(gross, 2),
                "commission_rub": round(commission, 2),
                "win_rate_pct": round(wins / len(rows) * 100.0, 1) if rows else 0.0,
                "profit_capture_pct": round(captured / positive_mfe * 100.0, 1) if positive_mfe else None,
                "average_hold_minutes": round(sum(holds) / len(holds)) if holds else 0,
                "average_mfe_pct": round(sum(mfes) / len(mfes), 3) if mfes else None,
                "average_mae_pct": round(sum(maes) / len(maes), 3) if maes else None,
                "early_exit_count": len(material_early_moves),
                "early_exit_raw_count": sum(1 for item in early_moves if item > 0.0),
                "average_post_exit_4h_pct": round(sum(early_moves) / len(early_moves), 3) if early_moves else None,
                "average_early_exit_4h_pct": round(sum(material_early_moves) / len(material_early_moves), 3) if material_early_moves else None,
            }
        )
    return sorted(result, key=lambda item: (item["net_pnl_rub"], item["symbol"]))


def summarize_trade_dimension(trades: Iterable[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        label = str(trade.get(field) or "Не указано").strip() or "Не указано"
        grouped.setdefault(label, []).append(trade)
    result: list[dict[str, Any]] = []
    for label, rows in grouped.items():
        net = sum(_as_float(row.get("pnl_rub")) for row in rows)
        commission = sum(_as_float(row.get("commission_rub")) for row in rows)
        wins = sum(1 for row in rows if _as_float(row.get("pnl_rub")) > 0.0)
        result.append(
            {
                "label": label,
                "trades": len(rows),
                "net_pnl_rub": round(net, 2),
                "commission_rub": round(commission, 2),
                "win_rate_pct": round(wins / len(rows) * 100.0, 1) if rows else 0.0,
            }
        )
    return sorted(result, key=lambda item: (item["net_pnl_rub"], item["label"]))


def build_trade_quality_overview(trades: Iterable[dict[str, Any]], missed_entries: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(trades)
    missed = list(missed_entries)
    net = sum(_as_float(row.get("pnl_rub")) for row in rows)
    commission = sum(_as_float(row.get("commission_rub")) for row in rows)
    gross = net + commission
    wins = sum(1 for row in rows if _as_float(row.get("pnl_rub")) > 0.0)
    losses = sum(1 for row in rows if _as_float(row.get("pnl_rub")) < 0.0)
    positive_mfe = sum(max(0.0, _as_float(row.get("mfe_pct"))) for row in rows)
    captured = sum(max(0.0, _as_float(row.get("realized_price_pct"))) for row in rows)
    early = [row for row in rows if is_material_early_exit(row)]
    return {
        "closed_trades": len(rows),
        "wins": wins,
        "losses": losses,
        "net_pnl_rub": round(net, 2),
        "gross_pnl_rub": round(gross, 2),
        "commission_rub": round(commission, 2),
        "commission_share_pct": round(commission / gross * 100.0, 1) if gross > 0.0 else None,
        "win_rate_pct": round(wins / len(rows) * 100.0, 1) if rows else 0.0,
        "profit_capture_pct": round(captured / positive_mfe * 100.0, 1) if positive_mfe else None,
        "material_early_exit_count": len(early),
        "average_early_exit_4h_pct": round(
            sum(_as_float(row.get("post_exit_4h_pct")) for row in early) / len(early), 3
        ) if early else None,
        "missed_entries_count": len(missed),
        "missed_entries_move_4h_pct": round(
            sum(_as_float(row.get("move_4h_pct")) for row in missed) / len(missed), 3
        ) if missed else None,
    }
