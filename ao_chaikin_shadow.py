from __future__ import annotations

import json
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd

from active_contracts import get_instrument_history_symbol


MOSCOW_TZ = ZoneInfo("Europe/Moscow")

DECISION_ENTRY = "ВХОД"
DECISION_HOLD = "УДЕРЖАНИЕ"
DECISION_EXIT = "ВЫХОД"
DECISION_NO_ENTRY = "НЕТ ВХОДА"

DIRECTION_LONG = "ЛОНГ"
DIRECTION_SHORT = "ШОРТ"
DIRECTION_NONE = "НЕТ"

POSITION_FLAT = "НЕТ"

CHAIKIN_CONFIRMS = "ПОДТВЕРЖДАЕТ"
CHAIKIN_CONTRADICTS = "ПРОТИВОРЕЧИТ"
CHAIKIN_NEUTRAL = "НЕЙТРАЛЕН"
STRATEGY_VERSION = 3
DEFAULT_MINIMUM_STRENGTH_ATR_RATIO = 0.60
DEFAULT_EXIT_AO_RETENTION_RATIO = 0.70


def prepare_shadow_indicators(candles: pd.DataFrame) -> pd.DataFrame:
    """Calculate the shadow indicators without changing live strategy columns."""
    result = candles.copy()
    result["time"] = pd.to_datetime(result["time"], utc=True, errors="coerce")
    result = result.dropna(subset=["time", "high", "low", "close", "volume"])
    result = result.sort_values("time").reset_index(drop=True)
    midpoint = (result["high"].astype(float) + result["low"].astype(float)) / 2.0
    result["shadow_ao"] = midpoint.rolling(5).mean() - midpoint.rolling(34).mean()

    previous_close = result["close"].astype(float).shift(1)
    true_range = pd.concat(
        [
            result["high"].astype(float) - result["low"].astype(float),
            (result["high"].astype(float) - previous_close).abs(),
            (result["low"].astype(float) - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    result["shadow_atr"] = true_range.rolling(14).mean()

    price_range = (result["high"].astype(float) - result["low"].astype(float)).replace(0.0, float("nan"))
    money_flow_multiplier = (
        (result["close"].astype(float) - result["low"].astype(float))
        - (result["high"].astype(float) - result["close"].astype(float))
    ) / price_range
    accumulation = (money_flow_multiplier.fillna(0.0) * result["volume"].astype(float)).cumsum()
    result["shadow_chaikin"] = (
        accumulation.ewm(span=5, adjust=False).mean()
        - accumulation.ewm(span=20, adjust=False).mean()
    )
    result["candle_closed_at"] = result["time"] + pd.Timedelta(hours=1)
    return result.dropna(subset=["shadow_ao", "shadow_atr", "shadow_chaikin"]).reset_index(drop=True)


def _iso_moscow(value: Any) -> str:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.tz_convert(MOSCOW_TZ).isoformat()


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _direction_for_ao(ao_value: float) -> str:
    if ao_value > 0.0:
        return DIRECTION_LONG
    if ao_value < 0.0:
        return DIRECTION_SHORT
    return DIRECTION_NONE


def _chaikin_status(current: float, previous: float, direction: str) -> str:
    change = current - previous
    neutral_band = max(abs(current) * 0.02, 1e-9)
    if abs(change) <= neutral_band:
        return CHAIKIN_NEUTRAL
    confirms = change > 0.0 if direction == DIRECTION_LONG else change < 0.0
    return CHAIKIN_CONFIRMS if confirms else CHAIKIN_CONTRADICTS


def _opposite_ao_bars(frame: pd.DataFrame, index: int, position: str) -> int:
    count = 0
    for current_index in range(index, 0, -1):
        change = _number(frame.iloc[current_index]["shadow_ao"]) - _number(
            frame.iloc[current_index - 1]["shadow_ao"]
        )
        is_opposite = change < 0.0 if position == DIRECTION_LONG else change > 0.0
        if not is_opposite:
            break
        count += 1
    return count


def _confirmed_ao_zero_cross(frame: pd.DataFrame, index: int, direction: str) -> bool:
    """Require the crossover bar and a second closed strengthening bar."""
    if index < 2:
        return False
    before_cross = _number(frame.iloc[index - 2]["shadow_ao"])
    first = _number(frame.iloc[index - 1]["shadow_ao"])
    second = _number(frame.iloc[index]["shadow_ao"])
    if direction == DIRECTION_LONG:
        return before_cross <= 0.0 < first < second
    if direction == DIRECTION_SHORT:
        return before_cross >= 0.0 > first > second
    return False


def _pnl_rub(
    entry_price: float,
    exit_price: float,
    direction: str,
    point_value: float,
) -> float:
    price_move = exit_price - entry_price
    if direction == DIRECTION_SHORT:
        price_move *= -1.0
    return price_move * point_value


def _estimated_round_trip_commission(
    entry_price: float,
    current_price: float,
    point_value: float,
    commission_rate: float,
) -> float:
    entry_notional = abs(entry_price * point_value)
    current_notional = abs(current_price * point_value)
    return (entry_notional + current_notional) * max(0.0, commission_rate)


def evaluate_shadow_candle(
    frame: pd.DataFrame,
    index: int,
    previous: dict[str, Any] | None,
    *,
    symbol: str,
    point_value: float,
    minimum_strength_atr_ratio: float = DEFAULT_MINIMUM_STRENGTH_ATR_RATIO,
    exit_ao_retention_ratio: float = DEFAULT_EXIT_AO_RETENTION_RATIO,
    commission_rate: float = 0.00025,
) -> dict[str, Any]:
    if index < 1:
        raise ValueError("Для теневой стратегии нужны минимум две рассчитанные свечи")

    row = frame.iloc[index]
    previous_row = frame.iloc[index - 1]
    ao_value = _number(row["shadow_ao"])
    previous_ao = _number(previous_row["shadow_ao"])
    close_price = _number(row["close"])
    strength_pct = abs(ao_value) / close_price * 100.0 if close_price > 0.0 else 0.0
    atr = _number(row["shadow_atr"])
    strength_atr_ratio = abs(ao_value) / atr if atr > 0.0 else 0.0
    long_pattern = (
        ao_value > 0.0
        and ao_value > previous_ao
        and _confirmed_ao_zero_cross(frame, index, DIRECTION_LONG)
    )
    short_pattern = (
        ao_value < 0.0
        and ao_value < previous_ao
        and _confirmed_ao_zero_cross(frame, index, DIRECTION_SHORT)
    )

    position_before = str((previous or {}).get("position_after") or POSITION_FLAT)
    direction = position_before if position_before in {DIRECTION_LONG, DIRECTION_SHORT} else _direction_for_ao(ao_value)
    chaikin_value = _number(row["shadow_chaikin"])
    previous_chaikin = _number(previous_row["shadow_chaikin"])
    chaikin_status = _chaikin_status(chaikin_value, previous_chaikin, direction)
    opposite_bars = 0
    decision = DECISION_NO_ENTRY
    position_after = position_before
    reason = ""

    entry_time = str((previous or {}).get("entry_time") or "")
    entry_price = _number((previous or {}).get("entry_price"))
    best_price = _number((previous or {}).get("best_price"), entry_price)
    worst_price = _number((previous or {}).get("worst_price"), entry_price)
    peak_ao_magnitude = _number((previous or {}).get("peak_ao_magnitude"))
    ao_peak_retention_ratio = None
    price_confirms_exit = False

    if position_before == POSITION_FLAT:
        if long_pattern and strength_atr_ratio >= minimum_strength_atr_ratio:
            decision = DECISION_ENTRY
            direction = DIRECTION_LONG
        elif short_pattern and strength_atr_ratio >= minimum_strength_atr_ratio:
            decision = DECISION_ENTRY
            direction = DIRECTION_SHORT

        if decision == DECISION_ENTRY:
            position_after = direction
            entry_time = _iso_moscow(row["candle_closed_at"])
            entry_price = close_price
            best_price = close_price
            worst_price = close_price
            peak_ao_magnitude = abs(ao_value)
            ao_peak_retention_ratio = 1.0
            chaikin_status = _chaikin_status(chaikin_value, previous_chaikin, direction)
            reason = (
                f"Две закрытые свечи AO подтвердили пересечение нуля в сторону {direction.lower()}; "
                f"сила {strength_atr_ratio:.2f} ATR при пороге {minimum_strength_atr_ratio:.2f} ATR. "
                f"Поток объёма Чайкина используется как дополнительная оценка: {chaikin_status.lower()}."
            )
        elif (long_pattern or short_pattern) and strength_atr_ratio < minimum_strength_atr_ratio:
            reason = (
                f"AO недавно пересёк ноль, но сила {strength_atr_ratio:.2f} ATR ниже "
                f"порога {minimum_strength_atr_ratio:.2f} ATR."
            )
        elif ao_value > 0.0:
            reason = "AO выше нуля, но нет второй закрытой усиливающейся свечи сразу после пересечения."
        elif ao_value < 0.0:
            reason = "AO ниже нуля, но нет второй закрытой усиливающейся свечи сразу после пересечения."
        else:
            reason = "AO находится около нуля, направленного входа нет."
    else:
        opposite_bars = _opposite_ao_bars(frame, index, position_before)
        if position_before == DIRECTION_LONG:
            best_price = max(best_price, _number(row["high"], close_price))
            worst_price = min(worst_price, _number(row["low"], close_price))
        else:
            best_price = min(best_price, _number(row["low"], close_price))
            worst_price = max(worst_price, _number(row["high"], close_price))

        favorable_ao_magnitude = (
            max(0.0, ao_value)
            if position_before == DIRECTION_LONG
            else max(0.0, -ao_value)
        )
        peak_ao_magnitude = max(peak_ao_magnitude, favorable_ao_magnitude)
        ao_peak_retention_ratio = (
            favorable_ao_magnitude / peak_ao_magnitude
            if peak_ao_magnitude > 0.0
            else 0.0
        )
        previous_close = _number(previous_row["close"], close_price)
        price_confirms_exit = (
            close_price < previous_close
            if position_before == DIRECTION_LONG
            else close_price > previous_close
        )
        ao_crossed_against_position = (
            ao_value <= 0.0
            if position_before == DIRECTION_LONG
            else ao_value >= 0.0
        )
        momentum_exhausted = (
            opposite_bars >= 3
            and ao_peak_retention_ratio <= exit_ao_retention_ratio
            and price_confirms_exit
        )
        if ao_crossed_against_position or momentum_exhausted:
            decision = DECISION_EXIT
            position_after = POSITION_FLAT
            if ao_crossed_against_position:
                reason = (
                    "AO пересёк ноль против открытой позиции. Защитный выход выполнен "
                    "на окончании часовой свечи."
                )
            else:
                reason = (
                    f"Три последовательных столбца AO ослабляют движение, импульс сохранил "
                    f"только {ao_peak_retention_ratio * 100.0:.1f}% от пика, и цена подтвердила замедление. "
                    "Теневая позиция закрыта на окончании часовой свечи."
                )
        else:
            decision = DECISION_HOLD
            position_after = position_before
            reason = (
                f"Направление удерживается; ослаблений AO подряд: {opposite_bars} из 3, "
                f"сохранено {ao_peak_retention_ratio * 100.0:.1f}% импульса от пика. "
                f"Поток объёма Чайкина: {chaikin_status.lower()}."
            )

    gross_result = None
    estimated_commission = None
    estimated_net = None
    best_result = None
    capture_pct = None
    if entry_price > 0.0 and position_before in {DIRECTION_LONG, DIRECTION_SHORT}:
        gross_result = _pnl_rub(entry_price, close_price, position_before, point_value)
        estimated_commission = _estimated_round_trip_commission(
            entry_price,
            close_price,
            point_value,
            commission_rate,
        )
        estimated_net = gross_result - estimated_commission
        best_result = _pnl_rub(entry_price, best_price, position_before, point_value)
        if decision == DECISION_EXIT and best_result > 0.0:
            capture_pct = max(0.0, min(100.0, gross_result / best_result * 100.0))

    recorded_at = datetime.now(timezone.utc).astimezone(MOSCOW_TZ).isoformat()
    candle_closed_at = _iso_moscow(row["candle_closed_at"])
    return {
        "version": STRATEGY_VERSION,
        "key": f"{symbol.upper()}:{candle_closed_at}",
        "recorded_at": recorded_at,
        "candle_closed_at": candle_closed_at,
        "symbol": symbol.upper(),
        "decision": decision,
        "direction": direction,
        "position_before": position_before,
        "position_after": position_after,
        "price": round(close_price, 6),
        "ao": round(ao_value, 6),
        "previous_ao": round(previous_ao, 6),
        "ao_strength_pct": round(strength_pct, 4),
        "atr": round(atr, 6),
        "ao_strength_atr_ratio": round(strength_atr_ratio, 4),
        "minimum_strength_atr_ratio": round(minimum_strength_atr_ratio, 4),
        "peak_ao_magnitude": round(peak_ao_magnitude, 6),
        "ao_peak_retention_ratio": round(ao_peak_retention_ratio, 4) if ao_peak_retention_ratio is not None else None,
        "exit_ao_retention_ratio": round(exit_ao_retention_ratio, 4),
        "price_confirms_exit": price_confirms_exit,
        "opposite_ao_bars": opposite_bars,
        "chaikin": round(chaikin_value, 6),
        "chaikin_change": round(chaikin_value - previous_chaikin, 6),
        "chaikin_status": chaikin_status,
        "reason": reason,
        "entry_time": entry_time,
        "entry_price": round(entry_price, 6) if entry_price > 0.0 else None,
        "best_price": round(best_price, 6) if entry_price > 0.0 else None,
        "worst_price": round(worst_price, 6) if entry_price > 0.0 else None,
        "gross_result_rub_1lot": round(gross_result, 2) if gross_result is not None else None,
        "estimated_commission_rub_1lot": round(estimated_commission, 2) if estimated_commission is not None else None,
        "estimated_net_rub_1lot": round(estimated_net, 2) if estimated_net is not None else None,
        "best_result_rub_1lot": round(best_result, 2) if best_result is not None else None,
        "capture_pct": round(capture_pct, 1) if capture_pct is not None else None,
        "quantity_basis": "1 лот для диагностики",
        "commission_basis": "оценка по ставке брокера",
    }


def read_shadow_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for line in lines:
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(row, dict) and row.get("symbol") and row.get("candle_closed_at"):
            records.append(row)
    return records


def _parse_iso_datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=MOSCOW_TZ)


def _average_capture_pct(rows: list[dict[str, Any]], *, shadow: bool) -> float | None:
    captures: list[float] = []
    for row in rows:
        if shadow:
            if row.get("capture_pct") is not None:
                captures.append(max(0.0, min(100.0, _number(row.get("capture_pct")))))
            continue
        mfe = _number(row.get("mfe_pct"))
        if mfe <= 0.0:
            continue
        realized = _number(row.get("realized_price_pct"))
        captures.append(max(0.0, min(100.0, realized / mfe * 100.0)))
    return round(statistics.mean(captures), 1) if captures else None


def _strategy_metrics(
    rows: list[dict[str, Any]],
    *,
    shadow: bool,
) -> dict[str, Any]:
    net_values: list[float] = []
    gross_values: list[float] = []
    commission_values: list[float] = []
    for row in rows:
        if shadow:
            net_values.append(_number(row.get("estimated_net_rub_1lot")))
            gross_values.append(_number(row.get("gross_result_rub_1lot")))
            commission_values.append(_number(row.get("estimated_commission_rub_1lot")))
            continue
        quantity = max(1.0, abs(_number(row.get("qty_lots"), 1.0)))
        net = _number(row.get("pnl_rub")) / quantity
        commission = _number(row.get("commission_rub")) / quantity
        net_values.append(net)
        gross_values.append(net + commission)
        commission_values.append(commission)

    wins = [value for value in net_values if value > 0.0]
    losses = [value for value in net_values if value < 0.0]
    return {
        "closed_trades": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(rows) * 100.0, 1) if rows else None,
        "gross_result_rub_1lot": round(sum(gross_values), 2),
        "commission_rub_1lot": round(sum(commission_values), 2),
        "net_result_rub_1lot": round(sum(net_values), 2),
        "average_result_rub_1lot": round(statistics.mean(net_values), 2) if net_values else None,
        "median_result_rub_1lot": round(statistics.median(net_values), 2) if net_values else None,
        "average_win_rub_1lot": round(statistics.mean(wins), 2) if wins else None,
        "average_loss_rub_1lot": round(statistics.mean(losses), 2) if losses else None,
        "average_capture_pct": _average_capture_pct(rows, shadow=shadow),
    }


def _shadow_point_value(row: dict[str, Any]) -> float | None:
    entry_price = _number(row.get("entry_price"))
    exit_price = _number(row.get("price"))
    direction = str(row.get("direction") or "").upper()
    direction_sign = 1.0 if direction == DIRECTION_LONG else -1.0
    price_move = (exit_price - entry_price) * direction_sign
    gross_result = _number(row.get("gross_result_rub_1lot"))
    if abs(price_move) > 1e-12 and abs(gross_result) > 1e-12:
        return abs(gross_result / price_move)

    best_price = _number(row.get("best_price"))
    best_move = (best_price - entry_price) * direction_sign
    best_result = _number(row.get("best_result_rub_1lot"))
    if abs(best_move) > 1e-12 and abs(best_result) > 1e-12:
        return abs(best_result / best_move)
    return None


def build_shadow_exit_analytics(
    shadow_records: list[dict[str, Any]],
    *,
    horizons: tuple[int, ...] = (1, 2, 4, 8),
) -> dict[str, Any]:
    records_with_time = [
        (row, _parse_iso_datetime(row.get("candle_closed_at")))
        for row in shadow_records
        if int(_number(row.get("version"))) == STRATEGY_VERSION
    ]
    records_with_time = [(row, value) for row, value in records_with_time if value is not None]
    records_with_time.sort(key=lambda item: item[1])
    by_symbol: dict[str, list[tuple[dict[str, Any], datetime]]] = {}
    for row, candle_time in records_with_time:
        by_symbol.setdefault(str(row.get("symbol") or "").upper(), []).append((row, candle_time))

    exits = [
        (row, candle_time)
        for row, candle_time in records_with_time
        if row.get("decision") == DECISION_EXIT
        and row.get("exit_kind") != "СМЕНА КОНТРАКТА"
    ]
    aggregates = {
        hours: {"actual": [], "held": [], "deltas": []}
        for hours in horizons
        if hours > 0
    }
    trade_rows: list[dict[str, Any]] = []
    for exit_row, exit_time in exits:
        symbol = str(exit_row.get("symbol") or "").upper()
        point_value = _shadow_point_value(exit_row)
        if point_value is None:
            continue
        direction_sign = 1.0 if str(exit_row.get("direction") or "").upper() == DIRECTION_LONG else -1.0
        exit_price = _number(exit_row.get("price"))
        actual_net = _number(exit_row.get("estimated_net_rub_1lot"))
        future_rows = [
            future_row
            for future_row, future_time in by_symbol.get(symbol, [])
            if future_time > exit_time and future_row.get("price") is not None
        ]
        holds: dict[str, dict[str, Any]] = {}
        for hours, aggregate in aggregates.items():
            if len(future_rows) < hours:
                continue
            future = future_rows[hours - 1]
            delta = (_number(future.get("price")) - exit_price) * direction_sign * point_value
            held_net = actual_net + delta
            aggregate["actual"].append(actual_net)
            aggregate["held"].append(held_net)
            aggregate["deltas"].append(delta)
            holds[str(hours)] = {
                "price": round(_number(future.get("price")), 8),
                "candle_closed_at": future.get("candle_closed_at"),
                "net_result_rub_1lot": round(held_net, 2),
                "delta_rub_1lot": round(delta, 2),
            }
        trade_rows.append(
            {
                "key": exit_row.get("key"),
                "symbol": symbol,
                "direction": exit_row.get("direction"),
                "entry_time": exit_row.get("entry_time"),
                "exit_time": exit_row.get("candle_closed_at"),
                "actual_net_rub_1lot": round(actual_net, 2),
                "best_result_rub_1lot": round(_number(exit_row.get("best_result_rub_1lot")), 2),
                "capture_pct": exit_row.get("capture_pct"),
                "exit_reason": exit_row.get("reason"),
                "holds": holds,
            }
        )

    horizon_rows: list[dict[str, Any]] = []
    for hours, aggregate in aggregates.items():
        deltas = aggregate["deltas"]
        better = sum(1 for value in deltas if value > 0.01)
        worse = sum(1 for value in deltas if value < -0.01)
        horizon_rows.append(
            {
                "additional_hours": hours,
                "evaluated": len(deltas),
                "better": better,
                "worse": worse,
                "unchanged": len(deltas) - better - worse,
                "better_pct": round(better / len(deltas) * 100.0, 1) if deltas else None,
                "actual_net_rub_1lot": round(sum(aggregate["actual"]), 2),
                "held_net_rub_1lot": round(sum(aggregate["held"]), 2),
                "delta_rub_1lot": round(sum(deltas), 2),
                "average_delta_rub_1lot": round(statistics.mean(deltas), 2) if deltas else None,
            }
        )

    evaluated_horizons = [row for row in horizon_rows if row["evaluated"]]
    best_horizon = max(evaluated_horizons, key=lambda row: row["delta_rub_1lot"], default=None)
    captures = [
        _number(row.get("capture_pct"))
        for row, _ in exits
        if row.get("capture_pct") is not None
    ]
    gave_back_profit = sum(
        1
        for row, _ in exits
        if _number(row.get("estimated_net_rub_1lot")) < 0.0
        and _number(row.get("best_result_rub_1lot"))
        > _number(row.get("estimated_commission_rub_1lot"))
    )
    trade_rows.sort(key=lambda row: str(row.get("exit_time") or ""), reverse=True)
    return {
        "available": bool(exits),
        "basis": "следующие закрытые часовые свечи, один лот",
        "closed_trades": len(exits),
        "average_capture_pct": round(statistics.mean(captures), 1) if captures else None,
        "losses_after_profitable_move": gave_back_profit,
        "horizons": horizon_rows,
        "best_horizon": best_horizon if best_horizon and best_horizon["delta_rub_1lot"] > 0.0 else None,
        "trades": trade_rows[:60],
    }


def build_shadow_strategy_comparison(
    shadow_records: list[dict[str, Any]],
    live_trades: list[dict[str, Any]],
    *,
    history_symbol_resolver: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    resolver = history_symbol_resolver or (lambda symbol: symbol)
    current_version_records = [
        row
        for row in shadow_records
        if int(_number(row.get("version"))) == STRATEGY_VERSION
        and _parse_iso_datetime(row.get("candle_closed_at")) is not None
    ]
    if not current_version_records:
        return {"available": False, "basis": "один лот", "by_symbol": []}

    period_start = min(
        _parse_iso_datetime(row.get("candle_closed_at"))
        for row in current_version_records
    )
    period_end = max(
        _parse_iso_datetime(row.get("candle_closed_at"))
        for row in current_version_records
    )
    assert period_start is not None and period_end is not None
    shadow_closed = [row for row in current_version_records if row.get("decision") == DECISION_EXIT]
    matching_live: list[dict[str, Any]] = []
    for row in live_trades:
        exit_time = _parse_iso_datetime(row.get("exit_time"))
        # Сравниваем реализованные результаты в окне наблюдения: вход мог быть
        # раньше запуска теневого журнала, но закрытие уже относится к периоду.
        if exit_time is not None and period_start <= exit_time <= period_end:
            matching_live.append(row)

    shadow_by_symbol: dict[str, list[dict[str, Any]]] = {}
    live_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in shadow_closed:
        symbol = resolver(str(row.get("symbol") or "").upper())
        shadow_by_symbol.setdefault(symbol, []).append(row)
    for row in matching_live:
        symbol = resolver(str(row.get("symbol") or "").upper())
        live_by_symbol.setdefault(symbol, []).append(row)

    by_symbol: list[dict[str, Any]] = []
    for symbol in sorted(set(shadow_by_symbol) | set(live_by_symbol)):
        by_symbol.append(
            {
                "symbol": symbol,
                "current": _strategy_metrics(live_by_symbol.get(symbol, []), shadow=False),
                "shadow": _strategy_metrics(shadow_by_symbol.get(symbol, []), shadow=True),
            }
        )

    shadow_losses = [row for row in shadow_closed if _number(row.get("estimated_net_rub_1lot")) < 0.0]
    profitable_before_loss = sum(
        1
        for row in shadow_losses
        if _number(row.get("best_result_rub_1lot"))
        > _number(row.get("estimated_commission_rub_1lot"))
    )
    current_metrics = _strategy_metrics(matching_live, shadow=False)
    shadow_metrics = _strategy_metrics(shadow_closed, shadow=True)
    current_metrics["actual_net_result_rub"] = round(
        sum(_number(row.get("pnl_rub")) for row in matching_live),
        2,
    )
    return {
        "available": True,
        "basis": "закрытия за период, один лот после комиссии",
        "period_start": period_start.astimezone(MOSCOW_TZ).isoformat(),
        "period_end": period_end.astimezone(MOSCOW_TZ).isoformat(),
        "current": current_metrics,
        "shadow": shadow_metrics,
        "difference": {
            "win_rate_pct_points": round(
                _number(shadow_metrics.get("win_rate_pct"))
                - _number(current_metrics.get("win_rate_pct")),
                1,
            ),
            "net_result_rub_1lot": round(
                _number(shadow_metrics.get("net_result_rub_1lot"))
                - _number(current_metrics.get("net_result_rub_1lot")),
                2,
            ),
        },
        "exit_diagnostics": {
            "losses_after_profitable_move": profitable_before_loss,
            "losses_total": len(shadow_losses),
        },
        "by_symbol": by_symbol,
    }


def _build_rollover_exit(previous: dict[str, Any], replacement_symbol: str) -> dict[str, Any]:
    row = dict(previous)
    symbol = str(previous.get("symbol") or "").upper()
    candle_closed_at = str(previous.get("candle_closed_at") or "")
    gross_result = _number(previous.get("gross_result_rub_1lot"))
    best_result = _number(previous.get("best_result_rub_1lot"))
    capture_pct = None
    if best_result > 0.0:
        capture_pct = max(0.0, min(100.0, gross_result / best_result * 100.0))
    row.update(
        {
            "version": STRATEGY_VERSION,
            "key": f"{symbol}:{candle_closed_at}:rollover:{replacement_symbol}",
            "recorded_at": datetime.now(timezone.utc).astimezone(MOSCOW_TZ).isoformat(),
            "decision": DECISION_EXIT,
            "position_before": previous.get("position_after"),
            "position_after": POSITION_FLAT,
            "exit_kind": "СМЕНА КОНТРАКТА",
            "rollover_to_symbol": replacement_symbol,
            "reason": (
                f"Контракт {symbol} заменён на {replacement_symbol}. Теневая позиция закрыта "
                "по последней доступной цене старого контракта и не переносится между разными ценовыми шкалами."
            ),
            "capture_pct": round(capture_pct, 1) if capture_pct is not None else None,
        }
    )
    return row


def normalize_shadow_record_for_display(row: dict[str, Any]) -> dict[str, Any]:
    """Resolve retired symbols in a journal row returned to a user-facing view."""
    displayed = dict(row)
    raw_symbol = str(row.get("symbol") or "")
    active_symbol = get_instrument_history_symbol(raw_symbol)
    displayed["symbol"] = active_symbol
    raw_key = str(row.get("key") or "")
    if raw_key:
        key_symbol, separator, remainder = raw_key.partition(":")
        displayed["key"] = (
            f"{get_instrument_history_symbol(key_symbol)}{separator}{remainder}"
            if separator
            else get_instrument_history_symbol(key_symbol)
        )
    if row.get("exit_kind") == "СМЕНА КОНТРАКТА" and raw_symbol.upper() != active_symbol:
        displayed["reason"] = (
            f"Контракт заменён на {active_symbol}. Теневая позиция закрыта "
            "по последней доступной цене до rollover и не переносится между разными ценовыми шкалами."
        )
    return displayed


class AoChaikinShadowJournal:
    def __init__(
        self,
        path: Path,
        *,
        history_symbol_resolver: Callable[[str], str] | None = None,
    ) -> None:
        self.path = path
        self.history_symbol_resolver = history_symbol_resolver or (lambda symbol: symbol)
        self.records = read_shadow_records(path)
        self.latest_by_symbol: dict[str, dict[str, Any]] = {}
        current_records = [
            row
            for row in self.records
            if int(_number(row.get("version"))) == STRATEGY_VERSION
        ]
        for row in sorted(current_records, key=lambda item: str(item.get("candle_closed_at") or "")):
            self.latest_by_symbol[str(row.get("symbol") or "").upper()] = row

    def _append(self, row: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.records.append(row)
        self.latest_by_symbol[str(row["symbol"]).upper()] = row

    def observe(
        self,
        *,
        symbol: str,
        candles: pd.DataFrame,
        point_value: float,
        minimum_strength_atr_ratio: float = DEFAULT_MINIMUM_STRENGTH_ATR_RATIO,
        exit_ao_retention_ratio: float = DEFAULT_EXIT_AO_RETENTION_RATIO,
        commission_rate: float = 0.00025,
    ) -> list[dict[str, Any]]:
        frame = prepare_shadow_indicators(candles)
        if len(frame) < 2:
            return []

        symbol = symbol.upper()
        created: list[dict[str, Any]] = []
        history_symbol = self.history_symbol_resolver(symbol)
        for previous_symbol, stale_row in list(self.latest_by_symbol.items()):
            if previous_symbol == symbol:
                continue
            if self.history_symbol_resolver(previous_symbol) != history_symbol:
                continue
            if str(stale_row.get("position_after") or "") not in {DIRECTION_LONG, DIRECTION_SHORT}:
                continue
            rollover_exit = _build_rollover_exit(stale_row, symbol)
            self._append(rollover_exit)
            created.append(rollover_exit)

        previous = self.latest_by_symbol.get(symbol)
        previous_time = str((previous or {}).get("candle_closed_at") or "")
        indices: list[int] = []
        if not previous_time:
            indices = [len(frame) - 1]
        else:
            indices = [
                index
                for index in range(1, len(frame))
                if _iso_moscow(frame.iloc[index]["candle_closed_at"]) > previous_time
            ]

        for index in indices:
            row = evaluate_shadow_candle(
                frame,
                index,
                previous,
                symbol=symbol,
                point_value=point_value,
                minimum_strength_atr_ratio=minimum_strength_atr_ratio,
                exit_ao_retention_ratio=exit_ao_retention_ratio,
                commission_rate=commission_rate,
            )
            if str((previous or {}).get("key") or "") == str(row.get("key") or ""):
                continue
            self._append(row)
            previous = row
            created.append(row)
        return created


def build_shadow_strategy_payload(
    path: Path,
    *,
    enabled: bool,
    period_days: int = 30,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc).astimezone(MOSCOW_TZ)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=MOSCOW_TZ)
    cutoff = current_time.astimezone(MOSCOW_TZ) - timedelta(days=max(1, period_days))
    all_records = [
        normalize_shadow_record_for_display(row)
        for row in read_shadow_records(path)
        if int(_number(row.get("version"))) == STRATEGY_VERSION
    ]

    def parsed_time(row: dict[str, Any]) -> datetime:
        try:
            value = datetime.fromisoformat(str(row.get("candle_closed_at") or ""))
        except ValueError:
            return datetime.min.replace(tzinfo=MOSCOW_TZ)
        return value if value.tzinfo else value.replace(tzinfo=MOSCOW_TZ)

    records = [row for row in all_records if parsed_time(row).astimezone(MOSCOW_TZ) >= cutoff]
    records.sort(key=lambda item: (str(item.get("candle_closed_at") or ""), str(item.get("recorded_at") or "")), reverse=True)
    closed = [row for row in records if row.get("decision") == DECISION_EXIT]
    entries = [row for row in records if row.get("decision") == DECISION_ENTRY]
    no_entries = [row for row in records if row.get("decision") == DECISION_NO_ENTRY]
    net_values = [_number(row.get("estimated_net_rub_1lot")) for row in closed]
    captures = [_number(row.get("capture_pct")) for row in closed if row.get("capture_pct") is not None]

    latest_by_symbol: dict[str, dict[str, Any]] = {}
    for row in records:
        latest_by_symbol.setdefault(str(row.get("symbol") or "").upper(), row)
    open_positions = [
        row
        for row in latest_by_symbol.values()
        if str(row.get("position_after") or "") in {DIRECTION_LONG, DIRECTION_SHORT}
    ]
    open_positions.sort(key=lambda item: str(item.get("candle_closed_at") or ""), reverse=True)
    confirming_entries = sum(1 for row in entries if row.get("chaikin_status") == CHAIKIN_CONFIRMS)
    contradictory_entries = sum(1 for row in entries if row.get("chaikin_status") == CHAIKIN_CONTRADICTS)
    configured_strength_atr_ratio = next(
        (
            _number(row.get("minimum_strength_atr_ratio"), DEFAULT_MINIMUM_STRENGTH_ATR_RATIO)
            for row in records
            if row.get("minimum_strength_atr_ratio") is not None
        ),
        DEFAULT_MINIMUM_STRENGTH_ATR_RATIO,
    )
    configured_exit_ao_retention_ratio = next(
        (
            _number(row.get("exit_ao_retention_ratio"), DEFAULT_EXIT_AO_RETENTION_RATIO)
            for row in records
            if row.get("exit_ao_retention_ratio") is not None
        ),
        DEFAULT_EXIT_AO_RETENTION_RATIO,
    )

    return {
        "available": bool(records),
        "enabled": enabled,
        "generated_at": current_time.astimezone(MOSCOW_TZ).isoformat(),
        "period_days": max(1, period_days),
        "settings": {
            "timeframe": "1 час",
            "ao_periods": "5 и 34",
            "chaikin_periods": "5 и 20",
            "entry_rule": "две закрытые усиливающиеся свечи AO сразу после пересечения нуля",
            "minimum_strength_atr_ratio": configured_strength_atr_ratio,
            "exit_ao_retention_ratio": configured_exit_ao_retention_ratio,
            "exit_rule": "три ослабления AO, остаток не более 70% от пика и подтверждение ценой; защитный выход при пересечении нуля",
            "quantity_basis": "1 лот для диагностики",
        },
        "summary": {
            "checks": len(records),
            "entries": len(entries),
            "no_entries": len(no_entries),
            "closed_trades": len(closed),
            "open_positions": len(open_positions),
            "wins": sum(1 for value in net_values if value > 0.0),
            "losses": sum(1 for value in net_values if value < 0.0),
            "net_result_rub_1lot": round(sum(net_values), 2),
            "win_rate_pct": round(sum(1 for value in net_values if value > 0.0) / len(net_values) * 100.0, 1) if net_values else None,
            "average_capture_pct": round(sum(captures) / len(captures), 1) if captures else None,
            "chaikin_confirming_entries": confirming_entries,
            "chaikin_contradictory_entries": contradictory_entries,
        },
        "open_positions": open_positions[:30],
        "closed_trades": closed[:50],
        "decisions": records[:100],
    }
