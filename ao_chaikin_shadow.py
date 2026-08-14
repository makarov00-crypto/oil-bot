from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


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


def prepare_shadow_indicators(candles: pd.DataFrame) -> pd.DataFrame:
    """Calculate the shadow indicators without changing live strategy columns."""
    result = candles.copy()
    result["time"] = pd.to_datetime(result["time"], utc=True, errors="coerce")
    result = result.dropna(subset=["time", "high", "low", "close", "volume"])
    result = result.sort_values("time").reset_index(drop=True)
    midpoint = (result["high"].astype(float) + result["low"].astype(float)) / 2.0
    result["shadow_ao"] = midpoint.rolling(5).mean() - midpoint.rolling(34).mean()

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
    return result.dropna(subset=["shadow_ao", "shadow_chaikin"]).reset_index(drop=True)


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
    minimum_strength_pct: float = 0.7,
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
    long_pattern = previous_ao > 0.0 and ao_value > previous_ao
    short_pattern = previous_ao < 0.0 and ao_value < previous_ao

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

    if position_before == POSITION_FLAT:
        if long_pattern and strength_pct >= minimum_strength_pct:
            decision = DECISION_ENTRY
            direction = DIRECTION_LONG
        elif short_pattern and strength_pct >= minimum_strength_pct:
            decision = DECISION_ENTRY
            direction = DIRECTION_SHORT

        if decision == DECISION_ENTRY:
            position_after = direction
            entry_time = _iso_moscow(row["candle_closed_at"])
            entry_price = close_price
            best_price = close_price
            worst_price = close_price
            chaikin_status = _chaikin_status(chaikin_value, previous_chaikin, direction)
            reason = (
                f"Два последовательных столбца AO усиливаются в сторону {direction.lower()}, "
                f"сила {strength_pct:.2f}% при пороге {minimum_strength_pct:.2f}%. "
                f"Осциллятор Чайкина: {chaikin_status.lower()}."
            )
        elif (long_pattern or short_pattern) and strength_pct < minimum_strength_pct:
            reason = (
                f"Направление AO сформировано, но сила {strength_pct:.2f}% ниже "
                f"порога {minimum_strength_pct:.2f}%."
            )
        elif ao_value > 0.0:
            reason = "AO выше нуля, но двух последовательных усиливающихся столбцов для лонга нет."
        elif ao_value < 0.0:
            reason = "AO ниже нуля, но двух последовательных усиливающихся столбцов для шорта нет."
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

        if opposite_bars >= 3:
            decision = DECISION_EXIT
            position_after = POSITION_FLAT
            reason = (
                "Три последовательных столбца AO ослабляют открытое движение. "
                "Теневая позиция закрыта на окончании часовой свечи."
            )
        else:
            decision = DECISION_HOLD
            position_after = position_before
            reason = (
                f"Направление удерживается; противоположных столбцов AO подряд: {opposite_bars} из 3. "
                f"Осциллятор Чайкина: {chaikin_status.lower()}."
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
        "version": 1,
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
        "minimum_strength_pct": round(minimum_strength_pct, 4),
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


class AoChaikinShadowJournal:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.records = read_shadow_records(path)
        self.latest_by_symbol: dict[str, dict[str, Any]] = {}
        for row in sorted(self.records, key=lambda item: str(item.get("candle_closed_at") or "")):
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
        minimum_strength_pct: float = 0.7,
        commission_rate: float = 0.00025,
    ) -> list[dict[str, Any]]:
        frame = prepare_shadow_indicators(candles)
        if len(frame) < 2:
            return []

        symbol = symbol.upper()
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

        created: list[dict[str, Any]] = []
        for index in indices:
            row = evaluate_shadow_candle(
                frame,
                index,
                previous,
                symbol=symbol,
                point_value=point_value,
                minimum_strength_pct=minimum_strength_pct,
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
    all_records = read_shadow_records(path)

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
    configured_strength_pct = next(
        (
            _number(row.get("minimum_strength_pct"), 0.7)
            for row in records
            if row.get("minimum_strength_pct") is not None
        ),
        0.7,
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
            "minimum_strength_pct": configured_strength_pct,
            "exit_rule": "три последовательных столбца AO против позиции",
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
