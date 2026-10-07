from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any

import pandas as pd

from strategies.v35_signals import V35Candidate


RISK_MODEL_VERSION = "four-phase-risk-v35-frozen"
RISK_BUDGET_RUB = 1_000.0
COMMISSION_RATE_PER_SIDE = 0.00025
BREAKEVEN_TRIGGER_R = 1.0
PROFIT_TRIGGER_R = 1.25
TRAIL_DISTANCE_ATR = 1.2
TRAIL_LOOKBACK_30M = 3
INITIAL_STOP_MIN_ATR = 0.55
INITIAL_STOP_BUFFER_ATR = 0.10
OVERNIGHT_GAP_MINUTES = 120


@dataclass(frozen=True)
class ProtectionState:
    direction: str
    entry_price: float
    initial_stop: float
    current_stop: float
    initial_distance: float
    phase: str = "RISK"
    best_price: float = 0.0
    best_move: float = 0.0
    worst_move: float = 0.0
    breakeven_armed: bool = False
    profit_trail_armed: bool = False
    last_trail_30m: str = ""
    exit_reason: str = ""
    exit_price: float | None = None
    stop_gap_points: float = 0.0


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if pd.notna(result) else default


def _sign(direction: str) -> float:
    return 1.0 if direction == "LONG" else -1.0


def adverse_slippage_price(
    price: float,
    direction: str,
    tick_size: float,
    ticks: float,
    *,
    is_entry: bool,
) -> float:
    if tick_size <= 0 or ticks <= 0:
        return float(price)
    adjustment = tick_size * ticks * (_sign(direction) if is_entry else -_sign(direction))
    return round(float(price) + adjustment, 12)


def round_initial_stop(raw_stop: float, direction: str, tick_size: float) -> float:
    if tick_size <= 0:
        return raw_stop
    ticks = raw_stop / tick_size
    rounded = math.floor(ticks) * tick_size if direction == "LONG" else math.ceil(ticks) * tick_size
    return round(rounded, 12)


def initial_stop_for_candidate(
    candidate: V35Candidate,
    hourly: pd.DataFrame,
    entry_price: float,
    tick_size: float,
) -> float:
    signal_time = pd.Timestamp(candidate.signal_time)
    context = hourly.loc[hourly["closed_at"] <= signal_time].tail(3)
    if context.empty:
        raise ValueError("No completed hourly candles before candidate")
    sign = _sign(candidate.direction)
    if candidate.direction == "LONG":
        structure_stop = _finite(context["low"].min()) - candidate.atr * INITIAL_STOP_BUFFER_ATR
        minimum_stop = entry_price - candidate.atr * INITIAL_STOP_MIN_ATR
        raw_stop = min(structure_stop, minimum_stop)
    else:
        structure_stop = _finite(context["high"].max()) + candidate.atr * INITIAL_STOP_BUFFER_ATR
        minimum_stop = entry_price + candidate.atr * INITIAL_STOP_MIN_ATR
        raw_stop = max(structure_stop, minimum_stop)
    stop = round_initial_stop(raw_stop, candidate.direction, tick_size)
    if (entry_price - stop) * sign <= 0:
        raise ValueError("Initial stop must be on the loss side of entry")
    return stop


def position_size_for_risk(
    entry_price: float,
    stop_price: float,
    point_value: float,
    risk_budget_rub: float = RISK_BUDGET_RUB,
    commission_rate_per_side: float = COMMISSION_RATE_PER_SIDE,
) -> tuple[int, float]:
    per_lot_commission = (
        abs(entry_price * point_value) + abs(stop_price * point_value)
    ) * commission_rate_per_side
    per_lot_risk = abs(entry_price - stop_price) * point_value + per_lot_commission
    if per_lot_risk <= 0:
        return 0, 0.0
    return max(0, int(math.floor(risk_budget_rub / per_lot_risk))), per_lot_risk


def breakeven_stop_price(
    entry_price: float,
    direction: str,
    tick_size: float,
    *,
    commission_rate_per_side: float = COMMISSION_RATE_PER_SIDE,
    exit_slippage_ticks: float = 1.0,
) -> float:
    rate = commission_rate_per_side
    if direction == "LONG":
        raw = entry_price * (1 + rate) / (1 - rate)
        breakeven = math.ceil(raw / tick_size) * tick_size if tick_size > 0 else raw
    else:
        raw = entry_price * (1 - rate) / (1 + rate)
        breakeven = math.floor(raw / tick_size) * tick_size if tick_size > 0 else raw
    return adverse_slippage_price(
        breakeven, direction, tick_size, exit_slippage_ticks, is_entry=True
    )


def new_protection_state(direction: str, entry_price: float, initial_stop: float) -> ProtectionState:
    distance = abs(entry_price - initial_stop)
    if distance <= 0 or (entry_price - initial_stop) * _sign(direction) <= 0:
        raise ValueError("Invalid initial risk distance")
    return ProtectionState(
        direction=direction,
        entry_price=entry_price,
        initial_stop=initial_stop,
        current_stop=initial_stop,
        initial_distance=distance,
        best_price=entry_price,
    )


def _tighten_stop(current: float, proposed: float, direction: str) -> float:
    return max(current, proposed) if direction == "LONG" else min(current, proposed)


def trailing_stop(
    candidate: V35Candidate,
    completed_30m: pd.DataFrame,
    best_price: float,
    current_stop: float,
    tick_size: float,
) -> float:
    recent = completed_30m.tail(TRAIL_LOOKBACK_30M)
    if len(recent) < TRAIL_LOOKBACK_30M:
        return current_stop
    if candidate.direction == "LONG":
        raw = best_price - TRAIL_DISTANCE_ATR * candidate.atr
        rounded = math.floor(raw / tick_size) * tick_size if tick_size > 0 else raw
    else:
        raw = best_price + TRAIL_DISTANCE_ATR * candidate.atr
        rounded = math.ceil(raw / tick_size) * tick_size if tick_size > 0 else raw
    return _tighten_stop(current_stop, round(rounded, 12), candidate.direction)


def stop_fill_price(direction: str, stop_price: float, bar_open: float) -> float:
    return min(stop_price, bar_open) if direction == "LONG" else max(stop_price, bar_open)


def advance_on_15m_bar(
    state: ProtectionState,
    candidate: V35Candidate,
    bar: pd.Series,
    completed_30m: pd.DataFrame,
    tick_size: float,
) -> ProtectionState:
    """Advance one closed 15m bar. The stop is checked before intrabar extrema."""
    if state.exit_reason:
        return state
    sign = _sign(state.direction)
    low = _finite(bar["low"])
    high = _finite(bar["high"])
    bar_open = _finite(bar["open"])
    stop_hit = low <= state.current_stop if state.direction == "LONG" else high >= state.current_stop
    if stop_hit:
        raw_fill = stop_fill_price(state.direction, state.current_stop, bar_open)
        reason = {
            "RISK": "RISK_STOP",
            "BREAKEVEN": "BREAKEVEN_STOP",
            "PROFIT": "PROFIT_TRAILING_STOP",
        }[state.phase]
        return replace(
            state,
            exit_reason=reason,
            exit_price=raw_fill,
            stop_gap_points=max(0.0, (state.current_stop - raw_fill) * sign),
        )

    favorable = high - state.entry_price if state.direction == "LONG" else state.entry_price - low
    adverse = state.entry_price - low if state.direction == "LONG" else high - state.entry_price
    best_move = max(state.best_move, favorable)
    worst_move = max(state.worst_move, adverse)
    best_price = max(state.best_price, high) if state.direction == "LONG" else min(state.best_price, low)
    phase = state.phase
    current_stop = state.current_stop
    breakeven_armed = state.breakeven_armed
    profit_armed = state.profit_trail_armed
    if not breakeven_armed and best_move >= state.initial_distance * BREAKEVEN_TRIGGER_R:
        breakeven_armed = True
        phase = "BREAKEVEN"
        current_stop = _tighten_stop(
            current_stop,
            breakeven_stop_price(state.entry_price, state.direction, tick_size),
            state.direction,
        )
    if not profit_armed and best_move >= state.initial_distance * PROFIT_TRIGGER_R:
        profit_armed = True
        phase = "PROFIT"
    last_trail_30m = state.last_trail_30m
    latest_30m = (
        pd.Timestamp(completed_30m.iloc[-1]["closed_at"]).isoformat()
        if not completed_30m.empty
        else ""
    )
    if profit_armed and latest_30m and latest_30m != last_trail_30m:
        current_stop = trailing_stop(candidate, completed_30m, best_price, current_stop, tick_size)
        last_trail_30m = latest_30m
    return replace(
        state,
        current_stop=current_stop,
        phase=phase,
        best_price=best_price,
        best_move=best_move,
        worst_move=worst_move,
        breakeven_armed=breakeven_armed,
        profit_trail_armed=profit_armed,
        last_trail_30m=last_trail_30m,
    )


def has_overnight_gap(bar: pd.Series, next_bar: pd.Series) -> bool:
    gap = pd.Timestamp(next_bar["time"]) - pd.Timestamp(bar["closed_at"])
    return gap >= pd.Timedelta(minutes=OVERNIGHT_GAP_MINUTES)
