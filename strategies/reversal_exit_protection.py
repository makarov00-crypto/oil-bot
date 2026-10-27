from __future__ import annotations

import math
from dataclasses import dataclass, replace

import pandas as pd

EXIT_MODEL_VERSION = "reversal-r-protection-v1"
COMMISSION_RATE_PER_SIDE = 0.00025
BREAKEVEN_TRIGGER_R = 1.0
PROFIT_TRIGGER_R = 1.25
TRAIL_DISTANCE_ATR = 1.2
TRAIL_LOOKBACK_30M = 3


@dataclass(frozen=True)
class ReversalProtectionState:
    direction: str
    entry_price: float
    initial_stop: float
    current_stop: float
    initial_distance: float
    entry_atr: float
    phase: str = "RISK"
    best_price: float = 0.0
    best_move: float = 0.0
    worst_move: float = 0.0
    breakeven_armed: bool = False
    profit_trail_armed: bool = False
    last_trail_30m: str = ""
    exit_reason: str = ""


@dataclass(frozen=True)
class _ExitCandidate:
    direction: str
    atr: float


def _finite(value, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if pd.notna(result) else default


def _sign(direction: str) -> float:
    return 1.0 if direction == "LONG" else -1.0


def _tighten_stop(current: float, proposed: float, direction: str) -> float:
    return max(current, proposed) if direction == "LONG" else min(current, proposed)


def _stop_hit(direction: str, price: float, stop: float) -> bool:
    return price <= stop if direction == "LONG" else price >= stop


def _adverse_slippage_price(
    price: float,
    direction: str,
    tick_size: float,
    ticks: float,
) -> float:
    if tick_size <= 0.0 or ticks <= 0.0:
        return float(price)
    return round(float(price) + tick_size * ticks * _sign(direction), 12)


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
    return _adverse_slippage_price(
        breakeven,
        direction,
        tick_size,
        exit_slippage_ticks,
    )


def _path_efficiency(closes: pd.Series) -> float:
    values = pd.to_numeric(closes, errors="coerce").dropna()
    if len(values) < 2:
        return 0.0
    path = float(values.diff().abs().sum())
    return abs(float(values.iloc[-1] - values.iloc[0])) / path if path > 0 else 0.0


def closing_pattern(
    candidate: _ExitCandidate,
    completed_30m: pd.DataFrame,
    *,
    profit_phase: bool,
    confirm_bars: int = 2,
) -> str | None:
    if len(completed_30m) < max(5, confirm_bars + 2):
        return None
    recent = completed_30m.tail(confirm_bars)
    sign = _sign(candidate.direction)
    opposite_candles = (((recent["close"] - recent["open"]) * sign) < 0).all()
    opposite_body = float(recent["body"].sum()) >= candidate.atr * 0.28
    ao_reversal = ((recent["ao_delta"] * sign) < 0).all()
    latest_histogram = _finite(recent.iloc[-1]["macd_hist"])
    previous_histogram = _finite(recent.iloc[-2]["macd_hist"])
    histogram_opposite = latest_histogram * sign < 0
    histogram_accelerating = latest_histogram * sign < previous_histogram * sign
    if (
        opposite_candles
        and opposite_body
        and ao_reversal
        and histogram_opposite
        and histogram_accelerating
    ):
        return "CONFIRMED_REVERSAL"

    if profit_phase:
        tail = completed_30m.tail(5)
        price_range = _finite(tail["high"].max()) - _finite(tail["low"].min())
        body_mean = _finite(tail["body"].mean())
        efficiency = _path_efficiency(tail["close"])
        if (
            price_range <= candidate.atr * 0.55
            and body_mean <= candidate.atr * 0.10
            and efficiency < 0.30
        ):
            return "PROFIT_CONSOLIDATION"
    return None


def new_reversal_protection_state(
    direction: str,
    entry_price: float,
    initial_stop: float,
    *,
    entry_atr: float,
) -> ReversalProtectionState:
    normalized_direction = str(direction or "").upper()
    distance = abs(float(entry_price) - float(initial_stop))
    if normalized_direction not in {"LONG", "SHORT"}:
        raise ValueError("Direction must be LONG or SHORT")
    if entry_price <= 0.0 or distance <= 0.0:
        raise ValueError("Entry and initial stop must define positive risk")
    if (entry_price - initial_stop) * _sign(normalized_direction) <= 0.0:
        raise ValueError("Initial stop must be on the loss side of entry")
    if entry_atr <= 0.0:
        raise ValueError("Entry ATR must be positive")
    return ReversalProtectionState(
        direction=normalized_direction,
        entry_price=float(entry_price),
        initial_stop=float(initial_stop),
        current_stop=float(initial_stop),
        initial_distance=distance,
        entry_atr=float(entry_atr),
        best_price=float(entry_price),
    )


def _latest_completed_30m(completed_30m: pd.DataFrame) -> str:
    if completed_30m.empty:
        return ""
    column = "closed_at" if "closed_at" in completed_30m.columns else "time"
    if column not in completed_30m.columns:
        return ""
    value = pd.to_datetime(completed_30m.iloc[-1][column], utc=True, errors="coerce")
    return "" if pd.isna(value) else value.isoformat()


def _profit_trailing_stop(
    state: ReversalProtectionState,
    completed_30m: pd.DataFrame,
    tick_size: float,
) -> float:
    if len(completed_30m) < TRAIL_LOOKBACK_30M:
        return state.current_stop
    if state.direction == "LONG":
        raw = state.best_price - TRAIL_DISTANCE_ATR * state.entry_atr
        proposed = (
            math.floor(raw / tick_size + 1e-9) * tick_size
            if tick_size > 0.0
            else raw
        )
    else:
        raw = state.best_price + TRAIL_DISTANCE_ATR * state.entry_atr
        proposed = (
            math.ceil(raw / tick_size - 1e-9) * tick_size
            if tick_size > 0.0
            else raw
        )
    return _tighten_stop(state.current_stop, round(proposed, 12), state.direction)


def advance_reversal_protection(
    state: ReversalProtectionState,
    *,
    current_price: float,
    completed_30m: pd.DataFrame | None,
    tick_size: float,
) -> ReversalProtectionState:
    """Advance the versioned post-entry protection without changing initial risk.

    The initial RISK stop remains the responsibility of the hourly reversal
    strategy. Once breakeven is armed, the returned stop is checked on every
    poll and can only move toward profit.
    """
    if state.exit_reason or current_price <= 0.0:
        return state
    frame = completed_30m if completed_30m is not None else pd.DataFrame()
    if state.phase in {"BREAKEVEN", "PROFIT"} and _stop_hit(
        state.direction,
        current_price,
        state.current_stop,
    ):
        reason = "PROFIT_TRAILING_STOP" if state.phase == "PROFIT" else "BREAKEVEN_STOP"
        return replace(state, exit_reason=reason)

    sign = _sign(state.direction)
    favorable = (current_price - state.entry_price) * sign
    adverse = (state.entry_price - current_price) * sign
    best_price = (
        max(state.best_price, current_price)
        if state.direction == "LONG"
        else min(state.best_price, current_price)
    )
    best_move = max(state.best_move, favorable)
    worst_move = max(state.worst_move, adverse)
    phase = state.phase
    current_stop = state.current_stop
    breakeven_armed = state.breakeven_armed
    profit_trail_armed = state.profit_trail_armed

    if not breakeven_armed and best_move >= state.initial_distance * BREAKEVEN_TRIGGER_R:
        breakeven_armed = True
        phase = "BREAKEVEN"
        current_stop = _tighten_stop(
            current_stop,
            breakeven_stop_price(
                state.entry_price,
                state.direction,
                tick_size,
            ),
            state.direction,
        )
    if not profit_trail_armed and best_move >= state.initial_distance * PROFIT_TRIGGER_R:
        profit_trail_armed = True
        phase = "PROFIT"

    last_trail_30m = state.last_trail_30m
    latest_30m = _latest_completed_30m(frame)
    updated = replace(
        state,
        current_stop=current_stop,
        phase=phase,
        best_price=best_price,
        best_move=best_move,
        worst_move=worst_move,
        breakeven_armed=breakeven_armed,
        profit_trail_armed=profit_trail_armed,
    )
    if profit_trail_armed and latest_30m and latest_30m != last_trail_30m:
        updated = replace(
            updated,
            current_stop=_profit_trailing_stop(updated, frame, tick_size),
            last_trail_30m=latest_30m,
        )

    if updated.phase in {"BREAKEVEN", "PROFIT"} and _stop_hit(
        updated.direction,
        current_price,
        updated.current_stop,
    ):
        reason = "PROFIT_TRAILING_STOP" if updated.phase == "PROFIT" else "BREAKEVEN_STOP"
        return replace(updated, exit_reason=reason)

    if not frame.empty:
        close_reason = closing_pattern(
            _ExitCandidate(direction=updated.direction, atr=updated.entry_atr),
            frame,
            profit_phase=updated.phase == "PROFIT",
        )
        if close_reason:
            return replace(updated, exit_reason=close_reason)
    return updated
