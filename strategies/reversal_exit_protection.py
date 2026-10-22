from __future__ import annotations

import math
from dataclasses import dataclass, replace

import pandas as pd

from strategies.v35_risk import (
    BREAKEVEN_TRIGGER_R,
    PROFIT_TRIGGER_R,
    TRAIL_DISTANCE_ATR,
    TRAIL_LOOKBACK_30M,
    breakeven_stop_price,
    closing_pattern,
)


EXIT_MODEL_VERSION = "reversal-r-protection-v1"


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


def _sign(direction: str) -> float:
    return 1.0 if direction == "LONG" else -1.0


def _tighten_stop(current: float, proposed: float, direction: str) -> float:
    return max(current, proposed) if direction == "LONG" else min(current, proposed)


def _stop_hit(direction: str, price: float, stop: float) -> bool:
    return price <= stop if direction == "LONG" else price >= stop


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
