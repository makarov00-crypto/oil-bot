"""Live adapter for the AO/Chaikin shadow v3 rules, without broker side effects."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from ao_chaikin_shadow import (
    DECISION_ENTRY,
    DECISION_EXIT,
    DECISION_HOLD,
    DIRECTION_LONG,
    DIRECTION_SHORT,
    evaluate_shadow_candle,
    prepare_shadow_indicators,
)

STRATEGY_NAME = "ao_chaikin_1h"
RISK_MULTIPLIER = 0.5


def is_ao_chaikin_strategy(name: Any) -> bool:
    return str(name or "").strip().lower() == STRATEGY_NAME


def _closed_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Reject malformed history rather than create a crossover by dropping bad bars."""
    required = {"time", "high", "low", "close", "volume"}
    if not isinstance(df, pd.DataFrame) or not required.issubset(df.columns):
        return pd.DataFrame()
    candles = df.copy()
    if "is_complete" in candles:
        candles = candles.loc[candles["is_complete"].eq(True)].copy()
    candles["time"] = pd.to_datetime(candles["time"], utc=True, errors="coerce")
    if candles["time"].isna().any():
        return pd.DataFrame()
    candles = candles.loc[
        candles["time"] + pd.Timedelta(hours=1) <= pd.Timestamp.now(tz="UTC")
    ].copy()
    if len(candles) < 35 or candles["time"].duplicated().any():
        return pd.DataFrame()
    for name in ("high", "low", "close", "volume"):
        candles[name] = pd.to_numeric(candles[name], errors="coerce")
        if candles[name].isna().any() or not candles[name].map(math.isfinite).all():
            return pd.DataFrame()
    if (
        (candles[["high", "low", "close"]] <= 0).any().any()
        or (candles["volume"] < 0).any()
        or (candles["high"] < candles["low"]).any()
        or (candles["close"] > candles["high"]).any()
        or (candles["close"] < candles["low"]).any()
    ):
        return pd.DataFrame()
    result = prepare_shadow_indicators(candles)
    if any(not result[name].map(math.isfinite).all() for name in ("shadow_ao", "shadow_atr", "shadow_chaikin")):
        return pd.DataFrame()
    return result


def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    """Use v3 entry rules; Chaikin and higher timeframe bias do not veto entries."""
    frame = _closed_indicators(df)
    if len(frame) < 3 or float(frame.iloc[-1]["shadow_atr"]) <= 0:
        return "HOLD", "AO/Чайкин: недостаточно корректных закрытых часовых свечей."
    result = evaluate_shadow_candle(
        frame, len(frame) - 1, None,
        symbol=str(getattr(instrument, "symbol", "")), point_value=1.0,
    )
    signal = "HOLD"
    if result["decision"] == DECISION_ENTRY:
        signal = "LONG" if result["direction"] == DIRECTION_LONG else "SHORT"
    return signal, result["reason"]


def entry_ao_peak(df: pd.DataFrame, side: str) -> float:
    """Persist the signal candle's peak before the actual fill timestamp."""
    frame = _closed_indicators(df)
    if frame.empty:
        return 0.0
    if str(side).upper() in {"LONG", DIRECTION_LONG}:
        return max(0.0, float(frame.iloc[-1]["shadow_ao"]))
    if str(side).upper() in {"SHORT", DIRECTION_SHORT}:
        return max(0.0, -float(frame.iloc[-1]["shadow_ao"]))
    return 0.0


def build_entry_context(df: pd.DataFrame) -> dict[str, float]:
    """Use the same closed AO/Chaikin candle as the live entry evaluator."""
    frame = _closed_indicators(df)
    if len(frame) < 2:
        return {}
    current, previous = frame.iloc[-1], frame.iloc[-2]
    atr = float(current["shadow_atr"])
    return {
        "ao_5_34": round(float(current["shadow_ao"]), 6),
        "ao_previous": round(float(previous["shadow_ao"]), 6),
        "ao_strength_atr_ratio": round(abs(float(current["shadow_ao"])) / atr, 4) if atr > 0 else 0.0,
        "chaikin_5_20": round(float(current["shadow_chaikin"]), 6),
        "chaikin_previous": round(float(previous["shadow_chaikin"]), 6),
    }


def evaluate_position(
    df: pd.DataFrame,
    side: str,
    entry_price: float,
    entry_time: Any,
    peak_ao_abs: float = 0.0,
) -> dict[str, Any]:
    """Evaluate the latest closed bar, recovering missed AO peaks since the fill."""
    try:
        peak = float(peak_ao_abs or 0.0)
    except (ValueError, TypeError):
        peak = 0.0
    if not math.isfinite(peak) or peak < 0:
        peak = 0.0
    hold = {
        "decision": DECISION_HOLD, "should_exit": False,
        "peak_ao_magnitude": peak,
        "reason": "AO/Чайкин: недостаточно корректных данных для выхода.",
    }
    direction = {"LONG": DIRECTION_LONG, "SHORT": DIRECTION_SHORT,
                 DIRECTION_LONG: DIRECTION_LONG, DIRECTION_SHORT: DIRECTION_SHORT}.get(str(side).upper())
    entered_at = pd.to_datetime(entry_time, utc=True, errors="coerce")
    try:
        price = float(entry_price)
    except (ValueError, TypeError):
        return hold
    if direction is None or pd.isna(entered_at) or not math.isfinite(price) or price <= 0:
        return hold
    frame = _closed_indicators(df)
    if len(frame) < 2:
        return hold
    since_entry = frame.loc[frame["candle_closed_at"] > entered_at]
    if since_entry.empty:
        hold["reason"] = "AO/Чайкин: после входа ещё не закрылась новая часовая свеча."
        return hold
    sign = 1.0 if direction == DIRECTION_LONG else -1.0
    peak = max(peak, float((since_entry["shadow_ao"] * sign).clip(lower=0).max()))
    previous = {
        "position_after": direction, "entry_price": price,
        "entry_time": entered_at.isoformat(), "peak_ao_magnitude": peak,
        "best_price": price, "worst_price": price,
    }
    result = evaluate_shadow_candle(
        frame, len(frame) - 1, previous, symbol="", point_value=1.0,
    )
    result["should_exit"] = result["decision"] == DECISION_EXIT
    # Keep the peak's full precision across restarts; shadow display rounds to 6 places.
    result["peak_ao_magnitude"] = peak
    return result
