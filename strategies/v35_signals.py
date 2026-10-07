from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from strategies.v35_market_regime import classify_market_regime, regime_allows_pattern
from strategies.v35_pattern_library import classify_entry_pattern


STRATEGY_VERSION = "four-phase-hierarchical-v35-shadow-candidate"
MOSCOW = ZoneInfo("Europe/Moscow")
LAST_NEW_ENTRY_MOSCOW_HOUR = 22
LAST_NEW_ENTRY_MOSCOW_MINUTE = 30


@dataclass
class V35Candidate:
    symbol: str
    direction: str
    signal_time: str
    signal_price: float
    atr: float
    entry_pattern: str
    directional_bars: int
    body_sum_atr: float
    largest_body_atr: float
    path_efficiency_6: float
    range_atr_6: float
    ao: float
    ao_delta: float
    ao_zero_distance_atr: float
    macd_hist: float
    macd_hist_delta: float
    chaikin: float
    classic_pattern: str = "UNCLASSIFIED"
    pattern_confidence: int = 0
    pattern_blockers: tuple[str, ...] = ()
    pattern_supports: tuple[str, ...] = ()
    pattern_matches: tuple[str, ...] = ()
    setup_time: str | None = None
    confirmation_time: str | None = None
    execution_time: str | None = None
    confirmation_score: int = 0
    execution_score: int = 0
    market_regime: str = "UNCLASSIFIED"
    regime_confidence: int = 0
    regime_direction: str = "MIXED"
    regime_gate_reason: str = ""
    regime_reasons: tuple[str, ...] = ()
    regime_features: dict[str, float | int] | None = None


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if pd.notna(result) else default


def _sign(direction: str) -> float:
    return 1.0 if direction == "LONG" else -1.0


def enrich_candles(frame: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    """Build the exact indicator columns used by the frozen v35 replay."""
    required = {"time", "open", "high", "low", "close", "volume"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing candle columns: {', '.join(sorted(missing))}")
    df = frame.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.sort_values("time").drop_duplicates("time").reset_index(drop=True)
    if "is_complete" in df.columns:
        df = df.loc[df["is_complete"] != False].reset_index(drop=True)  # noqa: E712
    for name in ("open", "high", "low", "close", "volume"):
        df[name] = pd.to_numeric(df[name], errors="coerce")
    df = df.dropna(subset=["time", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
    midpoint = (df["high"] + df["low"]) / 2.0
    df["ao"] = midpoint.rolling(5).mean() - midpoint.rolling(34).mean()
    df["ao_delta"] = df["ao"].diff()
    previous_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr"] = true_range.rolling(14).mean()
    change = df["close"].diff()
    gain = change.clip(lower=0.0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    loss = (-change.clip(upper=0.0)).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    relative_strength = gain / loss.replace(0.0, float("nan"))
    df["rsi"] = (100.0 - 100.0 / (1.0 + relative_strength)).fillna(50.0)
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
    macd = df["close"].ewm(span=12, adjust=False).mean() - df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = macd
    df["macd_signal"] = macd.ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    price_range = (df["high"] - df["low"]).replace(0.0, float("nan"))
    multiplier = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / price_range
    adl = (multiplier.fillna(0.0) * df["volume"]).cumsum()
    df["chaikin"] = adl.ewm(span=5, adjust=False).mean() - adl.ewm(span=20, adjust=False).mean()
    df["body"] = (df["close"] - df["open"]).abs()
    df["body_atr"] = df["body"] / df["atr"].replace(0.0, float("nan"))
    df["range_atr"] = (df["high"] - df["low"]) / df["atr"].replace(0.0, float("nan"))
    df["closed_at"] = df["time"] + pd.Timedelta(minutes=interval_minutes)
    return df


def entry_time_has_session_buffer(moment: pd.Timestamp) -> bool:
    local = pd.Timestamp(moment).tz_convert(MOSCOW)
    return (local.hour, local.minute) < (
        LAST_NEW_ENTRY_MOSCOW_HOUR,
        LAST_NEW_ENTRY_MOSCOW_MINUTE,
    )


def _series_sign_changes(series: pd.Series) -> int:
    values = [1 if value > 0 else -1 if value < 0 else 0 for value in series.dropna().astype(float)]
    nonzero = [value for value in values if value]
    return sum(left != right for left, right in zip(nonzero, nonzero[1:]))


def _path_efficiency(closes: pd.Series) -> float:
    values = closes.astype(float)
    if len(values) < 2:
        return 0.0
    path = float(values.diff().abs().sum())
    return abs(float(values.iloc[-1] - values.iloc[0])) / path if path > 0 else 0.0


def _recent_zero_cross(frame: pd.DataFrame, index: int, direction: str, lookback: int = 3) -> bool:
    start = max(1, index - lookback + 1)
    for position in range(start, index + 1):
        previous = _finite(frame.iloc[position - 1]["ao"])
        current = _finite(frame.iloc[position]["ao"])
        if direction == "LONG" and previous <= 0 < current:
            return True
        if direction == "SHORT" and previous >= 0 > current:
            return True
    return False


def _is_consolidation(hourly: pd.DataFrame, index: int, atr: float) -> bool:
    recent = hourly.iloc[max(0, index - 5):index + 1]
    if len(recent) < 6 or atr <= 0:
        return False
    efficiency = _path_efficiency(recent["close"])
    range_atr = (_finite(recent["high"].max()) - _finite(recent["low"].min())) / atr
    mean_body_atr = _finite(recent["body_atr"].tail(4).mean())
    ao_crosses = _series_sign_changes(recent["ao"])
    hist_crosses = _series_sign_changes(recent["macd_hist"])
    tight_rotation = efficiency < 0.32 and range_atr < 1.35
    tiny_rotation = mean_body_atr < 0.14 and range_atr < 0.85
    zero_rotation = efficiency < 0.42 and ao_crosses >= 2 and hist_crosses >= 1
    return bool(tight_rotation or tiny_rotation or zero_rotation)


def find_hourly_setups(
    symbol: str,
    hourly: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    apply_regime_filter: bool = True,
) -> list[V35Candidate]:
    candidates: list[V35Candidate] = []
    used_waves: set[tuple[str, str]] = set()
    for index in range(40, len(hourly)):
        row = hourly.iloc[index]
        signal_time = pd.Timestamp(row["closed_at"])
        if signal_time < start or signal_time > end:
            continue
        atr = _finite(row["atr"])
        if atr <= 0 or _is_consolidation(hourly, index, atr):
            continue

        last3 = hourly.iloc[index - 2:index + 1]
        bullish = last3["close"] > last3["open"]
        bearish = last3["close"] < last3["open"]
        current_body_atr = _finite(row["body_atr"])
        long_price = bool(bullish.iloc[-1])
        short_price = bool(bearish.iloc[-1])
        if not long_price and not short_price:
            continue
        direction = "LONG" if long_price else "SHORT"
        sign = _sign(direction)
        aligned = bullish if direction == "LONG" else bearish
        directional_bodies = last3.loc[aligned, "body"].astype(float)
        body_sum_atr = float(directional_bodies.sum()) / atr
        largest_body_atr = _finite(last3.loc[aligned, "body_atr"].max())
        pattern_decision = classify_entry_pattern(hourly, index, direction)
        if not pattern_decision.allowed or pattern_decision.primary is None:
            continue
        primary = pattern_decision.primary
        if primary.name == "MOMENTUM_BREAKOUT" and primary.confidence < 80:
            continue
        standard_sequence = bool(
            aligned.iloc[-1]
            and aligned.iloc[-2]
            and int(aligned.sum()) >= 2
            and body_sum_atr >= 0.60
            and largest_body_atr >= 0.24
        )
        promoted_momentum = bool(
            primary.name == "MOMENTUM_BREAKOUT"
            and primary.confidence >= 80
            and standard_sequence
        )
        single_classic_impulse = bool(
            primary.name != "MOMENTUM_BREAKOUT"
            and current_body_atr >= 0.60
            and (primary.confidence >= 84 or current_body_atr >= 0.90)
        )
        if not standard_sequence and not single_classic_impulse:
            continue

        regime = classify_market_regime(hourly, index)
        regime_allowed, regime_gate_reason = regime_allows_pattern(
            regime, direction, primary.family, primary.confidence, largest_body_atr
        )
        if apply_regime_filter and not regime_allowed:
            continue

        ao = _finite(row["ao"])
        ao_deltas = hourly.iloc[index - 1:index + 1]["ao_delta"].astype(float)
        ao_accelerating = bool((ao_deltas * sign > 0).all())
        ao_turning_now = _finite(row["ao_delta"]) * sign > 0
        histogram = _finite(row["macd_hist"])
        hist_previous = _finite(hourly.iloc[index - 1]["macd_hist"])
        hist_before = _finite(hourly.iloc[index - 2]["macd_hist"])
        hist_aligned = histogram * sign > 0
        hist_accelerating = histogram * sign > hist_previous * sign
        hist_crossed_recently = (
            hist_before * sign <= 0 < histogram * sign
            or hist_previous * sign <= 0 < histogram * sign
        )
        histogram_turning_now = (histogram - hist_previous) * sign > 0
        last2 = hourly.iloc[index - 1:index + 1]
        last2_body_atr = float(last2["body"].sum()) / atr
        last2_largest_body_atr = _finite(last2["body_atr"].max())
        previous_structure = hourly.iloc[max(0, index - 6):index]
        breaks_structure = (
            _finite(row["close"]) > _finite(previous_structure["high"].max())
            if direction == "LONG"
            else _finite(row["close"]) < _finite(previous_structure["low"].min())
        )
        impulse_break = (
            ao_turning_now
            and histogram_turning_now
            and last2_body_atr >= 1.20
            and last2_largest_body_atr >= 0.65
            and (breaks_structure or last2_body_atr >= 1.80)
        )
        ao_supports = ao * sign > 0
        crossed = _recent_zero_cross(hourly, index, direction, lookback=3)
        recent_ao = hourly.iloc[index - 4:index + 1]["ao"].astype(float)
        near_zero = float(recent_ao.abs().min()) / atr
        rejected_zero = ao_supports and near_zero <= 0.28 and ao_accelerating and not crossed
        pattern_turn = bool(
            pattern_decision.allowed
            and aligned.iloc[-1]
            and ao_turning_now
            and histogram_turning_now
            and (single_classic_impulse or promoted_momentum)
        )
        if impulse_break and not ao_supports:
            entry_pattern = "IMPULSE_BREAK"
        elif crossed and ao_accelerating and hist_aligned and (hist_accelerating or hist_crossed_recently):
            entry_pattern = "AO_ZERO_CROSS"
        elif rejected_zero and hist_aligned and (hist_accelerating or hist_crossed_recently):
            entry_pattern = "AO_ZERO_REJECTION"
        elif pattern_turn:
            entry_pattern = "PATTERN_MOMENTUM"
        else:
            continue

        recent6 = hourly.iloc[index - 5:index + 1]
        efficiency = _path_efficiency(recent6["close"])
        range_atr = (_finite(recent6["high"].max()) - _finite(recent6["low"].min())) / atr
        if entry_pattern in {"AO_ZERO_CROSS", "AO_ZERO_REJECTION"} and efficiency < 0.55:
            continue
        wave_index = index - 1
        for position in range(index - 2, max(1, index - 8), -1):
            candle_aligned = (
                _finite(hourly.iloc[position]["close"]) - _finite(hourly.iloc[position]["open"])
            ) * sign > 0
            ao_delta_aligned = _finite(hourly.iloc[position]["ao_delta"]) * sign > 0
            if not candle_aligned or not ao_delta_aligned:
                break
            wave_index = position
        wave_key = (direction, pd.Timestamp(hourly.iloc[wave_index]["closed_at"]).isoformat())
        if wave_key in used_waves:
            continue
        wave_distance_atr = (
            (_finite(row["close"]) - _finite(hourly.iloc[wave_index]["open"])) * sign / atr
        )
        maximum_wave_distance = (
            4.50
            if entry_pattern == "IMPULSE_BREAK" or primary.name == "MOMENTUM_BREAKOUT"
            else 1.80
        )
        if wave_distance_atr > maximum_wave_distance:
            used_waves.add(wave_key)
            continue
        used_waves.add(wave_key)
        candidates.append(V35Candidate(
            symbol=symbol,
            direction=direction,
            signal_time=signal_time.isoformat(),
            signal_price=_finite(row["close"]),
            atr=atr,
            entry_pattern=entry_pattern,
            directional_bars=int(aligned.sum()),
            body_sum_atr=round(body_sum_atr, 4),
            largest_body_atr=round(largest_body_atr, 4),
            path_efficiency_6=round(efficiency, 4),
            range_atr_6=round(range_atr, 4),
            ao=round(ao, 6),
            ao_delta=round(_finite(row["ao_delta"]), 6),
            ao_zero_distance_atr=round(near_zero, 4),
            macd_hist=round(histogram, 6),
            macd_hist_delta=round(histogram - hist_previous, 6),
            chaikin=round(_finite(row["chaikin"]), 4),
            classic_pattern=primary.name,
            pattern_confidence=primary.confidence,
            pattern_blockers=tuple(item.name for item in pattern_decision.blockers),
            pattern_supports=tuple(item.name for item in pattern_decision.supports),
            pattern_matches=tuple(item.name for item in pattern_decision.matches),
            setup_time=signal_time.isoformat(),
            market_regime=regime.regime,
            regime_confidence=regime.confidence,
            regime_direction=regime.direction,
            regime_gate_reason=regime_gate_reason,
            regime_reasons=regime.reasons,
            regime_features=regime.features,
        ))
    return candidates


def _find_30m_confirmation(
    frame: pd.DataFrame,
    setup_time: pd.Timestamp,
    direction: str,
    atr_1h: float,
) -> tuple[pd.Series, int] | None:
    sign = _sign(direction)
    deadline = setup_time + pd.Timedelta(hours=2)
    eligible = frame.loc[(frame["closed_at"] > setup_time) & (frame["closed_at"] <= deadline)]
    for index, row in eligible.iterrows():
        if index < 2:
            continue
        previous = frame.iloc[index - 1]
        body_aligned = (_finite(row["close"]) - _finite(row["open"])) * sign > 0
        close_progress = (_finite(row["close"]) - _finite(previous["close"])) * sign > 0
        body_material = _finite(row["body"]) / atr_1h >= 0.08
        ao_turn = _finite(row["ao_delta"]) * sign > 0
        hist_turn = (_finite(row["macd_hist"]) - _finite(previous["macd_hist"])) * sign > 0
        hist_aligned = _finite(row["macd_hist"]) * sign > 0
        score = sum((body_aligned, close_progress, body_material, ao_turn, hist_turn or hist_aligned))
        if body_aligned and close_progress and body_material and score >= 4:
            return row, score
    return None


def _find_15m_execution(
    frame: pd.DataFrame,
    confirmation_time: pd.Timestamp,
    direction: str,
    atr_1h: float,
) -> tuple[pd.Series, int] | None:
    sign = _sign(direction)
    deadline = confirmation_time + pd.Timedelta(hours=4)
    eligible = frame.loc[(frame["closed_at"] > confirmation_time) & (frame["closed_at"] <= deadline)]
    for index, row in eligible.iterrows():
        if index < 3:
            continue
        previous = frame.iloc[index - 1]
        prior = frame.iloc[index - 3:index]
        body_aligned = (_finite(row["close"]) - _finite(row["open"])) * sign > 0
        body_atr_1h = _finite(row["body"]) / atr_1h
        close_progress = (_finite(row["close"]) - _finite(previous["close"])) * sign > 0
        breaks_micro_structure = (
            _finite(row["close"]) > _finite(prior["high"].max())
            if direction == "LONG"
            else _finite(row["close"]) < _finite(prior["low"].min())
        )
        ao_turn = _finite(row["ao_delta"]) * sign > 0
        hist_turn = (_finite(row["macd_hist"]) - _finite(previous["macd_hist"])) * sign > 0
        hist_aligned = _finite(row["macd_hist"]) * sign > 0
        score = sum((body_aligned, close_progress, body_atr_1h >= 0.04, breaks_micro_structure, ao_turn, hist_turn or hist_aligned))
        price_trigger = breaks_micro_structure or body_atr_1h >= 0.16
        indicator_trigger = ao_turn or hist_turn or (body_atr_1h >= 0.16 and hist_aligned)
        if body_aligned and close_progress and price_trigger and indicator_trigger and score >= 5:
            return row, score
    return None


def find_multitimeframe_candidates(
    symbol: str,
    frames: dict[int, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    apply_regime_filter: bool = True,
) -> list[V35Candidate]:
    setups = find_hourly_setups(symbol, frames[60], start, end, apply_regime_filter=apply_regime_filter)
    result: list[V35Candidate] = []
    last_execution_by_direction: dict[str, pd.Timestamp] = {}
    for setup in setups:
        setup_time = pd.Timestamp(setup.signal_time)
        confirmation = _find_30m_confirmation(frames[30], setup_time, setup.direction, setup.atr)
        if confirmation is None:
            continue
        confirmation_row, confirmation_score = confirmation
        confirmation_time = pd.Timestamp(confirmation_row["closed_at"])
        execution = _find_15m_execution(frames[15], confirmation_time, setup.direction, setup.atr)
        if execution is None:
            continue
        execution_row, execution_score = execution
        execution_time = pd.Timestamp(execution_row["closed_at"])
        if execution_time > end or not entry_time_has_session_buffer(execution_time):
            continue
        previous_execution = last_execution_by_direction.get(setup.direction)
        if previous_execution is not None and execution_time <= previous_execution + pd.Timedelta(hours=6):
            continue
        result.append(replace(
            setup,
            signal_time=execution_time.isoformat(),
            signal_price=_finite(execution_row["close"]),
            confirmation_time=confirmation_time.isoformat(),
            execution_time=execution_time.isoformat(),
            confirmation_score=confirmation_score,
            execution_score=execution_score,
        ))
        last_execution_by_direction[setup.direction] = execution_time
    return result


def short_entry_allowed(candidate: V35Candidate, body_threshold_atr: float = 2.25) -> bool:
    if candidate.direction != "SHORT":
        return True
    return bool(candidate.execution_score >= 6 or candidate.body_sum_atr >= body_threshold_atr)
