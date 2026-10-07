from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


PATTERN_LIBRARY_VERSION = "classic-geometry-v4"

PATTERN_CATALOG: dict[str, dict[str, str]] = {
    "MOMENTUM_BREAKOUT": {"family": "BREAKOUT", "description": "Импульсный пробой структуры"},
    "RANGE_BREAKOUT": {"family": "BREAKOUT", "description": "Пробой прямоугольного диапазона"},
    "BULL_FLAG_BREAKOUT": {"family": "CONTINUATION", "description": "Бычий флаг"},
    "BEAR_FLAG_BREAKDOWN": {"family": "CONTINUATION", "description": "Медвежий флаг"},
    "BULL_PENNANT_BREAKOUT": {"family": "CONTINUATION", "description": "Бычий вымпел"},
    "BEAR_PENNANT_BREAKDOWN": {"family": "CONTINUATION", "description": "Медвежий вымпел"},
    "ASCENDING_TRIANGLE_BREAKOUT": {"family": "BREAKOUT", "description": "Восходящий треугольник"},
    "DESCENDING_TRIANGLE_BREAKDOWN": {"family": "BREAKOUT", "description": "Нисходящий треугольник"},
    "SYMMETRICAL_TRIANGLE_BREAKOUT": {"family": "BREAKOUT", "description": "Симметричный треугольник"},
    "FALLING_WEDGE_BREAKOUT": {"family": "REVERSAL", "description": "Падающий клин"},
    "RISING_WEDGE_BREAKDOWN": {"family": "REVERSAL", "description": "Растущий клин"},
    "DOUBLE_BOTTOM_BREAKOUT": {"family": "REVERSAL", "description": "Двойное дно"},
    "DOUBLE_TOP_BREAKOUT": {"family": "REVERSAL", "description": "Двойная вершина"},
    "HEAD_AND_SHOULDERS_BREAKDOWN": {"family": "REVERSAL", "description": "Голова и плечи"},
    "INVERSE_HEAD_AND_SHOULDERS_BREAKOUT": {"family": "REVERSAL", "description": "Обратная голова и плечи"},
    "FAILED_BREAKOUT_REVERSAL": {"family": "REVERSAL", "description": "Ложный пробой с возвратом"},
    "TREND_PULLBACK_RESUMPTION": {"family": "CONTINUATION", "description": "Продолжение тренда после отката"},
}

PATTERN_SPECIFICITY = {
    name: index
    for index, name in enumerate(PATTERN_CATALOG, start=1)
}


@dataclass(frozen=True)
class PatternMatch:
    name: str
    family: str
    direction: str
    confidence: int
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PatternDecision:
    allowed: bool
    primary: PatternMatch | None
    matches: tuple[PatternMatch, ...]
    blockers: tuple[PatternMatch, ...]
    supports: tuple[PatternMatch, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "primary": self.primary.as_dict() if self.primary else None,
            "matches": [item.as_dict() for item in self.matches],
            "blockers": [item.as_dict() for item in self.blockers],
            "supports": [item.as_dict() for item in self.supports],
        }


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if pd.notna(result) else default


def _sign(direction: str) -> float:
    return 1.0 if direction == "LONG" else -1.0


def _path_efficiency(closes: pd.Series) -> float:
    values = closes.astype(float)
    if len(values) < 2:
        return 0.0
    path = float(values.diff().abs().sum())
    return abs(float(values.iloc[-1] - values.iloc[0])) / path if path > 0 else 0.0


def _sign_changes(series: pd.Series) -> int:
    signs = [1 if value > 0 else -1 if value < 0 else 0 for value in series.dropna().astype(float)]
    nonzero = [value for value in signs if value]
    return sum(left != right for left, right in zip(nonzero, nonzero[1:]))


def _aligned_candles(frame: pd.DataFrame, direction: str) -> pd.Series:
    return (frame["close"].astype(float) - frame["open"].astype(float)) * _sign(direction) > 0


def _linear_slope(values: pd.Series) -> float:
    points = values.astype(float).reset_index(drop=True)
    count = len(points)
    if count < 2:
        return 0.0
    x_mean = (count - 1) / 2.0
    y_mean = _finite(points.mean())
    denominator = sum((position - x_mean) ** 2 for position in range(count))
    if denominator <= 0:
        return 0.0
    return sum(
        (position - x_mean) * (_finite(value) - y_mean)
        for position, value in enumerate(points)
    ) / denominator


def _line_fit_error(values: pd.Series, slope: float, atr: float) -> float:
    points = values.astype(float).reset_index(drop=True)
    if points.empty or atr <= 0:
        return float("inf")
    intercept = _finite(points.mean()) - slope * (len(points) - 1) / 2.0
    error = sum(
        abs(_finite(value) - (intercept + slope * position))
        for position, value in enumerate(points)
    ) / len(points)
    return error / atr


def _indicator_turn(frame: pd.DataFrame, index: int, direction: str) -> bool:
    sign = _sign(direction)
    current = frame.iloc[index]
    previous = frame.iloc[index - 1]
    return bool(
        _finite(current["ao_delta"]) * sign > 0
        and (_finite(current["macd_hist"]) - _finite(previous["macd_hist"])) * sign > 0
    )


def _local_extrema(values: pd.Series, kind: str) -> list[int]:
    points = values.astype(float).reset_index(drop=True)
    result: list[int] = []
    for position in range(1, len(points) - 1):
        left, value, right = points.iloc[position - 1], points.iloc[position], points.iloc[position + 1]
        if kind == "high" and value > left and value >= right:
            result.append(position)
        if kind == "low" and value < left and value <= right:
            result.append(position)
    return result


def _breaks_prior_structure(frame: pd.DataFrame, index: int, direction: str, lookback: int) -> bool:
    if index < lookback:
        return False
    prior = frame.iloc[index - lookback:index]
    close = _finite(frame.iloc[index]["close"])
    if direction == "LONG":
        return close > _finite(prior["high"].max())
    return close < _finite(prior["low"].min())


def _momentum_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    sign = _sign(direction)
    recent = frame.iloc[index - 1:index + 1]
    if len(recent) < 2 or not bool(_aligned_candles(recent, direction).all()):
        return None
    body_sum = float(recent["body"].sum()) / atr
    largest_body = _finite(recent["body_atr"].max())
    ao_turn = _finite(frame.iloc[index]["ao_delta"]) * sign > 0
    ao_near_zero = abs(_finite(frame.iloc[index]["ao"])) / atr <= 0.35
    histogram_turn = (
        _finite(frame.iloc[index]["macd_hist"]) - _finite(frame.iloc[index - 1]["macd_hist"])
    ) * sign > 0
    breaks = _breaks_prior_structure(frame, index, direction, 6)
    if body_sum < 1.20 or largest_body < 0.65 or not ao_turn or not ao_near_zero or not histogram_turn or not breaks:
        return None
    confidence = 65
    confidence += min(15, int(max(0.0, body_sum - 1.20) * 10))
    confidence += 10 if largest_body >= 1.0 else 5
    confidence += 5 if _finite(frame.iloc[index]["macd_hist"]) * sign > 0 else 0
    return PatternMatch(
        "MOMENTUM_BREAKOUT",
        "BREAKOUT",
        direction,
        min(95, confidence),
        ("two_directional_candles", "structure_break", "ao_near_zero", "ao_turn", "macd_hist_turn"),
    )


def _range_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 8:
        return None
    sign = _sign(direction)
    base = frame.iloc[index - 8:index]
    current = frame.iloc[index]
    width_atr = (_finite(base["high"].max()) - _finite(base["low"].min())) / atr
    efficiency = _path_efficiency(base["close"])
    body_atr = _finite(current["body_atr"])
    breaks = _breaks_prior_structure(frame, index, direction, 8)
    ao_turn = _finite(current["ao_delta"]) * sign > 0
    hist_turn = (
        _finite(current["macd_hist"]) - _finite(frame.iloc[index - 1]["macd_hist"])
    ) * sign > 0
    if width_atr > 1.80 or efficiency > 0.42 or body_atr < 0.40 or not breaks or not ao_turn or not hist_turn:
        return None
    confidence = 65
    confidence += 10 if width_atr <= 1.35 else 5
    confidence += 10 if body_atr >= 0.65 else 5
    confidence += 5 if _finite(current["macd_hist"]) * sign > 0 else 0
    return PatternMatch(
        "RANGE_BREAKOUT",
        "BREAKOUT",
        direction,
        min(90, confidence),
        ("compressed_base", "low_path_efficiency", "range_break", "indicator_acceleration"),
    )


def _trend_pullback_resumption(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 14:
        return None
    sign = _sign(direction)
    context = frame.iloc[index - 13:index - 2]
    pullback = frame.iloc[index - 4:index - 1]
    trigger = frame.iloc[index - 1:index + 1]
    context_move = (
        _finite(context.iloc[-1]["close"]) - _finite(context.iloc[0]["close"])
    ) * sign / atr
    ema_slope = (
        _finite(context.iloc[-1]["ema20"]) - _finite(context.iloc[0]["ema20"])
    ) * sign / atr
    has_pullback = bool((~_aligned_candles(pullback, direction)).any())
    resumes = bool(_aligned_candles(trigger, direction).all())
    ao_near_zero = float(frame.iloc[index - 5:index + 1]["ao"].abs().min()) / atr <= 0.35
    ao_resumes = _finite(frame.iloc[index]["ao_delta"]) * sign > 0
    hist_resumes = (
        _finite(frame.iloc[index]["macd_hist"]) - _finite(frame.iloc[index - 1]["macd_hist"])
    ) * sign > 0
    structure_held = (
        _finite(frame.iloc[index]["close"]) - _finite(frame.iloc[index]["ema50"])
    ) * sign > -0.20 * atr
    if context_move < 1.0 or ema_slope < 0.25 or not has_pullback or not resumes:
        return None
    if not ao_near_zero or not ao_resumes or not hist_resumes or not structure_held:
        return None
    confidence = 70
    confidence += 10 if context_move >= 1.75 else 5
    confidence += 5 if ema_slope >= 0.5 else 0
    confidence += 5 if _finite(frame.iloc[index]["macd_hist"]) * sign > 0 else 0
    return PatternMatch(
        "TREND_PULLBACK_RESUMPTION",
        "CONTINUATION",
        direction,
        min(90, confidence),
        ("prior_trend", "controlled_pullback", "ao_zero_rejection", "two_candle_resumption"),
    )


def _flag_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 17:
        return None
    sign = _sign(direction)
    pole = frame.iloc[index - 16:index - 7]
    flag = frame.iloc[index - 7:index]
    current = frame.iloc[index]
    pole_move = (_finite(pole.iloc[-1]["close"]) - _finite(pole.iloc[0]["open"])) * sign / atr
    pole_efficiency = _path_efficiency(pole["close"])
    high_slope = _linear_slope(flag["high"]) / atr
    low_slope = _linear_slope(flag["low"]) / atr
    close_slope = _linear_slope(flag["close"]) / atr
    first_width = _finite(flag.iloc[:3]["high"].max()) - _finite(flag.iloc[:3]["low"].min())
    last_width = _finite(flag.iloc[-3:]["high"].max()) - _finite(flag.iloc[-3:]["low"].min())
    width_ratio = last_width / first_width if first_width > 0 else 1.0
    countertrend = close_slope * sign < -0.015
    parallel_channel = abs(high_slope - low_slope) <= 0.08
    stable_width = 0.55 <= width_ratio <= 1.25
    breakout = (
        _finite(current["close"]) > _finite(flag["high"].max())
        if direction == "LONG"
        else _finite(current["close"]) < _finite(flag["low"].min())
    )
    if pole_move < 1.80 or pole_efficiency < 0.58:
        return None
    if not countertrend or not parallel_channel or not stable_width:
        return None
    if not breakout or _finite(current["body_atr"]) < 0.35 or not _indicator_turn(frame, index, direction):
        return None
    name = "BULL_FLAG_BREAKOUT" if direction == "LONG" else "BEAR_FLAG_BREAKDOWN"
    return PatternMatch(
        name,
        "CONTINUATION",
        direction,
        82,
        ("strong_pole", "countertrend_channel", "stable_channel_width", "flag_break"),
    )


def _pennant_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 19:
        return None
    sign = _sign(direction)
    pole = frame.iloc[index - 18:index - 9]
    pennant = frame.iloc[index - 9:index]
    current = frame.iloc[index]
    pole_move = (_finite(pole.iloc[-1]["close"]) - _finite(pole.iloc[0]["open"])) * sign / atr
    pole_efficiency = _path_efficiency(pole["close"])
    high_slope = _linear_slope(pennant["high"]) / atr
    low_slope = _linear_slope(pennant["low"]) / atr
    first_width = _finite(pennant.iloc[:3]["high"].max()) - _finite(pennant.iloc[:3]["low"].min())
    last_width = _finite(pennant.iloc[-3:]["high"].max()) - _finite(pennant.iloc[-3:]["low"].min())
    width_ratio = last_width / first_width if first_width > 0 else 1.0
    converging = high_slope < -0.02 and low_slope > 0.02 and width_ratio <= 0.72
    breakout = (
        _finite(current["close"]) > _finite(pennant.iloc[-3:]["high"].max()) + 0.05 * atr
        if direction == "LONG"
        else _finite(current["close"]) < _finite(pennant.iloc[-3:]["low"].min()) - 0.05 * atr
    )
    if pole_move < 1.80 or pole_efficiency < 0.58 or not converging:
        return None
    if not breakout or _finite(current["body_atr"]) < 0.35 or not _indicator_turn(frame, index, direction):
        return None
    name = "BULL_PENNANT_BREAKOUT" if direction == "LONG" else "BEAR_PENNANT_BREAKDOWN"
    return PatternMatch(
        name,
        "CONTINUATION",
        direction,
        85,
        ("strong_pole", "converging_highs_lows", "range_compression", "pennant_break"),
    )


def _triangle_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 13:
        return None
    base = frame.iloc[index - 12:index]
    current = frame.iloc[index]
    high_slope_raw = _linear_slope(base["high"])
    low_slope_raw = _linear_slope(base["low"])
    high_slope = high_slope_raw / atr
    low_slope = low_slope_raw / atr
    high_error = _line_fit_error(base["high"], high_slope_raw, atr)
    low_error = _line_fit_error(base["low"], low_slope_raw, atr)
    first_width = _finite(base.iloc[:4]["high"].max()) - _finite(base.iloc[:4]["low"].min())
    last_width = _finite(base.iloc[-4:]["high"].max()) - _finite(base.iloc[-4:]["low"].min())
    width_ratio = last_width / first_width if first_width > 0 else 1.0
    if high_error > 0.55 or low_error > 0.55 or width_ratio > 0.82:
        return None
    current_close = _finite(current["close"])
    if direction == "LONG" and abs(high_slope) <= 0.045 and low_slope >= 0.025:
        name = "ASCENDING_TRIANGLE_BREAKOUT"
        breakout = current_close > _finite(base["high"].max())
    elif direction == "SHORT" and abs(low_slope) <= 0.045 and high_slope <= -0.025:
        name = "DESCENDING_TRIANGLE_BREAKDOWN"
        breakout = current_close < _finite(base["low"].min())
    elif high_slope <= -0.025 and low_slope >= 0.025:
        name = "SYMMETRICAL_TRIANGLE_BREAKOUT"
        breakout = (
            current_close > _finite(base.iloc[-4:]["high"].max()) + 0.05 * atr
            if direction == "LONG"
            else current_close < _finite(base.iloc[-4:]["low"].min()) - 0.05 * atr
        )
    else:
        return None
    if not breakout or _finite(current["body_atr"]) < 0.30 or not _indicator_turn(frame, index, direction):
        return None
    return PatternMatch(
        name,
        "BREAKOUT",
        direction,
        84,
        ("converging_boundaries", "range_compression", "boundary_break", "indicator_turn"),
    )


def _wedge_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 15:
        return None
    wedge = frame.iloc[index - 14:index]
    current = frame.iloc[index]
    high_slope_raw = _linear_slope(wedge["high"])
    low_slope_raw = _linear_slope(wedge["low"])
    high_slope = high_slope_raw / atr
    low_slope = low_slope_raw / atr
    high_error = _line_fit_error(wedge["high"], high_slope_raw, atr)
    low_error = _line_fit_error(wedge["low"], low_slope_raw, atr)
    first_width = _finite(wedge.iloc[:4]["high"].max()) - _finite(wedge.iloc[:4]["low"].min())
    last_width = _finite(wedge.iloc[-4:]["high"].max()) - _finite(wedge.iloc[-4:]["low"].min())
    width_ratio = last_width / first_width if first_width > 0 else 1.0
    if high_error > 0.60 or low_error > 0.60 or width_ratio > 0.78:
        return None
    current_close = _finite(current["close"])
    if direction == "SHORT":
        geometry = high_slope > 0.015 and low_slope > high_slope + 0.015
        breakout = current_close < _finite(wedge.iloc[-4:]["low"].min()) - 0.05 * atr
        name = "RISING_WEDGE_BREAKDOWN"
    else:
        geometry = low_slope < -0.015 and high_slope < low_slope - 0.015
        breakout = current_close > _finite(wedge.iloc[-4:]["high"].max()) + 0.05 * atr
        name = "FALLING_WEDGE_BREAKOUT"
    if not geometry or not breakout:
        return None
    if _finite(current["body_atr"]) < 0.35 or not _indicator_turn(frame, index, direction):
        return None
    return PatternMatch(
        name,
        "REVERSAL",
        direction,
        83,
        ("same_direction_boundaries", "converging_wedge", "wedge_break", "indicator_turn"),
    )


def _head_shoulders_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 28:
        return None
    history = frame.iloc[index - 27:index].reset_index(drop=True)
    current = frame.iloc[index]
    kind = "low" if direction == "LONG" else "high"
    values = history["low"] if direction == "LONG" else history["high"]
    extrema = _local_extrema(values, kind)
    formation: tuple[int, int, int, float] | None = None
    for left_index in range(len(extrema) - 2):
        for head_index in range(left_index + 1, len(extrema) - 1):
            for right_index in range(head_index + 1, len(extrema)):
                left, head, right = extrema[left_index], extrema[head_index], extrema[right_index]
                if not (3 <= head - left <= 10 and 3 <= right - head <= 10 and right >= len(history) - 9):
                    continue
                left_value, head_value, right_value = map(_finite, (values.iloc[left], values.iloc[head], values.iloc[right]))
                shoulders_close = abs(left_value - right_value) / atr <= 0.50
                prominence = (
                    min(left_value, right_value) - head_value
                    if direction == "LONG"
                    else head_value - max(left_value, right_value)
                ) / atr
                if not shoulders_close or prominence < 0.50:
                    continue
                if direction == "LONG":
                    neck_one = _finite(history.iloc[left:head + 1]["high"].max())
                    neck_two = _finite(history.iloc[head:right + 1]["high"].max())
                else:
                    neck_one = _finite(history.iloc[left:head + 1]["low"].min())
                    neck_two = _finite(history.iloc[head:right + 1]["low"].min())
                if abs(neck_one - neck_two) / atr > 1.0:
                    continue
                formation = (left, head, right, (neck_one + neck_two) / 2.0)
    if formation is None:
        return None
    _, _, _, neckline = formation
    breakout = (
        _finite(current["close"]) > neckline + 0.05 * atr
        if direction == "LONG"
        else _finite(current["close"]) < neckline - 0.05 * atr
    )
    if not breakout or _finite(current["body_atr"]) < 0.35 or not _indicator_turn(frame, index, direction):
        return None
    name = "INVERSE_HEAD_AND_SHOULDERS_BREAKOUT" if direction == "LONG" else "HEAD_AND_SHOULDERS_BREAKDOWN"
    return PatternMatch(
        name,
        "REVERSAL",
        direction,
        86,
        ("balanced_shoulders", "prominent_head", "neckline_break", "indicator_turn"),
    )


def _double_extreme_breakout(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 22:
        return None
    sign = _sign(direction)
    history = frame.iloc[index - 21:index]
    current = frame.iloc[index]
    values = history["low"].astype(float) if direction == "LONG" else history["high"].astype(float)
    extrema: list[int] = []
    for position in range(1, len(values) - 1):
        left, value, right = values.iloc[position - 1], values.iloc[position], values.iloc[position + 1]
        is_extreme = (
            value < left and value <= right
            if direction == "LONG"
            else value > left and value >= right
        )
        if is_extreme:
            extrema.append(position)
    pair: tuple[int, int] | None = None
    for left_position in extrema:
        for right_position in extrema:
            if 4 <= right_position - left_position <= 16:
                distance = abs(values.iloc[right_position] - values.iloc[left_position]) / atr
                if distance <= 0.35:
                    pair = (left_position, right_position)
    if pair is None:
        return None
    left_position, right_position = pair
    if right_position < len(history) - 8:
        return None
    between = history.iloc[left_position:right_position + 1]
    neckline = _finite(between["high"].max()) if direction == "LONG" else _finite(between["low"].min())
    first_extreme = _finite(values.iloc[left_position])
    second_extreme = _finite(values.iloc[right_position])
    formation_depth = (
        neckline - max(first_extreme, second_extreme)
        if direction == "LONG"
        else min(first_extreme, second_extreme) - neckline
    ) / atr
    after_second = history.iloc[right_position + 1:]
    invalidated = (
        _finite(after_second["low"].min(), first_extreme) < min(first_extreme, second_extreme) - 0.15 * atr
        if direction == "LONG"
        else _finite(after_second["high"].max(), first_extreme) > max(first_extreme, second_extreme) + 0.15 * atr
    )
    breakout = _finite(current["close"]) > neckline if direction == "LONG" else _finite(current["close"]) < neckline
    body_ok = _finite(current["body_atr"]) >= 0.40
    ao_turn = _finite(current["ao_delta"]) * sign > 0
    hist_turn = (
        _finite(current["macd_hist"]) - _finite(frame.iloc[index - 1]["macd_hist"])
    ) * sign > 0
    if formation_depth < 0.60 or invalidated or not breakout or not body_ok or not ao_turn or not hist_turn:
        return None
    name = "DOUBLE_BOTTOM_BREAKOUT" if direction == "LONG" else "DOUBLE_TOP_BREAKOUT"
    return PatternMatch(
        name,
        "REVERSAL",
        direction,
        75,
        ("two_similar_extremes", "neckline_break", "strong_trigger_candle", "indicator_turn"),
    )


def _failed_breakout_reversal(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 16:
        return None
    sign = _sign(direction)
    base = frame.iloc[index - 15:index - 3]
    probe = frame.iloc[index - 3:index]
    trigger = frame.iloc[index - 1:index + 1]
    current = frame.iloc[index]
    if len(base) < 12 or len(probe) < 3 or not bool(_aligned_candles(trigger, direction).all()):
        return None
    base_width = (_finite(base["high"].max()) - _finite(base["low"].min())) / atr
    base_efficiency = _path_efficiency(base["close"])
    if direction == "SHORT":
        boundary = _finite(base["high"].max())
        excursion = (_finite(probe["high"].max()) - boundary) / atr
        close_excursion = (_finite(probe["close"].max()) - boundary) / atr
        returned = _finite(current["close"]) < boundary - 0.10 * atr
    else:
        boundary = _finite(base["low"].min())
        excursion = (boundary - _finite(probe["low"].min())) / atr
        close_excursion = (boundary - _finite(probe["close"].min())) / atr
        returned = _finite(current["close"]) > boundary + 0.10 * atr
    body_sum = _finite(trigger["body"].sum()) / atr
    probe_body = _finite(probe["body_atr"].max())
    ao_turn = _finite(current["ao_delta"]) * sign > 0
    hist_turn = (
        _finite(current["macd_hist"]) - _finite(frame.iloc[index - 1]["macd_hist"])
    ) * sign > 0
    if base_width > 2.50 or base_efficiency > 0.45:
        return None
    if excursion < 0.35 or close_excursion < 0.15 or probe_body < 0.50:
        return None
    if not returned or body_sum < 1.0 or not ao_turn or not hist_turn:
        return None
    return PatternMatch(
        "FAILED_BREAKOUT_REVERSAL",
        "REVERSAL",
        direction,
        80,
        ("balanced_base", "false_structure_break", "return_inside_range", "two_candle_reversal", "indicator_turn"),
    )


def _tight_chop(frame: pd.DataFrame, index: int, atr: float) -> PatternMatch | None:
    if index < 10:
        return None
    recent = frame.iloc[index - 9:index + 1]
    efficiency = _path_efficiency(recent["close"])
    range_atr = (_finite(recent["high"].max()) - _finite(recent["low"].min())) / atr
    ao_crosses = _sign_changes(recent["ao"])
    hist_crosses = _sign_changes(recent["macd_hist"])
    mean_body = _finite(recent["body_atr"].tail(5).mean())
    if not (efficiency < 0.26 and range_atr < 1.65 and mean_body < 0.22 and (ao_crosses >= 2 or hist_crosses >= 2)):
        return None
    return PatternMatch(
        "TIGHT_CHOP",
        "BLOCKER",
        "NONE",
        85,
        ("low_path_efficiency", "narrow_range", "small_mixed_candles", "zero_rotation"),
    )


def _post_shock_range(frame: pd.DataFrame, index: int, atr: float) -> PatternMatch | None:
    if index < 12:
        return None
    history = frame.iloc[index - 11:index + 1]
    shock_positions = [
        position for position, value in enumerate(history["body_atr"].astype(float).iloc[:-3]) if value >= 1.50
    ]
    short_term = False
    reasons = ("large_prior_candle", "small_following_candles", "price_inside_shock_range")
    if shock_positions:
        shock_position = shock_positions[-1]
        after = history.iloc[shock_position + 1:]
        if len(after) >= 4:
            after_efficiency = _path_efficiency(after["close"])
            after_mean_body = _finite(after["body_atr"].mean())
            shock_high = _finite(history.iloc[shock_position]["high"])
            shock_low = _finite(history.iloc[shock_position]["low"])
            current_close = _finite(history.iloc[-1]["close"])
            remains_in_shock_range = shock_low <= current_close <= shock_high
            short_term = bool(after_efficiency < 0.36 and after_mean_body < 0.28 and remains_in_shock_range)

    # The VTB examples show a longer balance after a very large shock candle.
    # A 12-bar window misses that regime after one or two sessions, even though
    # price still rotates inside the shock range and momentum has not rebuilt.
    long_history = frame.iloc[max(0, index - 79):index + 1]
    long_shocks = [
        position for position, value in enumerate(long_history["body_atr"].astype(float).iloc[:-6])
        if value >= 3.0
    ]
    long_term = False
    if long_shocks:
        shock_position = long_shocks[-1]
        balance_seed = long_history.iloc[shock_position:shock_position + 4]
        after = long_history.iloc[shock_position + 4:]
        shock = long_history.iloc[shock_position]
        if len(after) >= 7:
            shock_high = _finite(balance_seed["high"].max())
            shock_low = _finite(balance_seed["low"].min())
            after_high = _finite(after["high"].max())
            after_low = _finite(after["low"].min())
            current_close = _finite(after.iloc[-1]["close"])
            contained = (
                shock_low <= current_close <= shock_high
                and after_high <= shock_high + 0.50 * atr
                and after_low >= shock_low - 0.50 * atr
            )
            recent = after.tail(12)
            compressed = _finite(recent["body_atr"].mean()) < 0.55
            ao_near_zero = _finite(recent["ao"].abs().mean()) / atr < 2.25
            long_term = bool(contained and compressed and ao_near_zero)
            if long_term:
                reasons = (
                    "very_large_prior_candle", "multi_session_balance_inside_shock",
                    "small_mixed_candles", "ao_near_zero",
                )
    if not short_term and not long_term:
        return None
    return PatternMatch(
        "POST_SHOCK_RANGE",
        "BLOCKER",
        "NONE",
        88 if long_term else 80,
        reasons,
    )


def _late_exhaustion(frame: pd.DataFrame, index: int, direction: str, atr: float) -> PatternMatch | None:
    if index < 6:
        return None
    sign = _sign(direction)
    recent = frame.iloc[index - 5:index + 1]
    move = (_finite(recent.iloc[-1]["close"]) - _finite(recent.iloc[0]["open"])) * sign / atr
    recent_bodies = recent["body_atr"].astype(float)
    compressed = _finite(recent_bodies.tail(2).mean()) < _finite(recent_bodies.iloc[:3].mean()) * 0.45
    ao_fading = _finite(recent.iloc[-1]["ao_delta"]) * sign < _finite(recent.iloc[-2]["ao_delta"]) * sign
    hist_fading = _finite(recent.iloc[-1]["macd_hist"]) * sign < _finite(recent.iloc[-2]["macd_hist"]) * sign
    if move < 2.60 or not compressed or not ao_fading or not hist_fading:
        return None
    return PatternMatch(
        "LATE_EXHAUSTION",
        "BLOCKER",
        direction,
        80,
        ("extended_move", "candle_compression", "ao_fading", "macd_hist_fading"),
    )


def classify_entry_pattern(
    frame: pd.DataFrame,
    index: int,
    direction: str,
    minimum_confidence: int = 70,
) -> PatternDecision:
    if index < 40:
        return PatternDecision(False, None, (), (), ())
    atr = _finite(frame.iloc[index]["atr"])
    if atr <= 0:
        return PatternDecision(False, None, (), (), ())
    classic_matches = tuple(
        match for match in (
            _range_breakout(frame, index, direction, atr),
            _trend_pullback_resumption(frame, index, direction, atr),
            _flag_breakout(frame, index, direction, atr),
            _pennant_breakout(frame, index, direction, atr),
            _triangle_breakout(frame, index, direction, atr),
            _wedge_breakout(frame, index, direction, atr),
            _double_extreme_breakout(frame, index, direction, atr),
            _head_shoulders_breakout(frame, index, direction, atr),
            _failed_breakout_reversal(frame, index, direction, atr),
        ) if match is not None
    )
    momentum = _momentum_breakout(frame, index, direction, atr)
    matches = classic_matches + ((momentum,) if momentum is not None else ())
    supports = ((momentum,) if momentum is not None else ())
    blockers = tuple(
        match for match in (
            _tight_chop(frame, index, atr),
            _post_shock_range(frame, index, atr),
            _late_exhaustion(frame, index, direction, atr),
        ) if match is not None
    )
    primary = max(
        matches,
        key=lambda item: (PATTERN_SPECIFICITY.get(item.name, 0), item.confidence),
    ) if matches else None
    blocker_confidence = max((item.confidence for item in blockers), default=0)
    allowed = bool(primary and primary.confidence >= minimum_confidence and blocker_confidence < primary.confidence)
    return PatternDecision(allowed, primary, matches, blockers, supports)
