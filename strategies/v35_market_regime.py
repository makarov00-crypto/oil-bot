from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


REGIME_MODEL_VERSION = "mechanical-regime-v1"
REGIMES = ("TREND", "PULLBACK", "CHOP", "SHOCK_AFTER_RANGE")


@dataclass(frozen=True)
class RegimeAssessment:
    regime: str
    confidence: int
    direction: str
    reasons: tuple[str, ...]
    features: dict[str, float | int]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if pd.notna(result) else default


def _path_efficiency(values: pd.Series) -> float:
    points = values.astype(float)
    if len(points) < 2:
        return 0.0
    path = float(points.diff().abs().sum())
    return abs(float(points.iloc[-1] - points.iloc[0])) / path if path > 0 else 0.0


def _linear_slope(values: pd.Series) -> float:
    points = values.astype(float).reset_index(drop=True)
    if len(points) < 2:
        return 0.0
    x_mean = (len(points) - 1) / 2.0
    y_mean = _finite(points.mean())
    denominator = sum((position - x_mean) ** 2 for position in range(len(points)))
    if denominator <= 0:
        return 0.0
    return sum(
        (position - x_mean) * (_finite(value) - y_mean)
        for position, value in enumerate(points)
    ) / denominator


def _sign_changes(values: pd.Series) -> int:
    signs = [1 if value > 0 else -1 if value < 0 else 0 for value in values.dropna().astype(float)]
    nonzero = [value for value in signs if value]
    return sum(left != right for left, right in zip(nonzero, nonzero[1:]))


def _direction_change_rate(closes: pd.Series) -> float:
    changes = closes.astype(float).diff().dropna()
    signs = [1 if value > 0 else -1 if value < 0 else 0 for value in changes]
    nonzero = [value for value in signs if value]
    if len(nonzero) < 2:
        return 0.0
    return sum(left != right for left, right in zip(nonzero, nonzero[1:])) / (len(nonzero) - 1)


def _overlap_ratio(frame: pd.DataFrame) -> float:
    if len(frame) < 2:
        return 0.0
    overlaps: list[float] = []
    for position in range(1, len(frame)):
        previous = frame.iloc[position - 1]
        current = frame.iloc[position]
        smaller_range = min(
            _finite(previous["high"]) - _finite(previous["low"]),
            _finite(current["high"]) - _finite(current["low"]),
        )
        if smaller_range <= 0:
            continue
        overlap = max(
            0.0,
            min(_finite(previous["high"]), _finite(current["high"]))
            - max(_finite(previous["low"]), _finite(current["low"])),
        )
        overlaps.append(min(1.0, overlap / smaller_range))
    return sum(overlaps) / len(overlaps) if overlaps else 0.0


def _structure_score(frame: pd.DataFrame, direction_sign: float) -> float:
    if len(frame) < 3:
        return 0.0
    high_progress = frame["high"].astype(float).diff().dropna() * direction_sign > 0
    low_progress = frame["low"].astype(float).diff().dropna() * direction_sign > 0
    return float(pd.concat([high_progress, low_progress]).mean())


def classify_market_regime(frame: pd.DataFrame, index: int | None = None) -> RegimeAssessment:
    """Classify only candles available at ``index``; future rows are never read."""
    if frame.empty:
        return RegimeAssessment("CHOP", 100, "MIXED", ("no_history",), {})
    if index is None:
        index = len(frame) - 1
    prefix = frame.iloc[: index + 1].tail(72).copy()
    if len(prefix) < 20:
        return RegimeAssessment(
            "CHOP", 70, "MIXED", ("insufficient_2_3_day_context",), {"bars": len(prefix)}
        )

    atr = _finite(prefix.iloc[-1].get("atr"))
    if atr <= 0:
        atr = _finite((prefix["high"].astype(float) - prefix["low"].astype(float)).tail(14).mean())
    if atr <= 0:
        return RegimeAssessment("CHOP", 100, "MIXED", ("invalid_atr",), {"bars": len(prefix)})

    context = prefix.tail(min(48, len(prefix)))
    recent = prefix.tail(min(12, len(prefix)))
    short = prefix.tail(min(6, len(prefix)))
    net_move_atr = (_finite(context.iloc[-1]["close"]) - _finite(context.iloc[0]["close"])) / atr
    slope_travel_atr = _linear_slope(context["close"]) * max(1, len(context) - 1) / atr
    direction_sign = 1.0 if slope_travel_atr >= 0 else -1.0
    direction = "LONG" if direction_sign > 0 else "SHORT"
    efficiency = _path_efficiency(context["close"])
    recent_efficiency = _path_efficiency(recent["close"])
    recent_range_atr = (
        _finite(recent["high"].max()) - _finite(recent["low"].min())
    ) / atr
    recent_move_atr = (
        _finite(recent.iloc[-1]["close"]) - _finite(recent.iloc[0]["close"])
    ) / atr
    short_move_atr = (
        _finite(short.iloc[-1]["close"]) - _finite(short.iloc[0]["close"])
    ) / atr
    direction_change_rate = _direction_change_rate(recent["close"])
    overlap_ratio = _overlap_ratio(recent)
    body_atr = recent["body"].astype(float) / atr
    small_body_ratio = float((body_atr < 0.18).mean())
    mean_body_atr = _finite(body_atr.mean())
    ao_amplitude_atr = _finite(recent["ao"].abs().quantile(0.75)) / atr
    ao_sign_changes = _sign_changes(recent["ao"])
    hist_sign_changes = _sign_changes(recent["macd_hist"])
    structure_score = _structure_score(context.tail(18), direction_sign)

    features: dict[str, float | int] = {
        "bars": len(prefix),
        "atr": round(atr, 8),
        "net_move_atr": round(net_move_atr, 4),
        "slope_travel_atr": round(slope_travel_atr, 4),
        "path_efficiency": round(efficiency, 4),
        "recent_path_efficiency": round(recent_efficiency, 4),
        "recent_range_atr": round(recent_range_atr, 4),
        "recent_move_atr": round(recent_move_atr, 4),
        "short_move_atr": round(short_move_atr, 4),
        "direction_change_rate": round(direction_change_rate, 4),
        "overlap_ratio": round(overlap_ratio, 4),
        "small_body_ratio": round(small_body_ratio, 4),
        "mean_body_atr": round(mean_body_atr, 4),
        "ao_amplitude_atr": round(ao_amplitude_atr, 4),
        "ao_sign_changes": ao_sign_changes,
        "hist_sign_changes": hist_sign_changes,
        "structure_score": round(structure_score, 4),
    }

    shock_window = context.tail(min(36, len(context))).copy()
    shock_bodies = shock_window["body"].astype(float) / atr
    shock_position = int(shock_bodies.reset_index(drop=True).idxmax())
    shock_body_atr = _finite(shock_bodies.iloc[shock_position])
    bars_since_shock = len(shock_window) - 1 - shock_position
    post_shock = shock_window.iloc[shock_position + 1:]
    features["shock_body_atr"] = round(shock_body_atr, 4)
    features["bars_since_shock"] = bars_since_shock
    if len(post_shock) >= 5:
        post_efficiency = _path_efficiency(post_shock["close"])
        post_overlap = _overlap_ratio(post_shock.tail(16))
        post_small_body_ratio = float(((post_shock["body"].astype(float) / atr) < 0.22).mean())
        post_mean_body_atr = _finite((post_shock["body"].astype(float) / atr).mean())
        post_range_atr = (
            _finite(post_shock["high"].max()) - _finite(post_shock["low"].min())
        ) / atr
        features.update({
            "post_shock_efficiency": round(post_efficiency, 4),
            "post_shock_overlap_ratio": round(post_overlap, 4),
            "post_shock_small_body_ratio": round(post_small_body_ratio, 4),
            "post_shock_mean_body_atr": round(post_mean_body_atr, 4),
            "post_shock_range_atr": round(post_range_atr, 4),
        })
        shock_after_range = bool(
            shock_body_atr >= 1.55
            and bars_since_shock <= 35
            and post_efficiency < 0.42
            and post_overlap >= 0.48
            and (post_small_body_ratio >= 0.48 or post_mean_body_atr <= 0.45)
            and post_range_atr <= 3.20
        )
        if shock_after_range:
            confidence = min(
                96,
                72
                + int(min(10.0, max(0.0, shock_body_atr - 1.55) * 8))
                + int(min(8.0, max(0.0, post_overlap - 0.48) * 20)),
            )
            return RegimeAssessment(
                "SHOCK_AFTER_RANGE",
                confidence,
                "MIXED",
                ("large_recent_shock", "post_shock_overlap", "post_shock_small_candles", "post_shock_low_efficiency"),
                features,
            )

    # A strong fresh impulse can start inside a mixed 2–3 day context. Treat it
    # as an emerging trend before the slower context statistics catch up.
    recent_direction_sign = 1.0 if recent_move_atr >= 0 else -1.0
    recent_direction = "LONG" if recent_direction_sign > 0 else "SHORT"
    recent_structure_score = _structure_score(recent, recent_direction_sign)
    features["recent_structure_score"] = round(recent_structure_score, 4)
    emerging_trend = bool(
        abs(recent_move_atr) >= 1.40
        and short_move_atr * recent_direction_sign >= 0.30
        and recent_efficiency >= 0.28
        and recent_range_atr >= 2.0
        and (recent_structure_score >= 0.45 or mean_body_atr >= 0.30)
    )
    if emerging_trend:
        return RegimeAssessment(
            "TREND",
            min(92, 68 + int(min(18.0, (abs(recent_move_atr) - 1.40) * 5))),
            recent_direction,
            ("fresh_directional_impulse", "recent_price_efficiency", "expanding_recent_range"),
            features,
        )

    chop_score = sum((
        recent_efficiency < 0.30,
        direction_change_rate >= 0.50,
        overlap_ratio >= 0.55,
        small_body_ratio >= 0.55,
        recent_range_atr <= 1.80,
        ao_amplitude_atr <= 0.35,
        ao_sign_changes >= 2 or hist_sign_changes >= 2,
    ))
    if chop_score >= 4 and (recent_efficiency < 0.36 or recent_range_atr <= 1.25):
        return RegimeAssessment(
            "CHOP",
            min(95, 55 + chop_score * 6),
            "MIXED",
            ("low_path_efficiency", "frequent_direction_changes", "candle_overlap", "small_candles_near_one_level"),
            features,
        )

    trend_strength = sum((
        abs(net_move_atr) >= 1.50,
        abs(slope_travel_atr) >= 1.20,
        efficiency >= 0.32,
        structure_score >= 0.56,
        abs(recent_move_atr) >= 0.55,
    ))
    trend_direction = "LONG" if net_move_atr + slope_travel_atr >= 0 else "SHORT"
    trend_sign = 1.0 if trend_direction == "LONG" else -1.0
    prior = context.iloc[:-6]
    prior_slope_atr = _linear_slope(prior["close"]) * max(1, len(prior) - 1) / atr if len(prior) >= 12 else 0.0
    counter_move = short_move_atr * trend_sign < -0.18
    controlled_pause = abs(short_move_atr) <= 0.45 and recent_efficiency < 0.42
    last2_move = (
        _finite(short.iloc[-1]["close"]) - _finite(short.iloc[-2]["close"])
    ) * trend_sign / atr
    indicator_near_zero = _finite(recent["ao"].abs().min()) / atr <= 0.35
    pullback = bool(
        abs(prior_slope_atr) >= 1.0
        and prior_slope_atr * trend_sign > 0
        and (counter_move or controlled_pause)
        and indicator_near_zero
        and last2_move > 0
    )
    features["prior_slope_travel_atr"] = round(prior_slope_atr, 4)
    if pullback:
        return RegimeAssessment(
            "PULLBACK",
            min(92, 68 + (8 if counter_move else 4) + (8 if last2_move >= 0.18 else 3)),
            trend_direction,
            ("established_prior_trend", "controlled_counter_move_or_pause", "indicator_near_zero", "trend_resumption_started"),
            features,
        )

    if trend_strength >= 3:
        return RegimeAssessment(
            "TREND",
            min(95, 58 + trend_strength * 7),
            trend_direction,
            ("directional_price_slope", "ordered_highs_lows", "sufficient_path_efficiency"),
            features,
        )

    return RegimeAssessment(
        "CHOP",
        58,
        "MIXED",
        ("no_stable_directional_structure", "mixed_price_path"),
        features,
    )


def regime_allows_pattern(
    assessment: RegimeAssessment,
    direction: str,
    pattern_family: str,
    pattern_confidence: int,
    largest_body_atr: float,
) -> tuple[bool, str]:
    if assessment.regime == "SHOCK_AFTER_RANGE":
        return False, "SHOCK_AFTER_RANGE"
    if assessment.regime == "CHOP":
        sign = 1.0 if direction == "LONG" else -1.0
        recent_move = _finite(assessment.features.get("recent_move_atr")) * sign
        if assessment.confidence < 70:
            return True, "LOW_CONFIDENCE_REGIME_OBSERVE"
        emerging_breakout = bool(
            pattern_family == "BREAKOUT"
            and pattern_confidence >= 84
            and largest_body_atr >= 0.60
            and recent_move >= 0.50
        )
        return emerging_breakout, "EMERGING_BREAKOUT" if emerging_breakout else "CHOP"
    aligned = assessment.direction == direction
    if assessment.regime == "PULLBACK":
        allowed = aligned and pattern_family in {"CONTINUATION", "BREAKOUT"} and pattern_confidence >= 70
        return allowed, "PULLBACK_ALIGNED" if allowed else "PULLBACK_MISMATCH"
    if assessment.regime == "TREND" and aligned:
        return True, "TREND_ALIGNED"
    reversal_override = bool(
        assessment.regime == "TREND"
        and not aligned
        and pattern_family == "REVERSAL"
        and pattern_confidence >= 88
        and largest_body_atr >= 0.75
    )
    return reversal_override, "STRONG_REVERSAL" if reversal_override else "TREND_DIRECTION_MISMATCH"
