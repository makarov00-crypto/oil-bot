from dataclasses import dataclass
from zoneinfo import ZoneInfo

from instrument_groups import is_brent_symbol


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
MIN_TREND_PATH_EFFICIENCY_6H = 0.35
MODERATE_VOLUME_IMPULSE_PATH_EFFICIENCY_6H = 0.50
MODERATE_VOLUME_IMPULSE_MIN_VOLUME_RATIO = 0.50
MODERATE_VOLUME_IMPULSE_MIN_BODY_RATIO = 1.25
MULTIDAY_REGIME_MIN_HOURS = 48
MULTIDAY_REGIME_WINDOW_HOURS = 72


@dataclass(frozen=True)
class ReversalProfile:
    min_volume_ratio: float
    strong_volume_ratio: float
    min_body_ratio: float
    strong_body_ratio: float
    rsi_long_min: float
    rsi_long_max: float
    rsi_short_min: float
    rsi_short_max: float
    late_rsi_long: float
    late_rsi_short: float
    late_stoch_high: float
    late_stoch_low: float
    max_distance_to_ema20_pct: float
    compression_atr_pct: float
    compression_range_pct: float
    compression_bb_width_pct: float
    chop_range_pct: float
    expansion_range_pct: float
    min_atr_pct: float


def get_profile(symbol: str, timeframe_minutes: int = 60) -> ReversalProfile:
    if is_brent_symbol(symbol):
        return ReversalProfile(
            min_volume_ratio=0.72,
            strong_volume_ratio=0.96,
            min_body_ratio=0.52,
            strong_body_ratio=0.88,
            rsi_long_min=43.0,
            rsi_long_max=74.0,
            rsi_short_min=26.0,
            rsi_short_max=58.0,
            late_rsi_long=72.0,
            late_rsi_short=28.0,
            late_stoch_high=92.0,
            late_stoch_low=8.0,
            max_distance_to_ema20_pct=0.022,
            compression_atr_pct=0.0020,
            compression_range_pct=0.016,
            compression_bb_width_pct=0.018,
            chop_range_pct=0.020,
            expansion_range_pct=0.024,
            min_atr_pct=0.0010,
        )
    return ReversalProfile(
        min_volume_ratio=0.75,
        strong_volume_ratio=1.00,
        min_body_ratio=0.55,
        strong_body_ratio=0.90,
        rsi_long_min=42.0,
        rsi_long_max=72.0,
        rsi_short_min=28.0,
        rsi_short_max=58.0,
        late_rsi_long=70.0,
        late_rsi_short=30.0,
        late_stoch_high=90.0,
        late_stoch_low=10.0,
        max_distance_to_ema20_pct=0.018,
        compression_atr_pct=0.0015,
        compression_range_pct=0.012,
        compression_bb_width_pct=0.015,
        chop_range_pct=0.016,
        expansion_range_pct=0.020,
        min_atr_pct=0.0008,
    )


def _bars_since_cross(macd: list[float], signal: list[float], direction: str) -> int | None:
    if len(macd) < 2 or len(signal) < 2:
        return None
    for idx in range(len(macd) - 1, 0, -1):
        prev_diff = macd[idx - 1] - signal[idx - 1]
        diff = macd[idx] - signal[idx]
        if direction == "LONG" and prev_diff <= 0 < diff:
            return len(macd) - 1 - idx
        if direction == "SHORT" and prev_diff >= 0 > diff:
            return len(macd) - 1 - idx
    return None


def _bars_since_zero_cross(values: list[float], direction: str) -> int | None:
    if len(values) < 2:
        return None
    for idx in range(len(values) - 1, 0, -1):
        previous = values[idx - 1]
        current = values[idx]
        if direction == "LONG" and previous <= 0.0 < current:
            return len(values) - 1 - idx
        if direction == "SHORT" and previous >= 0.0 > current:
            return len(values) - 1 - idx
    return None


def _macd_flip_count(macd: list[float], signal: list[float]) -> int:
    if len(macd) < 3 or len(signal) < 3:
        return 0
    flips = 0
    prev_sign = 0
    for m, s in zip(macd, signal):
        diff = m - s
        sign = 1 if diff > 0 else -1 if diff < 0 else 0
        if sign == 0:
            continue
        if prev_sign and sign != prev_sign:
            flips += 1
        prev_sign = sign
    return flips


def _path_efficiency(values) -> float:
    series = [float(value) for value in values]
    if len(series) < 2:
        return 0.0
    path = sum(abs(current - previous) for previous, current in zip(series, series[1:]))
    return abs(series[-1] - series[0]) / path if path > 0.0 else 0.0


def classify_multiday_direction(df, direction: str) -> dict[str, float | str | bool]:
    """Classify whether a direction is supported by the preceding 48-72 hours.

    The hourly strategy still chooses the exact entry from its latest candles.
    This slower view only decides whether the broader move is alive, newly
    emerging, or absent.  Every value is causal and uses completed rows already
    present in ``df``.
    """
    normalized_direction = str(direction or "").strip().upper()
    if normalized_direction not in {"LONG", "SHORT"}:
        return {"available": False, "regime": "unknown"}
    history = df.tail(MULTIDAY_REGIME_WINDOW_HOURS)
    if len(history) < MULTIDAY_REGIME_MIN_HOURS:
        return {"available": False, "regime": "unknown"}

    last = history.iloc[-1]
    tail_12 = history.tail(12)
    tail_24 = history.tail(24)
    tail_48 = history.tail(48)
    atr_values = [float(value) for value in tail_24["atr"].tolist() if float(value) > 0.0]
    atr = sorted(atr_values)[len(atr_values) // 2] if atr_values else 1e-9
    atr = max(atr, 1e-9)
    sign = 1.0 if normalized_direction == "LONG" else -1.0

    ema_stack = sign * (float(last["ema20"]) - float(last["ema50"])) > 0.0
    ema20_slope_12_atr = sign * (
        float(last["ema20"]) - float(tail_12.iloc[0]["ema20"])
    ) / atr
    ema50_slope_24_atr = sign * (
        float(last["ema50"]) - float(tail_24.iloc[0]["ema50"])
    ) / atr
    move_12_atr = sign * (
        float(last["close"]) - float(tail_12.iloc[0]["close"])
    ) / atr
    move_48_atr = sign * (
        float(last["close"]) - float(tail_48.iloc[0]["close"])
    ) / atr
    if normalized_direction == "LONG":
        alignment_24 = sum(
            float(row["close"]) >= float(row["ema20"])
            for _, row in tail_24.iterrows()
        ) / len(tail_24)
    else:
        alignment_24 = sum(
            float(row["close"]) <= float(row["ema20"])
            for _, row in tail_24.iterrows()
        ) / len(tail_24)
    efficiency_12 = _path_efficiency(tail_12["close"].tolist())
    efficiency_24 = _path_efficiency(tail_24["close"].tolist())
    macd_hist = float(last["macd"]) - float(last["macd_signal"])
    macd_aligned = sign * macd_hist >= 0.0
    ao_aligned = sign * float(last.get("ao", macd_hist)) >= 0.0

    stable_score = sum(
        (
            1.0 if ema_stack else 0.0,
            1.0 if ema20_slope_12_atr >= 0.08 else 0.0,
            1.0 if ema50_slope_24_atr >= 0.04 else 0.0,
            1.0 if move_48_atr >= 0.55 else 0.0,
            1.0 if alignment_24 >= 0.58 else 0.0,
            1.0 if efficiency_24 >= 0.20 else 0.0,
        )
    )
    stable_trend = stable_score >= 4.0 and move_48_atr > 0.0
    emerging_trend = (
        ema_stack
        and ema20_slope_12_atr >= 0.12
        and move_12_atr >= 0.75
        and efficiency_12 >= 0.45
        and macd_aligned
        and ao_aligned
    )
    regime = "trend" if stable_trend else "emerging" if emerging_trend else "no_trend"
    return {
        "available": True,
        "regime": regime,
        "stable_trend": stable_trend,
        "emerging_trend": emerging_trend,
        "stable_score": stable_score,
        "ema_stack": ema_stack,
        "ema20_slope_12_atr": round(ema20_slope_12_atr, 4),
        "ema50_slope_24_atr": round(ema50_slope_24_atr, 4),
        "move_12_atr": round(move_12_atr, 4),
        "move_48_atr": round(move_48_atr, 4),
        "alignment_24": round(alignment_24, 4),
        "efficiency_12": round(efficiency_12, 4),
        "efficiency_24": round(efficiency_24, 4),
        "macd_aligned": macd_aligned,
        "ao_aligned": ao_aligned,
    }


def _rsi_direction_ok(current: float, previous: float, earlier: float, direction: str, *, floor: float, ceiling: float) -> bool:
    if direction == "LONG":
        return floor <= current <= ceiling and current >= previous + 0.20 and previous >= earlier - 0.35
    if direction == "SHORT":
        return floor <= current <= ceiling and current <= previous - 0.20 and previous <= earlier + 0.35
    return False


def _ao_direction_ok(current: float, previous: float, earlier: float, direction: str) -> bool:
    if direction == "LONG":
        return (
            (current > 0 and previous <= 0)
            or (current >= previous >= earlier)
            or (current > previous and previous <= earlier and current > 0)
        )
    if direction == "SHORT":
        return (
            (current < 0 and previous >= 0)
            or (current <= previous <= earlier)
            or (current < previous and previous >= earlier and current < 0)
        )
    return False


def _ao_trend_tolerates(current: float, previous: float, direction: str) -> bool:
    tolerance = max(abs(previous) * 0.35, abs(current) * 0.20, 1e-9)
    if direction == "LONG":
        return current > 0 and current >= previous - tolerance
    if direction == "SHORT":
        return current < 0 and current <= previous + tolerance
    return False


def _is_late_evening_candle(value) -> bool:
    try:
        if hasattr(value, "tz_convert"):
            local_time = value.tz_convert(MOSCOW_TZ)
        elif hasattr(value, "astimezone"):
            local_time = value.astimezone(MOSCOW_TZ)
        else:
            return False
    except Exception:
        return False
    return local_time.hour >= 21


def _is_outside_main_session_candle(value) -> bool:
    try:
        if hasattr(value, "tz_convert"):
            local_time = value.tz_convert(MOSCOW_TZ)
        elif hasattr(value, "astimezone"):
            local_time = value.astimezone(MOSCOW_TZ)
        else:
            return False
    except Exception:
        return False
    return local_time.hour < 9 or local_time.hour >= 19


def classify_reversal_regime(df, symbol: str, timeframe_minutes: int = 60) -> dict[str, float | str | bool]:
    """Classify the local reversal regime from the same evidence used for entries.

    The result is direction-aware. A one-candle pause may retain an already
    confirmed expansion while price structure, participation and directional
    indicators still agree.
    """
    if len(df) < 8:
        return {
            "regime": "mixed",
            "direction": "",
            "confidence": 0.35,
            "continuation": False,
            "path_efficiency_6h": 0.0,
            "effective_expansion_range_pct": 0.0,
        }

    profile = get_profile(symbol, timeframe_minutes)

    def snapshot(frame) -> dict[str, float | str | bool]:
        last = frame.iloc[-1]
        prev = frame.iloc[-2]
        recent = frame.iloc[-8:]
        pre_recent = frame.iloc[-8:-1]
        close = float(last["close"])
        ema20 = float(last["ema20"])
        ema50 = float(last["ema50"])
        prev_ema20 = float(prev["ema20"])
        prev_close = float(prev["close"])
        volume_avg = float(last["volume_avg"])
        body_avg = float(last["body_avg"])
        volume_ratio = float(last["volume"]) / volume_avg if volume_avg > 0.0 else 0.0
        body_ratio = float(last["body"]) / body_avg if body_avg > 0.0 else 0.0
        atr_pct = float(last["atr"]) / close if close else 0.0
        bb_width_pct = (float(last["bb_upper"]) - float(last["bb_lower"])) / close if close else 0.0
        recent_range_pct = (
            (float(recent["high"].max()) - float(recent["low"].min())) / close if close else 0.0
        )
        pre_volume_ratios = [
            float(row["volume"]) / float(row["volume_avg"])
            for _, row in pre_recent.iterrows()
            if float(row.get("volume_avg", 0.0)) > 0.0
        ]
        compression_volume_ratio = (
            sum(pre_volume_ratios) / len(pre_volume_ratios)
            if pre_volume_ratios
            else volume_ratio
        )
        macd = float(last["macd"])
        macd_signal = float(last["macd_signal"])
        prev_hist = float(prev["macd"]) - float(prev["macd_signal"])
        macd_hist = macd - macd_signal
        ao = float(last.get("ao", macd_hist))
        chaikin = float(last.get("chaikin", 0.0))
        macd_series = [float(value) for value in recent["macd"].tolist()]
        macd_signal_series = [float(value) for value in recent["macd_signal"].tolist()]
        macd_flip_count = _macd_flip_count(macd_series, macd_signal_series)
        long_cross_age = _bars_since_cross(macd_series, macd_signal_series, "LONG")
        short_cross_age = _bars_since_cross(macd_series, macd_signal_series, "SHORT")
        soft_volume_floor = max(0.55, profile.min_volume_ratio - 0.30)
        soft_impulse_floor = max(0.45, profile.min_body_ratio - 0.35)
        fresh_impulse_override = (
            (
                (long_cross_age is not None and long_cross_age <= 2)
                or (short_cross_age is not None and short_cross_age <= 2)
            )
            and volume_ratio >= soft_volume_floor
            and body_ratio >= soft_impulse_floor
        )
        trend_up = close > ema20 and ema20 >= ema50 and ema20 >= prev_ema20
        trend_down = close < ema20 and ema20 <= ema50 and ema20 <= prev_ema20
        structure_up = close > ema20 and close > ema50 and ema20 >= prev_ema20 and close >= prev_close
        structure_down = close < ema20 and close < ema50 and ema20 <= prev_ema20 and close <= prev_close
        # Adapt expansion to current volatility, but never let it overlap the
        # instrument's compression range or exceed its conservative profile.
        effective_expansion_range_pct = max(
            profile.compression_range_pct * 1.15,
            min(profile.expansion_range_pct, atr_pct * 3.0),
        )
        expansion_up = (
            structure_up
            and macd_hist > 0.0
            and macd_hist > prev_hist
            and volume_ratio >= profile.strong_volume_ratio
            and body_ratio >= profile.strong_body_ratio
            and recent_range_pct >= effective_expansion_range_pct
        )
        expansion_down = (
            structure_down
            and macd_hist < 0.0
            and macd_hist < prev_hist
            and volume_ratio >= profile.strong_volume_ratio
            and body_ratio >= profile.strong_body_ratio
            and recent_range_pct >= effective_expansion_range_pct
        )
        compression = (
            atr_pct <= profile.compression_atr_pct
            and recent_range_pct <= profile.compression_range_pct
            and bb_width_pct <= profile.compression_bb_width_pct
            and compression_volume_ratio < 1.05
        )
        chop = (
            not compression
            and macd_flip_count >= 4
            and recent_range_pct <= profile.chop_range_pct
            and volume_ratio < profile.strong_volume_ratio
            and not fresh_impulse_override
        )
        if compression:
            regime, direction = "compression", ""
        elif chop:
            regime, direction = "chop", ""
        elif expansion_up:
            regime, direction = "expansion", "LONG"
        elif expansion_down:
            regime, direction = "expansion", "SHORT"
        elif trend_up:
            regime, direction = "trend", "LONG"
        elif trend_down:
            regime, direction = "trend", "SHORT"
        else:
            regime, direction = "mixed", ""
        path_efficiency_6h = _path_efficiency(recent["close"].tail(6).tolist())
        confidence = 0.45
        if regime == "expansion":
            confidence = min(
                0.95,
                0.55
                + min(path_efficiency_6h, 1.0) * 0.15
                + min(volume_ratio / max(profile.strong_volume_ratio, 1e-9), 2.0) * 0.075
                + min(body_ratio / max(profile.strong_body_ratio, 1e-9), 2.0) * 0.075,
            )
        elif regime == "trend":
            confidence = min(0.90, 0.55 + min(path_efficiency_6h, 1.0) * 0.25)
        elif regime in {"compression", "chop"}:
            confidence = 0.70
        return {
            "regime": regime,
            "direction": direction,
            "confidence": round(confidence, 4),
            "continuation": False,
            "path_efficiency_6h": round(path_efficiency_6h, 4),
            "effective_expansion_range_pct": effective_expansion_range_pct,
            "recent_range_pct": recent_range_pct,
            "volume_ratio": volume_ratio,
            "body_ratio": body_ratio,
            "structure_up": structure_up,
            "structure_down": structure_down,
            "macd_hist": macd_hist,
            "ao": ao,
            "chaikin": chaikin,
        }

    current = snapshot(df)
    if current["regime"] != "mixed" or len(df) < 9:
        return current

    previous = snapshot(df.iloc[:-1])
    previous_direction = str(previous.get("direction") or "")
    if previous.get("regime") != "expansion" or previous_direction not in {"LONG", "SHORT"}:
        return current
    directional_continuation = (
        previous_direction == "LONG"
        and bool(current.get("structure_up"))
        and float(current.get("macd_hist") or 0.0) > 0.0
        and float(current.get("ao") or 0.0) > 0.0
        and float(current.get("chaikin") or 0.0) > 0.0
    ) or (
        previous_direction == "SHORT"
        and bool(current.get("structure_down"))
        and float(current.get("macd_hist") or 0.0) < 0.0
        and float(current.get("ao") or 0.0) < 0.0
        and float(current.get("chaikin") or 0.0) < 0.0
    )
    participation_ok = (
        float(current.get("volume_ratio") or 0.0) >= profile.min_volume_ratio
        and float(current.get("body_ratio") or 0.0) >= profile.min_body_ratio
        and float(current.get("path_efficiency_6h") or 0.0) > MIN_TREND_PATH_EFFICIENCY_6H
    )
    if directional_continuation and participation_ok:
        current["regime"] = "expansion"
        current["direction"] = previous_direction
        current["continuation"] = True
        current["confidence"] = round(max(float(current.get("confidence") or 0.0), 0.70), 4)
    return current


def evaluate_signal_core(
    df,
    config,
    instrument,
    higher_tf_bias: str,
    *,
    timeframe_minutes: int,
    strategy_label: str,
    _regime_override: str | None = None,
    _allow_mixed_confirmation: bool = True,
) -> tuple[str, str]:
    profile = get_profile(instrument.symbol, timeframe_minutes)
    if len(df) < 8:
        return "HOLD", f"Сигнал HOLD ({strategy_label}): недостаточно свечей для разворотной логики."

    last = df.iloc[-1]
    prev = df.iloc[-2]
    recent = df.iloc[-8:]
    pre_recent = df.iloc[-8:-1]

    close = float(last["close"])
    prev_close = float(prev["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    prev_ema20 = float(prev["ema20"])
    rsi = float(last["rsi"])
    prev_rsi = float(prev["rsi"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    stoch_k = float(last.get("stoch_k", 50.0))
    stoch_d = float(last.get("stoch_d", 50.0))
    prev_stoch_k = float(prev.get("stoch_k", 50.0))
    prev_stoch_d = float(prev.get("stoch_d", 50.0))
    prev2_rsi = float(df.iloc[-3]["rsi"])
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = float(last["body"])
    body_avg = float(last["body_avg"])
    atr_pct = float(last["atr"]) / close if close else 0.0
    bb_width_pct = (float(last["bb_upper"]) - float(last["bb_lower"])) / close if close else 0.0
    volume_ratio = (volume / volume_avg) if volume_avg > 0 else 0.0
    body_ratio = (body / body_avg) if body_avg > 0 else 0.0
    pre_volume_ratios = [
        float(row["volume"]) / float(row["volume_avg"])
        for _, row in pre_recent.iterrows()
        if float(row.get("volume_avg", 0.0)) > 0.0
    ]
    compression_volume_ratio = (
        sum(pre_volume_ratios) / len(pre_volume_ratios)
        if pre_volume_ratios
        else volume_ratio
    )
    distance_to_ema20_pct = abs(close - ema20) / close if close else 0.0
    late_evening = _is_late_evening_candle(last.get("time"))
    outside_main_session = _is_outside_main_session_candle(last.get("time"))
    recent_high = float(pre_recent["high"].max())
    recent_low = float(pre_recent["low"].min())
    recent_range_pct = (float(recent["high"].max()) - float(recent["low"].min())) / close if close else 0.0

    macd_series = [float(value) for value in recent["macd"].tolist()]
    macd_signal_series = [float(value) for value in recent["macd_signal"].tolist()]
    long_cross_age = _bars_since_cross(macd_series, macd_signal_series, "LONG")
    short_cross_age = _bars_since_cross(macd_series, macd_signal_series, "SHORT")
    recent_cross_age_limit = 4
    late_cross_age_limit = 6
    recent_long_cross = long_cross_age is not None and long_cross_age <= recent_cross_age_limit
    recent_short_cross = short_cross_age is not None and short_cross_age <= recent_cross_age_limit
    macd_hist = macd - macd_signal
    prev_hist = prev_macd - prev_macd_signal
    ao = float(last.get("ao", macd_hist))
    prev_ao = float(prev.get("ao", prev_hist))
    prev2 = df.iloc[-3]
    prev2_ao = float(prev2.get("ao", prev2.get("macd", 0.0) - prev2.get("macd_signal", 0.0)))
    ao_context = [float(value) for value in df["ao"].tail(14).tolist()]
    long_ao_cross_age = _bars_since_zero_cross(ao_context, "LONG")
    short_ao_cross_age = _bars_since_zero_cross(ao_context, "SHORT")
    chaikin = float(last.get("chaikin", 0.0))
    prev_chaikin = float(prev.get("chaikin", 0.0))
    chaikin_delta = chaikin - prev_chaikin
    regime_context = classify_reversal_regime(df, instrument.symbol, timeframe_minutes)
    trend_up = close > ema20 and ema20 >= ema50 and ema20 >= prev_ema20
    trend_down = close < ema20 and ema20 <= ema50 and ema20 <= prev_ema20
    structure_up = close > ema20 and close > ema50 and ema20 >= prev_ema20 and close >= prev_close
    structure_down = close < ema20 and close < ema50 and ema20 <= prev_ema20 and close <= prev_close
    expansion_up = regime_context["regime"] == "expansion" and regime_context["direction"] == "LONG"
    expansion_down = regime_context["regime"] == "expansion" and regime_context["direction"] == "SHORT"
    compression = regime_context["regime"] == "compression"
    soft_volume_floor = max(0.55, profile.min_volume_ratio - 0.30)
    soft_impulse_floor = max(0.45, profile.min_body_ratio - 0.35)
    soft_volatility_floor = profile.min_atr_pct * 0.60
    detected_regime = str(regime_context["regime"])
    regime = _regime_override or detected_regime
    path_efficiency_6h = float(regime_context["path_efficiency_6h"])
    entry_regime_quality_ok = (
        regime in {"trend", "expansion"}
        and path_efficiency_6h > MIN_TREND_PATH_EFFICIENCY_6H
    )
    trend_like_regime = regime in {"trend", "expansion"}

    ao_long_ok = _ao_direction_ok(ao, prev_ao, prev2_ao, "LONG") or (ao >= prev_ao and macd_hist > prev_hist)
    ao_short_ok = _ao_direction_ok(ao, prev_ao, prev2_ao, "SHORT") or (ao <= prev_ao and macd_hist < prev_hist)

    breakout_up = close > recent_high and close > ema20
    breakout_down = close < recent_low and close < ema20
    # In compression, a weak cross is usually just another move inside the range.
    # Admit only a fresh, visible breakout so the bot does not churn in a squeeze.
    compression_volume_floor = max(profile.min_volume_ratio, 1.10)
    compression_impulse_floor = max(profile.min_body_ratio, 0.90)
    compression_long_ok = (
        regime == "compression"
        and recent_long_cross
        and breakout_up
        and ao_long_ok
        and volume_ratio >= compression_volume_floor
        and body_ratio >= compression_impulse_floor
    )
    compression_short_ok = (
        regime == "compression"
        and recent_short_cross
        and breakout_down
        and ao_short_ok
        and volume_ratio >= compression_volume_floor
        and body_ratio >= compression_impulse_floor
    )

    long_rsi_ceiling = min(
        profile.rsi_long_max,
        profile.late_rsi_long,
        68.0 if outside_main_session else profile.late_rsi_long,
    )
    rsi_long_ok = _rsi_direction_ok(
        rsi,
        prev_rsi,
        prev2_rsi,
        "LONG",
        floor=max(profile.rsi_long_min, 45.0),
        ceiling=long_rsi_ceiling,
    )
    rsi_short_ok = _rsi_direction_ok(
        rsi,
        prev_rsi,
        prev2_rsi,
        "SHORT",
        floor=0.0,
        ceiling=min(profile.rsi_short_max, 55.0),
    )
    rsi_long_extreme_bad = rsi < max(profile.rsi_long_min - 10.0, 35.0) and rsi < prev_rsi
    rsi_short_extreme_bad = rsi > min(profile.rsi_short_max + 10.0, 65.0) and rsi > prev_rsi
    ao_long_supported = ao_long_ok or (trend_like_regime and _ao_trend_tolerates(ao, prev_ao, "LONG"))
    ao_short_supported = ao_short_ok or (trend_like_regime and _ao_trend_tolerates(ao, prev_ao, "SHORT"))
    chaikin_long_ok = chaikin >= prev_chaikin and (chaikin > 0 or chaikin_delta > 0)
    chaikin_short_ok = chaikin <= prev_chaikin and (chaikin < 0 or chaikin_delta < 0)
    volume_ok = volume_ratio >= profile.min_volume_ratio
    soft_volume_ok = volume_ratio >= soft_volume_floor
    impulse_ok = body_ratio >= profile.min_body_ratio
    soft_impulse_ok = body_ratio >= soft_impulse_floor
    # A new hourly position must be backed by the current candle, rather than
    # by a weak continuation of an earlier move.  This is deliberately shared
    # by every unified-reversal instrument.
    entry_volume_ok = volume_ratio >= max(profile.min_volume_ratio, 1.0)
    entry_impulse_ok = body_ratio >= max(profile.min_body_ratio, 0.75)
    volatility_ok = atr_pct >= profile.min_atr_pct
    soft_volatility_ok = atr_pct >= soft_volatility_floor
    macd_long_ok = (
        macd >= macd_signal
        and macd_hist >= (prev_hist - 1e-9)
        and long_cross_age is not None
        and long_cross_age <= recent_cross_age_limit
    )
    macd_short_ok = (
        macd <= macd_signal
        and macd_hist <= (prev_hist + 1e-9)
        and short_cross_age is not None
        and short_cross_age <= recent_cross_age_limit
    )
    early_long_ok = close >= ema20 and ema20 >= prev_ema20
    early_short_ok = close <= ema20 and ema20 <= prev_ema20
    strong_impulse_override = volume_ratio >= profile.strong_volume_ratio and body_ratio >= profile.strong_body_ratio
    emerging_long_pressure_ok = (
        recent_long_cross
        and close >= ema20
        and ao_long_supported
        and rsi_long_ok
        and body_ratio >= soft_impulse_floor
        and volume_ratio >= max(0.45, soft_volume_floor - 0.10)
    )
    emerging_short_pressure_ok = (
        recent_short_cross
        and close <= ema20
        and ao_short_supported
        and rsi_short_ok
        and body_ratio >= soft_impulse_floor
        and volume_ratio >= max(0.45, soft_volume_floor - 0.10)
    )
    # The opening hours can carry a real directional impulse before the latest
    # candle reaches the rolling average volume.  Admit only the narrow case
    # observed on NGZ6 and SRZ6: an established trend, a fresh MACD crossing,
    # a large directional body and two-candle RSI/AO/Chaikin confirmation.
    # The explicit reason below keeps this path at half size.
    moderate_volume_long_structure_ok = (
        (close >= ema20 and ema20 >= prev_ema20)
        or (close >= prev_close and distance_to_ema20_pct <= 0.003)
    )
    moderate_volume_short_structure_ok = (
        (close <= ema20 and ema20 <= prev_ema20)
        or (close <= prev_close and distance_to_ema20_pct <= 0.003)
    )
    moderate_volume_long_impulse_ok = (
        regime == "trend"
        and path_efficiency_6h >= MODERATE_VOLUME_IMPULSE_PATH_EFFICIENCY_6H
        and recent_long_cross
        and macd_long_ok
        and moderate_volume_long_structure_ok
        and ao_long_supported
        and rsi_long_ok
        and chaikin_long_ok
        and volume_ratio >= MODERATE_VOLUME_IMPULSE_MIN_VOLUME_RATIO
        and body_ratio >= MODERATE_VOLUME_IMPULSE_MIN_BODY_RATIO
        and soft_volatility_ok
        and not late_evening
        and not rsi_long_extreme_bad
    )
    moderate_volume_short_impulse_ok = (
        regime == "trend"
        and path_efficiency_6h >= MODERATE_VOLUME_IMPULSE_PATH_EFFICIENCY_6H
        and recent_short_cross
        and macd_short_ok
        and moderate_volume_short_structure_ok
        and ao_short_supported
        and rsi_short_ok
        and chaikin_short_ok
        and volume_ratio >= MODERATE_VOLUME_IMPULSE_MIN_VOLUME_RATIO
        and body_ratio >= MODERATE_VOLUME_IMPULSE_MIN_BODY_RATIO
        and soft_volatility_ok
        and not late_evening
        and not rsi_short_extreme_bad
    )
    moderate_volume_long_entry = moderate_volume_long_impulse_ok and not entry_volume_ok
    moderate_volume_short_entry = moderate_volume_short_impulse_ok and not entry_volume_ok
    long_entry_volume_ok = entry_volume_ok or moderate_volume_long_entry
    short_entry_volume_ok = entry_volume_ok or moderate_volume_short_entry
    long_volume_ok = soft_volume_ok or emerging_long_pressure_ok or (late_evening and volume_ratio >= 0.35 and chaikin_long_ok)
    short_volume_ok = soft_volume_ok or emerging_short_pressure_ok or (late_evening and volume_ratio >= 0.35 and chaikin_short_ok)
    evening_long_pressure_ok = (not late_evening) or volume_ratio >= 1.0 or (volume_ratio >= 0.35 and chaikin_long_ok)
    evening_short_pressure_ok = (not late_evening) or volume_ratio >= 1.0 or (volume_ratio >= 0.35 and chaikin_short_ok)
    slow_long_continuation_ok = (
        regime != "chop"
        and recent_long_cross
        and macd_long_ok
        and close >= ema20
        and ema20 >= prev_ema20
        and close >= prev_close
        and ao_long_supported
        and rsi_long_ok
        and long_volume_ok
        and evening_long_pressure_ok
        and soft_impulse_ok
    )
    slow_short_continuation_ok = (
        regime != "chop"
        and recent_short_cross
        and macd_short_ok
        and close <= ema20
        and ema20 <= prev_ema20
        and close <= prev_close
        and ao_short_supported
        and rsi_short_ok
        and short_volume_ok
        and evening_short_pressure_ok
        and soft_impulse_ok
    )
    # A continuation has no fresh MACD crossing to protect the entry.  It must
    # therefore prove that the current candle carries the trend, not just drift.
    continuation_volume_floor = max(profile.min_volume_ratio, 1.15)
    continuation_impulse_floor = max(profile.min_body_ratio, 0.90)
    trend_long_continuation_ok = (
        trend_like_regime
        and close >= ema20
        and ema20 >= prev_ema20
        and macd >= macd_signal
        and macd_hist >= 0
        and (macd_hist >= prev_hist or rsi >= prev_rsi)
        and rsi_long_ok
        and ao_long_ok
        and volume_ratio >= continuation_volume_floor
        and body_ratio >= continuation_impulse_floor
        and soft_volatility_ok
        and not rsi_long_extreme_bad
    )
    trend_short_continuation_ok = (
        trend_like_regime
        and close <= ema20
        and ema20 <= prev_ema20
        and macd <= macd_signal
        and macd_hist <= 0
        and (macd_hist <= prev_hist or rsi <= prev_rsi)
        and rsi_short_ok
        and ao_short_ok
        and volume_ratio >= continuation_volume_floor
        and body_ratio >= continuation_impulse_floor
        and soft_volatility_ok
        and not rsi_short_extreme_bad
    )
    expansion_entry_distance_limit = profile.max_distance_to_ema20_pct * 0.70
    fresh_expansion_entry = (
        regime != "expansion"
        or distance_to_ema20_pct <= expansion_entry_distance_limit
    )
    expansion_exhaustion_distance_floor = max(0.006, atr_pct * 1.50)
    exhausted_expansion_long = (
        expansion_up
        and stoch_k >= profile.late_stoch_high
        and rsi >= 65.0
        and distance_to_ema20_pct >= expansion_exhaustion_distance_floor
    )
    exhausted_expansion_short = (
        expansion_down
        and stoch_k <= profile.late_stoch_low
        and rsi <= 35.0
        and distance_to_ema20_pct >= expansion_exhaustion_distance_floor
    )

    # A fresh AO zero-line crossing catches the first hour of a movement that
    # can precede the slower MACD confirmation.  It is intentionally available
    # only in a mixed market: trend and expansion already use the normal path,
    # while chop/compression remain blocked.  The smaller position size is
    # applied by the allocator from the explicit reason below.
    ao_impulse_floor = max(float(last.get("atr", 0.0)) * 0.12, abs(close) * 0.0005)
    early_ao_long = prev2_ao <= 0.0 < prev_ao < ao and ao >= ao_impulse_floor
    early_ao_short = prev2_ao >= 0.0 > prev_ao > ao and -ao >= ao_impulse_floor
    early_momentum_long_ok = (
        regime == "mixed"
        and early_ao_long
        and close >= ema20
        and ema20 >= prev_ema20
        and rsi >= max(47.0, prev_rsi)
        and rsi <= long_rsi_ceiling
        and chaikin >= prev_chaikin
        and volume_ratio >= max(0.75, soft_volume_floor)
        and body_ratio >= max(0.60, soft_impulse_floor)
        and soft_volatility_ok
    )
    early_momentum_short_ok = (
        regime == "mixed"
        and early_ao_short
        and close <= ema20
        and ema20 <= prev_ema20
        and rsi <= min(53.0, prev_rsi)
        and rsi >= profile.late_rsi_short
        and chaikin <= prev_chaikin
        and volume_ratio >= max(0.75, soft_volume_floor)
        and body_ratio >= max(0.60, soft_impulse_floor)
        and soft_volatility_ok
    )

    # A zero-line cross can occur during a thin weekend session and receive its
    # real confirmation only after liquidity returns. Keep that context for up
    # to twelve completed hourly candles, but require a fresh AO saucer plus a
    # visible price breakout, strong volume/body and confirming money flow.
    delayed_ao_confirmation_bars = 12
    ao_saucer_long = prev2_ao > prev_ao and ao > prev_ao and ao > 0.0
    ao_saucer_short = prev2_ao < prev_ao and ao < prev_ao and ao < 0.0
    delayed_ao_long_ok = (
        regime == "mixed"
        and long_ao_cross_age is not None
        and 2 <= long_ao_cross_age <= delayed_ao_confirmation_bars
        and ao_saucer_long
        and path_efficiency_6h > MIN_TREND_PATH_EFFICIENCY_6H
        and breakout_up
        and close >= ema20
        and ema20 >= prev_ema20
        and macd_hist > 0.0
        and macd_hist >= prev_hist
        and rsi_long_ok
        and chaikin_long_ok
        and chaikin > 0.0
        and volume_ratio >= max(profile.strong_volume_ratio, 1.50)
        and body_ratio >= max(profile.strong_body_ratio, 1.25)
        and soft_volatility_ok
    )
    delayed_ao_short_ok = (
        regime == "mixed"
        and short_ao_cross_age is not None
        and 2 <= short_ao_cross_age <= delayed_ao_confirmation_bars
        and ao_saucer_short
        and path_efficiency_6h > MIN_TREND_PATH_EFFICIENCY_6H
        and breakout_down
        and close <= ema20
        and ema20 <= prev_ema20
        and macd_hist < 0.0
        and macd_hist <= prev_hist
        and rsi_short_ok
        and chaikin_short_ok
        and chaikin < 0.0
        and volume_ratio >= max(profile.strong_volume_ratio, 1.50)
        and body_ratio >= max(profile.strong_body_ratio, 1.25)
        and soft_volatility_ok
    )

    late_long = (
        long_cross_age is None
        or long_cross_age > late_cross_age_limit
        or (
            distance_to_ema20_pct >= profile.max_distance_to_ema20_pct
            and not strong_impulse_override
            and not ao_long_ok
            and (long_cross_age is None or long_cross_age > 3)
        )
    )
    late_short = (
        short_cross_age is None
        or short_cross_age > late_cross_age_limit
        or (
            distance_to_ema20_pct >= profile.max_distance_to_ema20_pct
            and not strong_impulse_override
            and not ao_short_ok
            and (short_cross_age is None or short_cross_age > 3)
        )
    )
    severe_late_distance = max(profile.max_distance_to_ema20_pct * 2.0, profile.max_distance_to_ema20_pct + 0.015)
    hard_late_long = late_long and (
        not trend_like_regime
        or (
            distance_to_ema20_pct >= severe_late_distance
            and rsi >= profile.late_rsi_long
            and stoch_k >= profile.late_stoch_high
            and not strong_impulse_override
            and not ao_long_supported
        )
    )
    hard_late_short = late_short and (
        not trend_like_regime
        or (
            distance_to_ema20_pct >= severe_late_distance
            and rsi <= profile.late_rsi_short
            and stoch_k <= profile.late_stoch_low
            and not strong_impulse_override
            and not ao_short_supported
        )
    )
    # RSI and stochastic are not exit triggers for an open trend position.
    # For a *new* entry, however, their joint extreme far from EMA20 usually
    # means the impulse has already spent its easy part.
    exhaustion_distance_floor = 0.005
    exhausted_long_entry = (
        rsi >= profile.late_rsi_long + 2.0
        and stoch_k >= profile.late_stoch_high - 10.0
        and distance_to_ema20_pct >= exhaustion_distance_floor
    )
    exhausted_short_entry = (
        rsi <= profile.late_rsi_short - 2.0
        and stoch_k <= profile.late_stoch_low + 10.0
        and distance_to_ema20_pct >= exhaustion_distance_floor
    )

    long_reasons = [
        f"режим={regime}",
        f"эффективность пути 6ч={path_efficiency_6h:.2f}",
        f"MACD cross вверх: {'да' if recent_long_cross else 'нет'}",
        f"RSI={rsi:.2f} и {'растёт' if rsi >= prev_rsi else 'падает'}",
        f"AO={ao:.4f} и {'растёт' if ao >= prev_ao else 'падает'}",
        f"поток Чайкина={chaikin:.2f} и {'растёт' if chaikin >= prev_chaikin else 'падает'}",
        f"Stochastic K/D={stoch_k:.1f}/{stoch_d:.1f}",
        f"объём x{volume_ratio:.2f}",
        f"импульс x{body_ratio:.2f}",
        f"ATR%={atr_pct:.4f}",
        f"distance EMA20={distance_to_ema20_pct:.4f}",
        f"вечер после 21: {'да' if late_evening else 'нет'}",
    ]
    short_reasons = [
        f"режим={regime}",
        f"эффективность пути 6ч={path_efficiency_6h:.2f}",
        f"MACD cross вниз: {'да' if recent_short_cross else 'нет'}",
        f"RSI={rsi:.2f} и {'падает' if rsi <= prev_rsi else 'растёт'}",
        f"AO={ao:.4f} и {'падает' if ao <= prev_ao else 'растёт'}",
        f"поток Чайкина={chaikin:.2f} и {'падает' if chaikin <= prev_chaikin else 'растёт'}",
        f"Stochastic K/D={stoch_k:.1f}/{stoch_d:.1f}",
        f"объём x{volume_ratio:.2f}",
        f"импульс x{body_ratio:.2f}",
        f"ATR%={atr_pct:.4f}",
        f"distance EMA20={distance_to_ema20_pct:.4f}",
        f"вечер после 21: {'да' if late_evening else 'нет'}",
    ]

    long_blockers: list[str] = []
    short_blockers: list[str] = []
    long_warnings: list[str] = []
    short_warnings: list[str] = []
    if regime not in {"trend", "expansion"}:
        long_blockers.append("новые входы разрешены только в устойчивом тренде")
        short_blockers.append("новые входы разрешены только в устойчивом тренде")
    if path_efficiency_6h <= MIN_TREND_PATH_EFFICIENCY_6H:
        efficiency_reason = (
            f"эффективность пути 6ч {path_efficiency_6h:.2f} <= "
            f"{MIN_TREND_PATH_EFFICIENCY_6H:.2f}: движение похоже на пилу"
        )
        long_blockers.append(efficiency_reason)
        short_blockers.append(efficiency_reason)
    if regime == "chop":
        long_blockers.append("режим chop: переворот запрещён")
        short_blockers.append("режим chop: переворот запрещён")
    if regime == "compression":
        if not compression_long_ok:
            long_blockers.append("режим compression: нужен пробой вверх на объёме")
        if not compression_short_ok:
            short_blockers.append("режим compression: нужен пробой вниз на объёме")
    if regime == "mixed":
        long_blockers.append("режим mixed: новые позиции запрещены")
        short_blockers.append("режим mixed: новые позиции запрещены")
    if regime == "expansion" and not fresh_expansion_entry:
        expansion_late_reason = (
            f"расширение уже ушло от EMA20 на {distance_to_ema20_pct:.4f} > "
            f"{expansion_entry_distance_limit:.4f}"
        )
        long_blockers.append(expansion_late_reason)
        short_blockers.append(expansion_late_reason)
    if not recent_long_cross and not trend_long_continuation_ok:
        long_blockers.append("нет свежего MACD cross вверх")
    if not recent_short_cross and not trend_short_continuation_ok:
        short_blockers.append("нет свежего MACD cross вниз")
    if rsi_long_extreme_bad and not slow_long_continuation_ok:
        long_blockers.append("RSI не подтверждает рост")
    if rsi_short_extreme_bad and not slow_short_continuation_ok:
        short_blockers.append("RSI не подтверждает снижение")
    if not rsi_long_ok and not slow_long_continuation_ok:
        if rsi > long_rsi_ceiling:
            long_blockers.append(
                f"длинный вход перегрет: RSI {rsi:.2f} выше предела {long_rsi_ceiling:.2f}"
            )
        else:
            long_blockers.append("RSI ещё не развернулся достаточно уверенно")
    if not rsi_short_ok and not slow_short_continuation_ok:
        short_blockers.append("RSI ещё не развернулся достаточно уверенно")
    if not ao_long_ok:
        long_blockers.append("AO не подтверждает рост")
    if not ao_short_ok:
        short_blockers.append("AO не подтверждает снижение")
    if not long_entry_volume_ok:
        long_blockers.append("объём слишком слабый")
    if not short_entry_volume_ok:
        short_blockers.append("объём слишком слабый")
    if not entry_impulse_ok:
        long_blockers.append("импульс свечи слишком слабый")
        short_blockers.append("импульс свечи слишком слабый")
    if not soft_volatility_ok:
        long_blockers.append("волатильность слишком низкая")
        short_blockers.append("волатильность слишком низкая")
    if hard_late_long:
        long_blockers.append("late entry: движение уже ушло")
    elif exhausted_long_entry:
        long_blockers.append("вход перегрет: RSI и Stochastic высоки далеко от EMA20")
    elif late_long:
        long_warnings.append("late entry мягкий: требуется свежий подтверждённый cross")
    if hard_late_short:
        short_blockers.append("late entry: движение уже ушло")
    elif exhausted_short_entry:
        short_blockers.append("вход перегрет: RSI и Stochastic низки далеко от EMA20")
    elif late_short:
        short_warnings.append("late entry мягкий: требуется свежий подтверждённый cross")
    if exhausted_expansion_long:
        long_blockers.append(
            "истощённое расширение вверх: дождаться новой часовой свечи после крайней зоны стохастического осциллятора"
        )
    if exhausted_expansion_short:
        short_blockers.append(
            "истощённое расширение вниз: дождаться новой часовой свечи после крайней зоны стохастического осциллятора"
        )

    regime_allows_long = regime in {"trend", "expansion"} or compression_long_ok
    regime_allows_short = regime in {"trend", "expansion"} or compression_short_ok
    normal_long_ok = (
        regime_allows_long
        and (trend_up or expansion_up or early_long_ok or moderate_volume_long_entry)
        and (recent_long_cross or trend_long_continuation_ok)
        # A qualified trend continuation deliberately has no recent MACD cross.
        # Requiring macd_long_ok here made that branch unreachable.
        and (macd_long_ok or trend_long_continuation_ok)
        and ao_long_ok
        and rsi_long_ok
        and long_entry_volume_ok
        and entry_impulse_ok
        and soft_volatility_ok
        and fresh_expansion_entry
        and (not hard_late_long or early_momentum_long_ok)
        and not exhausted_long_entry
        and not exhausted_expansion_long
        and not rsi_long_extreme_bad
    )
    normal_short_ok = (
        regime_allows_short
        and (trend_down or expansion_down or early_short_ok or moderate_volume_short_entry)
        and (recent_short_cross or trend_short_continuation_ok)
        and (macd_short_ok or trend_short_continuation_ok)
        and ao_short_ok
        and rsi_short_ok
        and short_entry_volume_ok
        and entry_impulse_ok
        and soft_volatility_ok
        and fresh_expansion_entry
        and (not hard_late_short or early_momentum_short_ok)
        and not exhausted_short_entry
        and not exhausted_expansion_short
        and not rsi_short_extreme_bad
    )
    long_ok = (entry_regime_quality_ok and (normal_long_ok or early_momentum_long_ok)) or delayed_ao_long_ok
    short_ok = (entry_regime_quality_ok and (normal_short_ok or early_momentum_short_ok)) or delayed_ao_short_ok
    if long_warnings:
        long_reasons.extend(long_warnings)
    if short_warnings:
        short_reasons.extend(short_warnings)

    if long_ok:
        if delayed_ao_long_ok:
            long_reasons.append(
                f"позднее подтверждение AO через {long_ao_cross_age} свечей после пересечения нуля: "
                "пробой подтверждён ценой, объёмом и потоком Чайкина; стартовый размер 50%"
            )
        elif early_momentum_long_ok and not normal_long_ok:
            long_reasons.append("ранний импульс AO: две растущие свечи выше нуля, стартовый размер 50%")
        elif moderate_volume_long_entry:
            long_reasons.append("направленный импульс при умеренном объёме, стартовый размер 50%")
        elif regime == "expansion":
            long_reasons.append("подтверждённое расширение вверх, стартовый размер 50%")
        return "LONG", f"Сигнал LONG ({strategy_label}): " + "; ".join(long_reasons) + "."
    if short_ok:
        if delayed_ao_short_ok:
            short_reasons.append(
                f"позднее подтверждение AO через {short_ao_cross_age} свечей после пересечения нуля: "
                "пробой подтверждён ценой, объёмом и потоком Чайкина; стартовый размер 50%"
            )
        elif early_momentum_short_ok and not normal_short_ok:
            short_reasons.append("ранний импульс AO: две снижающиеся свечи ниже нуля, стартовый размер 50%")
        elif moderate_volume_short_entry:
            short_reasons.append("направленный импульс при умеренном объёме, стартовый размер 50%")
        elif regime == "expansion":
            short_reasons.append("подтверждённое расширение вниз, стартовый размер 50%")
        return "SHORT", f"Сигнал SHORT ({strategy_label}): " + "; ".join(short_reasons) + "."
    if (
        _allow_mixed_confirmation
        and detected_regime == "mixed"
        and path_efficiency_6h > MIN_TREND_PATH_EFFICIENCY_6H
        and len(df) >= 9
    ):
        previous_df = df.iloc[:-1]
        current_shadow_signal, current_shadow_reason = evaluate_signal_core(
            df,
            config,
            instrument,
            higher_tf_bias,
            timeframe_minutes=timeframe_minutes,
            strategy_label=strategy_label,
            _regime_override="trend",
            _allow_mixed_confirmation=False,
        )
        if current_shadow_signal in {"LONG", "SHORT"}:
            previous_signal, previous_reason = evaluate_signal_core(
                previous_df,
                config,
                instrument,
                higher_tf_bias,
                timeframe_minutes=timeframe_minutes,
                strategy_label=strategy_label,
                _allow_mixed_confirmation=False,
            )
            previous_shadow_signal, _ = evaluate_signal_core(
                previous_df,
                config,
                instrument,
                higher_tf_bias,
                timeframe_minutes=timeframe_minutes,
                strategy_label=strategy_label,
                _regime_override="trend",
                _allow_mixed_confirmation=False,
            )
            previous_was_mixed_hold = previous_signal == "HOLD" and "режим=mixed" in previous_reason
        else:
            previous_was_mixed_hold = False
            previous_shadow_signal = "HOLD"
        if previous_was_mixed_hold and current_shadow_signal == previous_shadow_signal:
            mixed_reason = current_shadow_reason.replace(
                "режим=trend",
                "режим=mixed→impulse",
                1,
            )
            return (
                current_shadow_signal,
                mixed_reason[:-1]
                + "; mixed→impulse: два часовых подтверждения, стартовый размер 50%.",
            )
    return (
        "HOLD",
        f"Сигнал HOLD ({strategy_label}): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
