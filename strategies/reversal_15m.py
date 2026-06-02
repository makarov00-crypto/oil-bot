from dataclasses import dataclass
from zoneinfo import ZoneInfo

from instrument_groups import is_brent_symbol


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class Reversal15mProfile:
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


def get_profile(symbol: str, timeframe_minutes: int = 15) -> Reversal15mProfile:
    if timeframe_minutes >= 60 and is_brent_symbol(symbol):
        return Reversal15mProfile(
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
    if timeframe_minutes >= 60:
        return Reversal15mProfile(
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
    if is_brent_symbol(symbol):
        return Reversal15mProfile(
            min_volume_ratio=0.90,
            strong_volume_ratio=1.15,
            min_body_ratio=0.75,
            strong_body_ratio=1.10,
            rsi_long_min=46.0,
            rsi_long_max=70.0,
            rsi_short_min=30.0,
            rsi_short_max=56.0,
            late_rsi_long=68.0,
            late_rsi_short=32.0,
            late_stoch_high=88.0,
            late_stoch_low=12.0,
            max_distance_to_ema20_pct=0.012,
            compression_atr_pct=0.0012,
            compression_range_pct=0.010,
            compression_bb_width_pct=0.012,
            chop_range_pct=0.015,
            expansion_range_pct=0.018,
            min_atr_pct=0.0010,
        )
    return Reversal15mProfile(
        min_volume_ratio=0.95,
        strong_volume_ratio=1.18,
        min_body_ratio=0.80,
        strong_body_ratio=1.05,
        rsi_long_min=45.0,
        rsi_long_max=68.0,
        rsi_short_min=32.0,
        rsi_short_max=55.0,
        late_rsi_long=66.0,
        late_rsi_short=34.0,
        late_stoch_high=86.0,
        late_stoch_low=14.0,
        max_distance_to_ema20_pct=0.008,
        compression_atr_pct=0.0008,
        compression_range_pct=0.007,
        compression_bb_width_pct=0.010,
        chop_range_pct=0.010,
        expansion_range_pct=0.013,
        min_atr_pct=0.0005,
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


def evaluate_signal_core(
    df,
    config,
    instrument,
    higher_tf_bias: str,
    *,
    timeframe_minutes: int,
    strategy_label: str,
    timeframe_label: str,
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
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = float(last["body"])
    body_avg = float(last["body_avg"])
    atr_pct = float(last["atr"]) / close if close else 0.0
    bb_width_pct = (float(last["bb_upper"]) - float(last["bb_lower"])) / close if close else 0.0
    volume_ratio = (volume / volume_avg) if volume_avg > 0 else 0.0
    body_ratio = (body / body_avg) if body_avg > 0 else 0.0
    distance_to_ema20_pct = abs(close - ema20) / close if close else 0.0
    late_evening = _is_late_evening_candle(last.get("time"))
    recent_high = float(pre_recent["high"].max())
    recent_low = float(pre_recent["low"].min())
    recent_range_pct = (float(recent["high"].max()) - float(recent["low"].min())) / close if close else 0.0

    macd_series = [float(value) for value in recent["macd"].tolist()]
    macd_signal_series = [float(value) for value in recent["macd_signal"].tolist()]
    long_cross_age = _bars_since_cross(macd_series, macd_signal_series, "LONG")
    short_cross_age = _bars_since_cross(macd_series, macd_signal_series, "SHORT")
    recent_cross_age_limit = 4 if timeframe_minutes >= 60 else 6
    late_cross_age_limit = 6 if timeframe_minutes >= 60 else 8
    recent_long_cross = long_cross_age is not None and long_cross_age <= recent_cross_age_limit
    recent_short_cross = short_cross_age is not None and short_cross_age <= recent_cross_age_limit
    macd_hist = macd - macd_signal
    prev_hist = prev_macd - prev_macd_signal
    ao = float(last.get("ao", macd_hist))
    prev_ao = float(prev.get("ao", prev_hist))
    prev2 = df.iloc[-3]
    prev2_ao = float(prev2.get("ao", prev2.get("macd", 0.0) - prev2.get("macd_signal", 0.0)))
    chaikin = float(last.get("chaikin", 0.0))
    prev_chaikin = float(prev.get("chaikin", 0.0))
    chaikin_delta = chaikin - prev_chaikin
    macd_flip_count = _macd_flip_count(macd_series, macd_signal_series)

    trend_up = close > ema20 and ema20 >= ema50 and ema20 >= prev_ema20
    trend_down = close < ema20 and ema20 <= ema50 and ema20 <= prev_ema20
    structure_up = close > ema20 and close > ema50 and ema20 >= prev_ema20 and close >= prev_close
    structure_down = close < ema20 and close < ema50 and ema20 <= prev_ema20 and close <= prev_close
    expansion_up = structure_up and macd_hist > 0 and macd_hist > prev_hist and volume_ratio >= profile.strong_volume_ratio and body_ratio >= profile.strong_body_ratio and recent_range_pct >= profile.expansion_range_pct
    expansion_down = structure_down and macd_hist < 0 and macd_hist < prev_hist and volume_ratio >= profile.strong_volume_ratio and body_ratio >= profile.strong_body_ratio and recent_range_pct >= profile.expansion_range_pct
    compression = atr_pct <= profile.compression_atr_pct and recent_range_pct <= profile.compression_range_pct and bb_width_pct <= profile.compression_bb_width_pct and volume_ratio < 1.05
    soft_volume_floor = max(0.55, profile.min_volume_ratio - 0.30)
    soft_impulse_floor = max(0.45, profile.min_body_ratio - 0.35)
    soft_volatility_floor = profile.min_atr_pct * 0.60
    fresh_impulse_override = (
        (
            (long_cross_age is not None and long_cross_age <= 2)
            or (short_cross_age is not None and short_cross_age <= 2)
        )
        and volume_ratio >= soft_volume_floor
        and body_ratio >= soft_impulse_floor
    )
    chop = (
        (not compression)
        and macd_flip_count >= 4
        and recent_range_pct <= profile.chop_range_pct
        and volume_ratio < profile.strong_volume_ratio
        and not fresh_impulse_override
    )

    if compression:
        regime = "compression"
    elif chop:
        regime = "chop"
    elif expansion_up or expansion_down:
        regime = "expansion"
    elif trend_up or trend_down:
        regime = "trend"
    else:
        regime = "mixed"

    breakout_up = close > recent_high and close > ema20
    breakout_down = close < recent_low and close < ema20
    compression_long_ok = regime == "compression" and breakout_up and volume_ratio >= soft_volume_floor and body_ratio >= soft_impulse_floor
    compression_short_ok = regime == "compression" and breakout_down and volume_ratio >= soft_volume_floor and body_ratio >= soft_impulse_floor

    rsi_long_ok = rsi >= max(profile.rsi_long_min - 5.0, 40.0) and rsi >= (prev_rsi - 1.0)
    rsi_short_ok = rsi <= min(profile.rsi_short_max + 5.0, 60.0) and rsi <= (prev_rsi + 1.0)
    rsi_long_extreme_bad = rsi < max(profile.rsi_long_min - 10.0, 35.0) and rsi < prev_rsi
    rsi_short_extreme_bad = rsi > min(profile.rsi_short_max + 10.0, 65.0) and rsi > prev_rsi
    ao_long_ok = (
        (ao >= prev_ao and prev_ao >= prev2_ao)
        or (ao > 0 and prev_ao <= 0)
        or (ao >= prev_ao and macd_hist > prev_hist)
    )
    ao_short_ok = (
        (ao <= prev_ao and prev_ao <= prev2_ao)
        or (ao < 0 and prev_ao >= 0)
        or (ao <= prev_ao and macd_hist < prev_hist)
    )
    chaikin_long_ok = chaikin >= prev_chaikin and (chaikin > 0 or chaikin_delta > 0)
    chaikin_short_ok = chaikin <= prev_chaikin and (chaikin < 0 or chaikin_delta < 0)
    volume_ok = volume_ratio >= profile.min_volume_ratio
    soft_volume_ok = volume_ratio >= soft_volume_floor
    long_volume_ok = soft_volume_ok or (late_evening and volume_ratio >= 0.35 and chaikin_long_ok)
    short_volume_ok = soft_volume_ok or (late_evening and volume_ratio >= 0.35 and chaikin_short_ok)
    impulse_ok = body_ratio >= profile.min_body_ratio
    soft_impulse_ok = body_ratio >= soft_impulse_floor
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
    evening_long_pressure_ok = (not late_evening) or volume_ratio >= 1.0 or (volume_ratio >= 0.35 and chaikin_long_ok)
    evening_short_pressure_ok = (not late_evening) or volume_ratio >= 1.0 or (volume_ratio >= 0.35 and chaikin_short_ok)
    slow_long_continuation_ok = (
        regime != "chop"
        and recent_long_cross
        and macd_long_ok
        and close >= ema20
        and ema20 >= prev_ema20
        and close >= prev_close
        and ao_long_ok
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
        and ao_short_ok
        and short_volume_ok
        and evening_short_pressure_ok
        and soft_impulse_ok
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

    long_reasons = [
        f"режим={regime}",
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
    if regime == "chop":
        long_blockers.append("режим chop: переворот запрещён")
        short_blockers.append("режим chop: переворот запрещён")
    if regime == "compression" and not (
        compression_long_ok
        or (macd_long_ok and ao_long_ok and early_long_ok and long_volume_ok and soft_impulse_ok)
    ):
        long_blockers.append("режим compression: нет пробоя с объёмом и импульсом")
    if regime == "compression" and not (
        compression_short_ok
        or (macd_short_ok and ao_short_ok and early_short_ok and short_volume_ok and soft_impulse_ok)
    ):
        short_blockers.append("режим compression: нет пробоя с объёмом и импульсом")
    if rsi_long_extreme_bad and not slow_long_continuation_ok:
        long_blockers.append("RSI не подтверждает рост")
    if rsi_short_extreme_bad and not slow_short_continuation_ok:
        short_blockers.append("RSI не подтверждает снижение")
    if not ao_long_ok and not slow_long_continuation_ok:
        long_blockers.append("AO не подтверждает рост")
    if not ao_short_ok and not slow_short_continuation_ok:
        short_blockers.append("AO не подтверждает снижение")
    if not long_volume_ok:
        long_blockers.append("объём слишком слабый")
    if not short_volume_ok:
        short_blockers.append("объём слишком слабый")
    if not soft_impulse_ok and not (slow_long_continuation_ok or slow_short_continuation_ok):
        long_blockers.append("импульс свечи слишком слабый")
        short_blockers.append("импульс свечи слишком слабый")
    if not soft_volatility_ok and not (slow_long_continuation_ok or slow_short_continuation_ok):
        long_blockers.append("волатильность слишком низкая")
        short_blockers.append("волатильность слишком низкая")
    if late_long and not slow_long_continuation_ok:
        long_blockers.append("late entry: движение уже ушло")
    if late_short and not slow_short_continuation_ok:
        short_blockers.append("late entry: движение уже ушло")

    regime_allows_long = regime in {"trend", "expansion", "compression", "mixed"} or (regime == "chop" and (fresh_impulse_override or (recent_long_cross and ao_long_ok and soft_impulse_ok)))
    regime_allows_short = regime in {"trend", "expansion", "compression", "mixed"} or (regime == "chop" and (fresh_impulse_override or (recent_short_cross and ao_short_ok and soft_impulse_ok)))
    long_ok = (
        regime_allows_long
        and (trend_up or expansion_up or compression_long_ok or early_long_ok)
        and recent_long_cross
        and macd_long_ok
        and ao_long_ok
        and long_volume_ok
        and soft_impulse_ok
        and soft_volatility_ok
        and not late_long
        and not rsi_long_extreme_bad
    )
    short_ok = (
        regime_allows_short
        and (trend_down or expansion_down or compression_short_ok or early_short_ok)
        and recent_short_cross
        and macd_short_ok
        and ao_short_ok
        and short_volume_ok
        and soft_impulse_ok
        and soft_volatility_ok
        and not late_short
        and not rsi_short_extreme_bad
    )
    if not long_ok and slow_long_continuation_ok:
        long_ok = True
        long_reasons.append("медленное продолжение вверх по MACD")
    if not short_ok and slow_short_continuation_ok:
        short_ok = True
        short_reasons.append("медленное продолжение вниз по MACD")

    if long_ok:
        return "LONG", f"Сигнал LONG ({strategy_label}): " + "; ".join(long_reasons) + "."
    if short_ok:
        return "SHORT", f"Сигнал SHORT ({strategy_label}): " + "; ".join(short_reasons) + "."
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


def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    return evaluate_signal_core(
        df,
        config,
        instrument,
        higher_tf_bias,
        timeframe_minutes=15,
        strategy_label="reversal_15m",
        timeframe_label="15м",
    )
