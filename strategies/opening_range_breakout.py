from datetime import time
from zoneinfo import ZoneInfo

import pandas as pd

from instrument_groups import get_instrument_group


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
FX_OPENING_RANGE_START = time(9, 0)
FX_OPENING_RANGE_CANDLES = 6
FX_OPENING_RANGE_FOLLOW_CANDLES = 6


def _get_today_opening_range(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["time_msk"] = result["time"].dt.tz_convert(MOSCOW_TZ)
    last_day = result["time_msk"].iloc[-1].date()
    session_df = result[
        (result["time_msk"].dt.date == last_day)
        & (result["time_msk"].dt.time >= FX_OPENING_RANGE_START)
    ].reset_index(drop=True)
    return session_df.iloc[:FX_OPENING_RANGE_CANDLES].copy()


def _get_today_session_df(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["time_msk"] = result["time"].dt.tz_convert(MOSCOW_TZ)
    last_day = result["time_msk"].iloc[-1].date()
    return result[
        (result["time_msk"].dt.date == last_day)
        & (result["time_msk"].dt.time >= FX_OPENING_RANGE_START)
    ].reset_index(drop=True)


def _opening_range_is_stale(
    df: pd.DataFrame,
    opening_high: float,
    opening_low: float,
    *,
    follow_candles: int = FX_OPENING_RANGE_FOLLOW_CANDLES,
) -> tuple[bool, str]:
    session_df = _get_today_session_df(df)
    if len(session_df) <= FX_OPENING_RANGE_CANDLES + follow_candles:
        return False, ""
    post_range_df = session_df.iloc[FX_OPENING_RANGE_CANDLES:].reset_index(drop=True)
    for index, row in post_range_df.iterrows():
        close = float(row["close"])
        ema20 = float(row["ema20"])
        if close > opening_high and close > ema20:
            age = len(post_range_df) - index - 1
            if age > follow_candles:
                return True, f"пробой вверх opening range уже зрелый: прошло {age} свечей"
            return False, ""
        if close < opening_low and close < ema20:
            age = len(post_range_df) - index - 1
            if age > follow_candles:
                return True, f"пробой вниз opening range уже зрелый: прошло {age} свечей"
            return False, ""
    return False, ""


def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]

    opening_range_df = _get_today_opening_range(df)
    if len(opening_range_df) < 3:
        return "HOLD", "Сигнал HOLD (opening_range_breakout): ещё не накопился opening range текущей сессии."

    close = float(last["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    rsi = float(last["rsi"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    atr_pct = float(last["atr"]) / close if close else 0.0
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = float(last["body"])
    body_avg = float(last["body_avg"])

    opening_high = float(opening_range_df["high"].max())
    opening_low = float(opening_range_df["low"].min())
    opening_mid = (opening_high + opening_low) / 2
    opening_width_pct = (opening_high - opening_low) / close if close else 0.0
    range_compression_ok = opening_width_pct <= 0.0060
    opening_range_stale, stale_reason = _opening_range_is_stale(df, opening_high, opening_low)
    if opening_range_stale:
        return (
            "HOLD",
            "Сигнал HOLD (opening_range_breakout): "
            + stale_reason
            + "; дальше движение должен оценивать range_break_continuation.",
        )

    breakout_up = close > opening_high and close > ema20
    breakout_down = close < opening_low and close < ema20
    soft_breakout_up = close >= opening_high * 0.999 and close > ema20
    soft_breakout_down = close <= opening_low * 1.001 and close < ema20
    volume_ok = volume_avg > 0 and volume >= volume_avg * 0.75
    impulse_ok = body_avg > 0 and body >= body_avg * 0.55
    momentum_up = macd > macd_signal and macd >= prev_macd
    momentum_down = macd < macd_signal and macd <= prev_macd
    fresh_macd_cross_up = macd > macd_signal and prev_macd <= prev_macd_signal
    fresh_macd_cross_down = macd < macd_signal and prev_macd >= prev_macd_signal
    volatility_ok = atr_pct >= 0.0005
    trend_up = close > ema20 and close > ema50
    trend_down = close < ema20 and close < ema50
    hold_above_range = close >= opening_high or close >= opening_mid
    hold_below_range = close <= opening_low or close <= opening_mid
    rsi_long_ok = 42.0 <= rsi <= 72.0
    rsi_short_ok = 28.0 <= rsi <= 58.0
    higher_tf_long_ok = higher_tf_bias != "SHORT"
    higher_tf_short_ok = higher_tf_bias != "LONG"
    is_fx = get_instrument_group(instrument.symbol).name == "fx"
    is_expensive_fx = instrument.symbol in {"USDRUBF", "UCM6"}
    volume_burst = volume_avg > 0 and volume >= volume_avg * 1.20
    impulse_burst = body_avg > 0 and body >= body_avg * 0.95
    if is_fx:
        volume_ok = volume_avg > 0 and volume >= volume_avg * 0.95
        impulse_ok = body_avg > 0 and body >= body_avg * 0.70
        volatility_ok = atr_pct >= 0.0008
        higher_tf_long_ok = higher_tf_bias == "LONG"
        higher_tf_short_ok = higher_tf_bias == "SHORT"
        volume_burst = volume_avg > 0 and volume >= volume_avg * (1.18 if is_expensive_fx else 1.15)
        impulse_burst = body_avg > 0 and body >= body_avg * (0.95 if is_expensive_fx else 0.90)
    commission_room_ok = True
    if is_fx:
        commission_room_ok = max(opening_width_pct, atr_pct) >= (0.0012 if is_expensive_fx else 0.0010)
    fx_fresh_impulse_long = (
        is_fx
        and soft_breakout_up
        and trend_up
        and hold_above_range
        and rsi_long_ok
        and fresh_macd_cross_up
        and volume_burst
        and impulse_burst
        and volatility_ok
        and commission_room_ok
    )
    fx_fresh_impulse_short = (
        is_fx
        and soft_breakout_down
        and trend_down
        and hold_below_range
        and rsi_short_ok
        and fresh_macd_cross_down
        and volume_burst
        and impulse_burst
        and volatility_ok
        and commission_room_ok
    )
    if fx_fresh_impulse_long:
        higher_tf_long_ok = True
    if fx_fresh_impulse_short:
        higher_tf_short_ok = True
    hard_or_strong_soft_up = breakout_up or (soft_breakout_up and volume_ok and impulse_ok and momentum_up)
    hard_or_strong_soft_down = breakout_down or (soft_breakout_down and volume_ok and impulse_ok and momentum_down)

    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"opening range {opening_low:.4f}-{opening_high:.4f}",
        f"сжатие opening range: {'да' if range_compression_ok else 'нет'}",
        f"пробой вверх opening high: {'да' if breakout_up else 'нет'}",
        f"мягкий breakout вверх: {'да' if soft_breakout_up else 'нет'}",
        f"цена выше EMA20 и EMA50: {'да' if trend_up else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 42-72",
        "объём достаточный" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает рост" if momentum_up else "MACD не поддерживает рост",
        "MACD только что пересёкся вверх" if fresh_macd_cross_up else "свежего разворота MACD вверх нет",
        f"ATR%={atr_pct:.4f}, минимум 0.0005",
        "запас движения перекрывает комиссию" if commission_room_ok else "запас движения мал для комиссии",
    ]
    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"opening range {opening_low:.4f}-{opening_high:.4f}",
        f"сжатие opening range: {'да' if range_compression_ok else 'нет'}",
        f"пробой вниз opening low: {'да' if breakout_down else 'нет'}",
        f"мягкий breakout вниз: {'да' if soft_breakout_down else 'нет'}",
        f"цена ниже EMA20 и EMA50: {'да' if trend_down else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 28-58",
        "объём достаточный" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает снижение" if momentum_down else "MACD не поддерживает снижение",
        "MACD только что пересёкся вниз" if fresh_macd_cross_down else "свежего разворота MACD вниз нет",
        f"ATR%={atr_pct:.4f}, минимум 0.0005",
        "запас движения перекрывает комиссию" if commission_room_ok else "запас движения мал для комиссии",
    ]

    long_blockers: list[str] = []
    short_blockers: list[str] = []

    if not higher_tf_long_ok:
        long_blockers.append(f"старший ТФ против LONG: {higher_tf_bias}")
    if not range_compression_ok:
        long_blockers.append("opening range слишком широкий, нет чистого стартового сжатия")
    if not hard_or_strong_soft_up:
        long_blockers.append("нет пробоя вверх opening range")
    if not trend_up:
        long_blockers.append("цена не удерживается выше EMA20 и EMA50")
    if not hold_above_range:
        long_blockers.append("цена не удерживается над opening range")
    if not rsi_long_ok:
        long_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 42-72")
    if not momentum_up:
        long_blockers.append("MACD не подтверждает развитие импульса вверх")
    if not impulse_ok:
        long_blockers.append("нет нужного импульса свечи")
    if not volume_ok:
        long_blockers.append("объём не поддерживает breakout")
    if not volatility_ok:
        long_blockers.append("волатильность слишком низкая")
    if not commission_room_ok:
        long_blockers.append("ожидаемое движение мало относительно комиссии")

    if not higher_tf_short_ok:
        short_blockers.append(f"старший ТФ против SHORT: {higher_tf_bias}")
    if not range_compression_ok:
        short_blockers.append("opening range слишком широкий, нет чистого стартового сжатия")
    if not hard_or_strong_soft_down:
        short_blockers.append("нет пробоя вниз opening range")
    if not trend_down:
        short_blockers.append("цена не удерживается ниже EMA20 и EMA50")
    if not hold_below_range:
        short_blockers.append("цена не удерживается под opening range")
    if not rsi_short_ok:
        short_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 28-58")
    if not momentum_down:
        short_blockers.append("MACD не подтверждает развитие импульса вниз")
    if not impulse_ok:
        short_blockers.append("нет нужного импульса свечи")
    if not volume_ok:
        short_blockers.append("объём не поддерживает breakout")
    if not volatility_ok:
        short_blockers.append("волатильность слишком низкая")
    if not commission_room_ok:
        short_blockers.append("ожидаемое движение мало относительно комиссии")

    long_score = sum(
        [
            0 if not higher_tf_long_ok else 1,
            1 if range_compression_ok else 0,
            2 if breakout_up else 1 if soft_breakout_up else 0,
            1 if trend_up else 0,
            1 if hold_above_range else 0,
            1 if rsi_long_ok else 0,
            1 if momentum_up else 0,
            1 if impulse_ok else 0,
            1 if volume_ok else 0,
            1 if volatility_ok else 0,
        ]
    )
    short_score = sum(
        [
            0 if not higher_tf_short_ok else 1,
            1 if range_compression_ok else 0,
            2 if breakout_down else 1 if soft_breakout_down else 0,
            1 if trend_down else 0,
            1 if hold_below_range else 0,
            1 if rsi_short_ok else 0,
            1 if momentum_down else 0,
            1 if impulse_ok else 0,
            1 if volume_ok else 0,
            1 if volatility_ok else 0,
        ]
    )

    long_ok = higher_tf_long_ok and hard_or_strong_soft_up and trend_up and hold_above_range and commission_room_ok and long_score >= 6
    short_ok = higher_tf_short_ok and hard_or_strong_soft_down and trend_down and hold_below_range and commission_room_ok and short_score >= 6
    if is_fx:
        long_ok = (
            higher_tf_long_ok
            and (breakout_up or fx_fresh_impulse_long)
            and trend_up
            and hold_above_range
            and (volume_ok or fx_fresh_impulse_long)
            and (impulse_ok or fx_fresh_impulse_long)
            and (momentum_up or fx_fresh_impulse_long)
            and volatility_ok
            and commission_room_ok
            and (long_score >= 8 or fx_fresh_impulse_long)
        )
        short_ok = (
            higher_tf_short_ok
            and (breakout_down or fx_fresh_impulse_short)
            and trend_down
            and hold_below_range
            and (volume_ok or fx_fresh_impulse_short)
            and (impulse_ok or fx_fresh_impulse_short)
            and (momentum_down or fx_fresh_impulse_short)
            and volatility_ok
            and commission_room_ok
            and (short_score >= 8 or fx_fresh_impulse_short)
        )

    if long_ok:
        return "LONG", "Сигнал LONG (opening_range_breakout): " + "; ".join(long_reasons) + "."
    if short_ok:
        return "SHORT", "Сигнал SHORT (opening_range_breakout): " + "; ".join(short_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD (opening_range_breakout): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
