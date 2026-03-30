from datetime import time
from zoneinfo import ZoneInfo

import pandas as pd


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
FX_OPENING_RANGE_START = time(9, 0)
FX_OPENING_RANGE_CANDLES = 6


def _get_today_opening_range(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["time_msk"] = result["time"].dt.tz_convert(MOSCOW_TZ)
    last_day = result["time_msk"].iloc[-1].date()
    session_df = result[
        (result["time_msk"].dt.date == last_day)
        & (result["time_msk"].dt.time >= FX_OPENING_RANGE_START)
    ].reset_index(drop=True)
    return session_df.iloc[:FX_OPENING_RANGE_CANDLES].copy()


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
    range_compression_ok = opening_width_pct <= 0.0045

    breakout_up = close > opening_high and close > ema20
    breakout_down = close < opening_low and close < ema20
    volume_ok = volume_avg > 0 and volume >= volume_avg * 0.90
    impulse_ok = body_avg > 0 and body >= body_avg * 0.60
    momentum_up = macd > macd_signal and macd >= prev_macd
    momentum_down = macd < macd_signal and macd <= prev_macd
    volatility_ok = atr_pct >= 0.0005
    trend_up = close > ema20 and close > ema50
    trend_down = close < ema20 and close < ema50
    hold_above_range = close >= opening_high or close >= opening_mid
    hold_below_range = close <= opening_low or close <= opening_mid
    rsi_long_ok = 42.0 <= rsi <= 72.0
    rsi_short_ok = 28.0 <= rsi <= 58.0
    higher_tf_long_ok = higher_tf_bias != "SHORT"
    higher_tf_short_ok = higher_tf_bias != "LONG"

    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"opening range {opening_low:.4f}-{opening_high:.4f}",
        f"сжатие opening range: {'да' if range_compression_ok else 'нет'}",
        f"пробой вверх opening high: {'да' if breakout_up else 'нет'}",
        f"цена выше EMA20 и EMA50: {'да' if trend_up else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 42-72",
        "объём достаточный" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает рост" if momentum_up else "MACD не поддерживает рост",
        f"ATR%={atr_pct:.4f}, минимум 0.0005",
    ]
    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"opening range {opening_low:.4f}-{opening_high:.4f}",
        f"сжатие opening range: {'да' if range_compression_ok else 'нет'}",
        f"пробой вниз opening low: {'да' if breakout_down else 'нет'}",
        f"цена ниже EMA20 и EMA50: {'да' if trend_down else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 28-58",
        "объём достаточный" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает снижение" if momentum_down else "MACD не поддерживает снижение",
        f"ATR%={atr_pct:.4f}, минимум 0.0005",
    ]

    long_blockers: list[str] = []
    short_blockers: list[str] = []

    if not higher_tf_long_ok:
        long_blockers.append(f"старший ТФ против LONG: {higher_tf_bias}")
    if not range_compression_ok:
        long_blockers.append("opening range слишком широкий, нет чистого стартового сжатия")
    if not breakout_up:
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

    if not higher_tf_short_ok:
        short_blockers.append(f"старший ТФ против SHORT: {higher_tf_bias}")
    if not range_compression_ok:
        short_blockers.append("opening range слишком широкий, нет чистого стартового сжатия")
    if not breakout_down:
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

    long_score = sum(
        [
            0 if not higher_tf_long_ok else 1,
            1 if range_compression_ok else 0,
            2 if breakout_up else 0,
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
            2 if breakout_down else 0,
            1 if trend_down else 0,
            1 if hold_below_range else 0,
            1 if rsi_short_ok else 0,
            1 if momentum_down else 0,
            1 if impulse_ok else 0,
            1 if volume_ok else 0,
            1 if volatility_ok else 0,
        ]
    )

    long_ok = higher_tf_long_ok and breakout_up and trend_up and hold_above_range and long_score >= 7
    short_ok = higher_tf_short_ok and breakout_down and trend_down and hold_below_range and short_score >= 7

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
