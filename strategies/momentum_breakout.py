def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    recent = df.iloc[-7:-1]

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
    bb_upper = float(last.get("bb_upper", close))
    bb_mid = float(last.get("bb_mid", ema20))

    range_high = float(recent["high"].max())
    range_low = float(recent["low"].min())
    close_above_trend = close > ema20 and close > ema50
    close_below_trend = close < ema20 and close < ema50
    breakout_up = close > range_high and close_above_trend
    breakout_down = close < range_low and close_below_trend
    soft_breakout_up = close > ema20 and close > ema50 and close >= range_high * 0.999
    soft_breakout_down = close < ema20 and close < ema50 and close <= range_low * 1.001
    volume_ok = volume_avg > 0 and volume >= volume_avg * 0.85
    impulse_ok = body_avg > 0 and body >= body_avg * 0.70
    macd_up = macd > macd_signal and macd >= prev_macd
    macd_down = macd < macd_signal and macd <= prev_macd
    volatility_ok = atr_pct >= 0.0010
    rsi_long_ok = 46.0 <= rsi <= 78.0
    rsi_short_ok = 22.0 <= rsi <= 54.0
    higher_tf_long_ok = higher_tf_bias != "SHORT"
    higher_tf_short_ok = higher_tf_bias != "LONG"
    ngj6_late_long_chase = False
    ngj6_volume_reversal_short = False
    ngj6_pullback_reclaim_long = False

    if instrument.symbol == "BRK6":
        volume_ok = volume_avg > 0 and volume >= volume_avg * 0.75
        impulse_ok = body_avg > 0 and body >= body_avg * 0.60
        rsi_long_ok = 44.0 <= rsi <= 78.0
        rsi_short_ok = 24.0 <= rsi <= 58.0
        volatility_ok = atr_pct >= 0.0008
        higher_tf_long_ok = higher_tf_bias == "LONG"
        higher_tf_short_ok = higher_tf_bias == "SHORT"
        soft_breakout_up = close > ema20 and close > ema50 and close >= range_high * 0.997
        soft_breakout_down = close < ema20 and close < ema50 and close <= range_low * 1.003
        macd_up = macd > macd_signal and (macd >= prev_macd or (macd - macd_signal) >= 0.05)
        macd_down = macd < macd_signal and (macd <= prev_macd or (macd_signal - macd) >= 0.05)

    if instrument.symbol == "GNM6":
        volume_ok = volume_avg > 0 and volume >= volume_avg * 1.05
        impulse_ok = body_avg > 0 and body >= body_avg * 0.90
        rsi_long_ok = 48.0 <= rsi <= 68.0
        rsi_short_ok = 24.0 <= rsi <= 50.0
        higher_tf_long_ok = higher_tf_bias == "LONG"
        higher_tf_short_ok = higher_tf_bias == "SHORT"

    if instrument.symbol in NATURAL_GAS_SYMBOLS:
        recent_above_ema20 = sum(1 for _, row in df.iloc[-6:-1].iterrows() if float(row["close"]) > float(row["ema20"]))
        recent_below_ema20 = sum(1 for _, row in df.iloc[-6:-1].iterrows() if float(row["close"]) < float(row["ema20"]))
        macd_hist = macd - macd_signal
        prev_macd_hist = prev_macd - prev_macd_signal
        volume_ok = volume_avg > 0 and volume >= volume_avg * 1.05
        impulse_ok = body_avg > 0 and body >= body_avg * 0.95
        rsi_long_ok = 42.0 <= rsi <= 72.0
        rsi_short_ok = 28.0 <= rsi <= 48.0
        volatility_ok = atr_pct >= 0.0015
        higher_tf_long_ok = higher_tf_bias == "LONG"
        higher_tf_short_ok = higher_tf_bias == "SHORT"
        soft_breakout_up = close > ema20 and close > ema50 and close >= range_high * 0.9985
        soft_breakout_down = close < ema20 and close < ema50 and close <= range_low * 1.0010
        macd_up = macd > macd_signal and (macd >= prev_macd or (macd - macd_signal) >= 0.02)
        macd_down = macd < macd_signal and (macd <= prev_macd or (macd_signal - macd) >= 0.02)
        ngj6_late_long_chase = (
            close_above_trend
            and rsi >= 61.0
            and recent_above_ema20 >= 4
            and (close >= bb_upper * 0.997 or (close - ema20) / close >= 0.0060)
            and macd_hist <= prev_macd_hist * 1.10
        )
        ngj6_pullback_reclaim_long = (
            close > ema20
            and close >= ema50
            and close >= bb_mid
            and close >= prev["close"]
            and recent_below_ema20 >= 2
            and volume_avg > 0
            and volume >= volume_avg * 1.00
            and body_avg > 0
            and body >= body_avg * 0.80
            and 40.0 <= rsi <= 64.0
            and macd_up
            and prev_macd <= prev_macd_signal + 0.0015
            and volatility_ok
        )
        ngj6_volume_reversal_short = (
            close < ema20
            and close < ema50
            and close <= bb_mid * 0.9995
            and close <= prev["close"]
            and (breakout_down or soft_breakout_down or close < range_low * 1.0020)
            and volume_avg > 0
            and volume >= volume_avg * 1.45
            and body_avg > 0
            and body >= body_avg * 1.10
            and 28.0 <= rsi <= 46.0
            and macd_down
            and macd_hist < prev_macd_hist
            and volatility_ok
        )
        higher_tf_long_ok = higher_tf_bias == "LONG" or ngj6_pullback_reclaim_long
        higher_tf_short_ok = higher_tf_bias == "SHORT" or ngj6_volume_reversal_short

    soft_breakout_quality_up = True
    soft_breakout_quality_down = True
    if instrument.symbol == "BRK6" or instrument.symbol in NATURAL_GAS_SYMBOLS:
        soft_volume_factor = 1.05 if instrument.symbol == "BRK6" else 1.15
        soft_impulse_factor = 0.85 if instrument.symbol == "BRK6" else 1.05
        soft_breakout_quality_up = (
            breakout_up
            or (
                soft_breakout_up
                and volume_avg > 0
                and volume >= volume_avg * soft_volume_factor
                and body_avg > 0
                and body >= body_avg * soft_impulse_factor
                and macd_up
            )
        )
        soft_breakout_quality_down = (
            breakout_down
            or (
                soft_breakout_down
                and volume_avg > 0
                and volume >= volume_avg * soft_volume_factor
                and body_avg > 0
                and body >= body_avg * soft_impulse_factor
                and macd_down
            )
        )

    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"пробой вверх диапазона {range_high:.4f}: {'да' if breakout_up else 'нет'}",
        f"мягкий breakout вверх: {'да' if soft_breakout_up else 'нет'}",
        f"цена выше EMA20 и EMA50: {'да' if close_above_trend else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 46-78",
        "объём выше среднего" if volume_ok else "объём слабый",
        "импульс есть" if impulse_ok else "импульс слабый",
        "MACD поддерживает рост" if macd_up else "MACD не поддерживает рост",
        f"ATR%={atr_pct:.4f}, минимум 0.0010",
    ]
    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"пробой вниз диапазона {range_low:.4f}: {'да' if breakout_down else 'нет'}",
        f"мягкий breakout вниз: {'да' if soft_breakout_down else 'нет'}",
        f"цена ниже EMA20 и EMA50: {'да' if close_below_trend else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 22-54",
        "объём выше среднего" if volume_ok else "объём слабый",
        "импульс есть" if impulse_ok else "импульс слабый",
        "MACD поддерживает снижение" if macd_down else "MACD не поддерживает снижение",
        f"ATR%={atr_pct:.4f}, минимум 0.0010",
    ]

    long_blockers: list[str] = []
    short_blockers: list[str] = []
    if not higher_tf_long_ok:
        long_blockers.append(f"старший ТФ не LONG, а {higher_tf_bias}")
    if not breakout_up and not soft_breakout_up:
        long_blockers.append("нет подтверждённого breakout вверх")
    elif not soft_breakout_quality_up:
        long_blockers.append("мягкий breakout вверх без сильного объёма/импульса")
    if not rsi_long_ok:
        long_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 46-78")
    if ngj6_late_long_chase:
        long_blockers.append(f"{instrument.symbol}: поздний breakout у верхней Bollinger после зрелого роста")
    if instrument.symbol in NATURAL_GAS_SYMBOLS and higher_tf_bias != "LONG" and not ngj6_pullback_reclaim_long:
        long_blockers.append(f"{instrument.symbol}: нет подтверждённого reclaim выше EMA20/BB-mid после пролива")
    if not macd_up:
        long_blockers.append("MACD не подтверждает рост")
    if not impulse_ok:
        long_blockers.append("импульс свечи слишком слабый")
    if not volume_ok:
        long_blockers.append("объём не подтверждает breakout")
    if not volatility_ok:
        long_blockers.append("волатильность ниже минимума")

    if not higher_tf_short_ok:
        short_blockers.append(f"старший ТФ не SHORT, а {higher_tf_bias}")
    if instrument.symbol in NATURAL_GAS_SYMBOLS and close >= bb_mid and not ngj6_volume_reversal_short:
        short_blockers.append(f"{instrument.symbol}: цена не закрепилась ниже средней Bollinger для шорта")
    if not breakout_down and not soft_breakout_down:
        short_blockers.append("нет подтверждённого breakout вниз")
    elif not soft_breakout_quality_down:
        short_blockers.append("мягкий breakout вниз без сильного объёма/импульса")
    if not rsi_short_ok:
        short_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 22-54")
    if not macd_down:
        short_blockers.append("MACD не подтверждает снижение")
    if not impulse_ok:
        short_blockers.append("импульс свечи слишком слабый")
    if not volume_ok:
        short_blockers.append("объём не подтверждает breakout")
    if not volatility_ok:
        short_blockers.append("волатильность ниже минимума")

    long_score = sum(
        [
            0 if not higher_tf_long_ok else 1,
            2 if breakout_up else 1 if soft_breakout_up else 0,
            1 if close_above_trend else 0,
            1 if rsi_long_ok else 0,
            1 if macd_up else 0,
            1 if impulse_ok else 0,
            1 if volume_ok else 0,
            1 if volatility_ok else 0,
        ]
    )
    short_score = sum(
        [
            0 if not higher_tf_short_ok else 1,
            2 if breakout_down else 1 if soft_breakout_down else 0,
            1 if close_below_trend else 0,
            1 if rsi_short_ok else 0,
            1 if macd_down else 0,
            1 if impulse_ok else 0,
            1 if volume_ok else 0,
            1 if volatility_ok else 0,
        ]
    )

    long_ok = higher_tf_long_ok and (breakout_up or soft_breakout_up) and close_above_trend and long_score >= 6
    short_ok = higher_tf_short_ok and (breakout_down or soft_breakout_down) and close_below_trend and short_score >= 6

    if instrument.symbol == "GNM6":
        long_ok = (
            higher_tf_long_ok
            and breakout_up
            and close_above_trend
            and macd_up
            and volume_ok
            and impulse_ok
            and rsi_long_ok
            and volatility_ok
            and long_score >= 7
        )
        short_ok = (
            higher_tf_short_ok
            and breakout_down
            and close_below_trend
            and macd_down
            and volume_ok
            and impulse_ok
            and rsi_short_ok
            and volatility_ok
            and short_score >= 7
        )

    if instrument.symbol == "BRK6":
        long_ok = (
            higher_tf_long_ok
            and close_above_trend
            and (breakout_up or soft_breakout_up)
            and soft_breakout_quality_up
            and rsi_long_ok
            and macd_up
            and volume_ok
            and impulse_ok
            and volatility_ok
            and long_score >= 5
        )
        short_ok = (
            higher_tf_short_ok
            and close_below_trend
            and (breakout_down or soft_breakout_down)
            and soft_breakout_quality_down
            and rsi_short_ok
            and macd_down
            and volume_ok
            and impulse_ok
            and volatility_ok
            and short_score >= 5
        )

    if instrument.symbol in NATURAL_GAS_SYMBOLS:
        long_ok = (
            higher_tf_long_ok
            and (close_above_trend or ngj6_pullback_reclaim_long)
            and ((breakout_up or soft_breakout_up) and soft_breakout_quality_up or ngj6_pullback_reclaim_long)
            and not ngj6_late_long_chase
            and rsi_long_ok
            and macd_up
            and impulse_ok
            and volume_ok
            and volatility_ok
            and (long_score >= 5 or ngj6_pullback_reclaim_long)
        )
        short_ok = (
            higher_tf_short_ok
            and close_below_trend
            and (breakout_down or (ngj6_volume_reversal_short and close <= range_low * 1.0015))
            and rsi_short_ok
            and macd_down
            and impulse_ok
            and volume_ok
            and volatility_ok
            and (short_score >= 8 or ngj6_volume_reversal_short)
        )

    if long_ok:
        return "LONG", "Сигнал LONG (momentum_breakout): " + "; ".join(long_reasons) + "."
    if short_ok:
        return "SHORT", "Сигнал SHORT (momentum_breakout): " + "; ".join(short_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD (momentum_breakout): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
NATURAL_GAS_SYMBOLS = {"NGJ6", "NGK6"}
