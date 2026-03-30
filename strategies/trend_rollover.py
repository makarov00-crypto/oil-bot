def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    recent = df.iloc[-8:-1]

    close = float(last["close"])
    prev_close = float(prev["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    prev_ema20 = float(prev["ema20"])
    rsi = float(last["rsi"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = float(last["body"])
    body_avg = float(last["body_avg"])
    atr_pct = float(last["atr"]) / close if close else 0.0

    recent_low = float(recent["low"].min())
    recent_high = float(recent["high"].max())

    close_below_trend = close < ema20 and close < ema50
    close_above_trend = close > ema20 and close > ema50
    ema20_turning_down = ema20 < prev_ema20
    ema20_turning_up = ema20 > prev_ema20
    breakdown_down = close < recent_low and close_below_trend
    soft_breakdown_down = close_below_trend and close <= recent_low * 1.0015
    breakout_up = close > recent_high and close_above_trend
    rollover_short = prev_close < float(prev["ema20"]) and close < ema20 and close <= prev_close
    rollover_long = prev_close > float(prev["ema20"]) and close > ema20 and close >= prev_close
    volume_ok = volume_avg > 0 and volume >= volume_avg * 0.75
    impulse_ok = body_avg > 0 and body >= body_avg * 0.60
    macd_down = macd < macd_signal and macd <= prev_macd and macd_signal <= prev_macd_signal
    macd_up = macd > macd_signal and macd >= prev_macd and macd_signal >= prev_macd_signal
    rsi_short_ok = 28.0 <= rsi <= 58.0
    rsi_long_ok = 42.0 <= rsi <= 72.0
    volatility_ok = atr_pct >= 0.0009
    higher_tf_short_ok = higher_tf_bias != "LONG"
    higher_tf_long_ok = higher_tf_bias != "SHORT"

    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"цена ниже EMA20 и EMA50: {'да' if close_below_trend else 'нет'}",
        f"EMA20 разворачивается вниз: {'да' if ema20_turning_down else 'нет'}",
        f"breakdown вниз {recent_low:.4f}: {'да' if breakdown_down else 'нет'}",
        f"мягкий breakdown вниз: {'да' if soft_breakdown_down else 'нет'}",
        f"rollover вниз: {'да' if rollover_short else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 28-58",
        "MACD поддерживает снижение" if macd_down else "MACD не поддерживает снижение",
        "объём выше базового" if volume_ok else "объём слабый",
        "импульс есть" if impulse_ok else "импульс слабый",
        f"ATR%={atr_pct:.4f}, минимум 0.0009",
    ]
    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"цена выше EMA20 и EMA50: {'да' if close_above_trend else 'нет'}",
        f"EMA20 разворачивается вверх: {'да' if ema20_turning_up else 'нет'}",
        f"breakout вверх {recent_high:.4f}: {'да' if breakout_up else 'нет'}",
        f"rollover вверх: {'да' if rollover_long else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 42-72",
        "MACD поддерживает рост" if macd_up else "MACD не поддерживает рост",
        "объём выше базового" if volume_ok else "объём слабый",
        "импульс есть" if impulse_ok else "импульс слабый",
        f"ATR%={atr_pct:.4f}, минимум 0.0009",
    ]

    short_blockers: list[str] = []
    long_blockers: list[str] = []
    if not higher_tf_short_ok:
        short_blockers.append(f"старший ТФ не SHORT, а {higher_tf_bias}")
    if not close_below_trend:
        short_blockers.append("цена не закрепилась ниже EMA20/EMA50")
    if not ema20_turning_down:
        short_blockers.append("EMA20 ещё не развернулась вниз")
    if not (breakdown_down or soft_breakdown_down or rollover_short):
        short_blockers.append("нет breakdown/rollover вниз")
    if not rsi_short_ok:
        short_blockers.append(f"RSI {rsi:.2f} вне зоны 28-58")
    if not macd_down:
        short_blockers.append("MACD не подтверждает снижение")
    if not volume_ok:
        short_blockers.append("объём слишком слабый")
    if not impulse_ok:
        short_blockers.append("импульс свечи слишком слабый")
    if not volatility_ok:
        short_blockers.append("волатильность ниже минимума")

    if not higher_tf_long_ok:
        long_blockers.append(f"старший ТФ не LONG, а {higher_tf_bias}")
    if not close_above_trend:
        long_blockers.append("цена не закрепилась выше EMA20/EMA50")
    if not ema20_turning_up:
        long_blockers.append("EMA20 ещё не развернулась вверх")
    if not (breakout_up or rollover_long):
        long_blockers.append("нет breakout/rollover вверх")
    if not rsi_long_ok:
        long_blockers.append(f"RSI {rsi:.2f} вне зоны 42-72")
    if not macd_up:
        long_blockers.append("MACD не подтверждает рост")
    if not volume_ok:
        long_blockers.append("объём слишком слабый")
    if not impulse_ok:
        long_blockers.append("импульс свечи слишком слабый")
    if not volatility_ok:
        long_blockers.append("волатильность ниже минимума")

    short_score = sum(
        [
            1 if higher_tf_short_ok else 0,
            1 if close_below_trend else 0,
            1 if ema20_turning_down else 0,
            2 if breakdown_down else 1 if soft_breakdown_down or rollover_short else 0,
            1 if rsi_short_ok else 0,
            1 if macd_down else 0,
            1 if volume_ok else 0,
            1 if impulse_ok else 0,
            1 if volatility_ok else 0,
        ]
    )
    long_score = sum(
        [
            1 if higher_tf_long_ok else 0,
            1 if close_above_trend else 0,
            1 if ema20_turning_up else 0,
            2 if breakout_up else 1 if rollover_long else 0,
            1 if rsi_long_ok else 0,
            1 if macd_up else 0,
            1 if volume_ok else 0,
            1 if impulse_ok else 0,
            1 if volatility_ok else 0,
        ]
    )

    short_ok = higher_tf_short_ok and close_below_trend and (breakdown_down or soft_breakdown_down or rollover_short) and short_score >= 6
    long_ok = higher_tf_long_ok and close_above_trend and (breakout_up or rollover_long) and long_score >= 6

    if short_ok:
        return "SHORT", "Сигнал SHORT (trend_rollover): " + "; ".join(short_reasons) + "."
    if long_ok:
        return "LONG", "Сигнал LONG (trend_rollover): " + "; ".join(long_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD (trend_rollover): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
