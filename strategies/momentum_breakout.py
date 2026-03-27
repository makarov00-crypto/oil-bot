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

    range_high = float(recent["high"].max())
    range_low = float(recent["low"].min())
    breakout_up = close > range_high and close > ema20 and close > ema50
    breakout_down = close < range_low and close < ema20 and close < ema50
    volume_ok = volume_avg > 0 and volume >= volume_avg * 1.05
    impulse_ok = body_avg > 0 and body >= body_avg * 0.95
    macd_up = macd > macd_signal and macd >= prev_macd and prev_macd >= prev_macd_signal
    macd_down = macd < macd_signal and macd <= prev_macd and prev_macd <= prev_macd_signal
    volatility_ok = atr_pct >= 0.0012
    rsi_long_ok = 48.0 <= rsi <= 75.0
    rsi_short_ok = 25.0 <= rsi <= 52.0

    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"пробой вверх диапазона {range_high:.4f}: {'да' if breakout_up else 'нет'}",
        f"цена выше EMA20 и EMA50: {'да' if close > ema20 and close > ema50 else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 48-75",
        "объём выше среднего" if volume_ok else "объём слабый",
        "импульс сильный" if impulse_ok else "импульс слабый",
        "MACD ускоряется вверх" if macd_up else "MACD не ускоряется вверх",
        f"ATR%={atr_pct:.4f}, минимум 0.0012",
    ]
    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"пробой вниз диапазона {range_low:.4f}: {'да' if breakout_down else 'нет'}",
        f"цена ниже EMA20 и EMA50: {'да' if close < ema20 and close < ema50 else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 25-52",
        "объём выше среднего" if volume_ok else "объём слабый",
        "импульс сильный" if impulse_ok else "импульс слабый",
        "MACD ускоряется вниз" if macd_down else "MACD не ускоряется вниз",
        f"ATR%={atr_pct:.4f}, минимум 0.0012",
    ]

    long_blockers: list[str] = []
    short_blockers: list[str] = []
    if higher_tf_bias != "LONG":
        long_blockers.append(f"старший ТФ не LONG, а {higher_tf_bias}")
    if not breakout_up:
        long_blockers.append("нет подтверждённого breakout вверх")
    if not rsi_long_ok:
        long_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 48-75")
    if not macd_up:
        long_blockers.append("MACD не подтверждает ускорение вверх")
    if not impulse_ok:
        long_blockers.append("импульс свечи слишком слабый")
    if not volume_ok:
        long_blockers.append("объём не подтверждает breakout")
    if not volatility_ok:
        long_blockers.append("волатильность ниже минимума")

    if higher_tf_bias != "SHORT":
        short_blockers.append(f"старший ТФ не SHORT, а {higher_tf_bias}")
    if not breakout_down:
        short_blockers.append("нет подтверждённого breakout вниз")
    if not rsi_short_ok:
        short_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 25-52")
    if not macd_down:
        short_blockers.append("MACD не подтверждает ускорение вниз")
    if not impulse_ok:
        short_blockers.append("импульс свечи слишком слабый")
    if not volume_ok:
        short_blockers.append("объём не подтверждает breakout")
    if not volatility_ok:
        short_blockers.append("волатильность ниже минимума")

    long_ok = all(
        [
            higher_tf_bias == "LONG",
            breakout_up,
            rsi_long_ok,
            macd_up,
            impulse_ok,
            volume_ok,
            volatility_ok,
        ]
    )
    short_ok = all(
        [
            higher_tf_bias == "SHORT",
            breakout_down,
            rsi_short_ok,
            macd_down,
            impulse_ok,
            volume_ok,
            volatility_ok,
        ]
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
