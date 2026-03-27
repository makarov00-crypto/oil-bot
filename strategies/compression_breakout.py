def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    recent = df.iloc[-7:-1]

    close = float(last["close"])
    ema20 = float(last["ema20"])
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
    bb_width_pct = (float(last["bb_upper"]) - float(last["bb_lower"])) / close if close else 0.0

    range_high = float(recent["high"].max())
    range_low = float(recent["low"].min())
    range_pct = (range_high - range_low) / close if close else 0.0
    compression_ok = range_pct <= 0.0065 or bb_width_pct <= 0.0080
    volume_ok = volume_avg > 0 and volume >= volume_avg * 1.02
    impulse_ok = body_avg > 0 and body >= body_avg * 0.70
    momentum_up = macd > macd_signal and macd >= prev_macd and prev_macd >= prev_macd_signal
    momentum_down = macd < macd_signal and macd <= prev_macd and prev_macd <= prev_macd_signal
    breakout_up = close > range_high and close > ema20
    breakout_down = close < range_low and close < ema20
    volatility_ok = atr_pct >= 0.0005
    rsi_long_ok = 42.0 <= rsi <= 68.0
    rsi_short_ok = 32.0 <= rsi <= 58.0

    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"диапазон сжат: {'да' if compression_ok else 'нет'}",
        f"пробой вверх диапазона {range_high:.4f}: {'да' if breakout_up else 'нет'}",
        f"цена {'выше' if close > ema20 else 'ниже'} EMA20",
        f"RSI={rsi:.2f} в рабочей зоне 42-68",
        "объём подтверждает пробой" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает рост" if momentum_up else "MACD не поддерживает рост",
        f"ATR%={atr_pct:.4f}, минимум 0.0005",
    ]
    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"диапазон сжат: {'да' if compression_ok else 'нет'}",
        f"пробой вниз диапазона {range_low:.4f}: {'да' if breakout_down else 'нет'}",
        f"цена {'ниже' if close < ema20 else 'выше'} EMA20",
        f"RSI={rsi:.2f} в рабочей зоне 32-58",
        "объём подтверждает пробой" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает снижение" if momentum_down else "MACD не поддерживает снижение",
        f"ATR%={atr_pct:.4f}, минимум 0.0005",
    ]

    long_blockers: list[str] = []
    short_blockers: list[str] = []
    if higher_tf_bias != "LONG":
        long_blockers.append(f"старший ТФ не LONG, а {higher_tf_bias}")
    if not compression_ok:
        long_blockers.append("до пробоя не было нормального сжатия диапазона")
    if not breakout_up:
        long_blockers.append("нет пробоя локального диапазона вверх")
    if not rsi_long_ok:
        long_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 42-68")
    if not momentum_up:
        long_blockers.append("MACD не подтверждает развитие импульса вверх")
    if not impulse_ok:
        long_blockers.append("нет нужного импульса свечи")
    if not volume_ok:
        long_blockers.append("объём не подтверждает пробой")
    if not volatility_ok:
        long_blockers.append("волатильность слишком низкая для breakout")

    if higher_tf_bias != "SHORT":
        short_blockers.append(f"старший ТФ не SHORT, а {higher_tf_bias}")
    if not compression_ok:
        short_blockers.append("до пробоя не было нормального сжатия диапазона")
    if not breakout_down:
        short_blockers.append("нет пробоя локального диапазона вниз")
    if not rsi_short_ok:
        short_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 32-58")
    if not momentum_down:
        short_blockers.append("MACD не подтверждает развитие импульса вниз")
    if not impulse_ok:
        short_blockers.append("нет нужного импульса свечи")
    if not volume_ok:
        short_blockers.append("объём не подтверждает пробой")
    if not volatility_ok:
        short_blockers.append("волатильность слишком низкая для breakout")

    long_ok = all(
        [
            higher_tf_bias == "LONG",
            compression_ok,
            breakout_up,
            rsi_long_ok,
            momentum_up,
            impulse_ok,
            volume_ok,
            volatility_ok,
        ]
    )
    short_ok = all(
        [
            higher_tf_bias == "SHORT",
            compression_ok,
            breakout_down,
            rsi_short_ok,
            momentum_down,
            impulse_ok,
            volume_ok,
            volatility_ok,
        ]
    )

    if long_ok:
        return "LONG", "Сигнал LONG (compression_breakout): " + "; ".join(long_reasons) + "."
    if short_ok:
        return "SHORT", "Сигнал SHORT (compression_breakout): " + "; ".join(short_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD (compression_breakout): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
