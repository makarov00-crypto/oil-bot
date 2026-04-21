from strategies.quality_filters import is_vbm6_post_gap_chop


def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    recent = df.iloc[-8:-2]

    close = float(last["close"])
    open_price = float(last["open"])
    high = float(last["high"])
    low = float(last["low"])
    prev_high = float(prev["high"])
    prev_low = float(prev["low"])
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
    body = abs(close - open_price)
    body_avg = float(last["body_avg"])

    range_high = float(recent["high"].max())
    range_low = float(recent["low"].min())
    attempted_breakout_up = prev_high > range_high
    attempted_breakout_down = prev_low < range_low
    failed_up = attempted_breakout_up and close < range_high and close < ema20
    failed_down = attempted_breakout_down and close > range_low and close > ema20

    volume_ok = volume_avg > 0 and volume >= volume_avg * 0.95
    impulse_ok = body_avg > 0 and body >= body_avg * 0.75
    rejection_down = macd < macd_signal and macd <= prev_macd and prev_macd >= prev_macd_signal
    rejection_up = macd > macd_signal and macd >= prev_macd and prev_macd <= prev_macd_signal
    trend_support_short = close < ema50
    trend_support_long = close > ema50
    volatility_ok = atr_pct >= 0.0004
    rsi_short_ok = 35.0 <= rsi <= 68.0
    rsi_long_ok = 32.0 <= rsi <= 60.0
    soft_higher_tf_reversal = instrument.symbol in {"IMOEXF", "RBM6", "SRM6"}
    higher_tf_short_ok = higher_tf_bias == "SHORT" or soft_higher_tf_reversal
    higher_tf_long_ok = higher_tf_bias == "LONG" or soft_higher_tf_reversal

    if instrument.symbol == "IMOEXF":
        trend_support_long = close > ema20
        trend_support_short = close < ema20
        volume_ok = volume_avg > 0 and volume >= volume_avg * 0.85
        impulse_ok = body_avg > 0 and body >= body_avg * 0.60
        rsi_long_ok = 35.0 <= rsi <= 65.0
        rsi_short_ok = 35.0 <= rsi <= 70.0
    if instrument.symbol in {"SRM6", "VBM6"}:
        trend_support_long = close > ema20
        trend_support_short = close < ema20
        volume_ok = volume_avg > 0 and volume >= volume_avg * 0.85
        impulse_ok = body_avg > 0 and body >= body_avg * 0.60
        rsi_long_ok = 34.0 <= rsi <= 64.0
        rsi_short_ok = 34.0 <= rsi <= 70.0
    if instrument.symbol == "VBM6":
        higher_tf_short_ok = higher_tf_bias == "SHORT"
        higher_tf_long_ok = (
            higher_tf_bias == "LONG"
            or (
                failed_down
                and close > ema20
                and close > ema50
                and close > prev_high
                and float(prev["close"]) > float(prev["ema20"])
                and volume_avg > 0
                and volume >= volume_avg * 1.15
                and body_avg > 0
                and body >= body_avg * 0.90
                and rejection_up
                and 36.0 <= rsi <= 60.0
            )
        )
    if instrument.symbol == "RBM6":
        trend_support_long = close > ema20
        trend_support_short = close < ema20
        volume_ok = volume_avg > 0 and volume >= volume_avg * 0.80
        impulse_ok = body_avg > 0 and body >= body_avg * 0.55
        rsi_long_ok = 28.0 <= rsi <= 64.0
        rsi_short_ok = 34.0 <= rsi <= 72.0
    post_gap_chop = instrument.symbol == "VBM6" and is_vbm6_post_gap_chop(df)

    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"ложный пробой вверх диапазона {range_high:.4f}: {'да' if failed_up else 'нет'}",
        f"цена {'ниже' if close < ema20 else 'выше'} EMA20 и {'ниже' if close < ema50 else 'выше'} EMA50",
        f"RSI={rsi:.2f} в рабочей зоне 35-68",
        "объём достаточный" if volume_ok else "объём слабый",
        "свеча отказа есть" if impulse_ok else "свеча отказа слабая",
        "MACD подтверждает разворот вниз" if rejection_down else "MACD не подтверждает разворот вниз",
        f"ATR%={atr_pct:.4f}, минимум 0.0004",
    ]
    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"ложный пробой вниз диапазона {range_low:.4f}: {'да' if failed_down else 'нет'}",
        f"цена {'выше' if close > ema20 else 'ниже'} EMA20 и {'выше' if close > ema50 else 'ниже'} EMA50",
        f"RSI={rsi:.2f} в рабочей зоне 32-60",
        "объём достаточный" if volume_ok else "объём слабый",
        "свеча отказа есть" if impulse_ok else "свеча отказа слабая",
        "MACD подтверждает разворот вверх" if rejection_up else "MACD не подтверждает разворот вверх",
        f"ATR%={atr_pct:.4f}, минимум 0.0004",
    ]

    short_blockers: list[str] = []
    long_blockers: list[str] = []

    if not higher_tf_short_ok:
        short_blockers.append(f"старший ТФ не SHORT, а {higher_tf_bias}")
    if not failed_up:
        short_blockers.append("нет ложного пробоя вверх с возвратом в диапазон")
    if not trend_support_short:
        short_blockers.append("цена ещё не закрепилась ниже EMA50")
    if not rsi_short_ok:
        short_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 35-68")
    if not rejection_down:
        short_blockers.append("MACD не подтверждает разворот вниз")
    if not impulse_ok:
        short_blockers.append("свеча отказа слишком слабая")
    if not volume_ok:
        short_blockers.append("объём не подтверждает возврат в диапазон")
    if not volatility_ok:
        short_blockers.append("волатильность слишком низкая")
    if post_gap_chop:
        short_blockers.append("VBM6 после гэпа перешёл в узкий боковик без чистого разворота")

    if not higher_tf_long_ok:
        long_blockers.append(f"старший ТФ не LONG, а {higher_tf_bias}")
    if not failed_down:
        long_blockers.append("нет ложного пробоя вниз с возвратом в диапазон")
    if not trend_support_long:
        long_blockers.append("цена ещё не закрепилась выше EMA50")
    if not rsi_long_ok:
        long_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 32-60")
    if not rejection_up:
        long_blockers.append("MACD не подтверждает разворот вверх")
    if not impulse_ok:
        long_blockers.append("свеча отказа слишком слабая")
    if not volume_ok:
        long_blockers.append("объём не подтверждает возврат в диапазон")
    if not volatility_ok:
        long_blockers.append("волатильность слишком низкая")
    if post_gap_chop:
        long_blockers.append("VBM6 после гэпа перешёл в узкий боковик без чистого разворота")

    short_ok = all(
        [
            higher_tf_short_ok,
            failed_up,
            trend_support_short,
            rsi_short_ok,
            rejection_down,
            impulse_ok,
            volume_ok,
            volatility_ok,
        ]
    )
    long_ok = all(
        [
            higher_tf_long_ok,
            failed_down,
            trend_support_long,
            rsi_long_ok,
            rejection_up,
            impulse_ok,
            volume_ok,
            volatility_ok,
        ]
    )

    if post_gap_chop:
        return (
            "HOLD",
            "Сигнал HOLD (failed_breakout): VBM6 после гэпа перешёл в узкий боковик "
            "без чистого разворота, объём выдохся, MACD не подтверждает новый вход.",
        )

    if short_ok:
        return "SHORT", "Сигнал SHORT (failed_breakout): " + "; ".join(short_reasons) + "."
    if long_ok:
        return "LONG", "Сигнал LONG (failed_breakout): " + "; ".join(long_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD (failed_breakout): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
