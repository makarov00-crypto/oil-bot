def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]

    close = float(last["close"])
    prev_close = float(prev["close"])
    ema20 = float(last["ema20"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_macd_signal = float(prev["macd_signal"])
    rsi = float(last["rsi"])
    prev_rsi = float(prev["rsi"])
    prev2_rsi = float(prev2["rsi"])
    stoch_k = float(last.get("stoch_k", 50.0))
    stoch_d = float(last.get("stoch_d", 50.0))
    prev_stoch_k = float(prev.get("stoch_k", 50.0))
    prev_stoch_d = float(prev.get("stoch_d", 50.0))
    prev2_stoch_k = float(prev2.get("stoch_k", 50.0))
    prev2_stoch_d = float(prev2.get("stoch_d", 50.0))
    volume = float(last["volume"])
    prev_volume = float(prev["volume"])
    volume_avg = float(last["volume_avg"])

    macd_cross_up = macd > macd_signal and prev_macd <= prev_macd_signal
    macd_cross_down = macd < macd_signal and prev_macd >= prev_macd_signal
    rsi_rising = rsi > prev_rsi > prev2_rsi
    rsi_falling = rsi < prev_rsi < prev2_rsi
    stoch_rising = stoch_k > prev_stoch_k > prev2_stoch_k and stoch_d > prev_stoch_d > prev2_stoch_d
    stoch_falling = stoch_k < prev_stoch_k < prev2_stoch_k and stoch_d < prev_stoch_d < prev2_stoch_d
    stoch_long_ok = stoch_k >= stoch_d and stoch_k <= 90.0
    stoch_short_ok = stoch_k <= stoch_d and stoch_k >= 10.0
    volume_rising = volume_avg > 0 and volume >= volume_avg * 0.95 and volume >= prev_volume
    close_supports_long = close >= prev_close
    close_supports_short = close <= prev_close

    long_reasons = [
        f"RSI растёт: {'да' if rsi_rising else 'нет'} ({prev2_rsi:.2f}->{prev_rsi:.2f}->{rsi:.2f})",
        f"Stochastic растёт: {'да' if stoch_rising else 'нет'} (%K {prev_stoch_k:.2f}->{stoch_k:.2f}, %D {prev_stoch_d:.2f}->{stoch_d:.2f})",
        f"MACD cross вверх: {'да' if macd_cross_up else 'нет'}",
        f"объём нарастает: {'да' if volume_rising else 'нет'}",
        f"цена не слабеет: {'да' if close_supports_long else 'нет'}",
    ]
    short_reasons = [
        f"RSI падает: {'да' if rsi_falling else 'нет'} ({prev2_rsi:.2f}->{prev_rsi:.2f}->{rsi:.2f})",
        f"Stochastic падает: {'да' if stoch_falling else 'нет'} (%K {prev_stoch_k:.2f}->{stoch_k:.2f}, %D {prev_stoch_d:.2f}->{stoch_d:.2f})",
        f"MACD cross вниз: {'да' if macd_cross_down else 'нет'}",
        f"объём нарастает: {'да' if volume_rising else 'нет'}",
        f"цена не слабеет против шорта: {'да' if close_supports_short else 'нет'}",
    ]

    long_ok = all([rsi_rising, stoch_rising, stoch_long_ok, macd_cross_up, volume_rising, close_supports_long])
    short_ok = all([rsi_falling, stoch_falling, stoch_short_ok, macd_cross_down, volume_rising, close_supports_short])

    if long_ok:
        return "LONG", "Сигнал LONG (macd_stoch_reversal): " + "; ".join(long_reasons) + "."
    if short_ok:
        return "SHORT", "Сигнал SHORT (macd_stoch_reversal): " + "; ".join(short_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD (macd_stoch_reversal): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "].",
    )
