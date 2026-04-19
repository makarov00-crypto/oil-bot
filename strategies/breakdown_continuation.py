from instrument_groups import get_instrument_group


def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    recent = df.iloc[-8:-2]

    close = float(last["close"])
    open_price = float(last["open"])
    high = float(last["high"])
    low = float(last["low"])
    prev_close = float(prev["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    rsi = float(last["rsi"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    atr_pct = float(last["atr"]) / close if close else 0.0
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = abs(close - open_price)
    body_avg = float(last["body_avg"])

    range_high = float(recent["high"].max())
    range_low = float(recent["low"].min())
    range_width_pct = (range_high - range_low) / close if close else 0.0
    volume_ok = volume_avg > 0 and volume >= volume_avg * 0.85
    impulse_ok = body_avg > 0 and body >= body_avg * 0.60
    momentum_down = macd < macd_signal and macd <= prev_macd
    momentum_up = macd > macd_signal and macd >= prev_macd
    trend_short = close < ema20 and close < ema50 and prev_close < ema20
    trend_long = close > ema20 and close > ema50 and prev_close > ema20
    breakdown_down = close < range_low and low <= range_low and trend_short
    breakout_up = close > range_high and high >= range_high and trend_long
    soft_breakdown_down = close <= range_low * 1.001 and trend_short
    soft_breakout_up = close >= range_high * 0.999 and trend_long
    continuation_short = trend_short and close <= prev_close and high <= ema20 * 1.002
    continuation_long = trend_long and close >= prev_close and low >= ema20 * 0.998
    volatility_ok = atr_pct >= 0.0004
    rsi_short_ok = 28.0 <= rsi <= 58.0
    rsi_long_ok = 42.0 <= rsi <= 72.0
    group_name = get_instrument_group(instrument.symbol).name
    is_fx = group_name == "fx"
    is_equity_continuation = group_name in {"equity_index", "equity_futures"}
    is_expensive_fx = instrument.symbol in {"USDRUBF", "UCM6"}
    if instrument.symbol == "SRM6":
        higher_tf_short_ok = higher_tf_bias == "SHORT"
        higher_tf_long_ok = higher_tf_bias == "LONG"
    else:
        higher_tf_short_ok = higher_tf_bias != "LONG"
        higher_tf_long_ok = higher_tf_bias != "SHORT"

    strict_imoexf_long = instrument.symbol == "IMOEXF"

    if is_fx:
        volume_ok = volume_avg > 0 and volume >= volume_avg * 0.95
        impulse_ok = body_avg > 0 and body >= body_avg * 0.70
        trend_short = close < ema20 and close < ema50 and ema20 <= ema50 * 1.0002
        trend_long = close > ema20 and close > ema50 and ema20 >= ema50 * 0.9998
        soft_breakdown_down = close <= range_low * 1.0005 and trend_short
        soft_breakout_up = close >= range_high * 0.9995 and trend_long
        continuation_short = trend_short and close <= prev_close and high <= ema20 * 1.0015
        continuation_long = trend_long and close >= prev_close and low >= ema20 * 0.9985
        volatility_ok = atr_pct >= 0.0006
        rsi_short_ok = 26.0 <= rsi <= 60.0
        rsi_long_ok = 40.0 <= rsi <= 74.0
        higher_tf_short_ok = higher_tf_bias == "SHORT"
        higher_tf_long_ok = higher_tf_bias == "LONG"
        if is_expensive_fx:
            volume_ok = volume_avg > 0 and volume >= volume_avg * 1.05
            impulse_ok = body_avg > 0 and body >= body_avg * 0.80
            volatility_ok = atr_pct >= 0.0008

    if is_equity_continuation:
        volume_ok = volume_avg > 0 and volume >= volume_avg * 0.95
        impulse_ok = body_avg > 0 and body >= body_avg * 0.80
        continuation_short = trend_short and close <= prev_close and high <= ema20 * 1.0015
        continuation_long = trend_long and close >= prev_close and low >= ema20 * 0.9985
        higher_tf_short_ok = higher_tf_bias == "SHORT"
        higher_tf_long_ok = higher_tf_bias == "LONG"

    if instrument.symbol in {"VBM6", "RBM6"}:
        volume_ok = volume_avg > 0 and volume >= volume_avg * 1.05
        impulse_ok = body_avg > 0 and body >= body_avg * 0.90
        continuation_short = trend_short and close <= prev_close and high <= ema20 * 1.0010
        continuation_long = trend_long and close >= prev_close and low >= ema20 * 0.9990

    commission_room_ok = True
    if is_fx:
        commission_room_ok = max(range_width_pct, atr_pct) >= (0.0011 if is_expensive_fx else 0.0009)

    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"пробой вниз диапазона {range_low:.4f}: {'да' if breakdown_down else 'нет'}",
        f"мягкий пробой вниз: {'да' if soft_breakdown_down else 'нет'}",
        f"продолжение вниз после слома: {'да' if continuation_short else 'нет'}",
        f"цена ниже EMA20 и EMA50: {'да' if trend_short else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 28-58",
        "объём достаточный" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает снижение" if momentum_down else "MACD не поддерживает снижение",
        f"ATR%={atr_pct:.4f}, минимум 0.0004",
        "запас движения перекрывает комиссию" if commission_room_ok else "запас движения мал для комиссии",
    ]
    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"пробой вверх диапазона {range_high:.4f}: {'да' if breakout_up else 'нет'}",
        f"мягкий пробой вверх: {'да' if soft_breakout_up else 'нет'}",
        f"продолжение вверх после пробоя: {'да' if continuation_long else 'нет'}",
        f"цена выше EMA20 и EMA50: {'да' if trend_long else 'нет'}",
        f"RSI={rsi:.2f} в рабочей зоне 42-72",
        "объём достаточный" if volume_ok else "объём слабый",
        "импульс свечи есть" if impulse_ok else "импульс свечи слабый",
        "MACD поддерживает рост" if momentum_up else "MACD не поддерживает рост",
        f"ATR%={atr_pct:.4f}, минимум 0.0004",
        "запас движения перекрывает комиссию" if commission_room_ok else "запас движения мал для комиссии",
    ]

    short_blockers: list[str] = []
    long_blockers: list[str] = []
    if not higher_tf_short_ok:
        short_blockers.append(f"старший ТФ против SHORT: {higher_tf_bias}")
    if not breakdown_down and not soft_breakdown_down:
        short_blockers.append("нет пробоя вниз локального диапазона")
    if not continuation_short:
        short_blockers.append("нет подтверждённого продолжения вниз после слома")
    if not trend_short:
        short_blockers.append("цена не закрепилась ниже EMA20 и EMA50")
    if not rsi_short_ok:
        short_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 28-58")
    if not momentum_down:
        short_blockers.append("MACD не подтверждает развитие движения вниз")
    if not impulse_ok:
        short_blockers.append("импульс свечи слишком слабый")
    if not volume_ok:
        short_blockers.append("объём не подтверждает продолжение")
    if not volatility_ok:
        short_blockers.append("волатильность слишком низкая")
    if not commission_room_ok:
        short_blockers.append("ожидаемое движение мало относительно комиссии")

    if not higher_tf_long_ok:
        long_blockers.append(f"старший ТФ против LONG: {higher_tf_bias}")
    if strict_imoexf_long:
        if not breakout_up:
            long_blockers.append("для IMOEXF нужен подтверждённый пробой вверх локального диапазона")
        if not volume_ok:
            long_blockers.append("для IMOEXF LONG нужен подтверждающий объём")
        if not impulse_ok:
            long_blockers.append("для IMOEXF LONG нужен более сильный импульс свечи")
    elif not breakout_up and not soft_breakout_up:
        long_blockers.append("нет пробоя вверх локального диапазона")
    if not continuation_long:
        long_blockers.append("нет подтверждённого продолжения вверх после пробоя")
    if not trend_long:
        long_blockers.append("цена не закрепилась выше EMA20 и EMA50")
    if not rsi_long_ok:
        long_blockers.append(f"RSI {rsi:.2f} вне рабочей зоны 42-72")
    if not momentum_up:
        long_blockers.append("MACD не подтверждает развитие движения вверх")
    if not impulse_ok:
        long_blockers.append("импульс свечи слишком слабый")
    if not volume_ok:
        long_blockers.append("объём не подтверждает продолжение")
    if not volatility_ok:
        long_blockers.append("волатильность слишком низкая")
    if not commission_room_ok:
        long_blockers.append("ожидаемое движение мало относительно комиссии")

    short_score = sum(
        [
            0 if not higher_tf_short_ok else 1,
            2 if breakdown_down else 1 if soft_breakdown_down else 0,
            2 if continuation_short else 0,
            1 if trend_short else 0,
            1 if rsi_short_ok else 0,
            1 if momentum_down else 0,
            1 if impulse_ok else 0,
            1 if volume_ok else 0,
            1 if volatility_ok else 0,
        ]
    )
    long_score = sum(
        [
            0 if not higher_tf_long_ok else 1,
            2 if breakout_up else 1 if soft_breakout_up else 0,
            2 if continuation_long else 0,
            1 if trend_long else 0,
            1 if rsi_long_ok else 0,
            1 if momentum_up else 0,
            1 if impulse_ok else 0,
            1 if volume_ok else 0,
            1 if volatility_ok else 0,
        ]
    )

    short_ok = (
        higher_tf_short_ok
        and (breakdown_down or soft_breakdown_down)
        and continuation_short
        and trend_short
        and momentum_down
        and short_score >= 6
    )
    if strict_imoexf_long:
        long_ok = (
            higher_tf_long_ok
            and breakout_up
            and continuation_long
            and trend_long
            and momentum_up
            and volume_ok
            and impulse_ok
            and long_score >= 7
        )
    else:
        long_ok = (
            higher_tf_long_ok
            and (breakout_up or soft_breakout_up)
            and continuation_long
            and trend_long
            and momentum_up
            and long_score >= 6
        )

    if is_fx:
        short_break_ok = breakdown_down or (soft_breakdown_down and volume_ok and impulse_ok and momentum_down)
        long_break_ok = breakout_up or (soft_breakout_up and volume_ok and impulse_ok and momentum_up)
        short_ok = (
            higher_tf_short_ok
            and trend_short
            and continuation_short
            and short_break_ok
            and rsi_short_ok
            and momentum_down
            and volume_ok
            and impulse_ok
            and volatility_ok
            and commission_room_ok
            and short_score >= 8
        )
        long_ok = (
            higher_tf_long_ok
            and trend_long
            and continuation_long
            and long_break_ok
            and rsi_long_ok
            and momentum_up
            and volume_ok
            and impulse_ok
            and volatility_ok
            and commission_room_ok
            and long_score >= 8
        )

    if instrument.symbol == "IMOEXF":
        short_ok = (
            higher_tf_short_ok
            and trend_short
            and continuation_short
            and breakdown_down
            and rsi_short_ok
            and momentum_down
            and volume_ok
            and impulse_ok
            and volatility_ok
            and short_score >= 8
        )

    if is_equity_continuation and not strict_imoexf_long:
        short_break_ok = breakdown_down or (soft_breakdown_down and volume_ok and impulse_ok)
        long_break_ok = breakout_up or (soft_breakout_up and volume_ok and impulse_ok)
        short_ok = (
            higher_tf_short_ok
            and trend_short
            and continuation_short
            and short_break_ok
            and rsi_short_ok
            and momentum_down
            and volume_ok
            and impulse_ok
            and volatility_ok
            and short_score >= 7
        )
        long_ok = (
            higher_tf_long_ok
            and trend_long
            and continuation_long
            and long_break_ok
            and rsi_long_ok
            and momentum_up
            and volume_ok
            and impulse_ok
            and volatility_ok
            and long_score >= 7
        )

    if short_ok:
        return "SHORT", "Сигнал SHORT (range_break_continuation): " + "; ".join(short_reasons) + "."
    if long_ok:
        return "LONG", "Сигнал LONG (range_break_continuation): " + "; ".join(long_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD (range_break_continuation): long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
