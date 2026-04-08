from instrument_groups import get_instrument_group, uses_pullback_trend_regime
from strategies.base import StrategyProfile


def get_strategy_profile(config, instrument) -> StrategyProfile:
    symbol = instrument.symbol
    group = get_instrument_group(symbol).name

    if group == "equity_index":
        return StrategyProfile(
            ema_slope_threshold=0.0003,
            near_ema20_pct=0.0080,
            volume_factor=0.95,
            atr_min_pct=0.0004,
            impulse_body_factor=0.80,
            long_rsi_min=41.0,
            long_rsi_max=60.0,
            short_rsi_min=30.0,
            short_rsi_max=59.0,
            rsi_exit_long=68.0,
            rsi_exit_short=32.0,
            allow_short=True,
        )

    if symbol == "NGJ6":
        return StrategyProfile(
            ema_slope_threshold=config.ema_slope_threshold,
            near_ema20_pct=0.0062,
            volume_factor=0.95,
            atr_min_pct=0.0014,
            impulse_body_factor=0.82,
            long_rsi_min=39.0,
            long_rsi_max=56.0,
            short_rsi_min=38.0,
            short_rsi_max=60.0,
            rsi_exit_long=config.rsi_exit_long,
            rsi_exit_short=config.rsi_exit_short,
            allow_short=True,
        )

    if symbol == "BRK6":
        return StrategyProfile(
            ema_slope_threshold=config.ema_slope_threshold,
            near_ema20_pct=0.0085,
            volume_factor=0.82,
            atr_min_pct=0.0010,
            impulse_body_factor=0.70,
            long_rsi_min=40.0,
            long_rsi_max=66.0,
            short_rsi_min=38.0,
            short_rsi_max=64.0,
            rsi_exit_long=config.rsi_exit_long,
            rsi_exit_short=config.rsi_exit_short,
            allow_short=True,
        )

    if symbol == "GNM6":
        return StrategyProfile(
            ema_slope_threshold=config.ema_slope_threshold,
            near_ema20_pct=0.0060,
            volume_factor=0.95,
            atr_min_pct=0.0015,
            impulse_body_factor=0.80,
            long_rsi_min=38.0,
            long_rsi_max=54.0,
            short_rsi_min=40.0,
            short_rsi_max=62.0,
            rsi_exit_long=config.rsi_exit_long,
            rsi_exit_short=config.rsi_exit_short,
            allow_short=True,
        )

    if group == "fx":
        return StrategyProfile(
            ema_slope_threshold=config.ema_slope_threshold,
            near_ema20_pct=0.0060,
            volume_factor=0.95,
            atr_min_pct=0.0007,
            impulse_body_factor=0.80,
            long_rsi_min=36.0,
            long_rsi_max=54.0,
            short_rsi_min=35.0,
            short_rsi_max=64.0,
            rsi_exit_long=config.rsi_exit_long,
            rsi_exit_short=config.rsi_exit_short,
            allow_short=True,
        )

    if group == "equity_futures":
        return StrategyProfile(
            ema_slope_threshold=0.00035,
            near_ema20_pct=0.0070,
            volume_factor=0.95,
            atr_min_pct=0.0004,
            impulse_body_factor=0.80,
            long_rsi_min=39.0,
            long_rsi_max=58.0,
            short_rsi_min=30.0,
            short_rsi_max=61.0,
            rsi_exit_long=67.0,
            rsi_exit_short=33.0,
            allow_short=True,
        )

    return StrategyProfile(
        ema_slope_threshold=config.ema_slope_threshold,
        near_ema20_pct=0.0060,
        volume_factor=max(config.volume_factor, 1.0),
        atr_min_pct=config.atr_min_pct,
        impulse_body_factor=0.80,
        long_rsi_min=config.long_rsi_min,
        long_rsi_max=config.long_rsi_max,
        short_rsi_min=config.short_rsi_min,
        short_rsi_max=config.short_rsi_max,
        rsi_exit_long=config.rsi_exit_long,
        rsi_exit_short=config.rsi_exit_short,
        allow_short=True,
    )


def evaluate_signal(df, config, instrument, higher_tf_bias) -> tuple[str, str]:
    profile = get_strategy_profile(config, instrument)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    close = float(last["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    ema200 = float(last["ema200"])
    ema50_slope = float(last["ema50_slope"])
    rsi = float(last["rsi"])
    macd = float(last["macd"])
    macd_signal = float(last["macd_signal"])
    prev_macd = float(prev["macd"])
    prev_prev = df.iloc[-3]
    prev_prev_macd = float(prev_prev["macd"])
    atr_pct = float(last["atr"]) / close if close else 0.0
    volume = float(last["volume"])
    volume_avg = float(last["volume_avg"])
    body = float(last["body"])
    body_avg = float(last["body_avg"])
    near_ema20 = abs(close - ema20) / close <= profile.near_ema20_pct if close else False
    near_bb_mid = abs(close - float(last["bb_mid"])) / close <= profile.near_ema20_pct if close else False
    pullback_ok = (near_ema20 or near_bb_mid) if uses_pullback_trend_regime(instrument.symbol) else near_ema20
    volume_ok = volume_avg > 0 and volume >= volume_avg * profile.volume_factor
    impulse_ok = body_avg > 0 and body >= body_avg * profile.impulse_body_factor
    macd_turn_up = macd > macd_signal and macd > prev_macd and prev_macd >= prev_prev_macd
    macd_turn_down = profile.allow_short and macd < macd_signal and macd <= prev_macd
    if instrument.symbol == "NGJ6":
        macd_turn_down = macd_turn_down and prev_macd >= prev_prev_macd
    trend_long = close > ema50 and ema50_slope > profile.ema_slope_threshold
    trend_short = close < ema50 and ema50_slope < -profile.ema_slope_threshold
    if uses_pullback_trend_regime(instrument.symbol):
        trend_long = close > ema200 and close > ema50 and ema50_slope > 0
        trend_short = close < ema200 and close < ema50 and ema50_slope < 0
    not_overbought = close < float(last["bb_upper"])
    not_oversold = close > float(last["bb_lower"])
    volatility_ok = atr_pct >= profile.atr_min_pct
    long_blockers: list[str] = []
    short_blockers: list[str] = []

    long_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"цена {'выше' if close > ema50 else 'ниже'} EMA50",
        f"наклон EMA50={ema50_slope:.5f}",
        f"цена {'рядом' if near_ema20 else 'не рядом'} с EMA20",
        f"RSI={rsi:.2f} в диапазоне {profile.long_rsi_min:.0f}-{profile.long_rsi_max:.0f}",
        "есть импульс" if impulse_ok else "нет импульса",
        "объём выше среднего" if volume_ok else "объём слабый",
        "цена не у верхней Bollinger" if not_overbought else "цена у верхней Bollinger",
        f"ATR%={atr_pct:.4f}, минимум {profile.atr_min_pct:.4f}",
        "MACD смотрит вверх и ускоряется" if macd_turn_up else "MACD не подтверждает рост",
    ]
    short_reasons = [
        f"старший ТФ={higher_tf_bias}",
        f"цена {'ниже' if close < ema50 else 'выше'} EMA50",
        f"наклон EMA50={ema50_slope:.5f}",
        f"цена {'рядом' if near_ema20 else 'не рядом'} с EMA20",
        f"RSI={rsi:.2f} в диапазоне {profile.short_rsi_min:.0f}-{profile.short_rsi_max:.0f}",
        "есть импульс" if impulse_ok else "нет импульса",
        "объём выше среднего" if volume_ok else "объём слабый",
        "цена не у нижней Bollinger" if not_oversold else "цена у нижней Bollinger",
        f"ATR%={atr_pct:.4f}, минимум {profile.atr_min_pct:.4f}",
        "MACD смотрит вниз и ускоряется" if macd_turn_down else "MACD не подтверждает снижение",
    ]

    if higher_tf_bias != "LONG":
        long_blockers.append(f"старший ТФ не LONG, а {higher_tf_bias}")
    if not trend_long:
        long_blockers.append("нет подтверждённого восходящего тренда по EMA50")
    if uses_pullback_trend_regime(instrument.symbol):
        if not pullback_ok:
            long_blockers.append("нет отката к EMA20 или средней Bollinger")
    elif not near_ema20:
        long_blockers.append("цена ушла слишком далеко от EMA20")
    if not (profile.long_rsi_min <= rsi <= profile.long_rsi_max):
        long_blockers.append(f"RSI {rsi:.2f} вне зоны входа {profile.long_rsi_min:.0f}-{profile.long_rsi_max:.0f}")
    if not macd_turn_up:
        long_blockers.append("MACD не ускоряется вверх")
    if not impulse_ok:
        long_blockers.append("нет нужного импульса свечи")
    if not volume_ok:
        long_blockers.append("объём не подтверждает вход")
    if not not_overbought:
        long_blockers.append("цена уже у верхней границы Bollinger")
    if not volatility_ok:
        long_blockers.append("волатильность ниже минимума по ATR")

    if profile.allow_short:
        if higher_tf_bias != "SHORT":
            short_blockers.append(f"старший ТФ не SHORT, а {higher_tf_bias}")
        if not trend_short:
            short_blockers.append("нет подтверждённого нисходящего тренда по EMA50")
        if uses_pullback_trend_regime(instrument.symbol):
            if not pullback_ok:
                short_blockers.append("нет отката к EMA20 или средней Bollinger")
        elif not near_ema20:
            short_blockers.append("цена ушла слишком далеко от EMA20")
        if not (profile.short_rsi_min <= rsi <= profile.short_rsi_max):
            short_blockers.append(f"RSI {rsi:.2f} вне зоны входа {profile.short_rsi_min:.0f}-{profile.short_rsi_max:.0f}")
        if not macd_turn_down:
            short_blockers.append("MACD не ускоряется вниз")
        if not impulse_ok:
            short_blockers.append("нет нужного импульса свечи")
        if not volume_ok:
            short_blockers.append("объём не подтверждает вход")
        if not volatility_ok:
            short_blockers.append("волатильность ниже минимума по ATR")

    long_ok = all(
        [
            higher_tf_bias == "LONG",
            trend_long,
            pullback_ok,
            profile.long_rsi_min <= rsi <= profile.long_rsi_max,
            macd_turn_up,
            impulse_ok,
            volume_ok,
            not_overbought,
            volatility_ok,
        ]
    )
    short_ok = all(
        [
            profile.allow_short,
            higher_tf_bias == "SHORT",
            trend_short,
            pullback_ok,
            profile.short_rsi_min <= rsi <= profile.short_rsi_max,
            macd_turn_down,
            impulse_ok,
            volume_ok,
            volatility_ok,
        ]
    )
    if instrument.symbol == "NGJ6":
        # Газу нужен чуть более живой pullback, иначе он почти не торгуется.
        short_ok = short_ok and near_ema20

    if long_ok:
        return "LONG", "Сигнал LONG: " + "; ".join(long_reasons) + "."
    if short_ok:
        return "SHORT", "Сигнал SHORT: " + "; ".join(short_reasons) + "."
    return (
        "HOLD",
        "Сигнал HOLD: long не подтверждён ["
        + "; ".join(long_reasons)
        + "] short не подтверждён ["
        + "; ".join(short_reasons)
        + "]. Главные блокеры long: "
        + "; ".join(long_blockers[:3])
        + ". Главные блокеры short: "
        + "; ".join(short_blockers[:3])
        + ".",
    )
