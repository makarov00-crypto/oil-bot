def evaluate_williams_currency_signal(df, higher_tf_bias: str) -> tuple[str, str]:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    close = float(last["close"])
    lips = float(last["alligator_lips"])
    teeth = float(last["alligator_teeth"])
    jaws = float(last["alligator_jaws"])
    ao = float(last["ao"])
    prev_ao = float(prev["ao"])
    chaikin = float(last["chaikin"])
    prev_chaikin = float(prev["chaikin"])
    spread_pct = float(last["alligator_spread_pct"])

    above_alligator = close > max(lips, teeth, jaws)
    below_alligator = close < min(lips, teeth, jaws)
    alligator_bullish = lips > teeth > jaws
    alligator_bearish = lips < teeth < jaws
    ao_cross_up = prev_ao <= 0 < ao
    ao_cross_down = prev_ao >= 0 > ao
    chaikin_cross_up = prev_chaikin <= 0 < chaikin
    chaikin_cross_down = prev_chaikin >= 0 > chaikin
    flat_filter = spread_pct < 0.0010

    long_ok = all([higher_tf_bias == "LONG", not flat_filter, above_alligator, alligator_bullish, ao_cross_up, chaikin_cross_up])
    short_ok = all([higher_tf_bias == "SHORT", not flat_filter, below_alligator, alligator_bearish, ao_cross_down, chaikin_cross_down])

    long_blockers: list[str] = []
    short_blockers: list[str] = []
    if higher_tf_bias != "LONG":
        long_blockers.append(f"старший ТФ не LONG, а {higher_tf_bias}")
    if flat_filter:
        long_blockers.append("Аллигатор спит, рынок слишком плоский")
    if not above_alligator:
        long_blockers.append("цена не выше всех линий Аллигатора")
    if not alligator_bullish:
        long_blockers.append("линии Аллигатора не выстроены по-бычьи")
    if not ao_cross_up:
        long_blockers.append("AO не пересёк ноль вверх")
    if not chaikin_cross_up:
        long_blockers.append("Chaikin не пересёк ноль вверх")

    if higher_tf_bias != "SHORT":
        short_blockers.append(f"старший ТФ не SHORT, а {higher_tf_bias}")
    if flat_filter:
        short_blockers.append("Аллигатор спит, рынок слишком плоский")
    if not below_alligator:
        short_blockers.append("цена не ниже всех линий Аллигатора")
    if not alligator_bearish:
        short_blockers.append("линии Аллигатора не выстроены по-медвежьи")
    if not ao_cross_down:
        short_blockers.append("AO не пересёк ноль вниз")
    if not chaikin_cross_down:
        short_blockers.append("Chaikin не пересёк ноль вниз")

    if long_ok:
        return "LONG", f"Williams LONG: цена выше Аллигатора; AO {ao:.4f} пересёк ноль вверх; Chaikin {chaikin:.4f} пересёк ноль вверх; spread={spread_pct:.4f}."
    if short_ok:
        return "SHORT", f"Williams SHORT: цена ниже Аллигатора; AO {ao:.4f} пересёк ноль вниз; Chaikin {chaikin:.4f} пересёк ноль вниз; spread={spread_pct:.4f}."
    return "HOLD", f"Williams HOLD: блокеры long [{'; '.join(long_blockers[:3])}] блокеры short [{'; '.join(short_blockers[:3])}]."
