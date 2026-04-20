def is_vbm6_post_gap_chop(df) -> bool:
    if len(df) < 10:
        return False

    last = df.iloc[-1]
    close = float(last["close"])
    if close <= 0:
        return False

    recent = df.iloc[-5:]
    impulse_window = df.iloc[-10:-4]
    prev_window = df.iloc[-10:-5]

    def _body(row) -> float:
        return abs(float(row["close"]) - float(row["open"]))

    impulse_seen = False
    previous_close = None
    for _, row in impulse_window.iterrows():
        row_close = float(row["close"])
        row_open = float(row["open"])
        row_high = float(row["high"])
        row_low = float(row["low"])
        row_atr = float(row["atr"])
        row_volume = float(row["volume"])
        row_volume_avg = float(row["volume_avg"])
        row_body_avg = float(row["body_avg"])
        row_body = abs(row_close - row_open)
        row_range = row_high - row_low
        gap = abs(row_open - previous_close) if previous_close is not None else 0.0
        previous_close = row_close
        abnormal_gap = row_atr > 0 and gap >= row_atr * 1.8
        abnormal_body = row_body_avg > 0 and row_body >= row_body_avg * 2.0
        abnormal_range = row_atr > 0 and row_range >= row_atr * 2.0
        volume_burst = row_volume_avg > 0 and row_volume >= row_volume_avg * 1.25
        if volume_burst and (abnormal_gap or abnormal_body or abnormal_range):
            impulse_seen = True
            break

    if not impulse_seen:
        return False

    recent_high = float(recent["high"].max())
    recent_low = float(recent["low"].min())
    recent_range_pct = (recent_high - recent_low) / close
    atr_pct = float(last["atr"]) / close if close else 0.0
    previous_high = float(prev_window["high"].max())
    previous_low = float(prev_window["low"].min())
    no_new_extreme = recent_high <= previous_high * 1.001 and recent_low >= previous_low * 0.999
    low_participation = float(recent["volume"].mean()) <= float(recent["volume_avg"].mean()) * 0.95
    first_hist = abs(float(recent.iloc[0]["macd"]) - float(recent.iloc[0]["macd_signal"]))
    last_hist = abs(float(last["macd"]) - float(last["macd_signal"]))
    macd_fading = last_hist <= first_hist
    tight_range = recent_range_pct <= max(atr_pct * 2.5, 0.006)

    return no_new_extreme and low_participation and macd_fading and tight_range
