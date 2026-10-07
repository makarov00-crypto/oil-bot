from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from strategies.v35_signals import V35Candidate


MODEL_VERSION = "hierarchical-market-regime-v29-frozen"
BEAR_SHORT_STAGES = {"RANGE", "TRANSITION", "BREAKOUT_ATTEMPT"}


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if pd.notna(result) else default


def _path_efficiency(values: pd.Series) -> float:
    points = values.astype(float)
    if len(points) < 2:
        return 0.0
    travel = float(points.diff().abs().sum())
    return abs(float(points.iloc[-1] - points.iloc[0])) / travel if travel > 0 else 0.0


def _overlap_ratio(frame: pd.DataFrame) -> float:
    overlaps: list[float] = []
    for index in range(1, len(frame)):
        left = frame.iloc[index - 1]
        right = frame.iloc[index]
        smaller = min(
            _finite(left["high"]) - _finite(left["low"]),
            _finite(right["high"]) - _finite(right["low"]),
        )
        if smaller <= 0:
            continue
        overlap = max(
            0.0,
            min(_finite(left["high"]), _finite(right["high"]))
            - max(_finite(left["low"]), _finite(right["low"])),
        )
        overlaps.append(min(1.0, overlap / smaller))
    return sum(overlaps) / len(overlaps) if overlaps else 0.0


def instrument_snapshot(frame: pd.DataFrame, signal_time: pd.Timestamp) -> dict[str, Any] | None:
    available = frame.loc[frame["closed_at"] <= signal_time].tail(72)
    if len(available) < 20:
        return None
    context = available.tail(48)
    recent = available.tail(12)
    short = available.tail(6)
    atr = _finite(available.iloc[-1].get("atr"))
    if atr <= 0:
        atr = _finite((context["high"].astype(float) - context["low"].astype(float)).tail(14).mean())
    if atr <= 0:
        return None
    move_atr = (_finite(context.iloc[-1]["close"]) - _finite(context.iloc[0]["close"])) / atr
    recent_move_atr = (_finite(recent.iloc[-1]["close"]) - _finite(recent.iloc[0]["close"])) / atr
    short_move_atr = (_finite(short.iloc[-1]["close"]) - _finite(short.iloc[0]["close"])) / atr
    direction_sign = 1.0 if move_atr >= 0 else -1.0
    direction = "LONG" if direction_sign > 0 else "SHORT"
    body_atr = recent["body"].astype(float) / atr
    mean_body_atr = _finite(body_atr.mean())
    small_body_ratio = float((body_atr < 0.22).mean())
    overlap = _overlap_ratio(recent)
    context_efficiency = _path_efficiency(context["close"])
    recent_efficiency = _path_efficiency(recent["close"])
    recent_range_atr = (_finite(recent["high"].max()) - _finite(recent["low"].min())) / atr
    max_recent_body_atr = _finite(body_atr.max())
    ao = _finite(available.iloc[-1]["ao"])
    ao_delta = _finite(available.iloc[-1]["ao_delta"])
    histogram = _finite(available.iloc[-1]["macd_hist"])
    histogram_delta = histogram - _finite(available.iloc[-2]["macd_hist"])
    indicator_alignment = sum((
        ao * direction_sign > 0,
        ao_delta * direction_sign > 0,
        histogram * direction_sign > 0,
        histogram_delta * direction_sign > 0,
    ))
    prior = available.iloc[-13:-1]
    prior_high = _finite(prior["high"].max())
    prior_low = _finite(prior["low"].min())
    close = _finite(available.iloc[-1]["close"])
    breakout_direction = "LONG" if close > prior_high else "SHORT" if close < prior_low else "MIXED"
    last2 = available.tail(2)
    last2_move_atr = (_finite(last2.iloc[-1]["close"]) - _finite(last2.iloc[0]["open"])) / atr
    contraction = bool(
        _finite(body_atr.tail(3).mean()) < max(0.01, _finite(body_atr.iloc[:6].mean())) * 0.60
    )
    fading = bool(ao_delta * direction_sign <= 0 and histogram_delta * direction_sign <= 0)
    range_like = bool(
        abs(move_atr) < 1.20
        and context_efficiency < 0.32
        and (overlap >= 0.52 or small_body_ratio >= 0.55)
    )
    shock = bool(max_recent_body_atr >= 1.80 and recent_range_atr >= 2.50)
    pullback = bool(
        abs(move_atr) >= 1.50
        and recent_move_atr * direction_sign < -0.15
        and last2_move_atr * direction_sign > 0.05
    )
    exhausted = bool(
        abs(move_atr) >= 2.20
        and contraction
        and fading
        and abs(short_move_atr) <= 0.70
    )
    breakout = bool(
        breakout_direction != "MIXED"
        and abs(recent_move_atr) >= 0.80
        and recent_efficiency >= 0.32
        and max_recent_body_atr >= 0.45
    )
    early = bool(
        abs(recent_move_atr) >= 1.15
        and recent_efficiency >= 0.38
        and indicator_alignment >= 3
        and not exhausted
    )
    mature = bool(abs(move_atr) >= 1.50 and context_efficiency >= 0.30 and not exhausted)
    if shock:
        stage = "SHOCK"
    elif range_like:
        stage = "RANGE"
    elif exhausted:
        stage = "TREND_EXHAUSTED"
    elif pullback:
        stage = "PULLBACK"
    elif breakout:
        stage = "BREAKOUT_ATTEMPT"
        direction = breakout_direction
    elif early:
        stage = "TREND_EARLY"
        direction = "LONG" if recent_move_atr > 0 else "SHORT"
    elif mature:
        stage = "TREND_MATURE"
    else:
        stage = "TRANSITION"
        if abs(recent_move_atr) > abs(move_atr) * 0.5:
            direction = "LONG" if recent_move_atr > 0 else "SHORT"
    return {
        "stage": stage,
        "direction": direction,
        "move_atr": round(move_atr, 4),
        "recent_move_atr": round(recent_move_atr, 4),
        "short_move_atr": round(short_move_atr, 4),
        "path_efficiency": round(context_efficiency, 4),
        "recent_path_efficiency": round(recent_efficiency, 4),
        "recent_range_atr": round(recent_range_atr, 4),
        "overlap_ratio": round(overlap, 4),
        "small_body_ratio": round(small_body_ratio, 4),
        "mean_body_atr": round(mean_body_atr, 4),
        "max_recent_body_atr": round(max_recent_body_atr, 4),
        "indicator_alignment": indicator_alignment,
    }


def aggregate_directional_regime(
    snapshots: Iterable[dict[str, Any]],
    anchor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = list(snapshots)
    if not rows:
        return {"regime": "UNKNOWN", "breadth": 0.0, "median_move_atr": 0.0, "members": 0}
    moves = pd.Series([_finite(row["move_atr"]) for row in rows])
    up = float((moves >= 0.50).mean())
    down = float((moves <= -0.50).mean())
    median = _finite(moves.median())
    anchor_move = _finite(anchor.get("move_atr")) if anchor else median
    shock_share = sum(row["stage"] == "SHOCK" for row in rows) / len(rows)
    if shock_share >= 0.50 or (anchor and anchor["stage"] == "SHOCK"):
        regime = "SHOCK"
    elif up >= 0.60 and median >= 0.35 and anchor_move >= 0:
        regime = "BULL"
    elif down >= 0.60 and median <= -0.35 and anchor_move <= 0:
        regime = "BEAR"
    else:
        regime = "SIDEWAYS"
    return {
        "regime": regime,
        "breadth": round(max(up, down), 4),
        "up_share": round(up, 4),
        "down_share": round(down, 4),
        "median_move_atr": round(median, 4),
        "members": len(rows),
    }


def asset_group_for_symbol(symbol: str) -> str:
    normalized = str(symbol or "").upper()
    if normalized in {"USDRUBF", "CNYRUBF"}:
        return "FX_RUB"
    if normalized.startswith(("BR", "NG")):
        return "ENERGY"
    if normalized.startswith(("GD", "GL")):
        return "METAL"
    return "EQUITY"


def hierarchical_context(
    candidate: V35Candidate,
    hourly_by_symbol: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    signal_time = pd.Timestamp(candidate.signal_time)
    snapshots = {
        symbol: snapshot
        for symbol, frame in hourly_by_symbol.items()
        if (snapshot := instrument_snapshot(frame, signal_time)) is not None
    }
    local = snapshots.get(candidate.symbol)
    if local is None:
        raise ValueError(f"Missing local history for {candidate.symbol} at {candidate.signal_time}")
    equity_rows = [
        row for symbol, row in snapshots.items()
        if asset_group_for_symbol(symbol) == "EQUITY"
    ]
    global_regime = aggregate_directional_regime(equity_rows, snapshots.get("IMOEXF"))
    group = asset_group_for_symbol(candidate.symbol)
    group_rows = [
        row for symbol, row in snapshots.items()
        if asset_group_for_symbol(symbol) == group
    ]
    group_regime = aggregate_directional_regime(
        group_rows,
        snapshots.get("IMOEXF") if group == "EQUITY" else local,
    )
    return {
        "symbol": candidate.symbol,
        "direction": candidate.direction,
        "signal_time": candidate.signal_time,
        "asset_group": group,
        "global_regime": global_regime["regime"],
        "global_breadth": global_regime["breadth"],
        "group_regime": group_regime["regime"],
        "group_breadth": group_regime["breadth"],
        "local_stage": local["stage"],
        "local_direction": local["direction"],
        "local_features_v29": local,
    }


def hierarchical_gate(context: dict[str, Any]) -> tuple[bool, str]:
    regime = context["group_regime"]
    stage = context["local_stage"]
    direction = context["direction"]
    if regime == "BULL" and direction == "LONG" and stage == "TREND_EARLY":
        return True, "BULL_LONG_EARLY_TREND"
    if regime == "BEAR" and direction == "SHORT" and stage in BEAR_SHORT_STAGES:
        return True, f"BEAR_SHORT_{stage}"
    if regime in {"SHOCK", "SIDEWAYS"}:
        return False, f"GROUP_{regime}"
    if regime == "BULL" and direction != "LONG":
        return False, "BULL_DIRECTION_MISMATCH"
    if regime == "BEAR" and direction != "SHORT":
        return False, "BEAR_DIRECTION_MISMATCH"
    return False, f"STAGE_{stage}_NOT_ALLOWED_IN_{regime}"
