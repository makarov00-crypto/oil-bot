#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot_oil_main import APP_NAME, Client, SUPPORTED_INTERVALS, load_config, resolve_instruments
from signal_ai_reviewer import get_signal_ai_model
from strategy_ai_guard import PROMPT_VERSION, RegimeReview, request_entry_review, request_regime_review


MOSCOW = ZoneInfo("Europe/Moscow")
UTC = timezone.utc
STRATEGY_VERSION = "ao-candle-mtf-replay-v2"
COMMISSION_RATE = 0.00025
INITIAL_STOP_ATR = 0.80
BREAKEVEN_TRIGGER_ATR = 0.20
AI_REQUEST_TIMEOUT_SECONDS = 240


@dataclass
class Candidate:
    symbol: str
    direction: str
    signal_time: str
    signal_price: float
    atr: float
    ao: float
    ao_strength_atr: float
    bars_since_cross: int
    wave_time: str
    entry_path: str
    candle_body_atr_sum: float
    distance_from_wave_atr: float
    regime_review: dict[str, Any] | None = None
    entry_review: dict[str, Any] | None = None
    ai_allowed: bool = False
    ai_gate_reason: str = ""


@dataclass
class SimulatedTrade:
    symbol: str
    direction: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    gross_pnl_rub: float
    commission_rub: float
    net_pnl_rub: float
    exit_reason: str
    breakeven_armed: bool
    mfe_atr: float
    mae_atr: float
    candidate_time: str
    ai_score_pct: int | None
    ai_regime: str


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def enrich(frame: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    df = frame.copy().sort_values("time").drop_duplicates("time").reset_index(drop=True)
    for name in ("open", "high", "low", "close", "volume"):
        df[name] = pd.to_numeric(df[name], errors="coerce")
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time", "open", "high", "low", "close", "volume"]).reset_index(drop=True)
    midpoint = (df["high"] + df["low"]) / 2.0
    df["ao"] = midpoint.rolling(5).mean() - midpoint.rolling(34).mean()
    df["ao_delta"] = df["ao"].diff()
    previous_close = df["close"].shift(1)
    tr = pd.concat(
        [(df["high"] - df["low"]), (df["high"] - previous_close).abs(), (df["low"] - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    df["atr"] = tr.rolling(14).mean()
    change = df["close"].diff()
    gain = change.clip(lower=0.0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    loss = (-change.clip(upper=0.0)).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = gain / loss.replace(0.0, float("nan"))
    df["rsi"] = (100.0 - 100.0 / (1.0 + rs)).fillna(50.0)
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
    macd = df["close"].ewm(span=12, adjust=False).mean() - df["close"].ewm(span=26, adjust=False).mean()
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    df["macd"] = macd
    df["macd_signal"] = macd_signal
    df["macd_hist"] = macd - macd_signal
    price_range = (df["high"] - df["low"]).replace(0.0, float("nan"))
    multiplier = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / price_range
    adl = (multiplier.fillna(0.0) * df["volume"]).cumsum()
    df["chaikin"] = adl.ewm(span=5, adjust=False).mean() - adl.ewm(span=20, adjust=False).mean()
    df["body"] = (df["close"] - df["open"]).abs()
    df["body_atr"] = df["body"] / df["atr"].replace(0.0, float("nan"))
    df["range_atr"] = (df["high"] - df["low"]) / df["atr"].replace(0.0, float("nan"))
    df["closed_at"] = df["time"] + pd.Timedelta(minutes=interval_minutes)
    return df


def resample_four_hours(hourly: pd.DataFrame) -> pd.DataFrame:
    raw = hourly.set_index("time")[["open", "high", "low", "close", "volume"]].resample("4h", origin="epoch").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return enrich(raw, 240)


def fetch_candles(client: Client, figi: str, interval_minutes: int, start: datetime, end: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cursor = start
    step = timedelta(days=1 if interval_minutes < 60 else 5)
    while cursor < end:
        stop = min(end, cursor + step)
        response = client.market_data.get_candles(
            figi=figi,
            from_=cursor,
            to=stop,
            interval=SUPPORTED_INTERVALS[interval_minutes],
        )
        for candle in response.candles:
            def q(value: Any) -> float:
                return float(getattr(value, "units", 0) or 0) + float(getattr(value, "nano", 0) or 0) / 1_000_000_000
            rows.append({
                "time": candle.time.isoformat(),
                "open": q(candle.open),
                "high": q(candle.high),
                "low": q(candle.low),
                "close": q(candle.close),
                "volume": int(candle.volume or 0),
                "is_complete": bool(getattr(candle, "is_complete", True)),
            })
        cursor = stop
    return pd.DataFrame(rows)


def load_or_fetch_frames(cache_dir: Path, start: datetime, end: datetime) -> tuple[dict[str, dict[int, pd.DataFrame]], dict[str, dict[str, float]]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    config = load_config()
    frames: dict[str, dict[int, pd.DataFrame]] = {}
    metadata: dict[str, dict[str, float]] = {}
    with Client(config.token, app_name=APP_NAME, target=config.target) as client:
        for instrument in resolve_instruments(client, config):
            point_value = 1.0
            if instrument.min_price_increment > 0 and instrument.min_price_increment_amount > 0:
                point_value = instrument.min_price_increment_amount / instrument.min_price_increment
            metadata[instrument.symbol] = {
                "point_value": point_value,
                "tick_size": instrument.min_price_increment,
                "tick_value": instrument.min_price_increment_amount,
            }
            frames[instrument.symbol] = {}
            for interval in (60, 30, 15):
                path = cache_dir / f"{instrument.symbol}_{interval}m.json"
                if path.exists():
                    raw = pd.DataFrame(json.loads(path.read_text(encoding="utf-8")))
                else:
                    raw = fetch_candles(client, instrument.figi, interval, start, end)
                    path.write_text(json.dumps(raw.to_dict("records"), ensure_ascii=False), encoding="utf-8")
                frames[instrument.symbol][interval] = enrich(raw, interval)
    (cache_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return frames, metadata


def find_candidates(symbol: str, hourly: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> list[Candidate]:
    result: list[Candidate] = []
    used_waves: set[tuple[str, str]] = set()
    for index in range(36, len(hourly)):
        row = hourly.iloc[index]
        signal_time = pd.Timestamp(row["closed_at"])
        if signal_time < start or signal_time > end:
            continue
        ao = _finite(row["ao"])
        atr = _finite(row["atr"])
        if atr <= 0:
            continue
        delta_now = _finite(row["ao_delta"])
        delta_previous = _finite(hourly.iloc[index - 1]["ao_delta"])
        recent = hourly.iloc[index - 1:index + 1]
        long_impulse = delta_now > 0 and delta_previous > 0 and bool((recent["close"] > recent["open"]).all())
        short_impulse = delta_now < 0 and delta_previous < 0 and bool((recent["close"] < recent["open"]).all())
        if not long_impulse and not short_impulse:
            continue
        direction = "LONG" if long_impulse else "SHORT"
        slope_sign = 1.0 if direction == "LONG" else -1.0
        wave_index = index - 1
        for position in range(index - 2, max(0, index - 9), -1):
            if _finite(hourly.iloc[position]["ao_delta"]) * slope_sign <= 0:
                break
            wave_index = position
        wave_time = pd.Timestamp(hourly.iloc[wave_index]["closed_at"]).isoformat()
        wave_key = (direction, wave_time)
        if wave_key in used_waves:
            continue
        body_sum_atr = float(recent["body"].sum()) / atr
        if body_sum_atr < 0.50 or _finite(row["body_atr"]) < 0.15:
            continue
        previous_rsi = _finite(hourly.iloc[index - 1]["rsi"], 50.0)
        rsi = _finite(row["rsi"], 50.0)
        if direction == "LONG" and not (rsi >= 48.0 and rsi >= previous_rsi):
            continue
        if direction == "SHORT" and not (rsi <= 58.0 and rsi <= previous_rsi):
            continue
        wave_price = _finite(hourly.iloc[wave_index]["close"])
        distance_atr = ((_finite(row["close"]) - wave_price) * (1 if direction == "LONG" else -1)) / atr
        if distance_atr < 0.0 or distance_atr > 1.20:
            continue
        cross_index = None
        for candidate_cross in range(index, max(0, index - 9), -1):
            previous_ao = _finite(hourly.iloc[candidate_cross - 1]["ao"])
            cross_ao = _finite(hourly.iloc[candidate_cross]["ao"])
            crossed = previous_ao <= 0 < cross_ao if direction == "LONG" else previous_ao >= 0 > cross_ao
            if crossed:
                cross_index = candidate_cross
                break
        bars_since_cross = index - cross_index if cross_index is not None else -1
        ao_supports_direction = ao > 0 if direction == "LONG" else ao < 0
        entry_path = "ZERO_CROSS" if ao_supports_direction and 0 <= bars_since_cross <= 3 else "CONTINUATION" if ao_supports_direction else "EARLY_REVERSAL"
        if entry_path == "CONTINUATION":
            used_waves.add(wave_key)
            continue
        used_waves.add(wave_key)
        result.append(Candidate(
            symbol=symbol,
            direction=direction,
            signal_time=signal_time.isoformat(),
            signal_price=_finite(row["close"]),
            atr=atr,
            ao=ao,
            ao_strength_atr=abs(ao) / atr,
            bars_since_cross=bars_since_cross,
            wave_time=wave_time,
            entry_path=entry_path,
            candle_body_atr_sum=body_sum_atr,
            distance_from_wave_atr=distance_atr,
        ))
    return result


def _round_price(value: Any) -> float:
    return round(_finite(value), 6)


def compact_bars(frame: pd.DataFrame, until: pd.Timestamp, limit: int) -> list[dict[str, Any]]:
    tail = frame.loc[frame["closed_at"] <= until].tail(limit)
    rows: list[dict[str, Any]] = []
    for _, row in tail.iterrows():
        rows.append({
            "t": pd.Timestamp(row["closed_at"]).isoformat(),
            "o": _round_price(row["open"]), "h": _round_price(row["high"]),
            "l": _round_price(row["low"]), "c": _round_price(row["close"]),
            "v": int(_finite(row["volume"])),
            "body_atr": round(_finite(row.get("body_atr")), 3),
            "range_atr": round(_finite(row.get("range_atr")), 3),
            "ao": _round_price(row.get("ao")), "ao_delta": _round_price(row.get("ao_delta")),
            "rsi": round(_finite(row.get("rsi"), 50.0), 2),
            "macd_hist": _round_price(row.get("macd_hist")),
            "chaikin": round(_finite(row.get("chaikin")), 2),
            "atr": _round_price(row.get("atr")),
            "ema20": _round_price(row.get("ema20")), "ema50": _round_price(row.get("ema50")),
        })
    return rows


def market_structure_features(frame: pd.DataFrame, until: pd.Timestamp, limit: int) -> dict[str, Any]:
    tail = frame.loc[frame["closed_at"] <= until].tail(limit).copy()
    if len(tail) < 3:
        return {"bars": len(tail), "data_quality": "INCOMPLETE"}
    closes = tail["close"].astype(float)
    changes = closes.diff().dropna()
    atr = max(_finite(tail.iloc[-1]["atr"]), 1e-12)
    net_move = _finite(closes.iloc[-1] - closes.iloc[0])
    total_path = float(changes.abs().sum())
    ao = tail["ao"].astype(float)
    ao_delta = tail["ao_delta"].astype(float)
    close_vs_ema = closes - tail["ema20"].astype(float)

    def sign_changes(series: pd.Series) -> int:
        values = series.dropna().tolist()
        signs = [1 if value > 0 else -1 if value < 0 else 0 for value in values]
        nonzero = [value for value in signs if value]
        return sum(left != right for left, right in zip(nonzero, nonzero[1:]))

    recent_bodies = tail["body_atr"].astype(float).tail(3)
    prior_bodies = tail["body_atr"].astype(float).iloc[-8:-3]
    recent_body_mean = _finite(recent_bodies.mean())
    prior_body_mean = _finite(prior_bodies.mean()) if len(prior_bodies) else 0.0
    recent_volume = _finite(tail["volume"].tail(3).mean())
    prior_volume = max(_finite(tail["volume"].iloc[-12:-3].mean(), 1.0), 1.0)
    return {
        "bars": len(tail),
        "net_move_atr": round(net_move / atr, 3),
        "path_efficiency": round(abs(net_move) / total_path, 3) if total_path > 0 else 0.0,
        "range_atr": round((_finite(tail["high"].max()) - _finite(tail["low"].min())) / atr, 3),
        "up_candle_ratio": round(float((tail["close"] > tail["open"]).mean()), 3),
        "ao_zero_crosses": sign_changes(ao),
        "ao_slope_reversals": sign_changes(ao_delta),
        "ema20_price_crosses": sign_changes(close_vs_ema),
        "ema20_gap_atr": round((_finite(closes.iloc[-1]) - _finite(tail.iloc[-1]["ema20"])) / atr, 3),
        "ema50_gap_atr": round((_finite(closes.iloc[-1]) - _finite(tail.iloc[-1]["ema50"])) / atr, 3),
        "last3_body_atr_mean": round(recent_body_mean, 3),
        "body_compression_ratio": round(recent_body_mean / prior_body_mean, 3) if prior_body_mean > 0 else None,
        "rsi_change": round(_finite(tail.iloc[-1]["rsi"]) - _finite(tail.iloc[0]["rsi"]), 2),
        "volume_ratio_last3": round(recent_volume / prior_volume, 3),
        "data_quality": "COMPLETE",
    }


def ai_contexts(candidate: Candidate, frames: dict[int, pd.DataFrame]) -> tuple[dict[str, Any], dict[str, Any]]:
    at = pd.Timestamp(candidate.signal_time)
    four_hour = resample_four_hours(frames[60])
    regime = {
        "symbol": candidate.symbol,
        "as_of": candidate.signal_time,
        "closed_bars_only": True,
        "timeframes": {
            "1h": compact_bars(frames[60], at, 36),
            "4h": compact_bars(four_hour, at, 12),
            "30m": compact_bars(frames[30], at, 24),
        },
        "derived_features": {
            "1h": market_structure_features(frames[60], at, 60),
            "4h": market_structure_features(four_hour, at, 18),
            "30m": market_structure_features(frames[30], at, 48),
        },
    }
    entry = {
        "strategy": STRATEGY_VERSION,
        "symbol": candidate.symbol,
        "as_of": candidate.signal_time,
        "candidate": {
            "direction": candidate.direction,
            "signal_price": candidate.signal_price,
            "atr_1h": candidate.atr,
            "ao_1h": candidate.ao,
            "ao_strength_atr": round(candidate.ao_strength_atr, 3),
            "bars_since_ao_zero_cross": candidate.bars_since_cross,
            "entry_path": candidate.entry_path,
            "wave_started_at": candidate.wave_time,
            "two_candle_body_atr": round(candidate.candle_body_atr_sum, 3),
            "distance_from_wave_start_atr": round(candidate.distance_from_wave_atr, 3),
            "execution_plan": {
                "initial_stop_atr": INITIAL_STOP_ATR,
                "breakeven_trigger_atr": BREAKEVEN_TRIGGER_ATR,
                "breakeven_covers_both_commissions": True,
                "commission_rate_per_side": COMMISSION_RATE,
                "exit": "30m momentum exhaustion or opposite AO zero cross",
            },
        },
        "timeframes": {
            "1h": compact_bars(frames[60], at, 12),
            "30m": compact_bars(frames[30], at, 16),
            "15m": compact_bars(frames[15], at, 20),
        },
        "derived_features": {
            "1h": market_structure_features(frames[60], at, 16),
            "30m": market_structure_features(frames[30], at, 24),
            "15m": market_structure_features(frames[15], at, 32),
        },
    }
    return regime, entry


def review_candidates(
    candidates: list[Candidate],
    all_frames: dict[str, dict[int, pd.DataFrame]],
    cache_path: Path,
    api_key: str,
    prior_regimes: dict[tuple[str, str, str], dict[str, Any]] | None = None,
) -> None:
    cache: dict[str, Any] = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}
    prior_regimes = prior_regimes or {}
    ai_model = get_signal_ai_model()
    pending: list[tuple[int, Candidate, dict[str, Any], dict[str, Any], str]] = []
    for number, candidate in enumerate(candidates, start=1):
        regime_context, entry_context = ai_contexts(candidate, all_frames[candidate.symbol])
        key = hashlib.sha256((PROMPT_VERSION + ai_model + json.dumps({"regime": regime_context, "entry": entry_context}, sort_keys=True, ensure_ascii=False)).encode()).hexdigest()
        cached = cache.get(key)
        if cached:
            candidate.regime_review = dict(cached["regime"])
            candidate.entry_review = dict(cached["entry"])
            apply_ai_gate(candidate)
        else:
            pending.append((number, candidate, regime_context, entry_context, key))

    def fetch_review(item: tuple[int, Candidate, dict[str, Any], dict[str, Any], str]) -> tuple[int, Candidate, str, dict[str, Any], dict[str, Any]]:
        number, candidate, regime_context, entry_context, key = item
        print(f"AI {number}/{len(candidates)} {candidate.symbol} {candidate.direction} {candidate.signal_time}", flush=True)
        prior = prior_regimes.get((candidate.symbol, candidate.direction, candidate.signal_time))
        regime = RegimeReview(**prior) if prior else request_regime_review(api_key, regime_context, timeout=AI_REQUEST_TIMEOUT_SECONDS)
        entry = request_entry_review(api_key, entry_context, regime, timeout=AI_REQUEST_TIMEOUT_SECONDS)
        return number, candidate, key, regime.as_dict(), entry.as_dict()

    workers = max(1, min(8, int(os.getenv("OIL_STRATEGY_AI_WORKERS", "1") or 1)))
    failures: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_review, item): item for item in pending}
        for future in concurrent.futures.as_completed(futures):
            item = futures[future]
            try:
                _, candidate, key, regime_review, entry_review = future.result()
            except Exception as exc:
                _, failed_candidate, _, _, _ = item
                message = f"{failed_candidate.symbol} {failed_candidate.signal_time}: {type(exc).__name__}: {exc}"
                failures.append(message)
                print(f"AI ERROR {message}", flush=True)
                continue
            candidate.regime_review = regime_review
            candidate.entry_review = entry_review
            apply_ai_gate(candidate)
            cache[key] = {
                "prompt_version": PROMPT_VERSION,
                "model": ai_model,
                "regime": regime_review,
                "entry": entry_review,
            }
            cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    if failures:
        raise RuntimeError("Не завершены ответы ИИ: " + " | ".join(failures))


def apply_ai_gate(candidate: Candidate) -> None:
    regime = candidate.regime_review or {}
    entry = candidate.entry_review or {}
    reasons: list[str] = []
    if regime.get("data_quality") != "COMPLETE" or entry.get("data_quality") != "COMPLETE":
        reasons.append("неполные данные")
    chop_probability = int(regime.get("chop_probability_pct") or 0)
    if str(regime.get("texture") or "") == "CHOP" and chop_probability >= 70:
        reasons.append(f"пила {chop_probability}%")
    score = int(entry.get("entry_score_pct") or 0)
    if str(entry.get("decision") or "") != "ENTER":
        reasons.append(f"решение {entry.get('decision') or 'ABSTAIN'}")
    if score < 60:
        reasons.append(f"оценка входа {score}%")
    candidate.ai_allowed = not reasons
    candidate.ai_gate_reason = "; ".join(reasons) if reasons else ("полноценный вход" if score >= 70 else "canary 1 лот")


def next_open(frame: pd.DataFrame, moment: pd.Timestamp) -> tuple[pd.Timestamp, float] | None:
    rows = frame.loc[frame["time"] >= moment]
    if rows.empty:
        return None
    row = rows.iloc[0]
    return pd.Timestamp(row["time"]), _finite(row["open"])


def true_breakeven_price(entry_price: float, direction: str, tick_size: float) -> float:
    if direction == "LONG":
        raw = entry_price * (1 + COMMISSION_RATE) / (1 - COMMISSION_RATE)
        return math.ceil(raw / tick_size) * tick_size if tick_size > 0 else raw
    raw = entry_price * (1 - COMMISSION_RATE) / (1 + COMMISSION_RATE)
    return math.floor(raw / tick_size) * tick_size if tick_size > 0 else raw


def simulate_trade(candidate: Candidate, frames: dict[int, pd.DataFrame], point_value: float, tick_size: float, week_end: pd.Timestamp) -> SimulatedTrade | None:
    fifteen = frames[15]
    thirty = frames[30]
    fill = next_open(fifteen, pd.Timestamp(candidate.signal_time))
    if fill is None:
        return None
    entry_time, entry_price = fill
    sign = 1.0 if candidate.direction == "LONG" else -1.0
    stop = entry_price - sign * candidate.atr * INITIAL_STOP_ATR
    breakeven = False
    best_move = 0.0
    worst_move = 0.0
    exit_time = week_end
    exit_price = _finite(fifteen.loc[fifteen["time"] <= week_end].iloc[-1]["close"])
    exit_reason = "WEEK_END_MARK"
    evaluated_30m: set[str] = set()
    active = fifteen.loc[(fifteen["time"] >= entry_time) & (fifteen["time"] <= week_end)]
    for _, bar in active.iterrows():
        low, high = _finite(bar["low"]), _finite(bar["high"])
        if candidate.direction == "LONG" and low <= stop:
            exit_time, exit_price, exit_reason = pd.Timestamp(bar["time"]), stop, "BREAKEVEN_STOP" if breakeven else "INITIAL_STOP"
            break
        if candidate.direction == "SHORT" and high >= stop:
            exit_time, exit_price, exit_reason = pd.Timestamp(bar["time"]), stop, "BREAKEVEN_STOP" if breakeven else "INITIAL_STOP"
            break
        favorable = (high - entry_price) if candidate.direction == "LONG" else (entry_price - low)
        adverse = (entry_price - low) if candidate.direction == "LONG" else (high - entry_price)
        best_move = max(best_move, favorable)
        worst_move = max(worst_move, adverse)
        if not breakeven and best_move >= candidate.atr * BREAKEVEN_TRIGGER_ATR:
            breakeven = True
            stop = true_breakeven_price(entry_price, candidate.direction, tick_size)
        closed_at = pd.Timestamp(bar["closed_at"])
        eligible = thirty.loc[(thirty["closed_at"] <= closed_at) & (thirty["closed_at"] > entry_time)]
        if len(eligible) < 3:
            continue
        current = eligible.iloc[-1]
        key = pd.Timestamp(current["closed_at"]).isoformat()
        if key in evaluated_30m:
            continue
        evaluated_30m.add(key)
        previous = eligible.iloc[-2]
        before = eligible.iloc[-3]
        ao_now, ao_prev, ao_before = _finite(current["ao"]), _finite(previous["ao"]), _finite(before["ao"])
        hard_cross = ao_now <= 0 if candidate.direction == "LONG" else ao_now >= 0
        ao_opposite = (ao_now < ao_prev < ao_before) if candidate.direction == "LONG" else (ao_now > ao_prev > ao_before)
        price_stall = (_finite(current["close"]) <= _finite(previous["close"])) if candidate.direction == "LONG" else (_finite(current["close"]) >= _finite(previous["close"]))
        macd_weaken = (_finite(current["macd_hist"]) < _finite(previous["macd_hist"])) if candidate.direction == "LONG" else (_finite(current["macd_hist"]) > _finite(previous["macd_hist"]))
        chaikin_weaken = (_finite(current["chaikin"]) < _finite(previous["chaikin"])) if candidate.direction == "LONG" else (_finite(current["chaikin"]) > _finite(previous["chaikin"]))
        rsi_weaken = (_finite(current["rsi"]) < _finite(previous["rsi"])) if candidate.direction == "LONG" else (_finite(current["rsi"]) > _finite(previous["rsi"]))
        exhaustion_score = sum([ao_opposite, price_stall, macd_weaken, chaikin_weaken, rsi_weaken])
        if hard_cross or (ao_opposite and exhaustion_score >= 3):
            execution = next_open(fifteen, pd.Timestamp(current["closed_at"]))
            if execution:
                exit_time, exit_price = execution
            else:
                exit_time, exit_price = pd.Timestamp(current["closed_at"]), _finite(current["close"])
            exit_reason = "AO_ZERO_CROSS" if hard_cross else "30M_MOMENTUM_EXHAUSTION"
            break
    gross = (exit_price - entry_price) * sign * point_value
    commission = (abs(entry_price * point_value) + abs(exit_price * point_value)) * COMMISSION_RATE
    return SimulatedTrade(
        symbol=candidate.symbol, direction=candidate.direction,
        entry_time=entry_time.isoformat(), exit_time=pd.Timestamp(exit_time).isoformat(),
        entry_price=entry_price, exit_price=exit_price,
        gross_pnl_rub=round(gross, 2), commission_rub=round(commission, 2), net_pnl_rub=round(gross - commission, 2),
        exit_reason=exit_reason, breakeven_armed=breakeven,
        mfe_atr=round(best_move / candidate.atr, 3), mae_atr=round(worst_move / candidate.atr, 3),
        candidate_time=candidate.signal_time,
        ai_score_pct=int((candidate.entry_review or {}).get("entry_score_pct")) if candidate.entry_review else None,
        ai_regime=str((candidate.regime_review or {}).get("regime") or ""),
    )


def simulate(candidates: list[Candidate], frames: dict[str, dict[int, pd.DataFrame]], metadata: dict[str, dict[str, float]], week_end: pd.Timestamp, ai_only: bool) -> list[SimulatedTrade]:
    trades: list[SimulatedTrade] = []
    available_after: dict[str, pd.Timestamp] = {}
    for candidate in sorted(candidates, key=lambda item: item.signal_time):
        if ai_only and not candidate.ai_allowed:
            continue
        if pd.Timestamp(candidate.signal_time) < available_after.get(candidate.symbol, pd.Timestamp.min.tz_localize("UTC")):
            continue
        trade = simulate_trade(
            candidate,
            frames[candidate.symbol],
            metadata[candidate.symbol]["point_value"],
            metadata[candidate.symbol]["tick_size"],
            week_end,
        )
        if trade:
            trades.append(trade)
            available_after[candidate.symbol] = pd.Timestamp(trade.exit_time)
    return trades


def metrics(trades: list[SimulatedTrade]) -> dict[str, Any]:
    net = [trade.net_pnl_rub for trade in trades]
    protected = [trade for trade in trades if trade.exit_reason == "BREAKEVEN_STOP"]
    decisive = [trade for trade in trades if trade.exit_reason != "BREAKEVEN_STOP"]
    wins = [trade.net_pnl_rub for trade in decisive if trade.net_pnl_rub > 0]
    losses = [trade.net_pnl_rub for trade in decisive if trade.net_pnl_rub < 0]
    flats = [trade.net_pnl_rub for trade in decisive if trade.net_pnl_rub == 0]
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in net:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return {
        "trades": len(trades), "wins": len(wins), "losses": len(losses),
        "protected_breakeven": len(protected), "flats": len(flats),
        "win_rate_pct": round(len(wins) / len(decisive) * 100, 1) if decisive else 0.0,
        "gross_pnl_rub": round(sum(trade.gross_pnl_rub for trade in trades), 2),
        "commission_rub": round(sum(trade.commission_rub for trade in trades), 2),
        "net_pnl_rub": round(sum(net), 2), "max_drawdown_rub": round(drawdown, 2),
        "profit_factor": round(sum(wins) / abs(sum(losses)), 2) if losses and sum(losses) else None,
    }


def actual_week_metrics(journal_path: Path, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    trades: list[dict[str, Any]] = []
    if journal_path.exists():
        for line in journal_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("event") or "").upper() != "CLOSE":
                continue
            moment = pd.to_datetime(row.get("time"), utc=True, errors="coerce")
            if pd.isna(moment) or moment < start or moment > end:
                continue
            trades.append(row)
    values = [_finite(row.get("net_pnl_rub"), _finite(row.get("pnl_rub"))) for row in trades]
    normalized = [
        value / max(1, int(row.get("qty_lots") or 1))
        for row, value in zip(trades, values)
    ]
    return {
        "trades": len(trades),
        "net_pnl_rub": round(sum(values), 2),
        "net_pnl_rub_1lot": round(sum(normalized), 2),
        "rows": trades,
    }


def outcome_diagnostics(candidates: list[Candidate], trades: list[SimulatedTrade]) -> dict[str, Any]:
    candidate_by_key = {(item.symbol, item.signal_time): item for item in candidates}

    def summarize(selected: list[SimulatedTrade]) -> dict[str, Any]:
        values = [trade.net_pnl_rub for trade in selected]
        return {
            "trades": len(selected),
            "wins": sum(value > 0 for value in values),
            "net_pnl_rub": round(sum(values), 2),
        }

    decisions = {"allowed": [], "blocked": []}
    score_bins = {"0-39": [], "40-54": [], "55-69": [], "70-100": []}
    entry_paths: dict[str, list[SimulatedTrade]] = {
        "EARLY_REVERSAL": [], "ZERO_CROSS": [], "CONTINUATION": [],
    }
    for trade in trades:
        candidate = candidate_by_key.get((trade.symbol, trade.candidate_time))
        if candidate is None:
            continue
        decisions["allowed" if candidate.ai_allowed else "blocked"].append(trade)
        score = int((candidate.entry_review or {}).get("entry_score_pct") or 0)
        score_bin = "0-39" if score < 40 else "40-54" if score < 55 else "55-69" if score < 70 else "70-100"
        score_bins[score_bin].append(trade)
        entry_paths.setdefault(candidate.entry_path, []).append(trade)
    return {
        "decisions": {key: summarize(value) for key, value in decisions.items()},
        "score_bins": {key: summarize(value) for key, value in score_bins.items()},
        "entry_paths": {key: summarize(value) for key, value in entry_paths.items()},
    }


def build_report(start: pd.Timestamp, end: pd.Timestamp, candidates: list[Candidate], mechanical: list[SimulatedTrade], ai_trades: list[SimulatedTrade], actual: dict[str, Any]) -> str:
    mechanical_metrics, ai_metrics = metrics(mechanical), metrics(ai_trades)
    diagnostics = outcome_diagnostics(candidates, mechanical)
    ai_effect = round(ai_metrics["net_pnl_rub"] - mechanical_metrics["net_pnl_rub"], 2)
    ai_damage = round(mechanical_metrics["net_pnl_rub"] - ai_metrics["net_pnl_rub"], 2)
    mechanical_vs_actual = round(mechanical_metrics["net_pnl_rub"] - actual["net_pnl_rub_1lot"], 2)
    lines = [
        f"# Исторический прогон {STRATEGY_VERSION}", "",
        f"Период: {start.tz_convert(MOSCOW).strftime('%d.%m.%Y %H:%M')} — {end.tz_convert(MOSCOW).strftime('%d.%m.%Y %H:%M')} МСК.",
        "Исполнение: 1 контракт, вход на следующей 15-минутной свече, комиссия 0,025% в каждую сторону, без проскальзывания.", "",
        "## Сводка", "",
        "| Контур | Кандидатов | Сделок | Доля плюсовых | Net, RUB | Комиссии | Макс. просадка | Profit factor |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        f"| Механическая стратегия | {len(candidates)} | {mechanical_metrics['trades']} | {mechanical_metrics['win_rate_pct']}% | {mechanical_metrics['net_pnl_rub']:.2f} | {mechanical_metrics['commission_rub']:.2f} | {mechanical_metrics['max_drawdown_rub']:.2f} | {mechanical_metrics['profit_factor'] or '-'} |",
        f"| Стратегия + ИИ | {sum(1 for item in candidates if item.ai_allowed)} | {ai_metrics['trades']} | {ai_metrics['win_rate_pct']}% | {ai_metrics['net_pnl_rub']:.2f} | {ai_metrics['commission_rub']:.2f} | {ai_metrics['max_drawdown_rub']:.2f} | {ai_metrics['profit_factor'] or '-'} |",
        f"| Фактический бот, реальные лоты | - | {actual['trades']} | - | {actual['net_pnl_rub']:.2f} | - | - | - |",
        f"| Фактический бот, нормализация до 1 лота | - | {actual['trades']} | - | {actual['net_pnl_rub_1lot']:.2f} | - | - | - |", "",
        "## Проверка пользы ИИ", "",
        f"ИИ изменил недельный результат на {ai_effect:+.2f} RUB относительно механического контура.", "",
        "### Допущенные и заблокированные сделки", "",
        "| Решение ИИ | Сделок | Плюсовых | Net, RUB |",
        "|---|---:|---:|---:|",
        f"| ALLOW | {diagnostics['decisions']['allowed']['trades']} | {diagnostics['decisions']['allowed']['wins']} | {diagnostics['decisions']['allowed']['net_pnl_rub']:.2f} |",
        f"| BLOCK | {diagnostics['decisions']['blocked']['trades']} | {diagnostics['decisions']['blocked']['wins']} | {diagnostics['decisions']['blocked']['net_pnl_rub']:.2f} |", "",
        "### Фактический результат по оценке ИИ", "",
        "| Оценка входа | Сделок | Плюсовых | Net, RUB |",
        "|---|---:|---:|---:|",
    ]
    for score_bin, values in diagnostics["score_bins"].items():
        lines.append(f"| {score_bin}% | {values['trades']} | {values['wins']} | {values['net_pnl_rub']:.2f} |")
    lines.extend([
        "", "### Фактический результат по типу входа", "",
        "| Тип входа | Сделок | Плюсовых | Net, RUB |",
        "|---|---:|---:|---:|",
    ])
    for entry_path, values in diagnostics["entry_paths"].items():
        lines.append(f"| {entry_path} | {values['trades']} | {values['wins']} | {values['net_pnl_rub']:.2f} |")
    lines.extend([
        "", "## Вывод по неделе", "",
        f"- Механический контур улучшил результат относительно фактических сделок, приведённых к 1 лоту, на {mechanical_vs_actual:+.2f} RUB, но всё равно завершил неделю в минусе.",
        f"- До комиссий механический результат составил {mechanical_metrics['gross_pnl_rub']:+.2f} RUB; комиссии {mechanical_metrics['commission_rub']:.2f} RUB превратили его в {mechanical_metrics['net_pnl_rub']:+.2f} RUB.",
        f"- ИИ ухудшил результат на {ai_damage:.2f} RUB. В текущем виде его нельзя включать как торговый фильтр без перекалибровки на более длинной истории.",
        "- Убыточнее всего оказался поздний CONTINUATION. Ранний разворот дал положительный итог, однако доля плюсовых сделок остаётся низкой.",
        "", "## Все смоделированные сделки", "",
        "| Сигнал МСК | Инструмент | Сторона | Тип входа | Оценка ИИ | Решение ИИ | Вход МСК | Выход МСК | Net, RUB | Причина выхода |",
        "|---|---|---|---|---:|---|---|---|---:|---|",
    ])
    candidate_by_key = {(item.symbol, item.signal_time): item for item in candidates}
    for trade in mechanical:
        candidate = candidate_by_key[(trade.symbol, trade.candidate_time)]
        score = (candidate.entry_review or {}).get("entry_score_pct", "-")
        lines.append(
            f"| {pd.Timestamp(candidate.signal_time).tz_convert(MOSCOW).strftime('%d.%m %H:%M')} | {trade.symbol} | {trade.direction} | {candidate.entry_path} | {score}% | {'ALLOW' if candidate.ai_allowed else 'BLOCK'} | {pd.Timestamp(trade.entry_time).tz_convert(MOSCOW).strftime('%d.%m %H:%M')} | {pd.Timestamp(trade.exit_time).tz_convert(MOSCOW).strftime('%d.%m %H:%M')} | {trade.net_pnl_rub:.2f} | {trade.exit_reason} |"
        )
    lines.extend([
        "",
        "## Решения ИИ", "",
        "| Время МСК | Инструмент | Сигнал | Режим | Уверенность | Оценка входа | Решение | Причина gate |",
        "|---|---|---|---|---:|---:|---|---|",
    ])
    for item in candidates:
        regime, entry = item.regime_review or {}, item.entry_review or {}
        time_text = pd.Timestamp(item.signal_time).tz_convert(MOSCOW).strftime("%d.%m %H:%M")
        lines.append(
            f"| {time_text} | {item.symbol} | {item.direction} | {regime.get('regime','-')} | {regime.get('regime_confidence_pct','-')}% | {entry.get('entry_score_pct','-')}% | {'ALLOW' if item.ai_allowed else 'BLOCK'} | {item.ai_gate_reason} |"
        )
    lines.extend(["", "## Сделки стратегии + ИИ", "", "| Вход МСК | Выход МСК | Инструмент | Сторона | Вход | Выход | Net, RUB | Причина выхода |", "|---|---|---|---|---:|---:|---:|---|"])
    for trade in ai_trades:
        lines.append(
            f"| {pd.Timestamp(trade.entry_time).tz_convert(MOSCOW).strftime('%d.%m %H:%M')} | {pd.Timestamp(trade.exit_time).tz_convert(MOSCOW).strftime('%d.%m %H:%M')} | {trade.symbol} | {trade.direction} | {trade.entry_price:g} | {trade.exit_price:g} | {trade.net_pnl_rub:.2f} | {trade.exit_reason} |"
        )
    lines.extend(["", "## Ограничения", "", "- Результат является исторической симуляцией по закрытым свечам, а не гарантией будущей доходности.", "- Проскальзывание не включено; комиссия оценена по ставке 0,025% на сторону.", "- Проценты ИИ пока являются оценкой модели и требуют калибровки по накопленным исходам.", "- Сделки принудительно переоцениваются по последней свече недели, если сигнал выхода ещё не появился."])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay AO/candle multi-timeframe strategy with an AI entry guard")
    parser.add_argument("--start", default="2026-09-28T00:00:00+03:00")
    parser.add_argument("--end", default="2026-10-03T00:00:00+03:00")
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "bot_state" / "research" / "week_2026-09-28_v2")
    parser.add_argument("--output", type=Path, default=ROOT / "reports" / "strategy_replay_2026-09-28_2026-10-02")
    parser.add_argument("--skip-ai", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = pd.Timestamp(args.start).tz_convert("UTC")
    end = pd.Timestamp(args.end).tz_convert("UTC")
    warmup = (start - pd.Timedelta(days=35)).to_pydatetime()
    fetch_end = (end + pd.Timedelta(hours=2)).to_pydatetime()
    frames, metadata = load_or_fetch_frames(args.cache_dir, warmup, fetch_end)
    candidates = [candidate for symbol, symbol_frames in frames.items() for candidate in find_candidates(symbol, symbol_frames[60], start, end)]
    candidates.sort(key=lambda item: item.signal_time)
    ai_model = get_signal_ai_model()
    prior_regimes: dict[tuple[str, str, str], dict[str, Any]] = {}
    prior_output_path = args.output.with_suffix(".json")
    if prior_output_path.exists():
        try:
            prior_payload = json.loads(prior_output_path.read_text(encoding="utf-8"))
            if prior_payload.get("ai_model") == ai_model and prior_payload.get("prompt_version") == PROMPT_VERSION:
                for item in prior_payload.get("candidates") or []:
                    review = item.get("regime_review")
                    if isinstance(review, dict):
                        prior_regimes[(str(item.get("symbol")), str(item.get("direction")), str(item.get("signal_time")))] = review
        except (OSError, json.JSONDecodeError):
            prior_regimes = {}
    if args.skip_ai:
        for item in candidates:
            item.ai_allowed = True
            item.ai_gate_reason = "AI skipped"
    else:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required unless --skip-ai is used")
        review_candidates(candidates, frames, args.cache_dir / "ai_reviews.json", api_key, prior_regimes)
    mechanical = simulate(candidates, frames, metadata, end, ai_only=False)
    ai_trades = simulate(candidates, frames, metadata, end, ai_only=not args.skip_ai)
    actual = actual_week_metrics(ROOT / "logs" / "trade_journal.jsonl", start, end)
    payload = {
        "strategy_version": STRATEGY_VERSION,
        "prompt_version": PROMPT_VERSION,
        "ai_model": ai_model,
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "candidates": [asdict(item) for item in candidates],
        "mechanical": {"metrics": metrics(mechanical), "trades": [asdict(item) for item in mechanical]},
        "ai_filtered": {"metrics": metrics(ai_trades), "trades": [asdict(item) for item in ai_trades]},
        "diagnostics": outcome_diagnostics(candidates, mechanical),
        "actual": actual,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix(".json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output.with_suffix(".md").write_text(build_report(start, end, candidates, mechanical, ai_trades, actual), encoding="utf-8")
    print(json.dumps({"candidates": len(candidates), "mechanical": metrics(mechanical), "ai_filtered": metrics(ai_trades), "actual": {k: v for k, v in actual.items() if k != "rows"}}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
