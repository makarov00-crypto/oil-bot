from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "bot_state"
LOG_DIR = BASE_DIR / "logs"
V35_STATUS_PATH = STATE_DIR / "_v35_shadow_status.json"
V35_PORTFOLIO_PATH = STATE_DIR / "_v35_shadow_portfolio.json"
V35_CANDIDATE_PATH = LOG_DIR / "v35_shadow_candidates.jsonl"
V35_EVENT_PATH = LOG_DIR / "v35_shadow_positions.jsonl"
V35_CYCLE_PATH = LOG_DIR / "v35_shadow_cycles.jsonl"
RUNTIME_STATUS_PATH = STATE_DIR / "_runtime_status.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    result: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return result
    for line in lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            result.append(value)
    return result


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _in_period(row: dict[str, Any], cutoff: datetime | None, *keys: str) -> bool:
    if cutoff is None:
        return True
    for key in keys:
        parsed = _parse_time(row.get(key))
        if parsed is not None:
            return parsed >= cutoff
    return False


def _counter_rows(counter: Counter[str], total: int, limit: int = 8) -> list[dict[str, Any]]:
    return [
        {
            "key": key or "UNKNOWN",
            "count": count,
            "share": round(count / total * 100.0, 1) if total else 0.0,
        }
        for key, count in counter.most_common(limit)
    ]


def _position_row(position: dict[str, Any]) -> dict[str, Any]:
    protection = position.get("protection") if isinstance(position.get("protection"), dict) else {}
    candidate = position.get("candidate") if isinstance(position.get("candidate"), dict) else {}
    direction = str(position.get("direction") or candidate.get("direction") or "")
    sign = 1.0 if direction == "LONG" else -1.0
    entry = _as_float(protection.get("entry_price"))
    current = _as_float(position.get("last_price"), entry)
    stop = _as_float(protection.get("current_stop"))
    quantity = _as_int(position.get("quantity"))
    point_value = _as_float(position.get("point_value"))
    initial_risk = _as_float(position.get("initial_risk_rub"))
    gross = (current - entry) * sign * point_value * quantity
    commission = (abs(entry * point_value) + abs(current * point_value)) * 0.0004 * quantity
    net = gross - commission
    phase = str(protection.get("phase") or "RISK")
    return {
        "candidate_id": str(position.get("candidate_id") or ""),
        "symbol": str(position.get("symbol") or ""),
        "contract_symbol": str(position.get("contract_symbol") or position.get("symbol") or ""),
        "direction": direction,
        "opened_at": str(position.get("opened_at") or ""),
        "last_observed_at": str(position.get("last_observed_at") or ""),
        "phase": phase,
        "quantity": quantity,
        "entry_price": entry,
        "last_price": current,
        "stop": stop,
        "initial_stop": _as_float(protection.get("initial_stop")),
        "initial_risk_rub": round(initial_risk, 2),
        "unrealized_net_pnl_rub": round(net, 2),
        "current_r": round(net / initial_risk, 3) if initial_risk else 0.0,
        "distance_to_stop_pct": round(abs(current - stop) / current * 100.0, 3) if current else 0.0,
        "best_move_r": round(
            _as_float(protection.get("best_move")) / _as_float(protection.get("initial_distance"), 1.0),
            3,
        ),
        "entry_pattern": str(candidate.get("entry_pattern") or ""),
        "classic_pattern": str(candidate.get("classic_pattern") or ""),
        "market_regime": str(candidate.get("market_regime") or ""),
        "protected": phase in {"BREAKEVEN", "PROFIT"},
    }


def _candidate_row(row: dict[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate") if isinstance(row.get("candidate"), dict) else {}
    return {
        "candidate_id": str(row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or ""),
        "contract_symbol": str(row.get("contract_symbol") or row.get("symbol") or ""),
        "signal_time": str(row.get("signal_time") or ""),
        "direction": str(row.get("direction") or candidate.get("direction") or ""),
        "v10_allowed": bool(row.get("v10_allowed")),
        "gate_allowed": bool(row.get("gate_allowed")),
        "gate_reason": str(row.get("gate_reason") or ""),
        "global_regime": str(row.get("global_regime") or ""),
        "group_regime": str(row.get("group_regime") or ""),
        "local_stage": str(row.get("local_stage") or ""),
        "market_regime": str(candidate.get("market_regime") or ""),
        "regime_confidence": _as_int(candidate.get("regime_confidence")),
        "regime_direction": str(candidate.get("regime_direction") or ""),
        "entry_pattern": str(candidate.get("entry_pattern") or ""),
        "classic_pattern": str(candidate.get("classic_pattern") or ""),
        "pattern_confidence": _as_int(candidate.get("pattern_confidence")),
        "confirmation_score": _as_int(candidate.get("confirmation_score")),
        "execution_score": _as_int(candidate.get("execution_score")),
        "directional_bars": _as_int(candidate.get("directional_bars")),
        "body_sum_atr": round(_as_float(candidate.get("body_sum_atr")), 3),
        "largest_body_atr": round(_as_float(candidate.get("largest_body_atr")), 3),
        "path_efficiency_6": round(_as_float(candidate.get("path_efficiency_6")), 3),
        "ao": round(_as_float(candidate.get("ao")), 5),
        "macd_hist": round(_as_float(candidate.get("macd_hist")), 5),
    }


def build_v35_dashboard_payload(
    *,
    days: int | None = 7,
    symbol: str | None = None,
    now: datetime | None = None,
    status_path: Path = V35_STATUS_PATH,
    portfolio_path: Path = V35_PORTFOLIO_PATH,
    candidate_path: Path = V35_CANDIDATE_PATH,
    event_path: Path = V35_EVENT_PATH,
    cycle_path: Path = V35_CYCLE_PATH,
    runtime_path: Path = RUNTIME_STATUS_PATH,
) -> dict[str, Any]:
    generated_at = now or datetime.now(timezone.utc)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    generated_at = generated_at.astimezone(timezone.utc)
    cutoff = generated_at - timedelta(days=max(1, days)) if days else None
    selected_symbol = str(symbol or "").strip().upper()

    status = _read_json(status_path)
    portfolio = _read_json(portfolio_path)
    runtime = _read_json(runtime_path)
    raw_candidates = _read_jsonl(candidate_path)
    raw_events = _read_jsonl(event_path)
    raw_cycles = _read_jsonl(cycle_path)
    candidates = [
        row for row in raw_candidates
        if _in_period(row, cutoff, "signal_time", "recorded_at")
        and (not selected_symbol or str(row.get("symbol") or "").upper() == selected_symbol)
    ]
    events = [
        row for row in raw_events
        if _in_period(row, cutoff, "event_time", "recorded_at")
        and (not selected_symbol or str(row.get("symbol") or "").upper() == selected_symbol)
    ]

    positions_source = portfolio.get("positions") if isinstance(portfolio.get("positions"), dict) else {}
    positions = [
        _position_row(value)
        for key, value in positions_source.items()
        if isinstance(value, dict) and (not selected_symbol or str(key).upper() == selected_symbol)
    ]
    positions.sort(key=lambda item: item["opened_at"], reverse=True)

    market_source = status.get("market_snapshot") if isinstance(status.get("market_snapshot"), dict) else {}
    market_map: list[dict[str, Any]] = []
    for market_symbol, raw in market_source.items():
        if not isinstance(raw, dict) or (selected_symbol and str(market_symbol).upper() != selected_symbol):
            continue
        features = raw.get("features") if isinstance(raw.get("features"), dict) else {}
        market_map.append({
            "symbol": str(market_symbol),
            "regime": str(raw.get("regime") or "UNKNOWN"),
            "confidence": _as_int(raw.get("confidence")),
            "direction": str(raw.get("direction") or "MIXED"),
            "local_stage": str(raw.get("local_stage") or "UNKNOWN"),
            "group_regime": str(raw.get("group_regime") or "UNKNOWN"),
            "group_breadth": round(_as_float(raw.get("group_breadth")) * 100.0, 1),
            "global_regime": str(raw.get("global_regime") or "UNKNOWN"),
            "closed_at": str(raw.get("closed_at") or ""),
            "recent_move_atr": round(_as_float(features.get("recent_move_atr")), 2),
            "path_efficiency": round(_as_float(features.get("recent_path_efficiency")) * 100.0, 1),
            "overlap_ratio": round(_as_float(features.get("overlap_ratio")) * 100.0, 1),
            "small_body_ratio": round(_as_float(features.get("small_body_ratio")) * 100.0, 1),
        })
    market_map.sort(key=lambda item: item["symbol"])

    candidate_rows = [_candidate_row(row) for row in candidates]
    candidate_rows.sort(key=lambda item: item["signal_time"], reverse=True)
    closed = [row for row in events if str(row.get("event") or "").upper() == "CLOSE"]
    closed.sort(key=lambda item: str(item.get("event_time") or item.get("recorded_at") or ""))
    opened_ids = {
        str(row.get("candidate_id") or "")
        for row in events
        if str(row.get("event") or "").upper() == "OPEN"
    }
    closed_ids = {str(row.get("candidate_id") or "") for row in closed}
    v10_allowed = sum(1 for row in candidates if bool(row.get("v10_allowed")))
    gate_allowed = sum(1 for row in candidates if bool(row.get("gate_allowed")))
    profitable = sum(1 for row in closed if _as_float(row.get("net_pnl_rub")) > 0)
    losses = sum(1 for row in closed if _as_float(row.get("net_pnl_rub")) < 0)
    net_total = sum(_as_float(row.get("net_pnl_rub")) for row in closed)
    r_total = sum(_as_float(row.get("net_r_multiple")) for row in closed)
    gross_profit = sum(max(0.0, _as_float(row.get("net_pnl_rub"))) for row in closed)
    gross_loss = abs(sum(min(0.0, _as_float(row.get("net_pnl_rub"))) for row in closed))
    protected = sum(1 for row in positions if row["protected"])

    equity_value = 0.0
    equity_curve: list[dict[str, Any]] = [{"time": "", "value": 0.0}]
    for row in closed:
        equity_value += _as_float(row.get("net_pnl_rub"))
        equity_curve.append({
            "time": str(row.get("event_time") or row.get("recorded_at") or ""),
            "value": round(equity_value, 2),
            "symbol": str(row.get("symbol") or ""),
        })

    regimes = Counter(row["regime"] for row in market_map)
    if not regimes:
        regimes = Counter(str(row.get("candidate", {}).get("market_regime") or "UNKNOWN") for row in candidates)
    patterns = Counter(str(row.get("candidate", {}).get("classic_pattern") or "UNKNOWN") for row in candidates)
    gate_reasons = Counter(str(row.get("gate_reason") or "UNKNOWN") for row in candidates if not row.get("gate_allowed"))
    exit_reasons = Counter(str(row.get("exit_reason") or "UNKNOWN") for row in closed)

    status_updated = _parse_time(status.get("updated_at"))
    portfolio_updated = _parse_time(portfolio.get("updated_at"))
    freshest = max((item for item in (status_updated, portfolio_updated) if item), default=None)
    age_seconds = max(0.0, (generated_at - freshest).total_seconds()) if freshest else None
    is_fresh = age_seconds is not None and age_seconds <= 1_200

    symbols = sorted({
        *[str(value) for value in status.get("symbols") or [] if value],
        *[str(value) for value in market_source if value],
        *[str(row.get("symbol")) for row in raw_candidates if row.get("symbol")],
        *[str(row.get("symbol")) for row in raw_events if row.get("symbol")],
        *[str(value) for value in positions_source if value],
    })
    recent_cycles = raw_cycles[-96:]
    failed_cycles = sum(1 for row in recent_cycles if str(row.get("status") or "") == "failed")
    latest_cycle_audit = recent_cycles[-1] if recent_cycles else {}
    recent_events = sorted(
        events,
        key=lambda item: str(item.get("event_time") or item.get("recorded_at") or ""),
        reverse=True,
    )[:120]
    recent_closed = list(reversed(closed[-80:]))

    return {
        "meta": {
            "strategy_version": str(status.get("strategy_version") or portfolio.get("strategy_version") or "v3.5"),
            "mode": "SHADOW",
            "order_submission_enabled": bool(portfolio.get("order_submission_enabled", False)),
            "generated_at": generated_at.isoformat(),
            "updated_at": freshest.isoformat() if freshest else "",
            "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
            "fresh": is_fresh,
            "session": str(runtime.get("session") or "UNKNOWN"),
            "days": days,
            "selected_symbol": selected_symbol,
            "symbols": symbols,
        },
        "kpis": {
            "candidates": len(candidates),
            "v10_allowed": v10_allowed,
            "gate_allowed": gate_allowed,
            "acceptance_rate": round(gate_allowed / len(candidates) * 100.0, 1) if candidates else 0.0,
            "active_positions": len(positions),
            "protected_positions": protected,
            "opened_trades": len(opened_ids),
            "closed_trades": len(closed_ids),
            "profitable_trades": profitable,
            "losing_trades": losses,
            "win_rate": round(profitable / len(closed) * 100.0, 1) if closed else 0.0,
            "realized_net_pnl_rub": round(net_total, 2),
            "average_r": round(r_total / len(closed), 3) if closed else 0.0,
            "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss else (None if not gross_profit else 99.0),
            "skipped_candidates": sum(1 for row in events if str(row.get("event") or "").upper() == "SKIP"),
        },
        "latest_cycle": {
            "updated_at": str(status.get("updated_at") or ""),
            "window_start": str(status.get("window_start") or ""),
            "window_end": str(status.get("window_end") or ""),
            "symbols_count": len(status.get("symbols") or []),
            "multitimeframe_candidates": _as_int(status.get("multitimeframe_candidates")),
            "v10_allowed": _as_int(status.get("v10_allowed")),
            "hierarchical_allowed": _as_int(status.get("hierarchical_allowed")),
            "new_records": _as_int(status.get("new_records")),
        },
        "funnel": [
            {"key": "CANDIDATES", "count": len(candidates)},
            {"key": "MECHANICS", "count": v10_allowed},
            {"key": "HIERARCHY", "count": gate_allowed},
            {"key": "OPENED", "count": len(opened_ids)},
            {"key": "CLOSED", "count": len(closed_ids)},
        ],
        "positions": positions,
        "market_map": market_map,
        "candidates": candidate_rows[:160],
        "closed_trades": recent_closed,
        "events": recent_events,
        "equity_curve": equity_curve,
        "breakdowns": {
            "regimes": _counter_rows(regimes, len(market_map) if market_map else len(candidates)),
            "patterns": _counter_rows(patterns, len(candidates)),
            "gate_reasons": _counter_rows(gate_reasons, sum(gate_reasons.values())),
            "exit_reasons": _counter_rows(exit_reasons, len(closed)),
        },
        "data_quality": {
            "status_available": bool(status),
            "portfolio_available": bool(portfolio),
            "candidates_available": candidate_path.exists(),
            "events_available": event_path.exists(),
            "candidate_rows_total": len(raw_candidates),
            "event_rows_total": len(raw_events),
            "cycle_audit_available": cycle_path.exists(),
            "cycle_rows_total": len(raw_cycles),
            "recent_failed_cycles": failed_cycles,
            "latest_cycle_status": str(latest_cycle_audit.get("status") or "UNKNOWN"),
            "latest_cycle_duration_seconds": _as_float(latest_cycle_audit.get("duration_seconds")),
        },
    }


def build_v35_dashboard_html() -> str:
    return r'''<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex, nofollow, noarchive" />
  <title>Стратегия v3.5 · Oil Bot</title>
  <link rel="icon" href="/favicon.ico" type="image/svg+xml" />
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Sora:wght@500;600;700&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
  <style>
    :root{--bg:#040814;--panel:#0a1222;--panel2:#0d172a;--line:rgba(133,170,218,.15);--text:#eef6ff;--muted:#8297b2;--cyan:#49d6ff;--blue:#718bff;--green:#36dfa1;--red:#ff6d88;--amber:#ffc865;--shadow:0 22px 60px rgba(0,0,0,.35)}
    *{box-sizing:border-box} body{margin:0;min-height:100vh;color:var(--text);font:500 16px/1.45 Manrope,Arial,sans-serif;background:radial-gradient(circle at 12% -6%,rgba(73,214,255,.17),transparent 27%),radial-gradient(circle at 88% 2%,rgba(113,139,255,.14),transparent 24%),linear-gradient(180deg,#08111f,var(--bg) 34%);}
    .site-header{position:sticky;top:0;z-index:30;backdrop-filter:blur(20px);background:rgba(4,8,20,.79);border-bottom:1px solid var(--line)}
    .site-header__inner{max-width:1500px;margin:auto;padding:18px 28px;display:flex;align-items:center;justify-content:space-between;gap:20px}.site-brand__eyebrow{color:#56e5ff;font:700 13px/1 JetBrains Mono,monospace;letter-spacing:.16em;text-transform:uppercase;margin-bottom:7px}.site-brand__title{font:800 20px/1.15 Manrope,sans-serif}.site-nav{display:flex;flex-wrap:wrap;gap:9px}.site-nav__link{color:#adbed5;text-decoration:none;padding:11px 16px;border-radius:999px;border:1px solid var(--line);background:rgba(73,214,255,.035);font-size:15px;font-weight:800}.site-nav__link.is-active{color:white;border-color:rgba(73,214,255,.35);background:linear-gradient(135deg,rgba(73,214,255,.2),rgba(113,139,255,.22))}
    .wrap{max-width:1500px;margin:auto;padding:30px}.hero{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;margin-bottom:22px}.eyebrow{color:var(--cyan);font:700 13px/1 JetBrains Mono,monospace;letter-spacing:.15em;text-transform:uppercase}.hero h1{margin:10px 0;font:800 clamp(32px,3vw,46px)/1.05 Manrope,sans-serif;letter-spacing:-.035em}.hero p{margin:0;color:#9aadc5;max-width:790px;font-size:17px;line-height:1.55}.hero-status{display:flex;gap:9px;align-items:center;flex-wrap:wrap;justify-content:flex-end}.pill{display:inline-flex;align-items:center;gap:8px;padding:10px 14px;border:1px solid var(--line);border-radius:999px;background:rgba(8,16,30,.72);color:#c8d7e9;font-size:14px;font-weight:800}.dot{width:8px;height:8px;border-radius:50%;background:var(--muted);box-shadow:0 0 10px currentColor}.pill.good .dot{background:var(--green)}.pill.bad .dot{background:var(--red)}.pill.warn .dot{background:var(--amber)}
    .toolbar{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:14px 16px;margin-bottom:18px;border:1px solid var(--line);border-radius:16px;background:rgba(8,16,30,.76)}.segments{display:flex;gap:6px}.segment,select{appearance:none;border:1px solid transparent;border-radius:10px;background:transparent;color:#9eb1c9;padding:10px 14px;font:800 14px Manrope;cursor:pointer}.segment.active{color:white;background:rgba(73,214,255,.12);border-color:rgba(73,214,255,.22)}select{min-width:210px;background:#0a1425;border-color:var(--line);color:#dbeaff}.refresh{font:600 13px JetBrains Mono;color:#91a6bf}
    .kpis{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px;margin-bottom:18px}.card,.panel{border:1px solid var(--line);background:linear-gradient(180deg,rgba(13,23,42,.94),rgba(8,15,28,.92));box-shadow:var(--shadow)}.card{min-height:142px;border-radius:18px;padding:19px;position:relative;overflow:hidden}.card:after{content:"";position:absolute;inset:auto -25% -65% 25%;height:100px;background:radial-gradient(circle,rgba(73,214,255,.1),transparent 64%)}.label{color:#91a4bd;font-size:13px;font-weight:800;text-transform:uppercase;letter-spacing:.055em}.value{margin-top:13px;font:800 clamp(27px,2vw,34px)/1 Manrope;color:#f5f9ff;letter-spacing:-.035em}.note{margin-top:11px;color:#9dafc5;font-size:14px;line-height:1.4}.good-text{color:var(--green)!important}.bad-text{color:var(--red)!important}.warn-text{color:var(--amber)!important}
    .panel{border-radius:20px;padding:22px;margin-bottom:18px}.panel-head{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;margin-bottom:18px}.panel h2{margin:0;font:800 23px/1.25 Manrope,sans-serif}.panel-sub{margin-top:7px;color:#95a8c0;font-size:15px;line-height:1.45}.count{font:700 13px JetBrains Mono;color:#b8cae0}.two{display:grid;grid-template-columns:1.15fr .85fr;gap:18px}.three{display:grid;grid-template-columns:1.1fr .9fr .9fr;gap:18px}.subpanel{border:1px solid rgba(133,170,218,.11);border-radius:17px;background:rgba(5,11,22,.48);padding:19px;min-width:0}.subpanel h3{margin:0 0 16px;font:800 18px/1.3 Manrope,sans-serif;color:#e6f1ff}
    .position-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:14px}.position{border:1px solid rgba(73,214,255,.17);border-radius:17px;padding:19px;background:linear-gradient(145deg,rgba(73,214,255,.06),rgba(7,14,27,.72))}.position-top{display:flex;justify-content:space-between;gap:12px}.symbol{font:800 21px Manrope}.direction{font:800 14px JetBrains Mono}.direction.LONG{color:var(--green)}.direction.SHORT{color:var(--red)}.phase{margin-top:5px;color:#98abc2;font-size:14px}.price-line{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:17px 0}.price-cell span{display:block;color:#8da1b9;font-size:12px;text-transform:uppercase}.price-cell strong{display:block;margin-top:5px;font:700 16px JetBrains Mono}.progress{height:9px;background:#121d31;border-radius:99px;overflow:hidden}.progress i{display:block;height:100%;border-radius:99px;background:linear-gradient(90deg,var(--cyan),var(--green))}.position-foot{display:flex;justify-content:space-between;gap:12px;margin-top:14px;color:#a9bad0;font-size:14px}.empty{padding:34px;text-align:center;border:1px dashed rgba(133,170,218,.19);border-radius:15px;color:#9bacc1;font-size:15px}
    .market-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px}.market-card{min-height:205px;padding:19px;border-radius:16px;border:1px solid rgba(133,170,218,.13);background:linear-gradient(145deg,rgba(73,214,255,.035),rgba(6,13,25,.68))}.market-top{display:flex;align-items:center;justify-content:space-between;gap:10px}.market-top .primary{font-size:20px}.market-regime{font:800 13px JetBrains Mono}.market-regime.TREND{color:var(--green)}.market-regime.CHOP,.market-regime.SHOCK_AFTER_RANGE{color:var(--red)}.market-regime.PULLBACK{color:var(--amber)}.market-main{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-top:16px}.market-score{font:800 31px Manrope}.market-direction{font:800 14px JetBrains Mono}.market-details{display:grid;grid-template-columns:1fr 1fr;gap:10px 14px;margin-top:16px;color:#9bafc6;font-size:13px;line-height:1.35}.market-details strong{display:block;margin-top:3px;color:#deebfa;font:700 14px Manrope,sans-serif}
    .bars{display:grid;gap:13px}.bar-row{display:grid;grid-template-columns:minmax(145px,1.3fr) 3fr 48px;gap:12px;align-items:center}.bar-name{color:#c6d5e7;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.bar-track{height:10px;border-radius:99px;background:#121d31;overflow:hidden}.bar-fill{height:100%;border-radius:99px;background:linear-gradient(90deg,var(--blue),var(--cyan))}.bar-fill.reject{background:linear-gradient(90deg,#ff8b65,var(--red))}.bar-value{text-align:right;font:700 13px JetBrains Mono;color:#dbe8f7}
    .chart{height:240px;position:relative}.chart svg{width:100%;height:100%;overflow:visible}.chart-line{fill:none;stroke:var(--cyan);stroke-width:2.5;filter:drop-shadow(0 0 7px rgba(73,214,255,.45))}.chart-area{fill:url(#area)}.chart-zero{stroke:rgba(133,170,218,.2);stroke-dasharray:4 5}.chart-label{fill:#91a5be;font:12px JetBrains Mono}.chart-empty{height:100%;display:grid;place-items:center;color:#9caec4;font-size:15px}
    .table-wrap{overflow:auto;border:1px solid rgba(133,170,218,.1);border-radius:15px}table{border-collapse:collapse;width:100%;min-width:1160px;font-size:15px}th,td{text-align:left;padding:15px 13px;border-bottom:1px solid rgba(133,170,218,.1);vertical-align:top}th{position:sticky;top:0;background:#0b1526;color:#98abc2;font-size:12px;text-transform:uppercase;letter-spacing:.055em;z-index:2}tbody tr:hover{background:rgba(73,214,255,.035)}.mono{font-family:JetBrains Mono,monospace}.badge{display:inline-flex;padding:6px 9px;border-radius:999px;border:1px solid var(--line);font-size:12px;font-weight:800;color:#c5d4e6;background:rgba(133,170,218,.06)}.badge.ok{color:var(--green);border-color:rgba(54,223,161,.25);background:rgba(54,223,161,.07)}.badge.no{color:var(--red);border-color:rgba(255,109,136,.22);background:rgba(255,109,136,.06)}.primary{font-weight:800;color:#eaf4ff}.secondary{display:block;color:#97aac1;font-size:13px;margin-top:6px;max-width:340px;line-height:1.4}.timeline{display:grid;gap:10px;max-height:460px;overflow:auto}.event{display:grid;grid-template-columns:104px 90px minmax(0,1fr) auto;gap:12px;align-items:center;padding:13px;border-radius:12px;background:rgba(8,16,30,.64);border:1px solid rgba(133,170,218,.09)}.event-type{font:800 12px JetBrains Mono;color:var(--cyan)}.event-text{font-size:14px;color:#c0d0e2}.event-value{font:700 13px JetBrains Mono}.quality{display:flex;flex-wrap:wrap;gap:10px}.quality span{padding:10px 12px;border-radius:10px;background:rgba(8,16,30,.65);border:1px solid rgba(133,170,218,.1);color:#aebfd3;font-size:13px}.footer-note{color:#8297b0;font-size:13px;line-height:1.55;margin:18px 2px 4px}
    @media(max-width:1150px){.kpis{grid-template-columns:repeat(3,1fr)}.three{grid-template-columns:1fr 1fr}.three .subpanel:first-child{grid-column:1/-1}}@media(max-width:780px){body{font-size:16px}.wrap{padding:20px 14px}.site-header__inner{padding:14px;align-items:flex-start;flex-direction:column}.hero{align-items:flex-start;flex-direction:column}.hero p{font-size:16px}.hero-status{justify-content:flex-start}.toolbar{align-items:stretch;flex-direction:column}.kpis{grid-template-columns:repeat(2,1fr)}.two,.three{grid-template-columns:1fr}.three .subpanel:first-child{grid-column:auto}.market-grid,.position-grid{grid-template-columns:1fr}.market-card{min-height:0}.event{grid-template-columns:92px 75px 1fr}.event-value{grid-column:3}.price-line{grid-template-columns:1fr 1fr}.panel{padding:17px}}@media(max-width:460px){.kpis{grid-template-columns:1fr}.card{min-height:125px;padding:17px}.value{font-size:30px}.segments{overflow:auto}.market-details{grid-template-columns:1fr 1fr}}
  </style>
</head>
<body>
  __SITE_NAV__
  <main class="wrap">
    <section class="hero">
      <div><div class="eyebrow">Механическая стратегия · реальный поток брокера</div><h1>Стратегия v3.5</h1><p>Контроль режима рынка, технических паттернов, допуска входа и четырёх фаз защиты позиции.</p></div>
      <div class="hero-status"><span class="pill warn"><i class="dot"></i>Теневая торговля</span><span class="pill good" id="dataPill"><i class="dot"></i><span>данные</span></span><span class="pill good"><i class="dot"></i>заявки отключены</span></div>
    </section>
    <section class="toolbar">
      <div class="segments" aria-label="Период"><button class="segment" data-days="1">Сегодня</button><button class="segment active" data-days="7">7 дней</button><button class="segment" data-days="30">30 дней</button><button class="segment" data-days="0">Всё</button></div>
      <select id="symbolFilter" aria-label="Инструмент"><option value="">Все инструменты</option></select>
      <div class="refresh" id="refreshTime">обновление…</div>
    </section>
    <section class="kpis" id="kpis"></section>
    <section class="panel"><div class="panel-head"><div><h2>Позиции в работе</h2><div class="panel-sub">Стопы пересчитываются на каждом цикле бота.</div></div><span class="count" id="positionCount">0 позиций</span></div><div class="position-grid" id="positions"></div></section>
    <section class="panel"><div class="panel-head"><div><h2>Карта рынка</h2><div class="panel-sub">Режим последних 2–3 дней, локальная стадия и качество движения на часовом графике.</div></div><span class="count" id="marketCount"></span></div><div class="market-grid" id="marketMap"></div></section>
    <section class="three">
      <div class="subpanel"><h3>Воронка входов</h3><div class="bars" id="funnel"></div></div>
      <div class="subpanel"><h3>Режим рынка</h3><div class="bars" id="regimes"></div></div>
      <div class="subpanel"><h3>Классические паттерны</h3><div class="bars" id="patterns"></div></div>
    </section>
    <section class="two" style="margin-top:14px">
      <div class="panel" style="margin:0"><div class="panel-head"><div><h2>Результат после комиссий</h2><div class="panel-sub">Накопленный итог закрытых виртуальных сделок.</div></div><span class="count" id="equitySummary">0 руб.</span></div><div class="chart" id="equityChart"></div></div>
      <div class="panel" style="margin:0"><div class="panel-head"><div><h2>Почему входы отклонены</h2><div class="panel-sub">Главные фильтры, которые защищают от пилы и слабого импульса.</div></div></div><div class="bars" id="rejections"></div></div>
    </section>
    <section class="panel" style="margin-top:14px"><div class="panel-head"><div><h2>Последние решения по входу</h2><div class="panel-sub">Свечи, осциллятор AO, гистограмма MACD, режим и совпадение с паттерном.</div></div><span class="count" id="candidateCount"></span></div><div class="table-wrap"><table><thead><tr><th>Время / инструмент</th><th>Направление</th><th>Режим</th><th>Паттерн</th><th>Импульс свечей</th><th>Подтверждение</th><th>Решение</th></tr></thead><tbody id="candidateRows"></tbody></table></div></section>
    <section class="two">
      <div class="panel" style="margin:0"><div class="panel-head"><div><h2>Закрытые сделки</h2><div class="panel-sub">Результат относительно исходного риска и причина выхода.</div></div></div><div class="table-wrap"><table style="min-width:720px"><thead><tr><th>Инструмент</th><th>Сторона</th><th>Вход → выход</th><th>Итог</th><th>К риску</th><th>Выход</th></tr></thead><tbody id="closedRows"></tbody></table></div></div>
      <div class="panel" style="margin:0"><div class="panel-head"><div><h2>Жизненный цикл</h2><div class="panel-sub">Вход, безубыток, защита прибыли и закрытие.</div></div></div><div class="timeline" id="events"></div></div>
    </section>
    <section class="panel" style="margin-top:14px"><div class="panel-head"><div><h2>Контроль контура</h2><div class="panel-sub">Полнота журналов и свежесть расчёта.</div></div></div><div class="quality" id="quality"></div></section>
    <div class="footer-note">Дашборд показывает техническую тень v3.5. Он не отправляет заявки брокеру. Денежный результат включает расчётную комиссию и моделируемое проскальзывание.</div>
  </main>
  <script>
    const state={days:7,symbol:''}; const $=id=>document.getElementById(id);
    const esc=value=>String(value??'').replace(/[&<>'"]/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[ch]));
    const num=(value,digits=0)=>new Intl.NumberFormat('ru-RU',{minimumFractionDigits:digits,maximumFractionDigits:digits}).format(Number(value||0));
    const money=value=>`${Number(value||0)>=0?'+':''}${num(value,2)} руб.`;
    const time=value=>{if(!value)return '—';const d=new Date(value);return Number.isNaN(d.getTime())?'—':d.toLocaleString('ru-RU',{timeZone:'Europe/Moscow',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});};
    const labels={CANDIDATES:'Кандидаты',MECHANICS:'Механика',HIERARCHY:'Иерархия рынка',OPENED:'Открыты',CLOSED:'Закрыты',TREND:'Тренд',CHOP:'Пила',PULLBACK:'Коррекция',SHOCK_AFTER_RANGE:'Импульс после боковика',UNKNOWN:'Нет данных',UNCLASSIFIED:'Не определено',NONE:'Нет',LONG:'Лонг',SHORT:'Шорт',MIXED:'Смешанное',BULL:'Рост',BEAR:'Падение',SIDEWAYS:'Боковик',SHOCK:'Шок',RANGE:'Диапазон',TRANSITION:'Переход',BREAKOUT_ATTEMPT:'Попытка пробоя',EMERGING_BREAKOUT:'Начало пробоя',TREND_EARLY:'Ранний тренд',TREND_MATURE:'Зрелый тренд',TREND_EXHAUSTED:'Затухание тренда',TREND_PULLBACK_RESUMPTION:'Возобновление тренда после коррекции',LATE_EXHAUSTION:'Позднее затухание движения',POST_SHOCK_RANGE:'Боковик после импульса',TIGHT_CHOP:'Узкая пила',RISK:'Риск',BREAKEVEN:'Безубыток',PROFIT:'Защита прибыли',OPEN:'Вход',PHASE:'Смена фазы',STOP_MOVED:'Стоп передвинут',CLOSE:'Закрытие',SKIP:'Пропуск',BLOCKER:'Блокирующий фактор',V10_SHORT_QUALITY_REJECTED:'Слабый импульс шорта',LOW_CONFIDENCE_REGIME_OBSERVE:'Недостаточная уверенность в режиме рынка',GROUP_SIDEWAYS:'Боковик группы',GLOBAL_SIDEWAYS:'Боковик рынка',GROUP_SHOCK:'Шоковый режим группы',ACTIVE_POSITION_EXISTS:'Позиция уже открыта',STALE_CANDIDATE:'Сигнал устарел',ONE_LOT_EXCEEDS_RISK:'Риск одного лота выше лимита',INVALID_RISK_PLAN:'Не построен риск-план',AO_ZERO_CROSS:'AO пересёк ноль',AO_ZERO_REJECTION:'Отскок AO от нулевой линии',IMPULSE_BREAK:'Импульсный пробой',PATTERN_MOMENTUM:'Импульс паттерна',PROFIT_TRAILING_STOP:'Защитный стоп прибыли',INITIAL_RISK_STOP:'Исходный риск-стоп',RISK_STOP:'Исходный риск-стоп',BREAKEVEN_STOP:'Стоп в безубытке',CONFIRMED_REVERSAL:'Подтверждённый разворот',PROFIT_CONSOLIDATION:'Консолидация прибыли',STRONG_REVERSAL:'Сильный разворот',BULL_LONG_EARLY_TREND:'Ранний лонг по растущему рынку',BEAR_SHORT_RANGE:'Шорт из диапазона на падающем рынке',BEAR_SHORT_TRANSITION:'Шорт на переходе к падению',BEAR_SHORT_BREAKOUT_ATTEMPT:'Шорт на попытке пробоя вниз',BULL_DIRECTION_MISMATCH:'Направление против растущего рынка',BEAR_DIRECTION_MISMATCH:'Направление против падающего рынка',TREND_ALIGNED:'По направлению тренда',TREND_DIRECTION_MISMATCH:'Направление против тренда',PULLBACK_ALIGNED:'Коррекция подтверждает направление',PULLBACK_MISMATCH:'Коррекция против направления',BREAKOUT:'Пробой',CONTINUATION:'Продолжение',REVERSAL:'Разворот',MOMENTUM_BREAKOUT:'Импульсный пробой структуры',RANGE_BREAKOUT:'Пробой диапазона',BULL_FLAG_BREAKOUT:'Бычий флаг',BEAR_FLAG_BREAKDOWN:'Медвежий флаг',BULL_PENNANT_BREAKOUT:'Бычий вымпел',BEAR_PENNANT_BREAKDOWN:'Медвежий вымпел',ASCENDING_TRIANGLE_BREAKOUT:'Восходящий треугольник',DESCENDING_TRIANGLE_BREAKDOWN:'Нисходящий треугольник',SYMMETRICAL_TRIANGLE_BREAKOUT:'Симметричный треугольник',FALLING_WEDGE_BREAKOUT:'Падающий клин',RISING_WEDGE_BREAKDOWN:'Растущий клин',DOUBLE_BOTTOM_BREAKOUT:'Двойное дно',DOUBLE_TOP_BREAKOUT:'Двойная вершина',HEAD_AND_SHOULDERS_BREAKDOWN:'Голова и плечи',INVERSE_HEAD_AND_SHOULDERS_BREAKOUT:'Обратная голова и плечи',FAILED_BREAKOUT_REVERSAL:'Ложный пробой с возвратом'};
    const label=value=>{const key=String(value||'');if(labels[key])return labels[key];if(key.startsWith('STAGE_'))return 'Стадия рынка не допускает вход';if(key.startsWith('BEAR_SHORT_'))return 'Шорт подтверждён падающим рынком';return key?key.replaceAll('_',' '):'—';};
    function renderBars(id,rows,reject=false){const target=$(id);const max=Math.max(1,...(rows||[]).map(x=>Number(x.count||0)));target.innerHTML=(rows||[]).length?(rows||[]).map(x=>`<div class="bar-row"><div class="bar-name" title="${esc(label(x.key))}">${esc(label(x.key))}</div><div class="bar-track"><div class="bar-fill ${reject?'reject':''}" style="width:${Math.max(2,Number(x.count||0)/max*100)}%"></div></div><div class="bar-value">${num(x.count)}</div></div>`).join(''):'<div class="empty">Данных пока нет</div>';}
    function renderKpis(data){const k=data.kpis;const cards=[['Кандидаты',num(k.candidates),`${num(k.gate_allowed)} допущено · ${num(k.acceptance_rate,1)}%`,''],['Позиции',num(k.active_positions),`${num(k.protected_positions)} защищено стопом`,''],['Закрыто',num(k.closed_trades),`${num(k.profitable_trades)} плюс · ${num(k.losing_trades)} минус`,''],['Результат',money(k.realized_net_pnl_rub),'после расчётных комиссий',k.realized_net_pnl_rub>0?'good-text':k.realized_net_pnl_rub<0?'bad-text':''],['Доля прибыльных',`${num(k.win_rate,1)}%`,`отношение прибыли к убытку ${k.profit_factor===null?'—':num(k.profit_factor,2)}`,''],['Средний результат',`${k.average_r>=0?'+':''}${num(k.average_r,3)} ед. риска`,`${num(k.skipped_candidates)} технических пропусков`,k.average_r>0?'good-text':k.average_r<0?'bad-text':'']];$('kpis').innerHTML=cards.map(c=>`<article class="card"><div class="label">${c[0]}</div><div class="value ${c[3]}">${c[1]}</div><div class="note">${c[2]}</div></article>`).join('');}
    function renderPositions(rows){$('positionCount').textContent=`${rows.length} поз.`;$('positions').innerHTML=rows.length?rows.map(p=>{const progress=Math.max(0,Math.min(100,(Number(p.current_r)+1)*40));return `<article class="position"><div class="position-top"><div><div class="symbol">${esc(p.symbol)}</div><div class="phase">${esc(label(p.phase))} · ${num(p.quantity)} лот.</div></div><div class="direction ${esc(p.direction)}">${esc(label(p.direction))}</div></div><div class="price-line"><div class="price-cell"><span>Вход</span><strong>${num(p.entry_price,4)}</strong></div><div class="price-cell"><span>Цена</span><strong>${num(p.last_price,4)}</strong></div><div class="price-cell"><span>Стоп</span><strong>${num(p.stop,4)}</strong></div></div><div class="progress"><i style="width:${progress}%"></i></div><div class="position-foot"><span class="${p.unrealized_net_pnl_rub>=0?'good-text':'bad-text'}">${money(p.unrealized_net_pnl_rub)} · ${p.current_r>=0?'+':''}${num(p.current_r,2)} ед. риска</span><span>${esc(label(p.classic_pattern||p.entry_pattern))}</span></div></article>`}).join(''):'<div class="empty">Активных теневых позиций нет</div>';}
    function renderMarket(rows){$('marketCount').textContent=`${rows.length} инструментов`;$('marketMap').innerHTML=rows.length?rows.map(r=>`<article class="market-card"><div class="market-top"><span class="primary">${esc(r.symbol)}</span><span class="market-regime ${esc(r.regime)}">${esc(label(r.regime))}</span></div><div class="market-main"><span class="market-score">${num(r.confidence)}%</span><span class="market-direction ${esc(r.direction)}">${esc(label(r.direction))}</span></div><div class="market-details"><span>Стадия <strong>${esc(label(r.local_stage))}</strong></span><span>Группа <strong>${esc(label(r.group_regime))}</strong></span><span>Движение <strong>${r.recent_move_atr>=0?'+':''}${num(r.recent_move_atr,2)} ср. диапазона</strong></span><span>Эффективность <strong>${num(r.path_efficiency,0)}%</strong></span><span>Перекрытие <strong>${num(r.overlap_ratio,0)}%</strong></span><span>Мелкие свечи <strong>${num(r.small_body_ratio,0)}%</strong></span></div></article>`).join(''):'<div class="empty">Карта появится после ближайшего полного расчёта свечей</div>';}
    function renderCandidates(rows){$('candidateCount').textContent=`${rows.length} решений`;$('candidateRows').innerHTML=rows.length?rows.map(c=>`<tr><td><span class="primary">${esc(c.symbol)}</span><span class="secondary">${time(c.signal_time)}</span></td><td><span class="badge ${c.direction==='LONG'?'ok':'no'}">${esc(label(c.direction))}</span></td><td><span class="primary">${esc(label(c.market_regime))}</span><span class="secondary">уверенность ${num(c.regime_confidence)}% · ${esc(label(c.global_regime))}/${esc(label(c.group_regime))}</span></td><td><span class="primary">${esc(label(c.classic_pattern||c.entry_pattern))}</span><span class="secondary">совпадение ${num(c.pattern_confidence)}%</span></td><td><span class="mono">${num(c.directional_bars)} свеч. · ${num(c.body_sum_atr,2)} ср. диапазона</span><span class="secondary">эффективность пути ${num(c.path_efficiency_6*100,1)}%</span></td><td><span class="mono">Осциллятор AO: ${num(c.ao,3)}</span><span class="secondary">Гистограмма MACD: ${num(c.macd_hist,3)} · оценка ${num(c.execution_score)}</span></td><td><span class="badge ${c.gate_allowed?'ok':'no'}">${c.gate_allowed?'ВХОД':'ПРОПУСК'}</span><span class="secondary">${esc(label(c.gate_reason))}</span></td></tr>`).join(''):'<tr><td colspan="7"><div class="empty">Решений за выбранный период нет</div></td></tr>';}
    function renderClosed(rows){$('closedRows').innerHTML=rows.length?rows.map(r=>`<tr><td class="primary">${esc(r.symbol)}</td><td>${esc(label(r.direction))}</td><td class="mono">${num(r.entry_price,4)} → ${num(r.exit_price,4)}</td><td class="mono ${Number(r.net_pnl_rub)>=0?'good-text':'bad-text'}">${money(r.net_pnl_rub)}</td><td class="mono">${Number(r.net_r_multiple)>=0?'+':''}${num(r.net_r_multiple,2)} ед. риска</td><td>${esc(label(r.exit_reason))}<span class="secondary">${time(r.event_time)}</span></td></tr>`).join(''):'<tr><td colspan="6"><div class="empty">Закрытых сделок пока нет</div></td></tr>';}
    function renderEvents(rows){$('events').innerHTML=rows.length?rows.slice(0,70).map(r=>{const value=r.net_pnl_rub!==undefined?money(r.net_pnl_rub):r.stop!==undefined?`стоп ${num(r.stop,4)}`:r.price!==undefined?num(r.price,4):'';return `<div class="event"><div class="event-type">${esc(label(String(r.event||'').toUpperCase()))}</div><div class="primary">${esc(r.symbol||'—')}</div><div class="event-text">${time(r.event_time||r.recorded_at)} · ${esc(label(r.phase||r.exit_reason||r.reason||r.direction))}</div><div class="event-value ${Number(r.net_pnl_rub||0)>=0?'good-text':''}">${value}</div></div>`}).join(''):'<div class="empty">Событий сопровождения пока нет</div>';}
    function renderEquity(rows){const target=$('equityChart');const values=(rows||[]).map(x=>Number(x.value||0));$('equitySummary').textContent=money(values.at(-1)||0);if(values.length<2){target.innerHTML='<div class="chart-empty">Кривая появится после первой закрытой сделки</div>';return;}const w=700,h=190,p=18,min=Math.min(0,...values),max=Math.max(0,...values),span=Math.max(1,max-min);const pts=values.map((v,i)=>`${p+i*(w-2*p)/Math.max(1,values.length-1)},${p+(max-v)*(h-2*p)/span}`);const zero=p+(max)*(h-2*p)/span;target.innerHTML=`<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><defs><linearGradient id="area" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#49d6ff" stop-opacity=".24"/><stop offset="1" stop-color="#49d6ff" stop-opacity="0"/></linearGradient></defs><line class="chart-zero" x1="${p}" x2="${w-p}" y1="${zero}" y2="${zero}"/><polygon class="chart-area" points="${pts.join(' ')} ${w-p},${h-p} ${p},${h-p}"/><polyline class="chart-line" points="${pts.join(' ')}"/><text class="chart-label" x="${p}" y="12">${money(max)}</text><text class="chart-label" x="${p}" y="${h-2}">${money(min)}</text></svg>`;}
    function renderQuality(data){const q=data.data_quality,m=data.meta,l=data.latest_cycle;const paused=['CLOSED','CLEARING'].includes(m.session);const cycleText=q.cycle_rows_total?`${num(q.cycle_rows_total)} · ошибок ${num(q.recent_failed_cycles)}`:'начнётся со следующего расчёта';const cells=[`Статус ${q.status_available?'есть':'нет'}`,`Портфель ${q.portfolio_available?'есть':'нет'}`,`Кандидатов в журнале ${num(q.candidate_rows_total)}`,`Событий ${num(q.event_rows_total)}`,`Расчётов ${cycleText}`,`Инструментов в цикле ${num(l.symbols_count)}`,`Возраст данных ${m.age_seconds===null?'—':num(m.age_seconds,0)+' сек.'}`,`Реальные заявки ${m.order_submission_enabled?'ВКЛЮЧЕНЫ':'отключены'}`];$('quality').innerHTML=cells.map(x=>`<span>${esc(x)}</span>`).join('');const pill=$('dataPill');pill.className=`pill ${paused?'warn':m.fresh?'good':'bad'}`;pill.querySelector('span').textContent=paused?'рынок закрыт · последний срез':m.fresh?'данные свежие':'данные устарели';$('refreshTime').textContent=`обновлено ${time(m.updated_at)} · авто 10 сек.`;}
    async function refresh(){const params=new URLSearchParams();if(state.days)params.set('days',state.days);else params.set('days','0');if(state.symbol)params.set('symbol',state.symbol);try{const response=await fetch(`/api/v35?${params}`,{cache:'no-store'});if(!response.ok)throw new Error(`HTTP ${response.status}`);const data=await response.json();renderKpis(data);renderPositions(data.positions||[]);renderMarket(data.market_map||[]);renderBars('funnel',data.funnel||[]);renderBars('regimes',data.breakdowns?.regimes||[]);renderBars('patterns',data.breakdowns?.patterns||[]);renderBars('rejections',data.breakdowns?.gate_reasons||[],true);renderEquity(data.equity_curve||[]);renderCandidates(data.candidates||[]);renderClosed(data.closed_trades||[]);renderEvents(data.events||[]);renderQuality(data);const select=$('symbolFilter');const current=state.symbol;select.innerHTML='<option value="">Все инструменты</option>'+data.meta.symbols.map(s=>`<option value="${esc(s)}">${esc(s)}</option>`).join('');select.value=current;}catch(error){$('refreshTime').textContent=`ошибка обновления: ${error.message}`;$('dataPill').className='pill bad';$('dataPill').querySelector('span').textContent='нет данных';}}
    document.querySelectorAll('.segment').forEach(button=>button.addEventListener('click',()=>{document.querySelectorAll('.segment').forEach(x=>x.classList.remove('active'));button.classList.add('active');state.days=Number(button.dataset.days);refresh();}));$('symbolFilter').addEventListener('change',event=>{state.symbol=event.target.value;refresh();});refresh();setInterval(refresh,10000);
  </script>
</body>
</html>'''
