"""Read-only diagnostics for the AO rollout and shadow AI opinions."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any


AO_STRATEGY = "ao_chaikin_1h"
AO_ROLLOUT_AT = datetime.fromisoformat("2026-09-22T22:28:28+03:00")
EXECUTED_STATUSES = {"confirmed_open", "recovered_open"}


def _context(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("context")
    return value if isinstance(value, dict) else {}


def _timestamp(value: Any) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value or ""))
    except ValueError:
        return None
    return result if result.tzinfo else None


def dedupe_ai_observations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Count a selected candidate once when it was also logged as deferred."""
    best: dict[tuple[str, str, str, str], tuple[tuple[int, str], dict[str, Any]]] = {}
    priorities = {"selected": 2, "deferred": 1}
    for row in rows:
        context = _context(row)
        key = (
            str(row.get("strategy") or ""),
            str(row.get("symbol") or "").upper(),
            str(row.get("signal") or "").upper(),
            str(context.get("candle_time") or row.get("observation_key") or ""),
        )
        rank = (
            priorities.get(str(row.get("decision") or "").lower(), 0),
            str(row.get("observed_at") or ""),
        )
        if key not in best or rank > best[key][0]:
            best[key] = (rank, row)
    return [item[1] for item in best.values()]


def build_ai_research(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"evaluated": 0, "market_favorable": 0, "enter": 0, "enter_correct": 0,
                 "abstain": 0, "abstain_correct": 0, "enter_move_pct_sum": 0.0,
                 "abstain_move_pct_sum": 0.0}
    )
    for row in dedupe_ai_observations(rows):
        context = _context(row)
        outcome = (context.get("shadow_ai_outcomes") or {}).get("4h")
        if not isinstance(outcome, dict) or "favorable" not in outcome:
            continue
        action = str((context.get("shadow_ai") or {}).get("action") or "").upper()
        if action not in {"ВХОД", "ENTER", "ВОЗДЕРЖАТЬСЯ", "ABSTAIN"}:
            continue
        strategy = str(row.get("strategy") or "unknown")
        favorable = bool(outcome["favorable"])
        move = float(outcome.get("move_pct") or 0.0)
        for key in ("all", strategy):
            group = groups[key]
            group["evaluated"] += 1
            group["market_favorable"] += int(favorable)
            if action in {"ВХОД", "ENTER"}:
                group["enter"] += 1
                group["enter_correct"] += int(favorable)
                group["enter_move_pct_sum"] += move
            else:
                group["abstain"] += 1
                group["abstain_correct"] += int(not favorable)
                group["abstain_move_pct_sum"] += move
    result: dict[str, Any] = {}
    for strategy, group in groups.items():
        total = group["evaluated"]
        enters = group["enter"]
        abstains = group["abstain"]
        result[strategy] = {
            "evaluated": total,
            "market_favorable": group["market_favorable"],
            "market_favorable_pct": round(100 * group["market_favorable"] / total, 1) if total else None,
            "enter": enters,
            "enter_correct": group["enter_correct"],
            "enter_correct_pct": round(100 * group["enter_correct"] / enters, 1) if enters else None,
            "enter_average_move_pct": round(group["enter_move_pct_sum"] / enters, 3) if enters else None,
            "abstain": abstains,
            "abstain_correct": group["abstain_correct"],
            "abstain_correct_pct": round(100 * group["abstain_correct"] / abstains, 1) if abstains else None,
            "abstain_average_move_pct": round(group["abstain_move_pct_sum"] / abstains, 3) if abstains else None,
        }
    return {"basis": "Движение цены через 4 часа в сторону сигнала, без комиссий и стопа.", "by_strategy": result}


def build_ao_execution_research(rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for row in dedupe_ai_observations(rows):
        if str(row.get("strategy") or "") != AO_STRATEGY:
            continue
        observed_at = _timestamp(row.get("observed_at"))
        if observed_at is None or observed_at < AO_ROLLOUT_AT:
            continue
        context = _context(row)
        candidates.append({
            "symbol": str(row.get("symbol") or "").upper(),
            "signal": str(row.get("signal") or "").upper(),
            "observed_at": observed_at.isoformat(),
            "decision": str(row.get("decision") or ""),
            "execution_status": str(context.get("execution_status") or ""),
            "defer_kind": str(context.get("defer_kind") or ""),
            "ai_action": str((context.get("shadow_ai") or {}).get("action") or ""),
            "ai_canary_result": str(context.get("ai_canary_result") or ""),
        })
    candidates.sort(key=lambda row: row["observed_at"], reverse=True)
    selected = [row for row in candidates if row["decision"] == "selected"]
    confirmed = [row for row in selected if row["execution_status"] in EXECUTED_STATUSES]
    return {
        "since": AO_ROLLOUT_AT.isoformat(),
        "candidates": len(candidates),
        "selected": len(selected),
        "confirmed": len(confirmed),
        "selected_unconfirmed": len(selected) - len(confirmed),
        "deferred": len(candidates) - len(selected),
        "recent": candidates[:20],
    }
