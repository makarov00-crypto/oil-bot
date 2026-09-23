"""Decision-chain analytics for the current AO entry strategy.

The four-hour price check is diagnostic only. Trading results use closed,
broker-journal trades and are never inferred for skipped candidates.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from strategy_research import AO_ROLLOUT_AT, AO_STRATEGY, EXECUTED_STATUSES, dedupe_ai_observations


def _context(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("context")
    return value if isinstance(value, dict) else {}


def _time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else None


def _action(row: dict[str, Any]) -> str:
    return str((_context(row).get("shadow_ai") or {}).get("action") or "").upper()


def build_signal_ai_entry_analytics(
    observations: list[dict[str, Any]],
    closed_trades: list[dict[str, Any]],
    *,
    recent_limit: int = 12,
) -> dict[str, Any]:
    """Reconcile one AO candidate per strategy/symbol/side/candle with closed trades."""
    candidates = [
        row for row in dedupe_ai_observations(observations)
        if row.get("strategy") == AO_STRATEGY
        and (at := _time(row.get("observed_at"))) is not None
        and at >= AO_ROLLOUT_AT
    ]
    candidates.sort(key=lambda row: str(row.get("observed_at") or ""), reverse=True)
    by_action = {"enter": {"reviewed": 0, "executed": 0, "closed": 0, "net_pnl_rub": 0.0, "wins": 0, "r_evaluated": 0, "r_sum": 0.0},
                 "abstain": {"reviewed": 0, "executed": 0, "closed": 0, "net_pnl_rub": 0.0, "wins": 0, "r_evaluated": 0, "r_sum": 0.0}}
    counts = {"candidates": len(candidates), "reviewed": 0, "enter": 0, "abstain": 0,
              "unavailable": 0, "other": 0, "selected": 0, "confirmed": 0,
              "closed": 0, "unmatched_closed": 0, "pending_closed": 0,
              "price_checked_4h": 0, "price_check_late": 0,
              "enter_favorable_4h": 0, "enter_checked_4h": 0,
              "abstain_unfavorable_4h": 0, "abstain_checked_4h": 0,
              "supported_losers": 0, "abstain_winners": 0}
    recent = []
    candidate_by_id = {}
    for row in candidates:
        ctx = _context(row)
        action = _action(row)
        group = "enter" if action in {"ВХОД", "ENTER"} else "abstain" if action in {"ВОЗДЕРЖАТЬСЯ", "ABSTAIN"} else ""
        status = str(ctx.get("shadow_ai_status") or "").lower()
        if group:
            counts["reviewed"] += 1
            counts[group] += 1
            by_action[group]["reviewed"] += 1
        elif status == "unavailable":
            counts["unavailable"] += 1
        else:
            counts["other"] += 1
        selected = str(row.get("decision") or "").lower() == "selected"
        confirmed = selected and str(ctx.get("execution_status") or "").lower() in EXECUTED_STATUSES
        counts["selected"] += int(selected)
        counts["confirmed"] += int(confirmed)
        if group and confirmed:
            by_action[group]["executed"] += 1
        outcome = (ctx.get("shadow_ai_outcomes") or {}).get("4h")
        if group and isinstance(outcome, dict) and isinstance(outcome.get("favorable"), bool):
            counts["price_checked_4h"] += 1
            counts[f"{group}_checked_4h"] += 1
            counts["enter_favorable_4h" if group == "enter" else "abstain_unfavorable_4h"] += int(
                outcome["favorable"] if group == "enter" else not outcome["favorable"]
            )
            observed = _time(row.get("observed_at"))
            evaluated = _time(outcome.get("evaluated_at"))
            if observed and evaluated and evaluated - observed > timedelta(hours=5):
                counts["price_check_late"] += 1
        candidate_id = str(ctx.get("candidate_id") or "")
        if candidate_id and confirmed:
            candidate_by_id[candidate_id] = row
        recent.append({
            "candidate_id": candidate_id,
            "symbol": row.get("symbol"), "signal": row.get("signal"),
            "observed_at": row.get("observed_at"), "decision": row.get("decision"),
            "execution_status": ctx.get("execution_status") or "",
            "defer_kind": ctx.get("defer_kind") or "",
            "ai_action": action, "ai_status": status,
            "ai_reason": (ctx.get("shadow_ai") or {}).get("reason") or ctx.get("shadow_ai_error") or "",
            "model": ctx.get("shadow_ai_model") or "",
            "prompt_version": ctx.get("shadow_ai_prompt_version") or "",
            "closed_net_pnl_rub": None,
        })
    recent_by_id = {row["candidate_id"]: row for row in recent if row["candidate_id"]}
    trade_groups: dict[str, dict[str, Any]] = {}
    for trade in closed_trades:
        if str(trade.get("strategy") or "") != AO_STRATEGY:
            continue
        entry = _time(trade.get("entry_time"))
        if entry is None or entry < AO_ROLLOUT_AT:
            continue
        complete = trade.get("fully_closed") is not False
        counts["closed"] += int(complete)
        candidate_id = str(trade.get("candidate_id") or "")
        candidate = candidate_by_id.get(candidate_id) if candidate_id else None
        if candidate is None and not candidate_id:
            matches = []
            for row in candidates:
                ctx = _context(row)
                observed = _time(row.get("observed_at"))
                if (observed and row.get("symbol") == trade.get("symbol")
                    and row.get("signal") == trade.get("side")
                    and row.get("decision") == "selected"
                    and str(ctx.get("execution_status") or "").lower() in EXECUTED_STATUSES
                    and -120 <= (entry - observed).total_seconds() <= 1200):
                    matches.append(row)
            if len(matches) == 1:
                candidate = matches[0]
        if candidate is None:
            counts["unmatched_closed"] += int(complete)
            continue
        key = str(_context(candidate).get("candidate_id") or "") or "|".join(
            [str(candidate.get("strategy")), str(candidate.get("symbol")),
             str(candidate.get("signal")), str(_context(candidate).get("candle_time"))]
        )
        group = "enter" if _action(candidate) in {"ВХОД", "ENTER"} else "abstain" if _action(candidate) in {"ВОЗДЕРЖАТЬСЯ", "ABSTAIN"} else ""
        if not group:
            continue
        net = trade.get("net_pnl_rub")
        if net is None:
            counts["unmatched_closed"] += int(complete)
            continue
        item = trade_groups.setdefault(key, {"action": group, "net": 0.0, "risk": 0.0, "complete": False})
        item["net"] += float(net)
        risk_per_contract = float(_context(candidate).get("risk_per_contract_rub") or 0)
        item["risk"] += risk_per_contract * max(0, int(trade.get("qty_lots") or 0))
        item["complete"] = item["complete"] or complete
    for key, item in trade_groups.items():
        if not item["complete"]:
            continue
        group = by_action[item["action"]]
        group["closed"] += 1
        group["net_pnl_rub"] += item["net"]
        group["wins"] += int(item["net"] > 0)
        if item["action"] == "enter" and item["net"] < 0:
            counts["supported_losers"] += 1
        if item["action"] == "abstain" and item["net"] > 0:
            counts["abstain_winners"] += 1
        if item["risk"] > 0:
            group["r_evaluated"] += 1
            group["r_sum"] += item["net"] / item["risk"]
        if key in recent_by_id:
            recent_by_id[key]["closed_net_pnl_rub"] = round(item["net"], 2)
    for group in by_action.values():
        group["net_pnl_rub"] = round(group["net_pnl_rub"], 2)
        r_sum = group.pop("r_sum")
        group["average_r"] = round(r_sum / group["r_evaluated"], 3) if group["r_evaluated"] else None
    counts["pending_closed"] = max(0, counts["confirmed"] - sum(group["closed"] for group in by_action.values()))
    return {
        "strategy": AO_STRATEGY, "since": AO_ROLLOUT_AT.isoformat(),
        "counts": counts, "by_action": by_action,
        "recent": recent[:recent_limit],
        "notes": {
            "effect": "На AO ИИ наблюдает и не блокирует входы. Группы являются наблюдательными, причинный эффект ИИ не измерен.",
            "price": "Проверка 4ч — движение цены в сторону сигнала без комиссии, стопа и размера позиции; поздняя цена после паузы рынка учтена отдельно.",
            "pnl": "NET — только закрытые сделки из журнала брокера; R рассчитан по сохранённому плановому риску на лот. Открытые и пропущенные кандидаты не имеют фактического результата.",
        },
    }
