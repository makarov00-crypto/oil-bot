from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from strategies.v35_hierarchy import hierarchical_context, hierarchical_gate
from strategies.v35_signals import (
    STRATEGY_VERSION,
    V35Candidate,
    find_multitimeframe_candidates,
    short_entry_allowed,
)


SHADOW_SCHEMA_VERSION = 1
ORDER_SUBMISSION_ENABLED = False
DEFAULT_SHORT_BODY_THRESHOLD_ATR = 2.25


def candidate_id(candidate: V35Candidate) -> str:
    raw = "|".join((
        STRATEGY_VERSION,
        candidate.symbol,
        candidate.signal_time,
        candidate.direction,
    ))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def read_shadow_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    result: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            result.append(payload)
    return result


class V35ShadowJournal:
    """Append-only journal for mechanical v35 decisions. It never submits orders."""

    def __init__(self, path: Path, status_path: Path):
        self.path = path
        self.status_path = status_path

    def observe(
        self,
        frames_by_symbol: dict[str, dict[int, pd.DataFrame]],
        *,
        start: pd.Timestamp,
        end: pd.Timestamp,
        contract_by_symbol: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        hourly_by_symbol = {
            symbol: frames[60]
            for symbol, frames in frames_by_symbol.items()
            if 60 in frames
        }
        known_ids = {
            str(row.get("candidate_id") or "")
            for row in read_shadow_records(self.path)
        }
        new_rows: list[dict[str, Any]] = []
        counts = {
            "multitimeframe_candidates": 0,
            "v10_allowed": 0,
            "hierarchical_allowed": 0,
            "new_records": 0,
        }
        for symbol, frames in frames_by_symbol.items():
            candidates = find_multitimeframe_candidates(symbol, frames, start, end)
            counts["multitimeframe_candidates"] += len(candidates)
            for candidate in candidates:
                uid = candidate_id(candidate)
                v10_allowed = short_entry_allowed(candidate, DEFAULT_SHORT_BODY_THRESHOLD_ATR)
                if v10_allowed:
                    counts["v10_allowed"] += 1
                context: dict[str, Any] = {}
                gate_allowed = False
                gate_reason = "V10_SHORT_QUALITY_REJECTED"
                if v10_allowed:
                    context = hierarchical_context(candidate, hourly_by_symbol)
                    gate_allowed, gate_reason = hierarchical_gate(context)
                if gate_allowed:
                    counts["hierarchical_allowed"] += 1
                if uid in known_ids:
                    continue
                row = {
                    "schema_version": SHADOW_SCHEMA_VERSION,
                    "strategy_version": STRATEGY_VERSION,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "candidate_id": uid,
                    "symbol": symbol,
                    "contract_symbol": (contract_by_symbol or {}).get(symbol, symbol),
                    "signal_time": candidate.signal_time,
                    "direction": candidate.direction,
                    "v10_allowed": v10_allowed,
                    "global_regime": context.get("global_regime", ""),
                    "group_regime": context.get("group_regime", ""),
                    "local_stage": context.get("local_stage", ""),
                    "gate_allowed": gate_allowed,
                    "gate_reason": gate_reason,
                    "entry_price": None,
                    "initial_stop": None,
                    "quantity": 0,
                    "risk_rub": 0.0,
                    "order_submission_enabled": ORDER_SUBMISSION_ENABLED,
                    "candidate": asdict(candidate),
                    "context": context,
                }
                new_rows.append(row)
                known_ids.add(uid)
        if new_rows:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                for row in new_rows:
                    handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        counts["new_records"] = len(new_rows)
        status = {
            "schema_version": SHADOW_SCHEMA_VERSION,
            "strategy_version": STRATEGY_VERSION,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "window_start": pd.Timestamp(start).isoformat(),
            "window_end": pd.Timestamp(end).isoformat(),
            "order_submission_enabled": ORDER_SUBMISSION_ENABLED,
            "symbols": sorted(frames_by_symbol),
            **counts,
        }
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.status_path.with_suffix(self.status_path.suffix + ".tmp")
        temporary.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(self.status_path)
        return status
