from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from strategies.v35_hierarchy import hierarchical_context, hierarchical_gate
from strategies.v35_risk import (
    COMMISSION_RATE_PER_SIDE,
    ProtectionState,
    adverse_slippage_price,
    advance_on_price_tick,
    initial_stop_from_structure,
    new_protection_state,
    position_size_for_risk,
)
from strategies.v35_signals import (
    STRATEGY_VERSION,
    V35Candidate,
    find_multitimeframe_candidates,
    short_entry_allowed,
)


SHADOW_SCHEMA_VERSION = 1
PORTFOLIO_SCHEMA_VERSION = 1
ORDER_SUBMISSION_ENABLED = False
DEFAULT_SHORT_BODY_THRESHOLD_ATR = 2.25
MAX_ENTRY_AGE_MINUTES = 30
MAX_PROCESSED_CANDIDATES = 2_000


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


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def _hourly_structure(candidate: V35Candidate, hourly: pd.DataFrame) -> dict[str, Any]:
    if hourly.empty or "closed_at" not in hourly:
        return {}
    signal_time = pd.Timestamp(candidate.signal_time)
    context = hourly.loc[hourly["closed_at"] <= signal_time].tail(3)
    if context.empty:
        return {}
    return {
        "low": float(context["low"].min()),
        "high": float(context["high"].max()),
        "closed_at": pd.Timestamp(context.iloc[-1]["closed_at"]).isoformat(),
    }


def _management_context(frame: pd.DataFrame) -> list[dict[str, Any]]:
    wanted = ("time", "closed_at", "open", "high", "low", "close", "body", "ao", "ao_delta", "macd_hist")
    if frame.empty or any(name not in frame for name in wanted):
        return []
    result: list[dict[str, Any]] = []
    for _, row in frame.tail(5).iterrows():
        item: dict[str, Any] = {}
        for name in wanted:
            value = row[name]
            if name in {"time", "closed_at"}:
                item[name] = pd.Timestamp(value).isoformat()
            else:
                item[name] = float(value)
        result.append(item)
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
                    "hourly_structure": _hourly_structure(candidate, frames.get(60, pd.DataFrame())),
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
            "management_context": {
                symbol: _management_context(frames.get(30, pd.DataFrame()))
                for symbol, frames in frames_by_symbol.items()
            },
            **counts,
        }
        _atomic_write_json(self.status_path, status)
        return status


class V35ShadowPortfolio:
    """Persistent 10-second lifecycle for accepted v35 shadow candidates."""

    def __init__(
        self,
        candidate_path: Path,
        event_path: Path,
        state_path: Path,
        status_path: Path,
    ):
        self.candidate_path = candidate_path
        self.event_path = event_path
        self.state_path = state_path
        self.status_path = status_path
        self.state = self._load_state()

    def _empty_state(self) -> dict[str, Any]:
        return {
            "schema_version": PORTFOLIO_SCHEMA_VERSION,
            "strategy_version": STRATEGY_VERSION,
            "updated_at": "",
            "order_submission_enabled": ORDER_SUBMISSION_ENABLED,
            "positions": {},
            "processed_candidate_ids": [],
            "opened_trades": 0,
            "closed_trades": 0,
            "skipped_candidates": 0,
            "realized_net_pnl_rub": 0.0,
        }

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._empty_state()
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self._empty_state()
        if not isinstance(payload, dict):
            return self._empty_state()
        base = self._empty_state()
        base.update(payload)
        base["order_submission_enabled"] = ORDER_SUBMISSION_ENABLED
        base["positions"] = payload.get("positions") if isinstance(payload.get("positions"), dict) else {}
        return base

    def _save(self, now: pd.Timestamp) -> None:
        self.state["updated_at"] = now.isoformat()
        self.state["order_submission_enabled"] = ORDER_SUBMISSION_ENABLED
        _atomic_write_json(self.state_path, self.state)

    def _event(self, event: dict[str, Any]) -> None:
        self.event_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": PORTFOLIO_SCHEMA_VERSION,
            "strategy_version": STRATEGY_VERSION,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "order_submission_enabled": ORDER_SUBMISSION_ENABLED,
            **event,
        }
        with self.event_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

    def _processed(self) -> set[str]:
        return {str(value) for value in self.state.get("processed_candidate_ids", []) if value}

    def _mark_processed(self, uid: str) -> None:
        values = [str(value) for value in self.state.get("processed_candidate_ids", []) if value]
        if uid not in values:
            values.append(uid)
        self.state["processed_candidate_ids"] = values[-MAX_PROCESSED_CANDIDATES:]

    def _fresh_candidates(self, now: pd.Timestamp) -> list[dict[str, Any]]:
        processed = self._processed()
        rows: list[dict[str, Any]] = []
        for row in read_shadow_records(self.candidate_path):
            uid = str(row.get("candidate_id") or "")
            if not uid or uid in processed or not bool(row.get("gate_allowed")):
                continue
            signal_time = pd.to_datetime(row.get("signal_time"), utc=True, errors="coerce")
            if pd.isna(signal_time):
                continue
            rows.append(row)
        return sorted(rows, key=lambda item: str(item.get("signal_time") or ""))

    def symbols_requiring_prices(self, now: pd.Timestamp | None = None) -> set[str]:
        moment = pd.Timestamp(now or datetime.now(timezone.utc))
        if moment.tzinfo is None:
            moment = moment.tz_localize("UTC")
        result = {str(symbol) for symbol in self.state.get("positions", {})}
        for row in self._fresh_candidates(moment):
            signal_time = pd.Timestamp(row["signal_time"])
            if moment - signal_time <= pd.Timedelta(minutes=MAX_ENTRY_AGE_MINUTES):
                result.add(str(row.get("symbol") or ""))
        result.discard("")
        return result

    def _management_frames(self) -> dict[str, pd.DataFrame]:
        if not self.status_path.exists():
            return {}
        try:
            payload = json.loads(self.status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        contexts = payload.get("management_context") if isinstance(payload, dict) else {}
        result: dict[str, pd.DataFrame] = {}
        if not isinstance(contexts, dict):
            return result
        for symbol, rows in contexts.items():
            if not isinstance(rows, list) or not rows:
                continue
            frame = pd.DataFrame(rows)
            for name in ("time", "closed_at"):
                if name in frame:
                    frame[name] = pd.to_datetime(frame[name], utc=True, errors="coerce")
            result[str(symbol)] = frame.dropna(subset=["closed_at"]).reset_index(drop=True)
        return result

    def _skip(self, row: dict[str, Any], reason: str, now: pd.Timestamp) -> None:
        uid = str(row.get("candidate_id") or "")
        self._mark_processed(uid)
        self.state["skipped_candidates"] = int(self.state.get("skipped_candidates", 0)) + 1
        self._event({
            "event": "SKIP",
            "candidate_id": uid,
            "symbol": row.get("symbol"),
            "direction": row.get("direction"),
            "event_time": now.isoformat(),
            "reason": reason,
        })

    def _open_candidate(
        self,
        row: dict[str, Any],
        price: float,
        meta: dict[str, float],
        now: pd.Timestamp,
    ) -> None:
        uid = str(row.get("candidate_id") or "")
        symbol = str(row.get("symbol") or "")
        if symbol in self.state["positions"]:
            self._skip(row, "ACTIVE_POSITION_EXISTS", now)
            return
        try:
            candidate = V35Candidate(**dict(row.get("candidate") or {}))
            structure = dict(row.get("hourly_structure") or {})
            tick_size = max(0.000001, float(meta.get("tick_size") or 0.0))
            point_value = max(0.000001, float(meta.get("point_value") or 0.0))
            entry_price = adverse_slippage_price(
                float(price), candidate.direction, tick_size, 1.0, is_entry=True
            )
            initial_stop = initial_stop_from_structure(
                candidate,
                structure_low=float(structure["low"]),
                structure_high=float(structure["high"]),
                entry_price=entry_price,
                tick_size=tick_size,
            )
            quantity, risk_per_lot = position_size_for_risk(
                entry_price, initial_stop, point_value
            )
        except (KeyError, TypeError, ValueError):
            self._skip(row, "INVALID_RISK_PLAN", now)
            return
        if quantity < 1:
            self._skip(row, "ONE_LOT_EXCEEDS_RISK", now)
            return
        protection = new_protection_state(candidate.direction, entry_price, initial_stop)
        self.state["positions"][symbol] = {
            "candidate_id": uid,
            "symbol": symbol,
            "contract_symbol": row.get("contract_symbol") or symbol,
            "direction": candidate.direction,
            "opened_at": now.isoformat(),
            "candidate": asdict(candidate),
            "protection": asdict(protection),
            "quantity": quantity,
            "point_value": point_value,
            "tick_size": tick_size,
            "risk_per_lot_rub": risk_per_lot,
            "initial_risk_rub": risk_per_lot * quantity,
            "last_price": entry_price,
            "last_observed_at": now.isoformat(),
        }
        self._mark_processed(uid)
        self.state["opened_trades"] = int(self.state.get("opened_trades", 0)) + 1
        self._event({
            "event": "OPEN",
            "candidate_id": uid,
            "symbol": symbol,
            "direction": candidate.direction,
            "event_time": now.isoformat(),
            "price": entry_price,
            "initial_stop": initial_stop,
            "quantity": quantity,
            "initial_risk_rub": round(risk_per_lot * quantity, 2),
            "phase": protection.phase,
        })

    def _advance_position(
        self,
        symbol: str,
        position: dict[str, Any],
        price: float,
        frame_30m: pd.DataFrame,
        now: pd.Timestamp,
    ) -> None:
        candidate = V35Candidate(**dict(position["candidate"]))
        previous = ProtectionState(**dict(position["protection"]))
        opened_at = pd.Timestamp(position["opened_at"])
        eligible_30m = (
            frame_30m.loc[frame_30m["closed_at"] > opened_at].reset_index(drop=True)
            if not frame_30m.empty and "closed_at" in frame_30m
            else pd.DataFrame()
        )
        updated = advance_on_price_tick(
            previous,
            candidate,
            float(price),
            eligible_30m,
            float(position["tick_size"]),
        )
        position["protection"] = asdict(updated)
        position["last_price"] = float(price)
        position["last_observed_at"] = now.isoformat()
        if updated.phase != previous.phase:
            self._event({
                "event": "PHASE",
                "candidate_id": position["candidate_id"],
                "symbol": symbol,
                "direction": candidate.direction,
                "event_time": now.isoformat(),
                "price": float(price),
                "phase": updated.phase,
                "stop": updated.current_stop,
            })
        if updated.current_stop != previous.current_stop:
            self._event({
                "event": "STOP_MOVED",
                "candidate_id": position["candidate_id"],
                "symbol": symbol,
                "direction": candidate.direction,
                "event_time": now.isoformat(),
                "price": float(price),
                "phase": updated.phase,
                "previous_stop": previous.current_stop,
                "stop": updated.current_stop,
            })
        if not updated.exit_reason:
            return
        exit_price = float(updated.exit_price or price)
        sign = 1.0 if candidate.direction == "LONG" else -1.0
        quantity = int(position["quantity"])
        point_value = float(position["point_value"])
        gross = (exit_price - updated.entry_price) * sign * point_value * quantity
        commission = (
            abs(updated.entry_price * point_value) + abs(exit_price * point_value)
        ) * COMMISSION_RATE_PER_SIDE * quantity
        net = gross - commission
        initial_risk = float(position["initial_risk_rub"])
        self.state["closed_trades"] = int(self.state.get("closed_trades", 0)) + 1
        self.state["realized_net_pnl_rub"] = round(
            float(self.state.get("realized_net_pnl_rub", 0.0)) + net, 2
        )
        self._event({
            "event": "CLOSE",
            "candidate_id": position["candidate_id"],
            "symbol": symbol,
            "direction": candidate.direction,
            "event_time": now.isoformat(),
            "entry_price": updated.entry_price,
            "exit_price": exit_price,
            "quantity": quantity,
            "gross_pnl_rub": round(gross, 2),
            "commission_rub": round(commission, 2),
            "net_pnl_rub": round(net, 2),
            "net_r_multiple": round(net / initial_risk, 4) if initial_risk > 0 else 0.0,
            "exit_reason": updated.exit_reason,
            "final_phase": updated.phase,
            "final_stop": updated.current_stop,
            "mfe_r": round(updated.best_move / updated.initial_distance, 4),
            "mae_r": round(updated.worst_move / updated.initial_distance, 4),
        })
        self.state["positions"].pop(symbol, None)

    def advance(
        self,
        prices: dict[str, float],
        instrument_meta: dict[str, dict[str, float]],
        *,
        now: pd.Timestamp | None = None,
    ) -> dict[str, Any]:
        moment = pd.Timestamp(now or datetime.now(timezone.utc))
        if moment.tzinfo is None:
            moment = moment.tz_localize("UTC")
        for row in self._fresh_candidates(moment):
            signal_time = pd.Timestamp(row["signal_time"])
            if moment - signal_time > pd.Timedelta(minutes=MAX_ENTRY_AGE_MINUTES):
                self._skip(row, "STALE_CANDIDATE", moment)
                continue
            symbol = str(row.get("symbol") or "")
            price = float(prices.get(symbol) or 0.0)
            meta = instrument_meta.get(symbol) or {}
            if price <= 0 or not meta:
                continue
            self._open_candidate(row, price, meta, moment)

        frames = self._management_frames()
        for symbol, position in list(self.state["positions"].items()):
            price = float(prices.get(symbol) or 0.0)
            if price <= 0:
                continue
            self._advance_position(
                symbol,
                position,
                price,
                frames.get(symbol, pd.DataFrame()),
                moment,
            )
        self._save(moment)
        return {
            "active_positions": len(self.state["positions"]),
            "opened_trades": int(self.state.get("opened_trades", 0)),
            "closed_trades": int(self.state.get("closed_trades", 0)),
            "skipped_candidates": int(self.state.get("skipped_candidates", 0)),
            "realized_net_pnl_rub": float(self.state.get("realized_net_pnl_rub", 0.0)),
            "order_submission_enabled": ORDER_SUBMISSION_ENABLED,
        }
