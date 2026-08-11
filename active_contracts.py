from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ACTIVE_CONTRACTS_PATH = BASE_DIR / "_active_contracts.json"
UTC = timezone.utc


def get_active_contracts_path() -> Path:
    raw = os.getenv("OIL_ACTIVE_CONTRACTS_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_ACTIVE_CONTRACTS_PATH


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _normalize_symbol(value: str | None) -> str:
    return str(value or "").strip().upper()


def _read_payload() -> dict[str, Any]:
    path = get_active_contracts_path()
    if not path.exists():
        return {"contracts": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("contracts"), list):
            return payload
    except Exception:
        pass
    return {"contracts": []}


def _write_payload(payload: dict[str, Any]) -> None:
    path = get_active_contracts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def list_active_contracts() -> list[dict[str, Any]]:
    contracts = _read_payload().get("contracts") or []
    result: list[dict[str, Any]] = []
    for item in contracts:
        if not isinstance(item, dict):
            continue
        template_symbol = _normalize_symbol(item.get("template_symbol"))
        active_symbol = _normalize_symbol(item.get("active_symbol"))
        if not template_symbol:
            continue
        disabled = bool(item.get("disabled"))
        result.append(
            {
                "template_symbol": template_symbol,
                "active_symbol": active_symbol,
                "disabled": disabled,
                "updated_at": item.get("updated_at") or "",
            }
        )
    result.sort(key=lambda item: item["template_symbol"])
    return result


def get_active_contract_symbol(template_symbol: str) -> str | None:
    normalized_template = _normalize_symbol(template_symbol)
    contracts = list_active_contracts()
    current_symbol = normalized_template
    visited: set[str] = set()
    while current_symbol and current_symbol not in visited:
        visited.add(current_symbol)
        item = next(
            (row for row in contracts if row["template_symbol"] == current_symbol),
            None,
        )
        if item is None:
            return current_symbol
        if item["disabled"]:
            return None
        next_symbol = item["active_symbol"] or current_symbol
        if next_symbol == current_symbol:
            return current_symbol
        current_symbol = next_symbol
    return current_symbol or normalized_template


def get_instrument_history_symbol(symbol: str) -> str:
    """Return the current contract identity used for cross-roll analytics."""
    normalized_symbol = _normalize_symbol(symbol)
    return get_active_contract_symbol(normalized_symbol) or normalized_symbol


def get_active_contract_template(symbol: str) -> str | None:
    normalized_symbol = _normalize_symbol(symbol)
    for item in list_active_contracts():
        if item["disabled"]:
            continue
        if item["active_symbol"] == normalized_symbol:
            return item["template_symbol"]
    return None


def replace_with_active_symbols(base_symbols: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for symbol in base_symbols:
        normalized = _normalize_symbol(symbol)
        if not normalized:
            continue
        active_symbol = get_active_contract_symbol(normalized)
        if not active_symbol:
            continue
        if active_symbol in seen:
            continue
        seen.add(active_symbol)
        result.append(active_symbol)
    return result


def upsert_active_contract(template_symbol: str, active_symbol: str | None, *, disabled: bool = False) -> dict[str, Any]:
    normalized_template = _normalize_symbol(template_symbol)
    normalized_active = _normalize_symbol(active_symbol)
    payload = _read_payload()
    contracts = payload.setdefault("contracts", [])
    now_iso = _utc_now_iso()

    for item in contracts:
        if _normalize_symbol(item.get("template_symbol")) != normalized_template:
            continue
        previous_active = _normalize_symbol(item.get("active_symbol"))
        item["active_symbol"] = normalized_active
        item["disabled"] = bool(disabled)
        item["updated_at"] = now_iso

        if previous_active and normalized_active and previous_active != normalized_active:
            for related_item in contracts:
                if related_item is item:
                    continue
                if _normalize_symbol(related_item.get("active_symbol")) != previous_active:
                    continue
                related_item["active_symbol"] = normalized_active
                related_item["updated_at"] = now_iso

            has_previous_alias = any(
                _normalize_symbol(related_item.get("template_symbol")) == previous_active
                for related_item in contracts
            )
            if previous_active != normalized_template and not has_previous_alias:
                contracts.append(
                    {
                        "template_symbol": previous_active,
                        "active_symbol": normalized_active,
                        "disabled": False,
                        "updated_at": now_iso,
                    }
                )
        _write_payload(payload)
        return {
            "status": "updated",
            "template_symbol": normalized_template,
            "active_symbol": normalized_active,
            "disabled": bool(disabled),
            "updated_at": now_iso,
        }

    contracts.append(
        {
            "template_symbol": normalized_template,
            "active_symbol": normalized_active,
            "disabled": bool(disabled),
            "updated_at": now_iso,
        }
    )
    _write_payload(payload)
    return {
        "status": "added",
        "template_symbol": normalized_template,
        "active_symbol": normalized_active,
        "disabled": bool(disabled),
        "updated_at": now_iso,
    }
