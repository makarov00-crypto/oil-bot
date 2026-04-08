from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CUSTOM_INSTRUMENTS_PATH = BASE_DIR / "bot_state" / "_custom_instruments.json"
UTC = timezone.utc
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9._-]{2,24}$")


def get_custom_instruments_path() -> Path:
    raw = os.getenv("OIL_CUSTOM_INSTRUMENTS_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_CUSTOM_INSTRUMENTS_PATH


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _read_payload() -> dict[str, Any]:
    path = get_custom_instruments_path()
    if not path.exists():
        return {"instruments": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("instruments"), list):
            return payload
    except Exception:
        pass
    return {"instruments": []}


def _write_payload(payload: dict[str, Any]) -> None:
    path = get_custom_instruments_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def list_custom_instruments() -> list[dict[str, Any]]:
    instruments = _read_payload().get("instruments") or []
    result: list[dict[str, Any]] = []
    for item in instruments:
        if not isinstance(item, dict):
            continue
        symbol = _normalize_symbol(item.get("symbol", ""))
        template_symbol = _normalize_symbol(item.get("template_symbol", "")) or _normalize_symbol(item.get("clone_from", ""))
        clone_from = _normalize_symbol(item.get("clone_from", "")) or template_symbol
        if not symbol or not template_symbol:
            continue
        result.append(
            {
                "symbol": symbol,
                "clone_from": clone_from,
                "template_symbol": template_symbol,
                "added_at": item.get("added_at") or "",
                "updated_at": item.get("updated_at") or "",
            }
        )
    result.sort(key=lambda item: item["symbol"])
    return result


def get_custom_instrument_map() -> dict[str, dict[str, Any]]:
    return {item["symbol"]: item for item in list_custom_instruments()}


def get_custom_clone_source(symbol: str) -> str | None:
    item = get_custom_instrument_map().get(_normalize_symbol(symbol))
    if not item:
        return None
    return item.get("template_symbol") or item.get("clone_from")


def merge_with_custom_symbols(base_symbols: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for symbol in list(base_symbols) + [item["symbol"] for item in list_custom_instruments()]:
        normalized = _normalize_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append(normalized)
    return merged


def validate_custom_symbol(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    if not SYMBOL_PATTERN.match(normalized):
        raise ValueError("Тикер должен содержать только латиницу, цифры, точку, дефис или подчёркивание.")
    return normalized


def upsert_custom_instrument(symbol: str, clone_from: str, template_symbol: str | None = None) -> dict[str, Any]:
    normalized_symbol = validate_custom_symbol(symbol)
    normalized_clone_from = validate_custom_symbol(clone_from)
    normalized_template = validate_custom_symbol(template_symbol or normalized_clone_from)
    payload = _read_payload()
    instruments = payload.setdefault("instruments", [])
    now_iso = _utc_now_iso()

    for item in instruments:
        if _normalize_symbol(item.get("symbol", "")) != normalized_symbol:
            continue
        item["clone_from"] = normalized_clone_from
        item["template_symbol"] = normalized_template
        item["updated_at"] = now_iso
        _write_payload(payload)
        return {
            "status": "updated",
            "symbol": normalized_symbol,
            "clone_from": normalized_clone_from,
            "template_symbol": normalized_template,
            "updated_at": now_iso,
        }

    instruments.append(
        {
            "symbol": normalized_symbol,
            "clone_from": normalized_clone_from,
            "template_symbol": normalized_template,
            "added_at": now_iso,
            "updated_at": now_iso,
        }
    )
    _write_payload(payload)
    return {
        "status": "added",
        "symbol": normalized_symbol,
        "clone_from": normalized_clone_from,
        "template_symbol": normalized_template,
        "added_at": now_iso,
        "updated_at": now_iso,
    }
