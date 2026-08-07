from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any, Iterable

import requests

from news_ai_analyzer import extract_chat_completion_text, extract_output_text


DEFAULT_AI_API_BASE_URL = "https://api.openai.com/v1"
DEFAULT_AI_API_MODE = "responses"

SYSTEM_INSTRUCTIONS = """Ты второй, теневой аналитический слой фьючерсного торгового бота.

Оценивай только переданный структурированный контекст. Не придумывай факты и не
используй внешние данные. Ты не управляешь сделками: результат нужен для сравнения
с работающей стратегией. При конфликте или нехватке данных выбирай ABSTAIN.

Оцени, стоит ли поддержать вход, удержание или выход. Учитывай направление MACD и
AO, RSI, объём, волатильность, режим рынка, новости и уже открытую позицию.
Используй только русские значения из схемы.
"""

SIGNAL_AI_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "reviews": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "symbol": {"type": "string"},
                    "action": {"type": "string", "enum": ["ВХОД", "УДЕРЖИВАТЬ", "ВЫЙТИ", "ПЕРЕВОРОТ", "ВОЗДЕРЖАТЬСЯ"]},
                    "direction": {"type": "string", "enum": ["ЛОНГ", "ШОРТ", "НЕЙТРАЛЬНО"]},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "reason": {"type": "string"},
                    "risk_note": {"type": "string"},
                },
                "required": ["symbol", "action", "direction", "confidence", "reason", "risk_note"],
            },
        }
    },
    "required": ["reviews"],
}


@dataclass(frozen=True)
class SignalAiReview:
    symbol: str
    action: str
    direction: str
    confidence: float
    reason: str
    risk_note: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def get_signal_ai_model() -> str:
    return os.getenv("OIL_SIGNAL_AI_MODEL", "").strip() or os.getenv("OIL_AI_MODEL", "").strip() or "gpt-4.1-mini"


def get_ai_api_mode() -> str:
    mode = os.getenv("OIL_AI_API_MODE", DEFAULT_AI_API_MODE).strip().lower()
    if mode not in {"responses", "chat_completions"}:
        raise ValueError(f"Неподдерживаемый режим AI API: {mode}")
    return mode


def get_ai_api_url(mode: str) -> str:
    base_url = os.getenv("OIL_AI_API_BASE_URL", DEFAULT_AI_API_BASE_URL).strip() or DEFAULT_AI_API_BASE_URL
    endpoint = "responses" if mode == "responses" else "chat/completions"
    return f"{base_url.rstrip('/')}/{endpoint}"


def build_signal_ai_prompt(candidates: Iterable[dict[str, Any]]) -> str:
    items: list[dict[str, Any]] = []
    for candidate in candidates:
        context = candidate.get("shadow_ai_context") if isinstance(candidate.get("shadow_ai_context"), dict) else {}
        items.append(
            {
                "symbol": str(candidate.get("symbol") or "").upper(),
                "strategy_signal": str(candidate.get("signal") or "").upper(),
                "strategy": str(candidate.get("strategy_name") or ""),
                "entry_reason": str(candidate.get("reason") or "")[:900],
                "priority_score": round(float(candidate.get("priority_score") or 0.0), 3),
                "entry_edge_score": round(float(candidate.get("entry_edge_score") or 0.0), 3),
                "market_regime": str(candidate.get("market_regime") or ""),
                "setup_quality": str(candidate.get("setup_quality_label") or ""),
                "context": context,
            }
        )
    return (
        "Верни один объект reviews для каждого кандидата. ВХОД означает, что ты поддерживаешь "
        "вход текущей стратегии; ВОЗДЕРЖАТЬСЯ означает, что преимущества недостаточно.\n\n"
        + json.dumps(items, ensure_ascii=False, separators=(",", ":"))
    )


def parse_signal_ai_reviews(payload: dict[str, Any]) -> dict[str, SignalAiReview]:
    rows = payload.get("reviews")
    if not isinstance(rows, list):
        return {}
    parsed: dict[str, SignalAiReview] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        try:
            confidence = max(0.0, min(1.0, float(item.get("confidence") or 0.0)))
        except (TypeError, ValueError):
            confidence = 0.0
        parsed[symbol] = SignalAiReview(
            symbol=symbol,
            action=str(item.get("action") or "ВОЗДЕРЖАТЬСЯ").upper(),
            direction=str(item.get("direction") or "НЕЙТРАЛЬНО").upper(),
            confidence=confidence,
            reason=str(item.get("reason") or "").strip(),
            risk_note=str(item.get("risk_note") or "").strip(),
        )
    return parsed


def request_signal_ai_reviews(
    api_key: str,
    candidates: Iterable[dict[str, Any]],
    *,
    timeout: int = 45,
) -> dict[str, SignalAiReview]:
    mode = get_ai_api_mode()
    model = get_signal_ai_model()
    prompt = build_signal_ai_prompt(candidates)
    if mode == "chat_completions":
        payload: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_INSTRUCTIONS},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_schema", "json_schema": {"name": "signal_ai_review", "schema": SIGNAL_AI_SCHEMA, "strict": True}},
        }
    else:
        payload = {
            "model": model,
            "instructions": SYSTEM_INSTRUCTIONS,
            "input": prompt,
            "text": {"format": {"type": "json_schema", "name": "signal_ai_review", "schema": SIGNAL_AI_SCHEMA, "strict": True}},
        }
    response = requests.post(
        get_ai_api_url(mode),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    text = extract_chat_completion_text(response.json()) if mode == "chat_completions" else extract_output_text(response.json())
    if not text:
        raise RuntimeError("ИИ не вернул теневой разбор сигналов")
    return parse_signal_ai_reviews(json.loads(text))
