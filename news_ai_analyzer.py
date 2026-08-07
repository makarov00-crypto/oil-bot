from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests

from news_bias import NewsBias


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_AI_API_BASE_URL = "https://api.openai.com/v1"
DEFAULT_AI_API_MODE = "responses"

SYSTEM_INSTRUCTIONS = """Ты новостной аналитик для фьючерсного бота на Мосбирже.

Твоя задача: прочитать уже отфильтрованные новости и вернуть строгий торговый смысл по каждому инструменту.
Не придумывай факты. Не используй внешние данные. Оценивай только переданные новости.

Правила:
- Если новость не даёт понятного торгового направления, ставь direction = NEUTRAL.
- Если новость скорее запрещает новый вход из-за режима торгов/риска, ставь direction = BLOCK.
- Не давай совет "купить/продать"; возвращай только структуру сигнала.
- Учитывай тип источника: Telegram быстрее, брокерская аналитика надёжнее, официальный источник самый надёжный.
- Ответ должен быть только JSON по заданной схеме.
"""

NEWS_AI_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "signals": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "symbol": {"type": "string"},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT", "BLOCK", "NEUTRAL"]},
                    "strength": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "horizon": {"type": "string", "enum": ["NOW", "INTRADAY", "BACKGROUND"]},
                    "event_type": {"type": "string"},
                    "reason": {"type": "string"},
                    "risk": {"type": "string"},
                },
                "required": [
                    "symbol",
                    "direction",
                    "strength",
                    "confidence",
                    "horizon",
                    "event_type",
                    "reason",
                    "risk",
                ],
            },
        }
    },
    "required": ["signals"],
}


@dataclass(frozen=True)
class NewsAiSignal:
    symbol: str
    direction: str
    strength: str
    confidence: float
    horizon: str
    event_type: str
    reason: str
    risk: str


def extract_output_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
        return payload["output_text"].strip()

    texts: list[str] = []
    for output_item in payload.get("output", []):
        if output_item.get("type") != "message":
            continue
        for content in output_item.get("content", []):
            if content.get("type") == "output_text":
                text = str(content.get("text") or "").strip()
                if text:
                    texts.append(text)
    return "\n\n".join(texts).strip()


def extract_chat_completion_text(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        return ""
    return str(message.get("content") or "").strip()


def get_news_ai_api_mode() -> str:
    mode = os.getenv("OIL_AI_API_MODE", DEFAULT_AI_API_MODE).strip().lower()
    if mode not in {"responses", "chat_completions"}:
        raise ValueError(f"Неподдерживаемый режим AI API: {mode}")
    return mode


def get_news_ai_api_url(api_mode: str) -> str:
    base_url = os.getenv("OIL_AI_API_BASE_URL", DEFAULT_AI_API_BASE_URL).strip() or DEFAULT_AI_API_BASE_URL
    endpoint = "responses" if api_mode == "responses" else "chat/completions"
    return f"{base_url.rstrip('/')}/{endpoint}"


def build_news_ai_prompt(items: Iterable[NewsBias]) -> str:
    rows: list[dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                "symbol": item.symbol,
                "category": item.category,
                "rule_direction": item.bias,
                "rule_strength": item.strength,
                "rule_reason": item.reason,
                "summary": item.summary,
                "horizon": item.horizon,
                "actionability": item.actionability,
                "source": item.source_label or item.source,
                "source_type": item.source_type,
                "source_speed": item.source_speed,
                "source_reliability": item.source_reliability,
                "confirming_sources": list(item.confirming_sources),
                "topics": list(item.topics),
                "text": item.message_text[:1200],
            }
        )
    return (
        "Проанализируй новости ниже и верни структурный вывод по каждому инструменту.\n"
        "Важно: если данных мало или новость двусмысленная, снижай confidence и ставь NEUTRAL.\n\n"
        f"{json.dumps(rows, ensure_ascii=False, indent=2)}"
    )


def parse_ai_signals(payload: dict[str, Any]) -> list[NewsAiSignal]:
    signals = payload.get("signals")
    if not isinstance(signals, list):
        return []

    parsed: list[NewsAiSignal] = []
    for item in signals:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            continue
        try:
            confidence = max(0.0, min(1.0, float(item.get("confidence") or 0.0)))
        except (TypeError, ValueError):
            confidence = 0.0
        parsed.append(
            NewsAiSignal(
                symbol=symbol,
                direction=str(item.get("direction") or "NEUTRAL").upper(),
                strength=str(item.get("strength") or "LOW").upper(),
                confidence=confidence,
                horizon=str(item.get("horizon") or "BACKGROUND").upper(),
                event_type=str(item.get("event_type") or "").strip(),
                reason=str(item.get("reason") or "").strip(),
                risk=str(item.get("risk") or "").strip(),
            )
        )
    return parsed


def request_news_ai_signals(
    api_key: str,
    model: str,
    items: Iterable[NewsBias],
    timeout: int = 90,
    attempts: int = 2,
) -> list[NewsAiSignal]:
    prompt = build_news_ai_prompt(items)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    api_mode = get_news_ai_api_mode()
    if api_mode == "chat_completions":
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_INSTRUCTIONS},
                {"role": "user", "content": prompt},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "news_ai_signals", "schema": NEWS_AI_SCHEMA, "strict": True},
            },
        }
    else:
        payload = {
            "model": model,
            "instructions": SYSTEM_INSTRUCTIONS,
            "input": prompt,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "news_ai_signals",
                    "schema": NEWS_AI_SCHEMA,
                    "strict": True,
                }
            },
        }
    response = None
    for attempt in range(max(1, attempts)):
        response = requests.post(get_news_ai_api_url(api_mode), headers=headers, json=payload, timeout=timeout)
        try:
            response.raise_for_status()
            break
        except requests.HTTPError:
            if response.status_code < 500 or attempt + 1 >= max(1, attempts):
                raise
            time.sleep(1.5 * (attempt + 1))
    if response is None:
        return []
    text = extract_chat_completion_text(response.json()) if api_mode == "chat_completions" else extract_output_text(response.json())
    if not text:
        return []
    return parse_ai_signals(json.loads(text))
