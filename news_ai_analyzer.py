from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

import requests

from news_bias import NewsBias


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

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


def request_news_ai_signals(api_key: str, model: str, items: Iterable[NewsBias], timeout: int = 90) -> list[NewsAiSignal]:
    prompt = build_news_ai_prompt(items)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
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
    response = requests.post(OPENAI_RESPONSES_URL, headers=headers, json=payload, timeout=timeout)
    response.raise_for_status()
    text = extract_output_text(response.json())
    if not text:
        return []
    return parse_ai_signals(json.loads(text))
