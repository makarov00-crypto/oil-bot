from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

import requests

from news_ai_analyzer import extract_chat_completion_text, extract_output_text
from signal_ai_reviewer import get_ai_api_mode, get_ai_api_url, get_signal_ai_model


REGIME_SYSTEM_INSTRUCTIONS = """Ты независимый классификатор режима рынка для фьючерсной стратегии.
Используй только переданные закрытые свечи и индикаторы. Сначала изучи развитие цены
за последние 2-3 торговых дня на 1ч, затем проверь 4ч и 30м. Не повторяй готовые
оценки торгового бота: их во входных данных нет. Определи один режим:
TREND_LONG, TREND_SHORT, CHOP или TRANSITION. Отдельно определи зрелость движения:
EARLY, MIDDLE, EXHAUSTED или NONE. Пила означает возвраты цены в один диапазон,
частые смены направления AO, переплетение средних и отсутствие чистого продвижения
относительно ATR. Тренд требует последовательного продвижения цены и устойчивого
импульса. При неполных или устаревших данных верни data_quality=INCOMPLETE.
Ответ должен строго соответствовать JSON-схеме."""


ENTRY_SYSTEM_INSTRUCTIONS = """Ты независимый контролёр входа стратегии AO_CANDLE_MTF_V1.
Стратегия ищет начало свежей волны на закрытых свечах 1ч: недавнее пересечение
AO(5,34) через ноль, усиление AO, 2 направленные свечи с достаточным телом,
RSI выше 50 и растёт для LONG либо ниже 50 и падает для SHORT. 30м и 15м
используются для проверки текущего момента. Chaikin и MACD подтверждают качество,
но не заменяют цену и AO.

Оцени ровно предложенный вход. Особенно проверяй, не прошла ли основная часть
движения, не уменьшаются ли тела свечей, не затухает ли AO и не началась ли пила.
Положительный AO сам по себе не разрешает поздний LONG; отрицательный AO сам по
себе не разрешает поздний SHORT. Оценка entry_score_pct означает обоснованность
того, что цена сначала пройдёт не менее 0.6 ATR в направлении входа, прежде чем
уйдёт на 0.4 ATR против него, в течение следующих четырёх часов.

Рубрика: соответствие режиму 25, свежесть волны 25, свечной импульс 20,
структура AO 15, согласованность таймфреймов 10, RSI/Chaikin/MACD/объём 5.
При уверенной пиле итог не выше 40; при истощённой волне не выше 35;
при сильном противоположном тренде не выше 30. Не используй будущие данные.
Ответ должен строго соответствовать JSON-схеме."""


REGIME_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "regime": {"type": "string", "enum": ["TREND_LONG", "TREND_SHORT", "CHOP", "TRANSITION"]},
        "regime_confidence_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "trend_maturity": {"type": "string", "enum": ["EARLY", "MIDDLE", "EXHAUSTED", "NONE"]},
        "chop_probability_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "evidence": {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 6},
        "data_quality": {"type": "string", "enum": ["COMPLETE", "INCOMPLETE"]},
    },
    "required": ["regime", "regime_confidence_pct", "trend_maturity", "chop_probability_pct", "evidence", "data_quality"],
}


ENTRY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "candidate_direction": {"type": "string", "enum": ["LONG", "SHORT"]},
        "entry_score_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "decision": {"type": "string", "enum": ["ALLOW", "BLOCK", "ABSTAIN"]},
        "late_entry_risk_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "timeframe_alignment": {"type": "string", "enum": ["ALIGNED", "MIXED", "CONFLICT"]},
        "evidence": {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 7},
        "invalidation": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5},
        "data_quality": {"type": "string", "enum": ["COMPLETE", "INCOMPLETE"]},
    },
    "required": ["candidate_direction", "entry_score_pct", "decision", "late_entry_risk_pct", "timeframe_alignment", "evidence", "invalidation", "data_quality"],
}


PROMPT_VERSION = hashlib.sha256(
    (REGIME_SYSTEM_INSTRUCTIONS + ENTRY_SYSTEM_INSTRUCTIONS + json.dumps(REGIME_SCHEMA, sort_keys=True) + json.dumps(ENTRY_SCHEMA, sort_keys=True)).encode()
).hexdigest()[:12]


@dataclass(frozen=True)
class RegimeReview:
    regime: str
    regime_confidence_pct: int
    trend_maturity: str
    chop_probability_pct: int
    evidence: list[str]
    data_quality: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EntryReview:
    candidate_direction: str
    entry_score_pct: int
    decision: str
    late_entry_risk_pct: int
    timeframe_alignment: str
    evidence: list[str]
    invalidation: list[str]
    data_quality: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_regime_prompt(context: dict[str, Any]) -> str:
    return "Определи режим рынка по полному контексту.\n\n" + json.dumps(context, ensure_ascii=False, separators=(",", ":"))


def build_entry_prompt(context: dict[str, Any], regime: RegimeReview | dict[str, Any]) -> str:
    regime_payload = regime.as_dict() if isinstance(regime, RegimeReview) else dict(regime)
    payload = {"regime_review": regime_payload, "entry_context": context}
    return "Оцени обоснованность только предложенного входа.\n\n" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _request_json(api_key: str, instructions: str, prompt: str, schema_name: str, schema: dict[str, Any], timeout: int) -> dict[str, Any]:
    mode = get_ai_api_mode()
    if mode == "chat_completions":
        payload: dict[str, Any] = {
            "model": get_signal_ai_model(),
            "messages": [{"role": "system", "content": instructions}, {"role": "user", "content": prompt}],
            "response_format": {"type": "json_schema", "json_schema": {"name": schema_name, "schema": schema, "strict": True}},
        }
    else:
        payload = {
            "model": get_signal_ai_model(),
            "instructions": instructions,
            "input": prompt,
            "text": {"format": {"type": "json_schema", "name": schema_name, "schema": schema, "strict": True}},
        }
    response = requests.post(
        get_ai_api_url(mode),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    body = response.json()
    text = extract_chat_completion_text(body) if mode == "chat_completions" else extract_output_text(body)
    if not text:
        raise RuntimeError("ИИ вернул пустой структурированный ответ")
    return json.loads(text)


def request_regime_review(api_key: str, context: dict[str, Any], *, timeout: int = 90) -> RegimeReview:
    item = _request_json(api_key, REGIME_SYSTEM_INSTRUCTIONS, build_regime_prompt(context), "market_regime_review", REGIME_SCHEMA, timeout)
    return RegimeReview(
        regime=str(item["regime"]),
        regime_confidence_pct=int(item["regime_confidence_pct"]),
        trend_maturity=str(item["trend_maturity"]),
        chop_probability_pct=int(item["chop_probability_pct"]),
        evidence=[str(value) for value in item["evidence"]],
        data_quality=str(item["data_quality"]),
    )


def request_entry_review(api_key: str, context: dict[str, Any], regime: RegimeReview | dict[str, Any], *, timeout: int = 90) -> EntryReview:
    item = _request_json(api_key, ENTRY_SYSTEM_INSTRUCTIONS, build_entry_prompt(context, regime), "strategy_entry_review", ENTRY_SCHEMA, timeout)
    return EntryReview(
        candidate_direction=str(item["candidate_direction"]),
        entry_score_pct=int(item["entry_score_pct"]),
        decision=str(item["decision"]),
        late_entry_risk_pct=int(item["late_entry_risk_pct"]),
        timeframe_alignment=str(item["timeframe_alignment"]),
        evidence=[str(value) for value in item["evidence"]],
        invalidation=[str(value) for value in item["invalidation"]],
        data_quality=str(item["data_quality"]),
    )
