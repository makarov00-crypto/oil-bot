from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

import requests

from news_ai_analyzer import extract_chat_completion_text, extract_output_text
from signal_ai_reviewer import get_ai_api_mode, get_ai_api_url, get_signal_ai_model


REGIME_SYSTEM_INSTRUCTIONS = """Ты независимый классификатор режима рынка для фьючерсной стратегии.
Используй только переданные закрытые свечи, индикаторы и рассчитанные числовые признаки.
Разделяй четыре разных понятия: структура 4ч, активная волна 1ч, текущая фаза 30м и
текстура рынка. Старший тренд является контекстом, но не имеет права автоматически
запрещать свежий разворот на 1ч/30м. Сначала изучи 2-3 торговых дня на 1ч, затем
сопоставь их с 4ч и 30м.

Пила определяется измеримо: низкая эффективность направленного движения, частые
пересечения AO через ноль, частые возвраты цены через EMA20, смешанные свечи и
сохранение цены внутри одного диапазона. Тренд требует чистого продвижения цены,
последовательности максимумов/минимумов и устойчивого импульса. TRANSITION означает
реальную смену фазы, а не универсальный ответ при любом конфликте таймфреймов.
При неполных или устаревших данных верни data_quality=INCOMPLETE. Ответ должен строго
соответствовать JSON-схеме."""


ENTRY_SYSTEM_INSTRUCTIONS = """Ты независимый контролёр входа стратегии AO_CANDLE_MTF_V2.
Стратегия допускает два типа входа на закрытых свечах 1ч:
1) EARLY_REVERSAL: AO ещё может находиться по старую сторону нуля, но минимум две
свечи подряд меняется в сторону кандидата, цена подтверждает разворот двумя
направленными свечами, RSI разворачивается. Пересечение нуля здесь не требуется.
2) ZERO_CROSS: недавнее пересечение AO(5,34) через ноль, усиление AO и две
направленные свечи.

Поздний CONTINUATION запрещён и не должен поступать кандидатом. Если основная часть
движения уже прошла, выбери BLOCK_LATE или WAIT_PULLBACK.

Поле bars_since_ao_zero_cross=-1 означает отсутствие недавнего пересечения и
является нормальным для EARLY_REVERSAL. Не штрафуй вход только за
это значение. Основные условия всех путей: свежий наклон AO, две направленные
свечи с достаточным телом, ограниченное расстояние от начала локальной волны и
RSI, движущийся в сторону кандидата. 30м и 15м используются для проверки текущего
момента. Chaikin и MACD подтверждают качество, но не заменяют цену и AO.

Оцени ровно предложенный вход и фактический денежный сценарий стратегии: первоначальный
стоп 0.80 ATR; после движения 0.20 ATR стоп переводится в настоящий безубыток с
компенсацией комиссий входа и выхода; прибыль удерживается до истощения импульса на
30м или обратного пересечения AO. Комиссия равна 0.025% на каждую сторону.
entry_score_pct означает вероятность положительного NET результата именно при этих
правилах, а не вероятность любого краткого движения в сторону входа.

Особенно проверяй, не прошла ли основная часть движения, не уменьшаются ли тела свечей,
не затухает ли AO и не началась ли пила. Положительный AO сам по себе не разрешает
LONG; отрицательный AO сам по себе не разрешает SHORT. Если направление выглядит
верным, но момент плохой, выбирай WAIT_PULLBACK, а не ENTER.

Рубрика: качество активной волны 25, свежесть момента 25, свечной импульс 15,
структура AO 15, измеримая текстура рынка 10, согласованность таймфреймов 5,
RSI/Chaikin/MACD/объём 5. Метка EXHAUSTED для старого движения не запрещает свежий
EARLY_REVERSAL против него. Сильный 4ч тренд уменьшает оценку контртрендового входа,
но не блокирует его, если на 1ч и 30м подтверждён новый импульс.

Решение обязано соответствовать баллу: ENTER при 60-100; WAIT_PULLBACK или один из
BLOCK при 0-59; ABSTAIN только при неполных данных. Не используй будущие данные.
Ответ должен строго соответствовать JSON-схеме."""


REGIME_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "regime": {"type": "string", "enum": ["TREND_LONG", "TREND_SHORT", "CHOP", "TRANSITION"]},
        "structure_4h": {"type": "string", "enum": ["LONG", "SHORT", "MIXED"]},
        "swing_1h": {"type": "string", "enum": ["LONG", "SHORT", "RANGE"]},
        "phase_30m": {"type": "string", "enum": ["ACCELERATING_LONG", "ACCELERATING_SHORT", "PULLBACK", "EXHAUSTING", "RANGE"]},
        "texture": {"type": "string", "enum": ["TREND", "CHOP", "TRANSITION"]},
        "regime_confidence_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "trend_maturity": {"type": "string", "enum": ["EARLY", "MIDDLE", "EXHAUSTED", "NONE"]},
        "chop_probability_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "evidence": {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 6},
        "data_quality": {"type": "string", "enum": ["COMPLETE", "INCOMPLETE"]},
    },
    "required": ["regime", "structure_4h", "swing_1h", "phase_30m", "texture", "regime_confidence_pct", "trend_maturity", "chop_probability_pct", "evidence", "data_quality"],
}


ENTRY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "candidate_direction": {"type": "string", "enum": ["LONG", "SHORT"]},
        "entry_score_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "decision": {"type": "string", "enum": ["ENTER", "WAIT_PULLBACK", "BLOCK_CHOP", "BLOCK_LATE", "BLOCK_AGAINST_STRUCTURE", "ABSTAIN"]},
        "expected_outcome": {"type": "string", "enum": ["PROFIT", "BREAKEVEN", "LOSS", "UNCERTAIN"]},
        "setup_phase": {"type": "string", "enum": ["EARLY", "ON_TIME", "LATE"]},
        "late_entry_risk_pct": {"type": "integer", "minimum": 0, "maximum": 100},
        "timeframe_alignment": {"type": "string", "enum": ["ALIGNED", "MIXED", "CONFLICT"]},
        "evidence": {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 7},
        "invalidation": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5},
        "data_quality": {"type": "string", "enum": ["COMPLETE", "INCOMPLETE"]},
    },
    "required": ["candidate_direction", "entry_score_pct", "decision", "expected_outcome", "setup_phase", "late_entry_risk_pct", "timeframe_alignment", "evidence", "invalidation", "data_quality"],
}


PROMPT_VERSION = hashlib.sha256(
    ("embedded-contract-v2" + REGIME_SYSTEM_INSTRUCTIONS + ENTRY_SYSTEM_INSTRUCTIONS + json.dumps(REGIME_SCHEMA, sort_keys=True) + json.dumps(ENTRY_SCHEMA, sort_keys=True)).encode()
).hexdigest()[:12]


@dataclass(frozen=True)
class RegimeReview:
    regime: str
    structure_4h: str
    swing_1h: str
    phase_30m: str
    texture: str
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
    expected_outcome: str
    setup_phase: str
    late_entry_risk_pct: int
    timeframe_alignment: str
    evidence: list[str]
    invalidation: list[str]
    data_quality: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_regime_prompt(context: dict[str, Any]) -> str:
    contract = {
        "regime": "TREND_LONG|TREND_SHORT|CHOP|TRANSITION",
        "structure_4h": "LONG|SHORT|MIXED",
        "swing_1h": "LONG|SHORT|RANGE",
        "phase_30m": "ACCELERATING_LONG|ACCELERATING_SHORT|PULLBACK|EXHAUSTING|RANGE",
        "texture": "TREND|CHOP|TRANSITION",
        "regime_confidence_pct": "integer 0..100",
        "trend_maturity": "EARLY|MIDDLE|EXHAUSTED|NONE",
        "chop_probability_pct": "integer 0..100",
        "evidence": ["2..6 strings"],
        "data_quality": "COMPLETE|INCOMPLETE",
    }
    return (
        "Определи режим рынка по полному контексту. Верни только JSON ровно с указанными ключами и значениями enum.\n\n"
        + json.dumps({"required_output_contract": contract, "market_context": context}, ensure_ascii=False, separators=(",", ":"))
    )


def build_entry_prompt(context: dict[str, Any], regime: RegimeReview | dict[str, Any]) -> str:
    regime_payload = regime.as_dict() if isinstance(regime, RegimeReview) else dict(regime)
    contract = {
        "candidate_direction": "LONG|SHORT",
        "entry_score_pct": "integer 0..100",
        "decision": "ENTER|WAIT_PULLBACK|BLOCK_CHOP|BLOCK_LATE|BLOCK_AGAINST_STRUCTURE|ABSTAIN",
        "expected_outcome": "PROFIT|BREAKEVEN|LOSS|UNCERTAIN",
        "setup_phase": "EARLY|ON_TIME|LATE",
        "late_entry_risk_pct": "integer 0..100",
        "timeframe_alignment": "ALIGNED|MIXED|CONFLICT",
        "evidence": ["2..7 strings"],
        "invalidation": ["1..5 strings"],
        "data_quality": "COMPLETE|INCOMPLETE",
    }
    payload = {"required_output_contract": contract, "regime_review": regime_payload, "entry_context": context}
    return "Оцени обоснованность только предложенного входа. Верни только JSON ровно с указанными ключами и значениями enum.\n\n" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


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
        structure_4h=str(item["structure_4h"]),
        swing_1h=str(item["swing_1h"]),
        phase_30m=str(item["phase_30m"]),
        texture=str(item["texture"]),
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
        expected_outcome=str(item["expected_outcome"]),
        setup_phase=str(item["setup_phase"]),
        late_entry_risk_pct=int(item["late_entry_risk_pct"]),
        timeframe_alignment=str(item["timeframe_alignment"]),
        evidence=[str(value) for value in item["evidence"]],
        invalidation=[str(value) for value in item["invalidation"]],
        data_quality=str(item["data_quality"]),
    )
