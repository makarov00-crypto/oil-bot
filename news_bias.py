import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from active_contracts import get_active_contract_symbol
from news_rules import CHANNEL_RULES, COMMON_BLOCK_TERMS, MOEX_DERIVATIVES, NEWS_RULES


UTC = timezone.utc


@dataclass(frozen=True)
class NewsMessage:
    channel: str
    text: str
    created_at: datetime
    message_id: int | None = None
    url: str = ""


@dataclass(frozen=True)
class NewsBias:
    symbol: str
    category: str
    bias: str
    strength: str
    source: str
    reason: str
    summary: str
    horizon: str
    actionability: str
    expires_at: datetime
    score: float
    message_text: str = ""
    message_url: str = ""
    topics: tuple[str, ...] = ()
    source_speed: float = 0.0
    source_reliability: float = 0.0
    source_type: str = ""
    source_label: str = ""
    source_count: int = 1
    confirming_sources: tuple[str, ...] = ()
    ai_direction: str = ""
    ai_strength: str = ""
    ai_confidence: float = 0.0
    ai_horizon: str = ""
    ai_event_type: str = ""
    ai_reason: str = ""
    ai_risk: str = ""


IMMEDIATE_TERMS = (
    "сейчас",
    "сегодня",
    "срочно",
    "только что",
    "оперативно",
    "немедленно",
    "внепланово",
    "экстренно",
)

BACKDROP_TERMS = (
    "ожидания",
    "ожидается",
    "может",
    "вероятно",
    "прогноз",
    "прогнозы",
    "оценка",
    "перспектива",
)


def build_reason(
    bias: str,
    keyword_hits: list[str],
    long_hits: list[str],
    short_hits: list[str],
    block_hits: list[str],
) -> str:
    topics = ", ".join(keyword_hits[:3])
    if bias == "BLOCK":
        blocks = ", ".join(block_hits[:2])
        return f"Биржевое или риск-событие: {blocks}. Темы: {topics}." if blocks else f"Риск-событие по теме: {topics}."
    if bias == "LONG":
        positives = ", ".join(long_hits[:2])
        return f"Позитивный контекст: {positives}. Темы: {topics}." if positives else f"Позитивный новостной контекст. Темы: {topics}."
    if bias == "SHORT":
        negatives = ", ".join(short_hits[:2])
        return f"Негативный контекст: {negatives}. Темы: {topics}." if negatives else f"Негативный новостной контекст. Темы: {topics}."
    return f"Нейтральный новостной фон. Темы: {topics}."


def normalize_text(text: str) -> str:
    return " ".join((text or "").lower().replace("\n", " ").split())


def phrase_in_text(text: str, phrase: str) -> bool:
    normalized_phrase = normalize_text(phrase)
    if not normalized_phrase:
        return False
    if any(sep in normalized_phrase for sep in (" ", "/", "-", ".")):
        return normalized_phrase in text
    pattern = rf"(?<![\w]){re.escape(normalized_phrase)}(?![\w])"
    return re.search(pattern, text) is not None


def collect_hits(text: str, phrases: tuple[str, ...]) -> list[str]:
    hits: list[str] = []
    for phrase in phrases:
        if phrase_in_text(text, phrase):
            hits.append(phrase)
    return hits


def source_quality_multiplier(speed_score: float, reliability_score: float) -> float:
    return 0.55 * speed_score + 0.45 * reliability_score


def classify_strength(score: int, source_weight: int, speed_score: float, reliability_score: float) -> str:
    weighted = score * source_weight * source_quality_multiplier(speed_score, reliability_score)
    if weighted >= 6:
        return "HIGH"
    if weighted >= 3:
        return "MEDIUM"
    return "LOW"


def classify_horizon(
    text: str,
    bias: str,
    strength: str,
    source: str,
    block_hits: list[str],
) -> str:
    if bias == "BLOCK" or block_hits:
        return "NOW"
    if source == MOEX_DERIVATIVES:
        return "NOW"
    if collect_hits(text, IMMEDIATE_TERMS):
        return "NOW"
    if strength == "HIGH":
        return "INTRADAY"
    if collect_hits(text, BACKDROP_TERMS):
        return "BACKGROUND"
    return "INTRADAY" if bias in {"LONG", "SHORT"} else "BACKGROUND"


def classify_actionability(bias: str, strength: str, horizon: str, source_speed: float) -> str:
    if bias == "BLOCK":
        return "BLOCK"
    if bias in {"LONG", "SHORT"} and horizon == "NOW" and strength in {"MEDIUM", "HIGH"}:
        return "ACTION"
    if bias in {"LONG", "SHORT"} and source_speed >= 0.85 and strength == "HIGH":
        return "ACTION"
    if bias in {"LONG", "SHORT"} and horizon == "INTRADAY":
        return "WATCH"
    return "BACKGROUND"


def build_summary(
    symbol: str,
    category: str,
    bias: str,
    strength: str,
    keyword_hits: list[str],
    long_hits: list[str],
    short_hits: list[str],
    block_hits: list[str],
) -> str:
    topics = keyword_hits[:2]
    if bias == "BLOCK":
        block_text = ", ".join(block_hits[:2]) if block_hits else "биржевое ограничение"
        return f"{symbol}: риск-стоп по теме {block_text}"
    if bias == "LONG":
        driver = ", ".join(long_hits[:2]) if long_hits else ", ".join(topics) or category
        return f"{symbol}: фон в лонг ({driver})"
    if bias == "SHORT":
        driver = ", ".join(short_hits[:2]) if short_hits else ", ".join(topics) or category
        return f"{symbol}: фон в шорт ({driver})"
    neutral_topic = ", ".join(topics) or category
    return f"{symbol}: новостной фон без явного сигнала ({neutral_topic})"


def detect_news_bias(message: NewsMessage) -> list[NewsBias]:
    text = normalize_text(message.text)
    channel_rule = CHANNEL_RULES.get(message.channel)
    if channel_rule is None:
        return []

    results: list[NewsBias] = []
    for rule in NEWS_RULES:
        keyword_hits = collect_hits(text, rule.keywords)
        if not keyword_hits:
            continue

        long_hits = collect_hits(text, rule.long_terms)
        short_hits = collect_hits(text, rule.short_terms)
        block_hits = collect_hits(text, rule.block_terms or COMMON_BLOCK_TERMS)

        bias = "NEUTRAL"
        score = len(keyword_hits)
        if channel_rule.can_block_entries and block_hits:
            bias = "BLOCK"
            score += len(block_hits) + rule.priority
        elif len(long_hits) > len(short_hits) and long_hits:
            bias = "LONG"
            score += len(long_hits) + rule.priority
        elif len(short_hits) > len(long_hits) and short_hits:
            bias = "SHORT"
            score += len(short_hits) + rule.priority
        elif long_hits or short_hits:
            # Conflicted message: keep it neutral but preserve low-strength context.
            bias = "NEUTRAL"
            score += max(len(long_hits), len(short_hits))
        else:
            continue

        strength = classify_strength(
            score,
            channel_rule.source_weight,
            channel_rule.speed_score,
            channel_rule.reliability_score,
        )
        horizon = classify_horizon(text, bias, strength, message.channel, block_hits)
        ttl = timedelta(minutes=channel_rule.default_ttl_minutes)
        target_symbol = get_active_contract_symbol(rule.symbol) or rule.symbol
        quality_multiplier = source_quality_multiplier(channel_rule.speed_score, channel_rule.reliability_score)
        weighted_score = round(score * channel_rule.source_weight * quality_multiplier, 2)
        results.append(
            NewsBias(
                symbol=target_symbol,
                category=rule.category,
                bias=bias,
                strength=strength,
                source=message.channel,
                reason=build_reason(bias, keyword_hits, long_hits, short_hits, block_hits),
                summary=build_summary(
                    target_symbol,
                    rule.category,
                    bias,
                    strength,
                    keyword_hits,
                    long_hits,
                    short_hits,
                    block_hits,
                ),
                horizon=horizon,
                actionability=classify_actionability(
                    bias,
                    strength,
                    horizon,
                    channel_rule.speed_score,
                ),
                expires_at=message.created_at.astimezone(UTC) + ttl,
                score=weighted_score,
                message_text=message.text,
                message_url=message.url,
                topics=tuple(keyword_hits[:3]),
                source_speed=channel_rule.speed_score,
                source_reliability=channel_rule.reliability_score,
                source_type=channel_rule.source_type,
                source_label=channel_rule.display_name,
                confirming_sources=(channel_rule.display_name,),
            )
        )

    return results


def strength_rank(value: str) -> int:
    return {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get((value or "").upper(), 0)


def actionability_rank(value: str) -> int:
    return {"BACKGROUND": 1, "WATCH": 2, "ACTION": 3, "BLOCK": 4}.get((value or "").upper(), 0)


def merge_confirming_biases(primary: NewsBias, secondary: NewsBias) -> NewsBias:
    source_names = tuple(dict.fromkeys((*primary.confirming_sources, *secondary.confirming_sources)))
    stronger_strength = primary.strength
    if strength_rank(secondary.strength) > strength_rank(primary.strength):
        stronger_strength = secondary.strength

    stronger_actionability = primary.actionability
    if actionability_rank(secondary.actionability) > actionability_rank(primary.actionability):
        stronger_actionability = secondary.actionability

    lead = primary if primary.score >= secondary.score else secondary
    support = secondary if lead is primary else primary
    reason = lead.reason
    if len(source_names) > 1:
        reason = f"{lead.reason} Подтверждено источниками: {', '.join(source_names[:4])}."

    return replace(
        lead,
        strength=stronger_strength,
        actionability=stronger_actionability,
        reason=reason,
        summary=lead.summary or support.summary,
        expires_at=max(primary.expires_at, secondary.expires_at),
        score=round(max(primary.score, secondary.score) + min(primary.score, secondary.score) * 0.35, 2),
        source_count=len(source_names),
        confirming_sources=source_names,
    )


def select_active_biases(biases: list[NewsBias], now: datetime | None = None) -> dict[str, NewsBias]:
    now = now or datetime.now(UTC)
    active: dict[str, NewsBias] = {}

    for item in biases:
        if item.expires_at <= now:
            continue
        current = active.get(item.symbol)
        if current is None:
            active[item.symbol] = item
        elif item.bias == current.bias and item.bias in {"LONG", "SHORT", "BLOCK"}:
            active[item.symbol] = merge_confirming_biases(current, item)
        elif item.score > current.score:
            active[item.symbol] = item
        elif current is not None and item.score == current.score and item.source == MOEX_DERIVATIVES:
            active[item.symbol] = item

    return active
