from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from news_rules import CHANNEL_RULES, COMMON_BLOCK_TERMS, MOEX_DERIVATIVES, NEWS_RULES


UTC = timezone.utc


@dataclass(frozen=True)
class NewsMessage:
    channel: str
    text: str
    created_at: datetime
    message_id: int | None = None


@dataclass(frozen=True)
class NewsBias:
    symbol: str
    bias: str
    strength: str
    source: str
    reason: str
    expires_at: datetime
    score: int
    message_text: str = ""


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


def classify_strength(score: int, source_weight: int) -> str:
    weighted = score * source_weight
    if weighted >= 6:
        return "HIGH"
    if weighted >= 3:
        return "MEDIUM"
    return "LOW"


def detect_news_bias(message: NewsMessage) -> list[NewsBias]:
    text = normalize_text(message.text)
    channel_rule = CHANNEL_RULES.get(message.channel)
    if channel_rule is None:
        return []

    results: list[NewsBias] = []
    for rule in NEWS_RULES:
        keyword_hits = [word for word in rule.keywords if word in text]
        if not keyword_hits:
            continue

        long_hits = [word for word in rule.long_terms if word in text]
        short_hits = [word for word in rule.short_terms if word in text]
        block_hits = [word for word in (rule.block_terms or COMMON_BLOCK_TERMS) if word in text]

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

        strength = classify_strength(score, channel_rule.source_weight)
        ttl = timedelta(minutes=channel_rule.default_ttl_minutes)
        results.append(
            NewsBias(
                symbol=rule.symbol,
                bias=bias,
                strength=strength,
                source=message.channel,
                reason=build_reason(bias, keyword_hits, long_hits, short_hits, block_hits),
                expires_at=message.created_at.astimezone(UTC) + ttl,
                score=score * channel_rule.source_weight,
                message_text=message.text,
            )
        )

    return results


def select_active_biases(biases: list[NewsBias], now: datetime | None = None) -> dict[str, NewsBias]:
    now = now or datetime.now(UTC)
    active: dict[str, NewsBias] = {}

    for item in biases:
        if item.expires_at <= now:
            continue
        current = active.get(item.symbol)
        if current is None or item.score > current.score:
            active[item.symbol] = item
        elif current is not None and item.score == current.score and item.source == MOEX_DERIVATIVES:
            active[item.symbol] = item

    return active
