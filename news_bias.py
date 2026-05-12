import re
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
    score: int
    message_text: str = ""
    message_url: str = ""
    topics: tuple[str, ...] = ()


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


def classify_strength(score: int, source_weight: int) -> str:
    weighted = score * source_weight
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


def classify_actionability(bias: str, strength: str, horizon: str) -> str:
    if bias == "BLOCK":
        return "BLOCK"
    if bias in {"LONG", "SHORT"} and horizon == "NOW" and strength in {"MEDIUM", "HIGH"}:
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

        strength = classify_strength(score, channel_rule.source_weight)
        horizon = classify_horizon(text, bias, strength, message.channel, block_hits)
        ttl = timedelta(minutes=channel_rule.default_ttl_minutes)
        results.append(
            NewsBias(
                symbol=rule.symbol,
                category=rule.category,
                bias=bias,
                strength=strength,
                source=message.channel,
                reason=build_reason(bias, keyword_hits, long_hits, short_hits, block_hits),
                summary=build_summary(
                    rule.symbol,
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
                ),
                expires_at=message.created_at.astimezone(UTC) + ttl,
                score=score * channel_rule.source_weight,
                message_text=message.text,
                message_url=message.url,
                topics=tuple(keyword_hits[:3]),
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
