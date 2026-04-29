from dataclasses import dataclass


MARKETTWITS = "markettwits"
MOEX_DERIVATIVES = "moex_derivatives"
MARKETSNAPSHOT = "marketsnapshot"


@dataclass(frozen=True)
class NewsRule:
    symbol: str
    keywords: tuple[str, ...]
    long_terms: tuple[str, ...]
    short_terms: tuple[str, ...]
    block_terms: tuple[str, ...] = ()
    priority: int = 1


@dataclass(frozen=True)
class ChannelRule:
    channel: str
    source_weight: int
    default_ttl_minutes: int
    can_block_entries: bool


CHANNEL_RULES: dict[str, ChannelRule] = {
    MARKETTWITS: ChannelRule(
        channel=MARKETTWITS,
        source_weight=1,
        default_ttl_minutes=45,
        can_block_entries=False,
    ),
    MARKETSNAPSHOT: ChannelRule(
        channel=MARKETSNAPSHOT,
        source_weight=2,
        default_ttl_minutes=60,
        can_block_entries=False,
    ),
    MOEX_DERIVATIVES: ChannelRule(
        channel=MOEX_DERIVATIVES,
        source_weight=3,
        default_ttl_minutes=90,
        can_block_entries=True,
    ),
}


COMMON_LONG_TERMS = (
    "рост",
    "вырос",
    "выросла",
    "поддержка",
    "позитив",
    "позитивно",
    "сильный спрос",
    "дефицит",
    "бычий",
    "ускорение вверх",
    "обновил максимум",
    "дивиденды",
    "снижение ставки",
    "эскалация",
    "риск перебоев поставок",
)

COMMON_SHORT_TERMS = (
    "падение",
    "снижение",
    "негатив",
    "негативно",
    "слабость",
    "давление",
    "санкции",
    "снижение спроса",
    "медвежий",
    "ускорение вниз",
    "обновил минимум",
    "рост ставки",
    "деэскалация",
)

COMMON_BLOCK_TERMS = (
    "приостановка",
    "изменение режима торгов",
    "изменение параметров риска",
    "повышение гарантийного обеспечения",
    "ограничение",
    "неторговый",
    "дискретный аукцион",
)


NEWS_RULES: tuple[NewsRule, ...] = (
    NewsRule(
        symbol="BRK6",
        keywords=(
            "нефть",
            "brent",
            "brent crude",
            "ice brent",
            "марка brent",
            "цена нефти",
            "рынок нефти",
            "opec",
            "опек",
            "опек+",
            "запасы нефти",
            "экспорт нефти",
            "$103",
            "$104",
            "$105",
        ),
        long_terms=COMMON_LONG_TERMS + ("сокращение добычи", "рост нефти", "дефицит нефти", "танкеры", "иран", "хуситы", "красное море", "ближний восток", "израиль", "военная операция", "удары", "превысила", "превысил", "выше"),
        short_terms=COMMON_SHORT_TERMS + ("рост запасов", "увеличение добычи", "избыток нефти", "добыча", "снизилась до", "ниже"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=2,
    ),
    NewsRule(
        symbol="NGJ6",
        keywords=("природный газ", "natural gas", "natgas", "lng", "спг", "ttf", "поставки газа", "рынок газа", "газовые хранилища"),
        long_terms=COMMON_LONG_TERMS + ("холод", "жара", "дефицит газа", "снижение запасов", "европа", "погода", "превысила", "превысил"),
        short_terms=COMMON_SHORT_TERMS + ("тёплая погода", "рост запасов газа", "слабый спрос", "европа", "погода", "снизилась до", "ниже"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=2,
    ),
    NewsRule(
        symbol="NGK6",
        keywords=("природный газ", "natural gas", "natgas", "lng", "спг", "ttf", "поставки газа", "рынок газа", "газовые хранилища"),
        long_terms=COMMON_LONG_TERMS + ("холод", "жара", "дефицит газа", "снижение запасов", "европа", "погода", "превысила", "превысил"),
        short_terms=COMMON_SHORT_TERMS + ("тёплая погода", "рост запасов газа", "слабый спрос", "европа", "погода", "снизилась до", "ниже"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=2,
    ),
    NewsRule(
        symbol="GNM6",
        keywords=(
            "золото",
            "gold",
            "xau",
            "gold futures",
            "safe haven",
            "защитный актив",
            "фрс",
            "серебро",
        ),
        long_terms=COMMON_LONG_TERMS + ("risk-off", "слабый доллар", "мягкая фрс", "геополитика", "геополитический риск", "иран", "израиль", "ближний восток", "хуситы", "военная операция", "превысило", "превысил"),
        short_terms=COMMON_SHORT_TERMS + ("сильный доллар", "рост доходностей", "доходности", "risk-on", "снизилось до", "ниже"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=2,
    ),
    NewsRule(
        symbol="USDRUBF",
        keywords=("usd/rub", "usdrub", "доллар/рубль", "доллар к рублю", "курс доллара", "курс рубля", "валютная пара usd/rub"),
        long_terms=COMMON_LONG_TERMS + ("ослабление рубля", "рост доллара", "спрос на валюту"),
        short_terms=COMMON_SHORT_TERMS + ("укрепление рубля", "продажа валютной выручки", "экспортеры", "цб", "минфин", "ставка", "санкции"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=3,
    ),
    NewsRule(
        symbol="CNYRUBF",
        keywords=("cny/rub", "cnyrub", "юань/рубль", "юань к рублю", "курс юаня", "курс cny", "китайская валюта"),
        long_terms=COMMON_LONG_TERMS + ("ослабление рубля", "рост юаня", "спрос на юань", "китай", "расчеты", "превысил", "превысила", "выше"),
        short_terms=COMMON_SHORT_TERMS + ("укрепление рубля", "слабость юаня", "китай", "расчеты", "снизился до", "ниже"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=3,
    ),
    NewsRule(
        symbol="IMOEXF",
        keywords=("imoex", "индекс мосбиржи", "индекс мосбиржи imoex", "московская биржа индекс", "индекс российского рынка"),
        long_terms=COMMON_LONG_TERMS + ("дивиденд", "позитив по рынку", "рост рынка", "рынок рф", "российский рынок"),
        short_terms=COMMON_SHORT_TERMS + ("негатив по рынку", "геополитический риск", "давление на рынок", "рынок рф", "российский рынок"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=2,
    ),
    NewsRule(
        symbol="SRM6",
        keywords=("сбер", "sber", "сбербанк", "sberbank", "акции сбера", "акции сбербанка"),
        long_terms=COMMON_LONG_TERMS + ("сильная отчетность", "дивиденды сбера", "рост прибыли", "банковский сектор", "кредитование"),
        short_terms=COMMON_SHORT_TERMS + ("санкции на банки", "слабая отчетность", "давление на банковский сектор", "банковский сектор", "кредитование"),
        block_terms=COMMON_BLOCK_TERMS,
        priority=2,
    ),
)
