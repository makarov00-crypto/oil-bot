from dataclasses import dataclass


@dataclass(frozen=True)
class InstrumentGroup:
    name: str
    description: str


COMMODITIES = InstrumentGroup(name="commodities", description="Commodities and energy futures")
FX = InstrumentGroup(name="fx", description="Currency futures")
EQUITY_INDEX = InstrumentGroup(name="equity_index", description="Equity index futures")
EQUITY_FUTURES = InstrumentGroup(name="equity_futures", description="Single-stock futures")


GROUP_BY_SYMBOL = {
    "BRK6": COMMODITIES,
    "NGJ6": COMMODITIES,
    "GNM6": COMMODITIES,
    "USDRUBF": FX,
    "CNYRUBF": FX,
    "IMOEXF": EQUITY_INDEX,
    "SRM6": EQUITY_FUTURES,
}


DEFAULT_SYMBOLS = ",".join(GROUP_BY_SYMBOL.keys())


def get_instrument_group(symbol: str) -> InstrumentGroup:
    return GROUP_BY_SYMBOL.get(symbol, COMMODITIES)


def is_currency_instrument(symbol: str) -> bool:
    return get_instrument_group(symbol).name == FX.name


def uses_pullback_trend_regime(symbol: str) -> bool:
    return get_instrument_group(symbol).name == COMMODITIES.name
