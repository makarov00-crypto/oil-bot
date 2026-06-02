from dataclasses import dataclass

from active_contracts import get_active_contract_template
from custom_instruments import get_custom_clone_source


@dataclass(frozen=True)
class InstrumentGroup:
    name: str
    description: str


COMMODITIES = InstrumentGroup(name="commodities", description="Commodities and energy futures")
FX = InstrumentGroup(name="fx", description="Currency futures")
EQUITY_INDEX = InstrumentGroup(name="equity_index", description="Equity index futures")
EQUITY_FUTURES = InstrumentGroup(name="equity_futures", description="Single-stock futures")
BOND_INDEX = InstrumentGroup(name="bond_index", description="Government bond index futures")


BRENT_SYMBOLS = {"BRK6", "BMM6"}
NATURAL_GAS_TEMPLATE_SYMBOLS = {"NGJ6", "NGK6"}
UNIFIED_REVERSAL_15M_SYMBOLS = {
    "USDRUBF",
    "CNYRUBF",
    "UCM6",
}
UNIFIED_REVERSAL_1H_SYMBOLS = {
    "BRK6",
    "BMM6",
    "GNM6",
    "NGJ6",
    "NGK6",
    "RNM6",
    "IMOEXF",
    "SRM6",
    "VBM6",
    "RBM6",
}

GROUP_BY_SYMBOL = {
    "BRK6": COMMODITIES,
    "BMM6": COMMODITIES,
    "NGJ6": COMMODITIES,
    "NGK6": COMMODITIES,
    "GNM6": COMMODITIES,
    "USDRUBF": FX,
    "CNYRUBF": FX,
    "UCM6": FX,
    "IMOEXF": EQUITY_INDEX,
    "RNM6": EQUITY_FUTURES,
    "SRM6": EQUITY_FUTURES,
    "VBM6": EQUITY_FUTURES,
    "RBM6": BOND_INDEX,
}

DEFAULT_SYMBOLS = ",".join(
    [
        "BMM6",
        "NGK6",
        "GNM6",
        "USDRUBF",
        "CNYRUBF",
        "UCM6",
        "IMOEXF",
        "RNM6",
        "SRM6",
        "VBM6",
    ]
)


def get_symbol_template(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if normalized in GROUP_BY_SYMBOL:
        return normalized
    template_symbol = get_custom_clone_source(normalized) or get_active_contract_template(normalized)
    return template_symbol or normalized


def get_instrument_group(symbol: str) -> InstrumentGroup:
    template_symbol = get_symbol_template(symbol)
    return GROUP_BY_SYMBOL.get(template_symbol, COMMODITIES)


def is_currency_instrument(symbol: str) -> bool:
    return get_instrument_group(symbol).name == FX.name


def is_brent_symbol(symbol: str) -> bool:
    return get_symbol_template(symbol) in BRENT_SYMBOLS


def uses_unified_reversal_15m(symbol: str) -> bool:
    return get_symbol_template(symbol) in UNIFIED_REVERSAL_15M_SYMBOLS


def uses_unified_reversal_1h(symbol: str) -> bool:
    return get_symbol_template(symbol) in UNIFIED_REVERSAL_1H_SYMBOLS


def uses_unified_reversal(symbol: str) -> bool:
    template = get_symbol_template(symbol)
    return template in UNIFIED_REVERSAL_15M_SYMBOLS or template in UNIFIED_REVERSAL_1H_SYMBOLS


def is_natural_gas_symbol(symbol: str) -> bool:
    return get_symbol_template(symbol) in NATURAL_GAS_TEMPLATE_SYMBOLS


def uses_pullback_trend_regime(symbol: str) -> bool:
    return get_instrument_group(symbol).name == COMMODITIES.name
