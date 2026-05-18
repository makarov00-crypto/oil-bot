from custom_instruments import get_custom_clone_source
from instrument_groups import GROUP_BY_SYMBOL

PRIMARY_STRATEGIES_BY_SYMBOL = {
    symbol: ["reversal_15m"]
    for symbol in GROUP_BY_SYMBOL
}

PRIMARY_STRATEGIES_BY_GROUP = {
    "commodities": ["reversal_15m"],
    "fx": ["reversal_15m"],
    "equity_index": ["reversal_15m"],
    "equity_futures": ["reversal_15m"],
    "bond_index": ["reversal_15m"],
}


SECONDARY_STRATEGIES_BY_GROUP = {
}


def get_primary_strategies(symbol: str) -> list[str]:
    normalized = str(symbol or "").strip().upper()
    if normalized in PRIMARY_STRATEGIES_BY_SYMBOL:
        return PRIMARY_STRATEGIES_BY_SYMBOL[normalized]
    template_symbol = get_custom_clone_source(normalized)
    if template_symbol:
        return get_primary_strategies(template_symbol)
    return ["reversal_15m"]


def get_secondary_strategies(symbol: str) -> list[str]:
    normalized = str(symbol or "").strip().upper()
    template_symbol = get_custom_clone_source(normalized)
    if template_symbol:
        return get_secondary_strategies(template_symbol)
    return []
