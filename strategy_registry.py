from instrument_groups import GROUP_BY_SYMBOL, get_symbol_template

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
    template_symbol = get_symbol_template(symbol)
    if template_symbol in PRIMARY_STRATEGIES_BY_SYMBOL:
        return PRIMARY_STRATEGIES_BY_SYMBOL[template_symbol]
    return ["reversal_15m"]


def get_secondary_strategies(symbol: str) -> list[str]:
    template_symbol = get_symbol_template(symbol)
    if template_symbol != str(symbol or "").strip().upper():
        return get_secondary_strategies(template_symbol)
    return []
