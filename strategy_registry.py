from instrument_groups import GROUP_BY_SYMBOL, get_symbol_template

PRIMARY_STRATEGIES_BY_GROUP = {
    "commodities": ["reversal_1h"],
    "fx": ["reversal_15m"],
    "equity_index": ["reversal_1h"],
    "equity_futures": ["reversal_1h"],
    "bond_index": ["reversal_1h"],
}


SECONDARY_STRATEGIES_BY_GROUP = {
}


def get_primary_strategies(symbol: str) -> list[str]:
    template_symbol = get_symbol_template(symbol)
    group = GROUP_BY_SYMBOL.get(template_symbol)
    if group is not None:
        return PRIMARY_STRATEGIES_BY_GROUP.get(group.name, ["reversal_15m"])
    return ["reversal_15m"]


def get_secondary_strategies(symbol: str) -> list[str]:
    template_symbol = get_symbol_template(symbol)
    if template_symbol != str(symbol or "").strip().upper():
        return get_secondary_strategies(template_symbol)
    return []
