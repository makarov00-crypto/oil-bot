from instrument_groups import GROUP_BY_SYMBOL, get_symbol_template
from strategies.ao_chaikin_1h import STRATEGY_NAME

PRIMARY_STRATEGIES_BY_GROUP = {
    "commodities": [STRATEGY_NAME],
    "fx": [STRATEGY_NAME],
    "equity_index": [STRATEGY_NAME],
    "equity_futures": [STRATEGY_NAME],
    "bond_index": [STRATEGY_NAME],
}


SECONDARY_STRATEGIES_BY_GROUP = {
}


def get_primary_strategies(symbol: str) -> list[str]:
    template_symbol = get_symbol_template(symbol)
    group = GROUP_BY_SYMBOL.get(template_symbol)
    if group is not None:
        return PRIMARY_STRATEGIES_BY_GROUP.get(group.name, [STRATEGY_NAME])
    return [STRATEGY_NAME]


def get_secondary_strategies(symbol: str) -> list[str]:
    template_symbol = get_symbol_template(symbol)
    if template_symbol != str(symbol or "").strip().upper():
        return get_secondary_strategies(template_symbol)
    return []
