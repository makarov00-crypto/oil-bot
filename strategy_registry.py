from custom_instruments import get_custom_clone_source
from instrument_groups import get_instrument_group

PRIMARY_STRATEGIES_BY_SYMBOL = {
    "BRK6": ["momentum_breakout", "trend_rollover", "trend_pullback"],
    "CNYRUBF": ["momentum_breakout", "opening_range_breakout", "range_break_continuation", "trend_pullback"],
    "GNM6": ["trend_pullback", "momentum_breakout", "trend_rollover"],
    "IMOEXF": ["range_break_continuation", "failed_breakout", "trend_pullback"],
    "NGJ6": ["trend_pullback", "momentum_breakout"],
    "NGK6": ["trend_pullback", "momentum_breakout"],
    "RBM6": ["failed_breakout", "range_break_continuation", "trend_pullback"],
    "SRM6": ["range_break_continuation", "failed_breakout", "trend_pullback"],
    "UCM6": ["trend_pullback", "range_break_continuation", "opening_range_breakout"],
    "USDRUBF": ["momentum_breakout", "opening_range_breakout", "range_break_continuation", "trend_pullback"],
}

PRIMARY_STRATEGIES_BY_GROUP = {
    "commodities": ["trend_pullback"],
    "fx": ["opening_range_breakout", "range_break_continuation", "trend_pullback"],
    "equity_index": ["range_break_continuation", "failed_breakout", "trend_pullback"],
    "equity_futures": ["range_break_continuation", "failed_breakout", "trend_pullback"],
    "bond_index": ["failed_breakout", "range_break_continuation", "trend_pullback"],
}


SECONDARY_STRATEGIES_BY_GROUP = {
    "fx": ["williams"],
}


def get_primary_strategies(symbol: str) -> list[str]:
    normalized = str(symbol or "").strip().upper()
    if normalized in PRIMARY_STRATEGIES_BY_SYMBOL:
        return PRIMARY_STRATEGIES_BY_SYMBOL[normalized]
    template_symbol = get_custom_clone_source(normalized)
    if template_symbol:
        return get_primary_strategies(template_symbol)
    return PRIMARY_STRATEGIES_BY_GROUP.get(get_instrument_group(normalized).name, ["trend_pullback"])


def get_secondary_strategies(symbol: str) -> list[str]:
    normalized = str(symbol or "").strip().upper()
    template_symbol = get_custom_clone_source(normalized)
    if template_symbol:
        return get_secondary_strategies(template_symbol)
    return SECONDARY_STRATEGIES_BY_GROUP.get(get_instrument_group(normalized).name, [])
