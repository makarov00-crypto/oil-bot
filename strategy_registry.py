from instrument_groups import get_instrument_group

PRIMARY_STRATEGIES_BY_SYMBOL = {
    "BRK6": ["momentum_breakout", "trend_pullback"],
    "GNM6": ["trend_rollover", "momentum_breakout", "trend_pullback"],
    "NGJ6": ["momentum_breakout", "trend_pullback"],
    "SRM6": ["range_break_continuation", "trend_pullback"],
}

PRIMARY_STRATEGIES_BY_GROUP = {
    "commodities": ["trend_pullback"],
    "fx": ["opening_range_breakout", "trend_pullback"],
    "equity_index": ["range_break_continuation", "failed_breakout", "trend_pullback"],
    "equity_futures": ["range_break_continuation", "failed_breakout", "trend_pullback"],
}


SECONDARY_STRATEGIES_BY_GROUP = {
    "fx": ["williams"],
}


def get_primary_strategies(symbol: str) -> list[str]:
    if symbol in PRIMARY_STRATEGIES_BY_SYMBOL:
        return PRIMARY_STRATEGIES_BY_SYMBOL[symbol]
    return PRIMARY_STRATEGIES_BY_GROUP.get(get_instrument_group(symbol).name, ["trend_pullback"])


def get_secondary_strategies(symbol: str) -> list[str]:
    return SECONDARY_STRATEGIES_BY_GROUP.get(get_instrument_group(symbol).name, [])
