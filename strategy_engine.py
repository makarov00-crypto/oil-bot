from strategy_registry import get_primary_strategies
from strategies.breakdown_continuation import evaluate_signal as evaluate_breakdown_continuation
from strategies.compression_breakout import evaluate_signal as evaluate_compression_breakout
from strategies.failed_breakout import evaluate_signal as evaluate_failed_breakout
from strategies.momentum_breakout import evaluate_signal as evaluate_momentum_breakout
from strategies.opening_range_breakout import evaluate_signal as evaluate_opening_range_breakout
from strategies.trend_pullback import evaluate_signal as evaluate_trend_pullback


PRIMARY_EVALUATORS = {
    "trend_pullback": evaluate_trend_pullback,
    "compression_breakout": evaluate_compression_breakout,
    "opening_range_breakout": evaluate_opening_range_breakout,
    "range_break_continuation": evaluate_breakdown_continuation,
    "breakdown_continuation": evaluate_breakdown_continuation,
    "failed_breakout": evaluate_failed_breakout,
    "momentum_breakout": evaluate_momentum_breakout,
}


def evaluate_primary_signal_bundle(df, config, instrument, higher_tf_bias: str) -> tuple[str, str, str]:
    strategy_names = get_primary_strategies(instrument.symbol)
    hold_reasons: list[str] = []

    for strategy_name in strategy_names:
        evaluator = PRIMARY_EVALUATORS[strategy_name]
        signal, reason = evaluator(df, config, instrument, higher_tf_bias)
        if signal != "HOLD":
            return signal, reason, strategy_name
        hold_reasons.append(f"{strategy_name}: {reason}")

    combined_reason = " | ".join(hold_reasons) if hold_reasons else "Нет доступных стратегий для инструмента."
    return "HOLD", combined_reason, strategy_names[0] if strategy_names else "trend_pullback"
