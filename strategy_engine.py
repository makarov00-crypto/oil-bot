from strategy_registry import get_primary_strategies
from strategies.reversal_1h import evaluate_signal as evaluate_reversal_1h
from strategies.reversal_15m import evaluate_signal as evaluate_reversal_15m


PRIMARY_EVALUATORS = {
    "reversal_15m": evaluate_reversal_15m,
    "reversal_1h": evaluate_reversal_1h,
}


def evaluate_primary_signal_bundle(df, config, instrument, higher_tf_bias: str) -> tuple[str, str, str]:
    strategy_names = get_primary_strategies(instrument.symbol)
    hold_reasons: list[str] = []

    for strategy_name in strategy_names:
        evaluator = PRIMARY_EVALUATORS.get(strategy_name)
        if evaluator is None:
            hold_reasons.append(f"{strategy_name}: стратегия больше не поддерживается в живом контуре")
            continue
        signal, reason = evaluator(df, config, instrument, higher_tf_bias)
        if signal != "HOLD":
            return signal, reason, strategy_name
        hold_reasons.append(f"{strategy_name}: {reason}")

    combined_reason = " | ".join(hold_reasons) if hold_reasons else "Нет доступных стратегий для инструмента."
    return "HOLD", combined_reason, strategy_names[0] if strategy_names else "reversal_15m"
