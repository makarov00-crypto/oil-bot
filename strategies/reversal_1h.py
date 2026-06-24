from strategies.reversal_core import evaluate_signal_core


def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    return evaluate_signal_core(
        df,
        config,
        instrument,
        higher_tf_bias,
        timeframe_minutes=60,
        strategy_label="reversal_1h",
    )
