from strategies.reversal_15m import evaluate_signal_core


def evaluate_signal(df, config, instrument, higher_tf_bias: str) -> tuple[str, str]:
    return evaluate_signal_core(
        df,
        config,
        instrument,
        higher_tf_bias,
        timeframe_minutes=60,
        strategy_label="reversal_1h",
        timeframe_label="1ч",
    )
