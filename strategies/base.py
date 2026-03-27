from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyProfile:
    ema_slope_threshold: float
    near_ema20_pct: float
    volume_factor: float
    atr_min_pct: float
    impulse_body_factor: float
    long_rsi_min: float
    long_rsi_max: float
    short_rsi_min: float
    short_rsi_max: float
    rsi_exit_long: float
    rsi_exit_short: float
    allow_short: bool
