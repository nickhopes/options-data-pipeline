"""Data aggregators for OHLC sessions."""

from pipeline.aggregators.base import BaseAggregator
from pipeline.aggregators.daily_sessions import (
    DailySessionAggregator,
    aggregate_daily_btc,
    aggregate_daily_eth,
    aggregate_daily_sol,
    aggregate_daily_hype,
)
from pipeline.aggregators.weekly_sessions import (
    WeeklySessionAggregator,
    aggregate_weekly_btc,
    aggregate_weekly_eth,
    aggregate_weekly_sol,
)
from pipeline.aggregators.monthly_sessions import (
    MonthlySessionAggregator,
    aggregate_monthly_btc,
    aggregate_monthly_eth,
    aggregate_monthly_sol,
)
from pipeline.aggregators.option_ohlc import (
    OptionOHLCAggregator,
    aggregate_option_ohlc,
)

__all__ = [
    "BaseAggregator",
    "DailySessionAggregator",
    "WeeklySessionAggregator",
    "MonthlySessionAggregator",
    "OptionOHLCAggregator",
    "aggregate_daily_btc",
    "aggregate_daily_eth",
    "aggregate_daily_sol",
    "aggregate_daily_hype",
    "aggregate_weekly_btc",
    "aggregate_weekly_eth",
    "aggregate_weekly_sol",
    "aggregate_monthly_btc",
    "aggregate_monthly_eth",
    "aggregate_monthly_sol",
    "aggregate_option_ohlc",
]
