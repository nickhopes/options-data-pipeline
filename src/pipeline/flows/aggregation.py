"""
Prefect flows for data aggregation.
"""

from prefect import flow, task

from pipeline.aggregators import daily_sessions, weekly_sessions, monthly_sessions, option_ohlc


# ============================================================================
# Daily Aggregation Tasks
# ============================================================================


@task(
    name="aggregate-daily-btc",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "daily", "btc"],
)
def aggregate_daily_btc():
    """Aggregate BTC daily sessions."""
    return daily_sessions.aggregate_daily_btc()


@task(
    name="aggregate-daily-eth",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "daily", "eth"],
)
def aggregate_daily_eth():
    """Aggregate ETH daily sessions."""
    return daily_sessions.aggregate_daily_eth()


@task(
    name="aggregate-daily-sol",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "daily", "sol"],
)
def aggregate_daily_sol():
    """Aggregate SOL daily sessions."""
    return daily_sessions.aggregate_daily_sol()


@task(
    name="aggregate-daily-hype",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "daily", "hype"],
)
def aggregate_daily_hype():
    """Aggregate HYPE daily sessions."""
    return daily_sessions.aggregate_daily_hype()


@flow(name="daily-aggregation-flow", log_prints=True)
def daily_aggregation_flow():
    """
    Aggregate all daily sessions.
    Runs BTC, ETH, SOL, HYPE aggregation in parallel.
    """
    print("Starting daily session aggregation...")

    # Submit tasks in parallel
    btc_future = aggregate_daily_btc.submit()
    eth_future = aggregate_daily_eth.submit()
    sol_future = aggregate_daily_sol.submit()
    hype_future = aggregate_daily_hype.submit()

    # Wait for results
    btc_result = btc_future.result()
    eth_result = eth_future.result()
    sol_result = sol_future.result()
    hype_result = hype_future.result()

    results = {
        "btc": btc_result,
        "eth": eth_result,
        "sol": sol_result,
        "hype": hype_result,
    }

    print(f"Daily aggregation completed: {results}")
    return results


# ============================================================================
# Weekly Aggregation Tasks
# ============================================================================


@task(
    name="aggregate-weekly-btc",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "weekly", "btc"],
)
def aggregate_weekly_btc():
    """Aggregate BTC weekly sessions."""
    return weekly_sessions.aggregate_weekly_btc()


@task(
    name="aggregate-weekly-eth",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "weekly", "eth"],
)
def aggregate_weekly_eth():
    """Aggregate ETH weekly sessions."""
    return weekly_sessions.aggregate_weekly_eth()


@task(
    name="aggregate-weekly-sol",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "weekly", "sol"],
)
def aggregate_weekly_sol():
    """Aggregate SOL weekly sessions."""
    return weekly_sessions.aggregate_weekly_sol()


@flow(name="weekly-aggregation-flow", log_prints=True)
def weekly_aggregation_flow():
    """
    Aggregate all weekly sessions.
    Runs BTC, ETH, SOL aggregation in parallel.
    """
    print("Starting weekly session aggregation...")

    # Submit tasks in parallel
    btc_future = aggregate_weekly_btc.submit()
    eth_future = aggregate_weekly_eth.submit()
    sol_future = aggregate_weekly_sol.submit()

    # Wait for results
    btc_result = btc_future.result()
    eth_result = eth_future.result()
    sol_result = sol_future.result()

    results = {
        "btc": btc_result,
        "eth": eth_result,
        "sol": sol_result,
    }

    print(f"Weekly aggregation completed: {results}")
    return results


# ============================================================================
# Monthly Aggregation Tasks
# ============================================================================


@task(
    name="aggregate-monthly-btc",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "monthly", "btc"],
)
def aggregate_monthly_btc():
    """Aggregate BTC monthly sessions."""
    return monthly_sessions.aggregate_monthly_btc()


@task(
    name="aggregate-monthly-eth",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "monthly", "eth"],
)
def aggregate_monthly_eth():
    """Aggregate ETH monthly sessions."""
    return monthly_sessions.aggregate_monthly_eth()


@task(
    name="aggregate-monthly-sol",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "monthly", "sol"],
)
def aggregate_monthly_sol():
    """Aggregate SOL monthly sessions."""
    return monthly_sessions.aggregate_monthly_sol()


@flow(name="monthly-aggregation-flow", log_prints=True)
def monthly_aggregation_flow():
    """
    Aggregate all monthly sessions.
    Runs BTC, ETH, SOL aggregation in parallel.
    """
    print("Starting monthly session aggregation...")

    # Submit tasks in parallel
    btc_future = aggregate_monthly_btc.submit()
    eth_future = aggregate_monthly_eth.submit()
    sol_future = aggregate_monthly_sol.submit()

    # Wait for results
    btc_result = btc_future.result()
    eth_result = eth_future.result()
    sol_result = sol_future.result()

    results = {
        "btc": btc_result,
        "eth": eth_result,
        "sol": sol_result,
    }

    print(f"Monthly aggregation completed: {results}")
    return results


# ============================================================================
# Option OHLC Aggregation Tasks
# ============================================================================


@task(
    name="aggregate-option-ohlc",
    retries=2,
    retry_delay_seconds=60,
    tags=["aggregation", "options", "ohlc"],
)
def aggregate_option_ohlc_task(hours_back: int = 3):
    """Aggregate BTC option trades into hourly OHLC."""
    return option_ohlc.aggregate_option_ohlc(hours_back)


@flow(name="option-ohlc-flow", log_prints=True)
def option_ohlc_flow(hours_back: int = 3):
    """
    Aggregate BTC option trades into hourly OHLC candles.
    Runs hourly to process recent trades.
    """
    print("Starting option OHLC aggregation...")

    result = aggregate_option_ohlc_task(hours_back)

    print(f"Option OHLC aggregation completed: {result}")
    return result


# ============================================================================
# Combined Aggregation Flow
# ============================================================================


@flow(name="all-aggregation-flow", log_prints=True)
def all_aggregation_flow():
    """Run all aggregation flows."""
    print("Starting all aggregation flows...")

    daily_result = daily_aggregation_flow()
    weekly_result = weekly_aggregation_flow()
    monthly_result = monthly_aggregation_flow()

    results = {
        "daily": daily_result,
        "weekly": weekly_result,
        "monthly": monthly_result,
    }

    print(f"All aggregation completed: {results}")
    return results
