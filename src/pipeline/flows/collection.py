"""
Prefect flows for data collection.
"""

from prefect import flow, task

from pipeline.collectors import binance, binance_1m, deribit, hyperliquid


# ============================================================================
# Binance Collection Tasks
# ============================================================================


@task(
    name="sync-binance-btc",
    retries=3,
    retry_delay_seconds=60,
    tags=["binance", "btc", "collection"],
)
def sync_binance_btc():
    """Sync BTC OHLC from Binance."""
    return binance.sync_btc()


@task(
    name="sync-binance-eth",
    retries=3,
    retry_delay_seconds=60,
    tags=["binance", "eth", "collection"],
)
def sync_binance_eth():
    """Sync ETH OHLC from Binance."""
    return binance.sync_eth()


@task(
    name="sync-binance-sol",
    retries=3,
    retry_delay_seconds=60,
    tags=["binance", "sol", "collection"],
)
def sync_binance_sol():
    """Sync SOL OHLC from Binance."""
    return binance.sync_sol()


@flow(name="binance-sync-flow", log_prints=True)
def binance_sync_flow():
    """
    Synchronize all Binance OHLC data.
    Runs BTC, ETH, SOL sync in parallel.
    """
    print("Starting Binance OHLC sync...")

    # Submit tasks in parallel
    btc_future = sync_binance_btc.submit()
    eth_future = sync_binance_eth.submit()
    sol_future = sync_binance_sol.submit()

    # Wait for results
    btc_result = btc_future.result()
    eth_result = eth_future.result()
    sol_result = sol_future.result()

    results = {
        "btc": btc_result,
        "eth": eth_result,
        "sol": sol_result,
    }

    print(f"Binance sync completed: {results}")
    return results


# ============================================================================
# Hyperliquid Collection Tasks
# ============================================================================


@task(
    name="sync-hyperliquid-hype",
    retries=3,
    retry_delay_seconds=60,
    tags=["hyperliquid", "hype", "collection"],
)
def sync_hyperliquid_hype():
    """Sync HYPE/USDC OHLC from Hyperliquid."""
    return hyperliquid.sync_hype()


@flow(name="hyperliquid-sync-flow", log_prints=True)
def hyperliquid_sync_flow():
    """Synchronize Hyperliquid HYPE/USDC data."""
    print("Starting Hyperliquid HYPE sync...")
    result = sync_hyperliquid_hype()
    print(f"Hyperliquid sync completed: {result}")
    return result


# ============================================================================
# Deribit Collection Tasks
# ============================================================================


@task(
    name="sync-deribit-options",
    retries=3,
    retry_delay_seconds=120,
    tags=["deribit", "options", "collection"],
)
def sync_deribit_options():
    """Sync BTC option trades from Deribit."""
    return deribit.sync_deribit()


@flow(name="deribit-sync-flow", log_prints=True)
def deribit_sync_flow():
    """Synchronize Deribit BTC option trades."""
    print("Starting Deribit options sync...")
    result = sync_deribit_options()
    print(f"Deribit sync completed: {result}")
    return result


# ============================================================================
# Combined Collection Flow
# ============================================================================


@flow(name="all-collection-flow", log_prints=True)
def all_collection_flow():
    """
    Run all collection flows.
    Binance runs in parallel internally.
    """
    print("Starting all collection flows...")

    # Run all flows
    binance_result = binance_sync_flow()
    hype_result = hyperliquid_sync_flow()

    results = {
        "binance": binance_result,
        "hyperliquid": hype_result,
    }

    print(f"All collection completed: {results}")
    return results


# ============================================================================
# Binance 1-Minute Collection Tasks
# ============================================================================


@task(
    name="sync-binance-1m-all",
    retries=2,
    retry_delay_seconds=5,
    tags=["binance", "1m", "collection"],
)
def sync_binance_1m_all():
    """Sync all symbols' 1-minute OHLCV data."""
    return binance_1m.sync_all_1m()


@flow(name="binance-1m-sync-flow", log_prints=True)
def binance_1m_sync_flow():
    """
    Synchronize 1-minute OHLCV data for BTC, ETH, SOL.
    Designed to run every 5 seconds to keep current minute updated.
    """
    result = sync_binance_1m_all()
    # Compact output for frequent runs
    summary = {k: v.get("updated", 0) + v.get("inserted", 0) for k, v in result.items()}
    print(f"1m sync: {summary}")
    return result


# ============================================================================
# Binance 1-Minute Backfill Tasks
# ============================================================================


@task(
    name="backfill-binance-1m-btc",
    retries=1,
    tags=["binance", "1m", "backfill", "btc"],
)
def backfill_btc_1m_task():
    """Backfill BTC 1-minute historical data."""
    return binance_1m.backfill_btc_1m()


@task(
    name="backfill-binance-1m-eth",
    retries=1,
    tags=["binance", "1m", "backfill", "eth"],
)
def backfill_eth_1m_task():
    """Backfill ETH 1-minute historical data."""
    return binance_1m.backfill_eth_1m()


@task(
    name="backfill-binance-1m-sol",
    retries=1,
    tags=["binance", "1m", "backfill", "sol"],
)
def backfill_sol_1m_task():
    """Backfill SOL 1-minute historical data."""
    return binance_1m.backfill_sol_1m()


@flow(name="binance-1m-backfill-flow", log_prints=True)
def binance_1m_backfill_flow():
    """
    Backfill historical 1-minute data for all symbols.
    Run once to populate historical data, then use sync flow.
    """
    print("Starting 1-minute backfill for all symbols...")
    print("This may take 30-60 minutes depending on network speed.")

    # Run sequentially to avoid rate limits
    btc_result = backfill_btc_1m_task()
    print(f"BTC backfill: {btc_result}")

    eth_result = backfill_eth_1m_task()
    print(f"ETH backfill: {eth_result}")

    sol_result = backfill_sol_1m_task()
    print(f"SOL backfill: {sol_result}")

    results = {
        "btc": btc_result,
        "eth": eth_result,
        "sol": sol_result,
    }

    print(f"Backfill completed: {results}")
    return results
