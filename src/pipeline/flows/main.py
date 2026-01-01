"""
Main entry point for the data pipeline.
Configures and serves all Prefect deployments.
"""

import logging
import sys
from datetime import timedelta

from prefect import serve

from pipeline.flows.collection import (
    binance_sync_flow,
    binance_1m_sync_flow,
    binance_1m_gap_repair_flow,
    deribit_sync_flow,
    hyperliquid_sync_flow,
)
from pipeline.flows.aggregation import (
    daily_aggregation_flow,
    weekly_aggregation_flow,
    monthly_aggregation_flow,
    option_ohlc_flow,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    """
    Main entry point.
    Serves all flow deployments with their schedules.
    """
    logger.info("Starting Options Data Pipeline...")
    logger.info("Configuring flow deployments...")

    # ========================================================================
    # Collection Deployments
    # ========================================================================

    # Binance OHLC sync - every 5 minutes
    binance_deployment = binance_sync_flow.to_deployment(
        name="binance-every-5min",
        cron="*/5 * * * *",
        tags=["collection", "binance"],
        description="Sync BTC/ETH/SOL hourly OHLC from Binance every 5 minutes",
    )

    # Hyperliquid HYPE sync - every hour
    hyperliquid_deployment = hyperliquid_sync_flow.to_deployment(
        name="hyperliquid-hourly",
        cron="0 * * * *",
        tags=["collection", "hyperliquid"],
        description="Sync HYPE/USDC hourly OHLC from Hyperliquid",
    )

    # Deribit option trades - every hour
    deribit_deployment = deribit_sync_flow.to_deployment(
        name="deribit-hourly",
        cron="0 * * * *",
        tags=["collection", "deribit", "options"],
        description="Sync BTC option trades from Deribit",
    )

    # Binance 1-minute OHLCV sync - every 5 seconds
    binance_1m_deployment = binance_1m_sync_flow.to_deployment(
        name="binance-1m-every-5sec",
        interval=timedelta(seconds=5),
        tags=["collection", "binance", "1m"],
        description="Sync BTC/ETH/SOL 1-minute OHLCV from Binance every 5 seconds",
    )

    # Binance 1-minute gap repair - every hour at :30
    binance_1m_gap_repair_deployment = binance_1m_gap_repair_flow.to_deployment(
        name="binance-1m-gap-repair-hourly",
        cron="30 * * * *",
        tags=["maintenance", "binance", "1m", "repair"],
        description="Check and repair gaps in 1-minute OHLC data every hour",
    )

    # ========================================================================
    # Aggregation Deployments
    # ========================================================================

    # Daily session aggregation - at 11:00 UTC
    daily_deployment = daily_aggregation_flow.to_deployment(
        name="daily-11-utc",
        cron="0 11 * * *",
        tags=["aggregation", "daily"],
        description="Aggregate hourly data into daily sessions at 11:00 UTC",
    )

    # Weekly session aggregation - Friday at 11:00 UTC
    weekly_deployment = weekly_aggregation_flow.to_deployment(
        name="weekly-friday-11-utc",
        cron="0 11 * * 5",
        tags=["aggregation", "weekly"],
        description="Aggregate daily data into weekly sessions on Friday at 11:00 UTC",
    )

    # Monthly session aggregation - last Friday at 11:00 UTC
    # Note: Cron doesn't support "last Friday", so we run every Friday and check inside
    monthly_deployment = monthly_aggregation_flow.to_deployment(
        name="monthly-last-friday-11-utc",
        cron="0 11 * * 5",
        tags=["aggregation", "monthly"],
        description="Aggregate daily data into monthly sessions on last Friday at 11:00 UTC",
    )

    # Option OHLC aggregation - every hour at minute 5
    option_ohlc_deployment = option_ohlc_flow.to_deployment(
        name="option-ohlc-hourly",
        cron="5 * * * *",
        tags=["aggregation", "options", "ohlc"],
        description="Aggregate BTC option trades into hourly OHLC candles",
    )

    # ========================================================================
    # Serve all deployments
    # ========================================================================

    logger.info("Starting Prefect serve with all deployments...")
    logger.info("")
    logger.info("Configured schedules:")
    logger.info("  - Binance 1m sync:    every 5 seconds")
    logger.info("  - Binance 1m repair:  30 * * * *  (hourly at :30)")
    logger.info("  - Binance 1h sync:    */5 * * * * (every 5 min)")
    logger.info("  - Hyperliquid sync:   0 * * * *   (hourly)")
    logger.info("  - Deribit sync:       0 * * * *   (hourly)")
    logger.info("  - Option OHLC:        5 * * * *   (hourly at :05)")
    logger.info("  - Daily aggregation:  0 11 * * *  (11:00 UTC daily)")
    logger.info("  - Weekly aggregation: 0 11 * * 5  (Friday 11:00 UTC)")
    logger.info("  - Monthly aggregation: 0 11 * * 5 (last Friday 11:00 UTC)")
    logger.info("")
    logger.info("Open Prefect UI at http://localhost:4200")
    logger.info("")

    serve(
        binance_1m_deployment,
        binance_1m_gap_repair_deployment,
        binance_deployment,
        hyperliquid_deployment,
        deribit_deployment,
        option_ohlc_deployment,
        daily_deployment,
        weekly_deployment,
        monthly_deployment,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
