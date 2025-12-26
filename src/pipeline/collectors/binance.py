#!/usr/bin/env python3
"""
Unified Binance OHLC data synchronization module.
Supports BTC, ETH, SOL spot pairs.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from pipeline.db import get_db

logger = logging.getLogger(__name__)

# Binance API configuration
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
INTERVAL = "1h"

# Symbol to table mapping
SYMBOL_CONFIG = {
    "BTCUSDT": "binance_ohlc_btc",
    "ETHUSDT": "binance_ohlc_eth",
    "SOLUSDT": "binance_ohlc_sol",
}


def get_klines(
    symbol: str,
    start_time: int,
    end_time: Optional[int] = None,
    limit: int = 1000
) -> Optional[list]:
    """
    Fetch klines from Binance API.

    Args:
        symbol: Trading pair (e.g., 'BTCUSDT')
        start_time: Start timestamp in milliseconds
        end_time: End timestamp in milliseconds (optional)
        limit: Number of candles to fetch (max 1000)

    Returns:
        List of klines or None on error
    """
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": start_time,
        "limit": limit,
    }

    if end_time:
        params["endTime"] = end_time

    try:
        response = requests.get(BINANCE_API_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for {symbol}: {e}")
        return None


def get_last_timestamp(table: str) -> Optional[int]:
    """Get the most recent open_time from database."""
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(f"SELECT MAX(open_time) FROM {table}")
        result = cursor.fetchone()
        return result["max"] if result and result["max"] else None


def upsert_klines(table: str, klines: list) -> tuple[int, int]:
    """
    Insert or update klines in database.

    Returns:
        Tuple of (inserted, updated) counts
    """
    if not klines:
        return 0, 0

    db = get_db()
    inserted = 0
    updated = 0

    with db.connection() as conn:
        cursor = conn.cursor()
        for kline in klines:
            open_time = kline[0]
            open_price = kline[1]
            high_price = kline[2]
            low_price = kline[3]
            close_price = kline[4]

            try:
                cursor.execute(
                    f"""
                    INSERT INTO {table} (open_time, open, high, low, close)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (open_time)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        created_at = CURRENT_TIMESTAMP
                    RETURNING (xmax = 0) AS inserted
                    """,
                    (open_time, open_price, high_price, low_price, close_price),
                )
                result = cursor.fetchone()
                if result and result["inserted"]:
                    inserted += 1
                else:
                    updated += 1
            except Exception as e:
                logger.error(f"Error upserting record for timestamp {open_time}: {e}")

        conn.commit()

    return inserted, updated


def sync_symbol(symbol: str) -> dict:
    """
    Synchronize OHLC data for a single symbol.

    Args:
        symbol: Trading pair (e.g., 'BTCUSDT')

    Returns:
        Dict with sync results
    """
    if symbol not in SYMBOL_CONFIG:
        raise ValueError(f"Unknown symbol: {symbol}. Supported: {list(SYMBOL_CONFIG.keys())}")

    table = SYMBOL_CONFIG[symbol]
    now = datetime.now(timezone.utc)

    logger.info(f"Starting {symbol} sync")

    # Get last timestamp in database
    last_timestamp = get_last_timestamp(table)

    if last_timestamp:
        last_date = datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc)
        logger.info(f"Last record in DB: {last_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        # Go back 2 hours to ensure we update the current hour
        start_timestamp = last_timestamp - (2 * 60 * 60 * 1000)
    else:
        logger.info("No records in database, starting fresh sync")
        start_timestamp = int((now - timedelta(hours=24)).timestamp() * 1000)

    # Fetch klines
    klines = get_klines(symbol, start_timestamp, limit=100)

    if not klines:
        logger.warning(f"No data received from API for {symbol}")
        return {"symbol": symbol, "status": "error", "message": "No data from API"}

    if len(klines) == 0:
        logger.info(f"No new data available for {symbol}")
        return {"symbol": symbol, "status": "success", "inserted": 0, "updated": 0}

    # Upsert klines
    inserted, updated = upsert_klines(table, klines)

    logger.info(f"{symbol}: Fetched {len(klines)}, inserted {inserted}, updated {updated}")

    return {
        "symbol": symbol,
        "status": "success",
        "fetched": len(klines),
        "inserted": inserted,
        "updated": updated,
    }


def sync_btc() -> dict:
    """Sync BTC OHLC data."""
    return sync_symbol("BTCUSDT")


def sync_eth() -> dict:
    """Sync ETH OHLC data."""
    return sync_symbol("ETHUSDT")


def sync_sol() -> dict:
    """Sync SOL OHLC data."""
    return sync_symbol("SOLUSDT")


def sync_all() -> list[dict]:
    """Sync all supported symbols."""
    results = []
    for symbol in SYMBOL_CONFIG:
        try:
            result = sync_symbol(symbol)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to sync {symbol}: {e}")
            results.append({"symbol": symbol, "status": "error", "message": str(e)})
    return results


def main() -> int:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    results = sync_all()
    success = all(r["status"] == "success" for r in results)

    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        print(f"{status_icon} {r['symbol']}: {r}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
