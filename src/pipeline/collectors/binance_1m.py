#!/usr/bin/env python3
"""
Binance 1-minute OHLCV data synchronization module.
Supports BTC, ETH, SOL spot pairs with backfill capability.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

from pipeline.db import get_db

logger = logging.getLogger(__name__)

# Binance API configuration
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
INTERVAL = "1m"

# Symbol configuration with start dates
SYMBOL_CONFIG = {
    "BTCUSDT": {
        "table": "btc_ohlc_1m",
        "start_date": datetime(2019, 1, 1, tzinfo=timezone.utc),
    },
    "ETHUSDT": {
        "table": "eth_ohlc_1m",
        "start_date": datetime(2019, 1, 1, tzinfo=timezone.utc),
    },
    "SOLUSDT": {
        "table": "sol_ohlc_1m",
        "start_date": datetime(2020, 8, 11, 8, 0, tzinfo=timezone.utc),  # SOL listing date
    },
}


def get_klines(
    symbol: str,
    start_time: int,
    end_time: Optional[int] = None,
    limit: int = 1000
) -> Optional[list]:
    """
    Fetch 1-minute klines from Binance API.

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


def get_last_timestamp(table: str) -> Optional[datetime]:
    """Get the most recent timestamp from database."""
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(f"SELECT MAX(timestamp) FROM {table}")
        result = cursor.fetchone()
        return result["max"] if result and result["max"] else None


def get_record_count(table: str) -> int:
    """Get total record count from table."""
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        result = cursor.fetchone()
        return result["count"] if result else 0


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
            # Convert timestamp from ms to datetime
            ts_ms = kline[0]
            timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            open_price = kline[1]
            high_price = kline[2]
            low_price = kline[3]
            close_price = kline[4]
            volume = kline[5]

            try:
                cursor.execute(
                    f"""
                    INSERT INTO {table} (timestamp, open, high, low, close, volume, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (timestamp)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted
                    """,
                    (timestamp, open_price, high_price, low_price, close_price, volume),
                )
                result = cursor.fetchone()
                if result and result["inserted"]:
                    inserted += 1
                else:
                    updated += 1
            except Exception as e:
                logger.error(f"Error upserting record for timestamp {timestamp}: {e}")

        conn.commit()

    return inserted, updated


def sync_symbol(symbol: str, minutes_back: int = 5) -> dict:
    """
    Synchronize recent 1-minute OHLCV data for a symbol.

    Fetches last N minutes of data and upserts (updates current minute).

    Args:
        symbol: Trading pair (e.g., 'BTCUSDT')
        minutes_back: How many minutes of history to fetch

    Returns:
        Dict with sync results
    """
    if symbol not in SYMBOL_CONFIG:
        raise ValueError(f"Unknown symbol: {symbol}")

    config = SYMBOL_CONFIG[symbol]
    table = config["table"]
    now = datetime.now(timezone.utc)

    # Calculate start time (N minutes ago)
    start_timestamp = int((now.timestamp() - minutes_back * 60) * 1000)

    # Fetch klines
    klines = get_klines(symbol, start_timestamp, limit=minutes_back + 1)

    if not klines:
        return {"symbol": symbol, "status": "error", "message": "No data from API"}

    # Upsert klines
    inserted, updated = upsert_klines(table, klines)

    return {
        "symbol": symbol,
        "status": "success",
        "fetched": len(klines),
        "inserted": inserted,
        "updated": updated,
    }


def backfill_symbol(symbol: str, batch_size: int = 1000, delay: float = 0.1) -> dict:
    """
    Backfill historical 1-minute data for a symbol.

    Starts from configured start_date or last record in DB.
    Fetches in batches with rate limiting.

    Args:
        symbol: Trading pair (e.g., 'BTCUSDT')
        batch_size: Number of candles per API request (max 1000)
        delay: Delay between API requests in seconds

    Returns:
        Dict with backfill results
    """
    if symbol not in SYMBOL_CONFIG:
        raise ValueError(f"Unknown symbol: {symbol}")

    config = SYMBOL_CONFIG[symbol]
    table = config["table"]
    start_date = config["start_date"]

    # Check last timestamp in DB
    last_ts = get_last_timestamp(table)

    if last_ts:
        # Continue from last record
        start_timestamp = int(last_ts.timestamp() * 1000) + 60000  # +1 minute
        logger.info(f"{symbol}: Continuing backfill from {last_ts}")
    else:
        # Start from configured date
        start_timestamp = int(start_date.timestamp() * 1000)
        logger.info(f"{symbol}: Starting backfill from {start_date}")

    now = datetime.now(timezone.utc)
    end_timestamp = int(now.timestamp() * 1000)

    total_inserted = 0
    total_updated = 0
    total_fetched = 0
    batches = 0

    while start_timestamp < end_timestamp:
        # Fetch batch
        klines = get_klines(symbol, start_timestamp, limit=batch_size)

        if not klines:
            logger.warning(f"{symbol}: No data received at {start_timestamp}")
            break

        if len(klines) == 0:
            logger.info(f"{symbol}: Backfill complete - no more data")
            break

        # Upsert batch
        inserted, updated = upsert_klines(table, klines)
        total_inserted += inserted
        total_updated += updated
        total_fetched += len(klines)
        batches += 1

        # Update start for next batch
        last_kline_ts = klines[-1][0]
        start_timestamp = last_kline_ts + 60000  # +1 minute

        # Progress log every 100 batches
        if batches % 100 == 0:
            progress_date = datetime.fromtimestamp(last_kline_ts / 1000, tz=timezone.utc)
            logger.info(f"{symbol}: Batch {batches}, up to {progress_date}, total {total_fetched:,} records")

        # Rate limiting
        time.sleep(delay)

    logger.info(f"{symbol}: Backfill finished - {total_fetched:,} fetched, {total_inserted:,} inserted, {total_updated:,} updated")

    return {
        "symbol": symbol,
        "status": "success",
        "batches": batches,
        "fetched": total_fetched,
        "inserted": total_inserted,
        "updated": total_updated,
    }


def sync_btc_1m() -> dict:
    """Sync BTC 1-minute OHLCV data."""
    return sync_symbol("BTCUSDT")


def sync_eth_1m() -> dict:
    """Sync ETH 1-minute OHLCV data."""
    return sync_symbol("ETHUSDT")


def sync_sol_1m() -> dict:
    """Sync SOL 1-minute OHLCV data."""
    return sync_symbol("SOLUSDT")


def sync_all_1m() -> dict:
    """Sync all symbols' 1-minute data."""
    results = {}
    for symbol in SYMBOL_CONFIG:
        try:
            results[symbol.replace("USDT", "").lower()] = sync_symbol(symbol)
        except Exception as e:
            logger.error(f"Failed to sync {symbol}: {e}")
            results[symbol.replace("USDT", "").lower()] = {"status": "error", "message": str(e)}
    return results


def backfill_btc_1m() -> dict:
    """Backfill BTC 1-minute historical data."""
    return backfill_symbol("BTCUSDT")


def backfill_eth_1m() -> dict:
    """Backfill ETH 1-minute historical data."""
    return backfill_symbol("ETHUSDT")


def backfill_sol_1m() -> dict:
    """Backfill SOL 1-minute historical data."""
    return backfill_symbol("SOLUSDT")


def backfill_all_1m() -> dict:
    """Backfill all symbols' 1-minute historical data."""
    results = {}
    for symbol in SYMBOL_CONFIG:
        try:
            results[symbol.replace("USDT", "").lower()] = backfill_symbol(symbol)
        except Exception as e:
            logger.error(f"Failed to backfill {symbol}: {e}")
            results[symbol.replace("USDT", "").lower()] = {"status": "error", "message": str(e)}
    return results


def get_status() -> dict:
    """Get current status of 1-minute tables."""
    status = {}
    for symbol, config in SYMBOL_CONFIG.items():
        table = config["table"]
        key = symbol.replace("USDT", "").lower()

        count = get_record_count(table)
        last_ts = get_last_timestamp(table)

        status[key] = {
            "table": table,
            "records": count,
            "last_timestamp": last_ts.isoformat() if last_ts else None,
        }

    return status


# ============================================================================
# Gap Detection and Repair
# ============================================================================


def find_gaps(table: str, min_gap_minutes: int = 5, max_age_days: int = 7) -> list[dict]:
    """
    Find gaps in 1-minute data.

    Args:
        table: Table name to check
        min_gap_minutes: Minimum gap size to report (ignore small gaps)
        max_age_days: Only check gaps within this many days (0 = all time)

    Returns:
        List of gap dictionaries with start, end, and missing_minutes
    """
    db = get_db()

    age_filter = ""
    if max_age_days > 0:
        age_filter = f"WHERE timestamp > NOW() - interval '{max_age_days} days'"

    query = f"""
    WITH time_series AS (
        SELECT
            timestamp as ts,
            LAG(timestamp) OVER (ORDER BY timestamp) as prev_ts
        FROM {table}
        {age_filter}
    )
    SELECT
        prev_ts as gap_start,
        ts as gap_end,
        EXTRACT(EPOCH FROM (ts - prev_ts))/60 as gap_minutes
    FROM time_series
    WHERE ts - prev_ts > interval '{min_gap_minutes} minutes'
    ORDER BY gap_minutes DESC
    """

    with db.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    gaps = []
    for row in results:
        gaps.append({
            "gap_start": row["gap_start"],
            "gap_end": row["gap_end"],
            "missing_minutes": int(row["gap_minutes"]) - 1,
        })

    return gaps


def fill_gap(symbol: str, gap_start: datetime, gap_end: datetime, delay: float = 0.05) -> dict:
    """
    Fill a specific gap in the data.

    Args:
        symbol: Trading pair (e.g., 'BTCUSDT')
        gap_start: Start of gap (last known record before gap)
        gap_end: End of gap (first known record after gap)
        delay: Delay between API requests

    Returns:
        Dict with fill results
    """
    if symbol not in SYMBOL_CONFIG:
        raise ValueError(f"Unknown symbol: {symbol}")

    config = SYMBOL_CONFIG[symbol]
    table = config["table"]

    # Start 1 minute after gap_start
    start_ms = int(gap_start.timestamp() * 1000) + 60000
    end_ms = int(gap_end.timestamp() * 1000)

    total_inserted = 0
    total_updated = 0
    batches = 0

    current = start_ms
    while current < end_ms:
        klines = get_klines(symbol, current, end_time=end_ms, limit=1000)

        if not klines or len(klines) == 0:
            break

        inserted, updated = upsert_klines(table, klines)
        total_inserted += inserted
        total_updated += updated
        batches += 1

        current = klines[-1][0] + 60000
        time.sleep(delay)

    logger.info(f"{symbol}: Filled gap {gap_start} -> {gap_end}: {total_inserted} inserted, {total_updated} updated")

    return {
        "symbol": symbol,
        "gap_start": gap_start.isoformat(),
        "gap_end": gap_end.isoformat(),
        "inserted": total_inserted,
        "updated": total_updated,
        "batches": batches,
    }


def repair_gaps(min_gap_minutes: int = 5, max_age_days: int = 7) -> dict:
    """
    Find and fill all gaps in recent data for all symbols.

    Args:
        min_gap_minutes: Minimum gap size to repair
        max_age_days: Only repair gaps within this many days

    Returns:
        Dict with repair results for each symbol
    """
    results = {}

    for symbol, config in SYMBOL_CONFIG.items():
        table = config["table"]
        key = symbol.replace("USDT", "").lower()

        # Find gaps
        gaps = find_gaps(table, min_gap_minutes, max_age_days)

        if not gaps:
            results[key] = {
                "status": "ok",
                "gaps_found": 0,
                "gaps_filled": 0,
            }
            continue

        logger.info(f"{symbol}: Found {len(gaps)} gaps to repair")

        filled = 0
        total_inserted = 0

        for gap in gaps:
            try:
                result = fill_gap(symbol, gap["gap_start"], gap["gap_end"])
                if result["inserted"] > 0:
                    filled += 1
                    total_inserted += result["inserted"]
            except Exception as e:
                logger.error(f"{symbol}: Failed to fill gap {gap}: {e}")

        results[key] = {
            "status": "repaired" if filled > 0 else "ok",
            "gaps_found": len(gaps),
            "gaps_filled": filled,
            "records_inserted": total_inserted,
        }

    return results


def get_gap_summary() -> dict:
    """
    Get summary of gaps in all tables.

    Returns:
        Dict with gap statistics for each symbol
    """
    summary = {}

    for symbol, config in SYMBOL_CONFIG.items():
        table = config["table"]
        key = symbol.replace("USDT", "").lower()

        # Recent gaps (last 7 days)
        recent_gaps = find_gaps(table, min_gap_minutes=2, max_age_days=7)

        # Total gap count
        all_gaps = find_gaps(table, min_gap_minutes=60, max_age_days=0)

        summary[key] = {
            "recent_gaps_7d": len(recent_gaps),
            "recent_missing_minutes": sum(g["missing_minutes"] for g in recent_gaps),
            "large_gaps_all_time": len(all_gaps),
        }

    return summary
