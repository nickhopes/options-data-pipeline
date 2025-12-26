#!/usr/bin/env python3
"""
Hyperliquid HYPE/USDC OHLC data synchronization module.
"""

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from pipeline.db import get_db

logger = logging.getLogger(__name__)

# Hyperliquid API configuration
API_URL = "https://api.hyperliquid.xyz/info"
COIN = "@107"  # HYPE/USDC spot pair
INTERVAL = "1h"
MAX_CANDLES = 5000  # API limit per request

# HYPE token TGE was Nov 29, 2024
TGE_DATE = datetime(2024, 11, 29, tzinfo=timezone.utc)


def fetch_candles(start_time_ms: int, end_time_ms: int) -> Optional[list]:
    """Fetch candles from Hyperliquid API."""
    try:
        response = requests.post(
            API_URL,
            headers={"Content-Type": "application/json"},
            json={
                "type": "candleSnapshot",
                "req": {
                    "coin": COIN,
                    "interval": INTERVAL,
                    "startTime": start_time_ms,
                    "endTime": end_time_ms,
                },
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None


def ms_to_datetime(ms: int) -> datetime:
    """Convert milliseconds timestamp to datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def get_last_timestamp() -> Optional[int]:
    """Get the most recent timestamp from database."""
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute("SELECT MAX(datetime) FROM hyperliquid_hype_ohlc")
        result = cursor.fetchone()
        if result and result["max"]:
            return int(result["max"].timestamp() * 1000)
        return None


def upsert_candles(candles: list) -> tuple[int, int]:
    """Insert or update candles in database."""
    if not candles:
        return 0, 0

    db = get_db()
    inserted = 0

    with db.connection() as conn:
        cursor = conn.cursor()

        for candle in candles:
            dt = ms_to_datetime(candle["t"]).strftime("%Y-%m-%d %H:%M:%S")

            try:
                cursor.execute(
                    """
                    INSERT INTO hyperliquid_hype_ohlc (datetime, open, high, low, close)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (datetime) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close
                    """,
                    (dt, float(candle["o"]), float(candle["h"]), float(candle["l"]), float(candle["c"])),
                )
                inserted += 1
            except Exception as e:
                logger.error(f"Error upserting candle: {e}")

        conn.commit()

    return inserted, 0


def sync_hype(full_history: bool = False) -> dict:
    """
    Synchronize HYPE/USDC OHLC data.

    Args:
        full_history: If True, fetch all data from TGE date

    Returns:
        Dict with sync results
    """
    now = datetime.now(timezone.utc)
    logger.info("Starting HYPE/USDC sync")

    # Determine start time
    if full_history:
        start_date = TGE_DATE
        logger.info(f"Full history mode: starting from TGE {TGE_DATE.date()}")
    else:
        last_timestamp = get_last_timestamp()
        if last_timestamp:
            # Go back 2 hours to ensure updates
            start_date = ms_to_datetime(last_timestamp - (2 * 60 * 60 * 1000))
            logger.info(f"Incremental mode: starting from {start_date}")
        else:
            # First run: fetch last 24 hours
            start_date = now - timedelta(hours=24)
            logger.info("No data in DB, fetching last 24 hours")

    all_candles = []
    current_end = now

    # Fetch in chunks (working backwards for full history, forward for incremental)
    if full_history:
        # Work backwards from now
        while current_end > start_date:
            chunk_hours = min(MAX_CANDLES, int((current_end - start_date).total_seconds() / 3600) + 1)
            current_start = current_end - timedelta(hours=chunk_hours)

            if current_start < start_date:
                current_start = start_date

            start_ms = int(current_start.timestamp() * 1000)
            end_ms = int(current_end.timestamp() * 1000)

            logger.info(f"Fetching: {current_start.strftime('%Y-%m-%d %H:%M')} to {current_end.strftime('%Y-%m-%d %H:%M')}")

            candles = fetch_candles(start_ms, end_ms)
            if candles:
                all_candles.extend(candles)
                logger.info(f"Received {len(candles)} candles (total: {len(all_candles)})")

            current_end = current_start
            time.sleep(0.5)  # Rate limiting
    else:
        # Simple incremental fetch
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)

        candles = fetch_candles(start_ms, end_ms)
        if candles:
            all_candles = candles

    if not all_candles:
        logger.warning("No data received from API")
        return {"symbol": "HYPE/USDC", "status": "warning", "message": "No data from API"}

    # Remove duplicates and sort
    seen_times = set()
    unique_candles = []
    for candle in all_candles:
        t = candle["t"]
        if t not in seen_times:
            seen_times.add(t)
            unique_candles.append(candle)

    unique_candles.sort(key=lambda x: x["t"])

    # Upsert candles
    inserted, _ = upsert_candles(unique_candles)

    logger.info(f"HYPE/USDC: Fetched {len(unique_candles)}, inserted/updated {inserted}")

    return {
        "symbol": "HYPE/USDC",
        "status": "success",
        "fetched": len(unique_candles),
        "inserted": inserted,
    }


def main() -> int:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Check for --full flag
    full_history = "--full" in sys.argv

    result = sync_hype(full_history=full_history)
    status_icon = "✓" if result["status"] == "success" else "✗"
    print(f"{status_icon} {result['symbol']}: {result}")

    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
