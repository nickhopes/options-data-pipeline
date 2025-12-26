#!/usr/bin/env python3
"""
Deribit BTC Option Trades synchronization module.
Downloads historical and recent trades from Deribit API.
"""

import logging
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from psycopg2.extras import execute_values

from pipeline.db import get_db

logger = logging.getLogger(__name__)

# API configuration
HISTORY_API = "https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time"
LIVE_API = "https://www.deribit.com/api/v2/public/get_last_trades_by_currency_and_time"
BATCH_SIZE = 10000  # Max allowed by API
RATE_LIMIT_DELAY = 0.1  # seconds between requests
MAX_RETRIES = 3


def parse_instrument(name: str) -> tuple:
    """Parse instrument name like BTC-4JAN19-3500-P."""
    match = re.match(r"(\w+)-(\d+)([A-Z]+)(\d+)-(\d+)-([CP])", name)
    if match:
        day = int(match.group(2))
        month_str = match.group(3)
        year = int(match.group(4))
        strike = int(match.group(5))
        opt_type = match.group(6)

        months = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }
        month = months.get(month_str, 1)
        year = 2000 + year if year < 100 else year

        try:
            expiry = datetime(year, month, day).date()
            return expiry, strike, opt_type
        except ValueError:
            return None, None, None
    return None, None, None


def fetch_trades(start_ts: int, end_ts: int, use_history: bool = True) -> tuple[list, bool]:
    """Fetch trades from Deribit API with retry."""
    url = HISTORY_API if use_history else LIVE_API
    params = {
        "currency": "BTC",
        "kind": "option",
        "count": BATCH_SIZE,
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "sorting": "asc",
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if "result" in data and "trades" in data["result"]:
                return data["result"]["trades"], data["result"].get("has_more", False)
            return [], False
        except Exception as e:
            logger.warning(f"API error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)

    return [], False


def insert_trades(trades: list) -> int:
    """Bulk insert trades into database."""
    if not trades:
        return 0

    values = []
    for t in trades:
        expiry, strike, opt_type = parse_instrument(t["instrument_name"])
        if not expiry:
            continue

        ts = datetime.fromtimestamp(t["timestamp"] / 1000, tz=timezone.utc)

        values.append((
            str(t["trade_id"]),
            t["trade_seq"],
            ts,
            t["instrument_name"],
            expiry,
            strike,
            opt_type,
            t["direction"],
            t["price"],
            t["amount"],
            t.get("iv"),
            t.get("mark_price"),
            t.get("index_price"),
            t.get("tick_direction"),
        ))

    if not values:
        return 0

    db = get_db()
    with db.connection() as conn:
        cursor = conn.cursor()
        try:
            execute_values(
                cursor,
                """
                INSERT INTO btc_deribit_option_trades
                (trade_id, trade_seq, timestamp, instrument_name,
                 expiry_date, strike, option_type, direction, price, amount,
                 iv, mark_price, index_price, tick_direction)
                VALUES %s
                ON CONFLICT (trade_id) DO NOTHING
                """,
                values,
            )
            inserted = cursor.rowcount
            conn.commit()
            return inserted
        except Exception as e:
            conn.rollback()
            raise e


def get_last_loaded_date() -> Optional[datetime]:
    """Get the last loaded date from database."""
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute("SELECT MAX(timestamp)::date FROM btc_deribit_option_trades")
        result = cursor.fetchone()
        if result and result["max"]:
            return result["max"]
        return None


def load_day(date: datetime.date) -> int:
    """Load all trades for a single day."""
    day_start = int(datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    day_end = int(datetime.combine(date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)

    # Use history API for dates more than 1 day old
    use_history = date < (datetime.now(timezone.utc) - timedelta(days=1)).date()

    day_total = 0
    last_ts = day_start

    while True:
        trades, has_more = fetch_trades(last_ts, day_end, use_history)

        if not trades:
            break

        inserted = insert_trades(trades)
        day_total += inserted

        # Get last timestamp for pagination
        last_ts = trades[-1]["timestamp"] + 1

        if not has_more or len(trades) < BATCH_SIZE:
            break

        time.sleep(RATE_LIMIT_DELAY)

    return day_total


def sync_deribit(days_back: int = 2) -> dict:
    """
    Synchronize Deribit option trades.

    Args:
        days_back: Number of days to look back for updates

    Returns:
        Dict with sync results
    """
    logger.info("Starting Deribit BTC option trades sync")

    end_date = datetime.now(timezone.utc).date()

    # Check for resume point
    last_date = get_last_loaded_date()
    if last_date:
        # Resume from last date or go back a few days for updates
        start_date = max(last_date - timedelta(days=days_back), last_date)
        logger.info(f"Resuming from: {start_date}")
    else:
        # First run: fetch last 7 days
        start_date = end_date - timedelta(days=7)
        logger.info(f"No data in DB, fetching last 7 days")

    if start_date > end_date:
        logger.info("Already up to date!")
        return {"symbol": "BTC Options", "status": "success", "message": "Already up to date"}

    total_inserted = 0
    current = start_date

    while current <= end_date:
        try:
            day_total = load_day(current)
            total_inserted += day_total
            logger.info(f"{current.strftime('%Y-%m-%d')}: +{day_total:,} trades")
        except Exception as e:
            logger.error(f"Error on {current}: {e}")

        current += timedelta(days=1)
        time.sleep(RATE_LIMIT_DELAY)

    logger.info(f"Deribit sync completed: {total_inserted:,} trades inserted")

    return {
        "symbol": "BTC Options",
        "status": "success",
        "days_processed": (end_date - start_date).days + 1,
        "inserted": total_inserted,
    }


def main() -> int:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Check for days_back argument
    days_back = 2
    for arg in sys.argv[1:]:
        try:
            days_back = int(arg)
        except ValueError:
            pass

    result = sync_deribit(days_back=days_back)
    status_icon = "✓" if result["status"] == "success" else "✗"
    print(f"{status_icon} {result['symbol']}: {result}")

    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
