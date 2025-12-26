"""
Option OHLC Aggregator.
Aggregates option trades into hourly OHLC candles.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from pipeline.db import get_db

logger = logging.getLogger(__name__)


class OptionOHLCAggregator:
    """Aggregator for BTC option OHLC from trades."""

    def __init__(self):
        """Initialize option OHLC aggregator."""
        self.logger = logging.getLogger(f"{__name__}.OptionOHLCAggregator")
        self.source_table = "btc_deribit_option_trades"
        self.target_table = "btc_option_ohlc_hourly"

    def aggregate_new_hours(self, hours_back: int = 3) -> int:
        """
        Aggregate OHLC for recent hours that have new trades.
        Uses fast SQL aggregation.

        Args:
            hours_back: How many hours back to check for new data

        Returns:
            Number of records aggregated
        """
        now = datetime.now(timezone.utc)
        start_hour = (now - timedelta(hours=hours_back)).replace(
            minute=0, second=0, microsecond=0
        )

        self.logger.info(f"Aggregating OHLC from {start_hour} to {now}")

        # Fast SQL aggregation for recent hours
        # Distance formula:
        # - For CALL: if strike > index_price -> negative (OTM), if strike < index_price -> positive (ITM)
        #   distance = (index_price - strike) / index_price * 100
        # - For PUT: if strike < index_price -> negative (OTM), if strike > index_price -> positive (ITM)
        #   distance = (strike - index_price) / index_price * 100
        sql = """
        INSERT INTO btc_option_ohlc_hourly (
            hour_timestamp, instrument_name, expiry_date, strike, option_type,
            open_price, high_price, low_price, close_price,
            open_iv, high_iv, low_iv, close_iv,
            vwap, volume, trade_count, trade_ids,
            mark_price_open, mark_price_high, mark_price_low, mark_price_close,
            hours_to_expiry, index_price, distance
        )
        SELECT
            DATE_TRUNC('hour', t.timestamp) as hour_timestamp,
            t.instrument_name,
            t.expiry_date,
            t.strike,
            t.option_type,
            (ARRAY_AGG(t.price ORDER BY t.timestamp))[1] as open_price,
            MAX(t.price) as high_price,
            MIN(t.price) as low_price,
            (ARRAY_AGG(t.price ORDER BY t.timestamp DESC))[1] as close_price,
            (ARRAY_AGG(t.iv ORDER BY t.timestamp) FILTER (WHERE t.iv IS NOT NULL))[1] as open_iv,
            MAX(t.iv) as high_iv,
            MIN(t.iv) as low_iv,
            (ARRAY_AGG(t.iv ORDER BY t.timestamp DESC) FILTER (WHERE t.iv IS NOT NULL))[1] as close_iv,
            SUM(t.price * t.amount) / NULLIF(SUM(t.amount), 0) as vwap,
            SUM(t.amount) as volume,
            COUNT(*) as trade_count,
            STRING_AGG(t.trade_id::text, ',' ORDER BY t.timestamp) as trade_ids,
            (ARRAY_AGG(t.mark_price ORDER BY t.timestamp) FILTER (WHERE t.mark_price IS NOT NULL))[1] as mark_price_open,
            MAX(t.mark_price) as mark_price_high,
            MIN(t.mark_price) as mark_price_low,
            (ARRAY_AGG(t.mark_price ORDER BY t.timestamp DESC) FILTER (WHERE t.mark_price IS NOT NULL))[1] as mark_price_close,
            GREATEST(1, EXTRACT(EPOCH FROM (t.expiry_date + TIME '08:00' - DATE_TRUNC('hour', t.timestamp))) / 3600)::int as hours_to_expiry,
            (ARRAY_AGG(t.index_price ORDER BY t.timestamp) FILTER (WHERE t.index_price IS NOT NULL))[1] as index_price,
            CASE
                WHEN t.option_type = 'C' THEN
                    ((ARRAY_AGG(t.index_price ORDER BY t.timestamp) FILTER (WHERE t.index_price IS NOT NULL))[1] - t.strike)
                    / NULLIF((ARRAY_AGG(t.index_price ORDER BY t.timestamp) FILTER (WHERE t.index_price IS NOT NULL))[1], 0) * 100
                WHEN t.option_type = 'P' THEN
                    (t.strike - (ARRAY_AGG(t.index_price ORDER BY t.timestamp) FILTER (WHERE t.index_price IS NOT NULL))[1])
                    / NULLIF((ARRAY_AGG(t.index_price ORDER BY t.timestamp) FILTER (WHERE t.index_price IS NOT NULL))[1], 0) * 100
                ELSE NULL
            END as distance
        FROM btc_deribit_option_trades t
        WHERE t.timestamp >= %s
        GROUP BY DATE_TRUNC('hour', t.timestamp), t.instrument_name, t.expiry_date, t.strike, t.option_type
        ON CONFLICT (hour_timestamp, instrument_name) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            open_iv = EXCLUDED.open_iv,
            high_iv = EXCLUDED.high_iv,
            low_iv = EXCLUDED.low_iv,
            close_iv = EXCLUDED.close_iv,
            vwap = EXCLUDED.vwap,
            volume = EXCLUDED.volume,
            trade_count = EXCLUDED.trade_count,
            trade_ids = EXCLUDED.trade_ids,
            mark_price_open = EXCLUDED.mark_price_open,
            mark_price_high = EXCLUDED.mark_price_high,
            mark_price_low = EXCLUDED.mark_price_low,
            mark_price_close = EXCLUDED.mark_price_close,
            hours_to_expiry = EXCLUDED.hours_to_expiry,
            index_price = EXCLUDED.index_price,
            distance = EXCLUDED.distance
        """

        db = get_db()
        with db.cursor() as cursor:
            cursor.execute(sql, (start_hour,))
            inserted = cursor.rowcount
            db.commit()

        self.logger.info(f"Aggregated {inserted:,} OHLC records")
        return inserted

    def get_stats(self) -> Dict:
        """Get current OHLC stats."""
        db = get_db()
        with db.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT instrument_name) as instruments,
                    MAX(hour_timestamp) as latest_hour
                FROM btc_option_ohlc_hourly
            """)
            row = cursor.fetchone()

        return {
            "total_records": row["count"] if isinstance(row, dict) else row[0],
            "instruments": row["count_1"] if isinstance(row, dict) else row[1],
            "latest_hour": row["max"] if isinstance(row, dict) else row[2],
        }

    def run(self, hours_back: int = 3) -> Dict:
        """
        Run the option OHLC aggregation process.

        Args:
            hours_back: How many hours back to process

        Returns:
            Result dict with status and counts
        """
        self.logger.info("Starting BTC Option OHLC hourly aggregation")

        try:
            # Get stats before
            stats_before = self.get_stats()
            self.logger.info(
                f"Before: {stats_before['total_records']:,} records, "
                f"{stats_before['instruments']:,} instruments"
            )

            # Run aggregation
            inserted = self.aggregate_new_hours(hours_back)

            # Get stats after
            stats_after = self.get_stats()
            new_records = stats_after["total_records"] - stats_before["total_records"]

            self.logger.info(
                f"After: {stats_after['total_records']:,} records, "
                f"{stats_after['instruments']:,} instruments"
            )
            self.logger.info(f"New records: {new_records:,}")

            return {
                "status": "success",
                "aggregated": inserted,
                "new_records": new_records,
                "total_records": stats_after["total_records"],
                "latest_hour": str(stats_after["latest_hour"]),
            }

        except Exception as e:
            self.logger.error(f"Option OHLC aggregation failed: {e}")
            return {
                "status": "error",
                "message": str(e),
            }


def aggregate_option_ohlc(hours_back: int = 3) -> Dict:
    """Aggregate BTC option OHLC."""
    aggregator = OptionOHLCAggregator()
    return aggregator.run(hours_back)


def main() -> int:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    result = aggregate_option_ohlc()
    success = result["status"] == "success"

    status_icon = "✓" if success else "✗"
    print(f"{status_icon} BTC Option OHLC: {result}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
