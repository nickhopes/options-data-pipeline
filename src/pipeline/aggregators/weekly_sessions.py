"""
Weekly Sessions Aggregator.
Aggregates daily sessions into weekly options sessions (Friday 08:00 UTC → Friday 07:00 UTC, 7 days).
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pipeline.aggregators.base import BaseAggregator
from pipeline.db import get_db

logger = logging.getLogger(__name__)


# Instrument configuration
INSTRUMENT_CONFIG = {
    "BTCUSDT": {
        "target_table": "binance_ohlc_btc_weekly_sessions",
        "source_table": "binance_ohlc_btc_daily_sessions",
        "source": "binance",
    },
    "ETHUSDT": {
        "target_table": "binance_ohlc_eth_weekly_sessions",
        "source_table": "binance_ohlc_eth_daily_sessions",
        "source": "binance",
    },
    "SOLUSDT": {
        "target_table": "binance_ohlc_sol_weekly_sessions",
        "source_table": "binance_ohlc_sol_daily_sessions",
        "source": "binance",
    },
}


class WeeklySessionAggregator(BaseAggregator):
    """Aggregator for weekly options sessions."""

    def __init__(self, instrument: str = "BTCUSDT"):
        """Initialize weekly session aggregator."""
        if instrument not in INSTRUMENT_CONFIG:
            raise ValueError(f"Unsupported instrument: {instrument}")

        config = INSTRUMENT_CONFIG[instrument]
        super().__init__(source=config["source"], instrument=instrument)

        self.target_table = config["target_table"]
        self.source_table = config["source_table"]

    def get_session_boundaries(self, date: datetime) -> tuple:
        """
        Get session start and end times for a given date.
        Weekly session: Friday 08:00 UTC → next Friday 07:00 UTC (7 days)
        """
        current = date.replace(hour=8, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        # Friday = 4 in weekday()
        days_since_friday = (current.weekday() - 4) % 7
        if days_since_friday == 0 and current.hour < 8:
            days_since_friday = 7

        session_start = current - timedelta(days=days_since_friday)
        session_end = session_start + timedelta(days=7)

        return session_start, session_end

    def fetch_daily_sessions(self, session_start: datetime, session_end: datetime) -> List[Dict]:
        """Fetch daily sessions for a weekly period."""
        db = get_db()
        with db.cursor() as cursor:
            query = f"""
                SELECT datetime, open, high, low, close, chhightime, chlowtime
                FROM {self.source_table}
                WHERE datetime >= %s AND datetime < %s
                  AND source = %s AND instrument = %s
                ORDER BY datetime ASC
            """
            cursor.execute(query, (session_start, session_end, self.source, self.instrument))
            results = cursor.fetchall()

        return [dict(r) for r in results]

    def run(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Dict:
        """Run the weekly aggregation process."""
        self.logger.info(f"Starting weekly session aggregation for {self.source}/{self.instrument}")

        # Determine date range
        if not start_date:
            last_processed = self.get_last_processed_date(self.target_table)
            if last_processed:
                start_date = last_processed + timedelta(days=7)
            else:
                start_date = datetime.now(timezone.utc) - timedelta(days=365)

        if not end_date:
            end_date = datetime.now(timezone.utc) - timedelta(days=7)

        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        self.logger.info(f"Processing weekly sessions from {start_date.date()} to {end_date.date()}")

        # Process each week
        processed_count = 0
        error_count = 0
        batch_data = []
        current_date = start_date

        while current_date <= end_date:
            if limit and processed_count >= limit:
                break

            try:
                session_start, session_end = self.get_session_boundaries(current_date)
                daily_sessions = self.fetch_daily_sessions(session_start, session_end)

                if len(daily_sessions) < 5:
                    self.logger.warning(
                        f"Insufficient data for {current_date.date()}: {len(daily_sessions)} days"
                    )
                    current_date += timedelta(days=7)
                    continue

                aggregated = self.aggregate_ohlc(daily_sessions)

                if aggregated:
                    aggregated.update({
                        "datetime": session_start,
                        "source": self.source,
                        "instrument": self.instrument,
                        "session_hours": 168,
                    })

                    batch_data.append(aggregated)
                    processed_count += 1

                    if len(batch_data) >= 50:
                        self.batch_insert_or_update(self.target_table, batch_data)
                        self.logger.info(f"Batch inserted {len(batch_data)} weekly sessions")
                        batch_data = []

            except Exception as e:
                self.logger.error(f"Error processing weekly session {current_date.date()}: {e}")
                error_count += 1

            current_date += timedelta(days=7)

        # Insert remaining
        if batch_data:
            self.batch_insert_or_update(self.target_table, batch_data)
            self.logger.info(f"Final batch inserted {len(batch_data)} weekly sessions")

        self.logger.info(f"Weekly aggregation complete. Processed: {processed_count}, Errors: {error_count}")

        return {
            "instrument": self.instrument,
            "status": "success",
            "processed": processed_count,
            "errors": error_count,
        }


def aggregate_weekly_btc() -> Dict:
    """Aggregate BTC weekly sessions."""
    aggregator = WeeklySessionAggregator("BTCUSDT")
    return aggregator.run()


def aggregate_weekly_eth() -> Dict:
    """Aggregate ETH weekly sessions."""
    aggregator = WeeklySessionAggregator("ETHUSDT")
    return aggregator.run()


def aggregate_weekly_sol() -> Dict:
    """Aggregate SOL weekly sessions."""
    aggregator = WeeklySessionAggregator("SOLUSDT")
    return aggregator.run()


def aggregate_all_weekly() -> List[Dict]:
    """Aggregate weekly sessions for all instruments."""
    results = []
    for instrument in INSTRUMENT_CONFIG:
        try:
            aggregator = WeeklySessionAggregator(instrument)
            result = aggregator.run()
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to aggregate {instrument}: {e}")
            results.append({"instrument": instrument, "status": "error", "message": str(e)})
    return results


def main() -> int:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    results = aggregate_all_weekly()
    success = all(r["status"] == "success" for r in results)

    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        print(f"{status_icon} {r['instrument']}: {r}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
