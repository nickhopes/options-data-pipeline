"""
Monthly Sessions Aggregator.
Aggregates daily sessions into monthly options sessions (last Friday → last Friday next month).
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
        "target_table": "binance_ohlc_btc_monthly_sessions",
        "source_table": "binance_ohlc_btc_daily_sessions",
        "source": "binance",
    },
    "ETHUSDT": {
        "target_table": "binance_ohlc_eth_monthly_sessions",
        "source_table": "binance_ohlc_eth_daily_sessions",
        "source": "binance",
    },
    "SOLUSDT": {
        "target_table": "binance_ohlc_sol_monthly_sessions",
        "source_table": "binance_ohlc_sol_daily_sessions",
        "source": "binance",
    },
}


class MonthlySessionAggregator(BaseAggregator):
    """Aggregator for monthly options sessions."""

    def __init__(self, instrument: str = "BTCUSDT"):
        """Initialize monthly session aggregator."""
        if instrument not in INSTRUMENT_CONFIG:
            raise ValueError(f"Unsupported instrument: {instrument}")

        config = INSTRUMENT_CONFIG[instrument]
        super().__init__(source=config["source"], instrument=instrument)

        self.target_table = config["target_table"]
        self.source_table = config["source_table"]

    def find_last_friday(self, year: int, month: int) -> datetime:
        """Find the last Friday of a given month at 08:00 UTC."""
        if month == 12:
            last_day = datetime(year + 1, 1, 1, 8, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
        else:
            last_day = datetime(year, month + 1, 1, 8, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)

        last_day = last_day.replace(hour=8, minute=0, second=0, microsecond=0)

        # Friday = weekday 4
        while last_day.weekday() != 4:
            last_day -= timedelta(days=1)

        return last_day

    def get_session_boundaries(self, date: datetime) -> tuple:
        """
        Get session start and end times for a given date.
        Monthly session: Last Friday 08:00 UTC → next month's Last Friday 07:00 UTC
        """
        year = date.year
        month = date.month
        last_friday = self.find_last_friday(year, month)

        # If date is before this month's last Friday, use previous month
        if date < last_friday:
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1
            last_friday = self.find_last_friday(year, month)

        session_start = last_friday

        # Find last Friday of next month
        next_month = month + 1
        next_year = year
        if next_month > 12:
            next_month = 1
            next_year += 1

        session_end = self.find_last_friday(next_year, next_month)

        return session_start, session_end

    def fetch_daily_sessions(self, session_start: datetime, session_end: datetime) -> List[Dict]:
        """Fetch daily sessions for a monthly period."""
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
        months: Optional[int] = None,
    ) -> Dict:
        """Run the monthly aggregation process."""
        self.logger.info(f"Starting monthly session aggregation for {self.source}/{self.instrument}")

        # Determine date range
        if not end_date:
            end_date = datetime.now(timezone.utc)

        if not start_date:
            if months:
                start_date = end_date - timedelta(days=30 * months)
            else:
                last_processed = self.get_last_processed_date(self.target_table)
                if last_processed:
                    start_date = last_processed + timedelta(days=30)
                else:
                    start_date = datetime.now(timezone.utc) - timedelta(days=730)

        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        self.logger.info(f"Processing monthly sessions from {start_date.date()} to {end_date.date()}")

        # Process each month
        processed_count = 0
        error_count = 0
        batch_data = []
        current_date = start_date

        while current_date <= end_date:
            try:
                session_start, session_end = self.get_session_boundaries(current_date)
                daily_sessions = self.fetch_daily_sessions(session_start, session_end)

                if len(daily_sessions) < 20:
                    self.logger.warning(
                        f"Insufficient data for {current_date.date()}: {len(daily_sessions)} days"
                    )
                    # Move to next month
                    if current_date.month == 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 1)
                    continue

                aggregated = self.aggregate_ohlc(daily_sessions)

                if aggregated:
                    session_hours = int((session_end - session_start).total_seconds() / 3600)
                    session_days = int((session_end - session_start).total_seconds() / 86400)

                    aggregated.update({
                        "datetime": session_start,
                        "source": self.source,
                        "instrument": self.instrument,
                        "counthours": session_hours,
                        "countdays": session_days,
                        "session_end": session_end,
                        "year": session_start.year,
                        "month": session_start.month,
                    })

                    batch_data.append(aggregated)
                    processed_count += 1

                    if len(batch_data) >= 24:
                        self.batch_insert_or_update(self.target_table, batch_data)
                        self.logger.info(f"Batch inserted {len(batch_data)} monthly sessions")
                        batch_data = []

            except Exception as e:
                self.logger.error(f"Error processing monthly session {current_date.date()}: {e}")
                error_count += 1

            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        # Insert remaining
        if batch_data:
            self.batch_insert_or_update(self.target_table, batch_data)
            self.logger.info(f"Final batch inserted {len(batch_data)} monthly sessions")

        self.logger.info(f"Monthly aggregation complete. Processed: {processed_count}, Errors: {error_count}")

        return {
            "instrument": self.instrument,
            "status": "success",
            "processed": processed_count,
            "errors": error_count,
        }


def aggregate_monthly_btc() -> Dict:
    """Aggregate BTC monthly sessions."""
    aggregator = MonthlySessionAggregator("BTCUSDT")
    return aggregator.run()


def aggregate_monthly_eth() -> Dict:
    """Aggregate ETH monthly sessions."""
    aggregator = MonthlySessionAggregator("ETHUSDT")
    return aggregator.run()


def aggregate_monthly_sol() -> Dict:
    """Aggregate SOL monthly sessions."""
    aggregator = MonthlySessionAggregator("SOLUSDT")
    return aggregator.run()


def aggregate_all_monthly() -> List[Dict]:
    """Aggregate monthly sessions for all instruments."""
    results = []
    for instrument in INSTRUMENT_CONFIG:
        try:
            aggregator = MonthlySessionAggregator(instrument)
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

    results = aggregate_all_monthly()
    success = all(r["status"] == "success" for r in results)

    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        print(f"{status_icon} {r['instrument']}: {r}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
