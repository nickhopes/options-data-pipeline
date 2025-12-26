"""
Daily Sessions Aggregator.
Aggregates hourly OHLC data into daily options sessions (08:00 UTC → 07:00 UTC next day, 24 hours).
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
        "target_table": "binance_ohlc_btc_daily_sessions",
        "source_table": "binance_ohlc_btc",
        "use_timestamp_ms": True,
        "source": "binance",
    },
    "ETHUSDT": {
        "target_table": "binance_ohlc_eth_daily_sessions",
        "source_table": "binance_ohlc_eth",
        "use_timestamp_ms": True,
        "source": "binance",
    },
    "SOLUSDT": {
        "target_table": "binance_ohlc_sol_daily_sessions",
        "source_table": "binance_ohlc_sol",
        "use_timestamp_ms": True,
        "source": "binance",
    },
    "HYPEUSDC": {
        "target_table": "hyperliquid_ohlc_hype_daily_sessions",
        "source_table": "hyperliquid_hype_ohlc",
        "use_timestamp_ms": False,
        "source": "hyperliquid",
    },
}


class DailySessionAggregator(BaseAggregator):
    """Aggregator for daily options sessions."""

    def __init__(self, instrument: str = "BTCUSDT"):
        """Initialize daily session aggregator."""
        if instrument not in INSTRUMENT_CONFIG:
            raise ValueError(f"Unsupported instrument: {instrument}")

        config = INSTRUMENT_CONFIG[instrument]
        super().__init__(source=config["source"], instrument=instrument)

        self.target_table = config["target_table"]
        self.source_table = config["source_table"]
        self.use_timestamp_ms = config["use_timestamp_ms"]

    def get_session_boundaries(self, date: datetime) -> tuple:
        """
        Get session start and end times for a given date.
        Daily session: 08:00 UTC → 07:00 UTC next day (24 hours)
        """
        session_start = date.replace(hour=8, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        session_end = session_start + timedelta(hours=24)
        return session_start, session_end

    def fetch_hourly_data_bulk(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[datetime, List[Dict]]:
        """Fetch hourly data for multiple days at once."""
        first_session_start = start_date.replace(
            hour=8, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        last_session_start = end_date.replace(
            hour=8, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        last_session_end = last_session_start + timedelta(hours=24)

        db = get_db()
        with db.cursor() as cursor:
            if self.use_timestamp_ms:
                start_ms = int(first_session_start.timestamp() * 1000)
                end_ms = int(last_session_end.timestamp() * 1000)

                query = f"""
                    SELECT open_time, open, high, low, close
                    FROM {self.source_table}
                    WHERE open_time >= %s AND open_time < %s
                    ORDER BY open_time ASC
                """
                cursor.execute(query, (start_ms, end_ms))
            else:
                query = f"""
                    SELECT datetime, open, high, low, close
                    FROM {self.source_table}
                    WHERE datetime >= %s AND datetime < %s
                    ORDER BY datetime ASC
                """
                cursor.execute(query, (first_session_start, last_session_end))

            results = cursor.fetchall()

        # Initialize session containers
        sessions_data = {}
        current_date = start_date
        while current_date <= end_date:
            session_start, _ = self.get_session_boundaries(current_date)
            sessions_data[session_start] = []
            current_date += timedelta(days=1)

        # Distribute records to sessions
        for record in results:
            record_dict = dict(record)
            if self.use_timestamp_ms:
                record_dict["datetime"] = datetime.fromtimestamp(
                    record_dict["open_time"] / 1000, tz=timezone.utc
                )
            else:
                dt = record_dict["datetime"]
                if dt.tzinfo is None:
                    record_dict["datetime"] = dt.replace(tzinfo=timezone.utc)

            for session_start in sessions_data.keys():
                session_end = session_start + timedelta(hours=24)
                if session_start <= record_dict["datetime"] < session_end:
                    sessions_data[session_start].append(record_dict)
                    break

        return sessions_data

    def run(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Dict:
        """
        Run the daily aggregation process.

        Returns:
            Dict with aggregation results
        """
        self.logger.info(f"Starting daily session aggregation for {self.source}/{self.instrument}")

        # Determine date range
        if not start_date:
            last_processed = self.get_last_processed_date(self.target_table)
            if last_processed:
                start_date = last_processed + timedelta(days=1)
            else:
                start_date = datetime.now(timezone.utc) - timedelta(days=365)

        if not end_date:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)

        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        self.logger.info(f"Processing sessions from {start_date.date()} to {end_date.date()}")

        # Fetch all hourly data at once
        bulk_end_date = end_date if not limit else start_date + timedelta(days=limit - 1)
        sessions_hourly_data = self.fetch_hourly_data_bulk(start_date, bulk_end_date)
        self.logger.info(f"Fetched hourly data for {len(sessions_hourly_data)} sessions")

        # Process each day
        processed_count = 0
        error_count = 0
        batch_data = []
        current_date = start_date

        while current_date <= end_date:
            if limit and processed_count >= limit:
                break

            try:
                session_start, _ = self.get_session_boundaries(current_date)
                hourly_data = sessions_hourly_data.get(session_start, [])

                if len(hourly_data) < 20:
                    self.logger.warning(
                        f"Insufficient data for {current_date.date()}: {len(hourly_data)} hours"
                    )
                    current_date += timedelta(days=1)
                    continue

                aggregated = self.aggregate_ohlc(hourly_data)

                if aggregated:
                    aggregated.update({
                        "datetime": session_start,
                        "source": self.source,
                        "instrument": self.instrument,
                        "session_hours": 24,
                    })

                    batch_data.append(aggregated)
                    processed_count += 1

                    if len(batch_data) >= 100:
                        self.batch_insert_or_update(self.target_table, batch_data)
                        self.logger.info(f"Batch inserted {len(batch_data)} sessions")
                        batch_data = []

            except Exception as e:
                self.logger.error(f"Error processing session {current_date.date()}: {e}")
                error_count += 1

            current_date += timedelta(days=1)

        # Insert remaining
        if batch_data:
            self.batch_insert_or_update(self.target_table, batch_data)
            self.logger.info(f"Final batch inserted {len(batch_data)} sessions")

        self.logger.info(f"Daily aggregation complete. Processed: {processed_count}, Errors: {error_count}")

        return {
            "instrument": self.instrument,
            "status": "success",
            "processed": processed_count,
            "errors": error_count,
        }


def aggregate_daily_btc() -> Dict:
    """Aggregate BTC daily sessions."""
    aggregator = DailySessionAggregator("BTCUSDT")
    return aggregator.run()


def aggregate_daily_eth() -> Dict:
    """Aggregate ETH daily sessions."""
    aggregator = DailySessionAggregator("ETHUSDT")
    return aggregator.run()


def aggregate_daily_sol() -> Dict:
    """Aggregate SOL daily sessions."""
    aggregator = DailySessionAggregator("SOLUSDT")
    return aggregator.run()


def aggregate_daily_hype() -> Dict:
    """Aggregate HYPE daily sessions."""
    aggregator = DailySessionAggregator("HYPEUSDC")
    return aggregator.run()


def aggregate_all_daily() -> List[Dict]:
    """Aggregate daily sessions for all instruments."""
    results = []
    for instrument in INSTRUMENT_CONFIG:
        try:
            aggregator = DailySessionAggregator(instrument)
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

    results = aggregate_all_daily()
    success = all(r["status"] == "success" for r in results)

    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        print(f"{status_icon} {r['instrument']}: {r}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
