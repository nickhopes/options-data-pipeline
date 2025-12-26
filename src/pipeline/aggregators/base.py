"""
Base Aggregator Class for Options Sessions.
Provides common functionality for all session aggregators.
"""

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from pipeline.db import get_db

logger = logging.getLogger(__name__)


class BaseAggregator:
    """Base class for session aggregation."""

    def __init__(self, source: str = "binance", instrument: str = "BTCUSDT"):
        """
        Initialize the aggregator.

        Args:
            source: Data source (e.g., 'binance')
            instrument: Trading instrument (e.g., 'BTCUSDT')
        """
        self.source = source
        self.instrument = instrument
        self.db = get_db()
        self.logger = logging.getLogger(self.__class__.__name__)

    def calculate_percentage_change(self, open_price: Decimal, target_price: Decimal) -> Decimal:
        """
        Calculate percentage change from open to target price.

        Args:
            open_price: Opening price
            target_price: Target price (high, low, or close)

        Returns:
            Percentage change rounded to 4 decimal places
        """
        if open_price == 0:
            return Decimal(0)

        change = ((target_price - open_price) / open_price) * 100
        return round(Decimal(str(change)), 4)

    def find_extremums(self, hourly_data: List[Dict]) -> Tuple[Dict, Dict]:
        """
        Find first and second extremums in hourly data.

        Args:
            hourly_data: List of hourly OHLC records

        Returns:
            Tuple of (first_extremum, second_extremum) dictionaries
        """
        if not hourly_data:
            return ({}, {})

        extremums = []

        for i, record in enumerate(hourly_data):
            open_price = Decimal(str(record["open"]))
            high_price = Decimal(str(record["high"]))
            low_price = Decimal(str(record["low"]))

            ch_high = self.calculate_percentage_change(open_price, high_price)
            ch_low = self.calculate_percentage_change(open_price, low_price)

            if abs(ch_high) > abs(ch_low):
                extremums.append({"value": ch_high, "time": i, "type": "HIGH"})
                if ch_low != 0:
                    extremums.append({"value": ch_low, "time": i, "type": "LOW"})
            else:
                if ch_low != 0:
                    extremums.append({"value": ch_low, "time": i, "type": "LOW"})
                extremums.append({"value": ch_high, "time": i, "type": "HIGH"})

        extremums.sort(key=lambda x: abs(x["value"]), reverse=True)

        first_extremum = extremums[0] if len(extremums) > 0 else {}
        second_extremum = extremums[1] if len(extremums) > 1 else {}

        return first_extremum, second_extremum

    def aggregate_ohlc(self, records: List[Dict]) -> Dict:
        """
        Aggregate multiple records into single OHLC.

        Args:
            records: List of OHLC records

        Returns:
            Aggregated OHLC dictionary
        """
        if not records:
            return {}

        records.sort(key=lambda x: x.get("open_time") or x.get("datetime"))

        open_price = Decimal(str(records[0]["open"]))
        close_price = Decimal(str(records[-1]["close"]))

        high_price = Decimal(0)
        low_price = Decimal("999999999")
        ch_high_time = 0
        ch_low_time = 0

        for i, record in enumerate(records):
            rec_high = Decimal(str(record["high"]))
            rec_low = Decimal(str(record["low"]))

            if rec_high > high_price:
                high_price = rec_high
                ch_high_time = i

            if rec_low < low_price:
                low_price = rec_low
                ch_low_time = i

        move = high_price - low_price

        ch_high = self.calculate_percentage_change(open_price, high_price)
        ch_low = self.calculate_percentage_change(open_price, low_price)
        ch_close = self.calculate_percentage_change(open_price, close_price)

        if abs(ch_high) > abs(ch_low):
            ch_max = ch_high
        else:
            ch_max = ch_low

        if ch_high_time < ch_low_time:
            first_extremum_type = "HIGH"
        elif ch_low_time < ch_high_time:
            first_extremum_type = "LOW"
        else:
            first_extremum_type = "HIGH" if abs(ch_high) >= abs(ch_low) else "LOW"

        return {
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "move": move,
            "chhigh": ch_high,
            "chlow": ch_low,
            "chclose": ch_close,
            "chmax": ch_max,
            "chhightime": ch_high_time,
            "chlowtime": ch_low_time,
            "firstextremumtype": first_extremum_type,
        }

    def insert_or_update(self, table_name: str, data: Dict, unique_key: str = "datetime"):
        """
        Insert or update record in the database.

        Args:
            table_name: Name of the table
            data: Data dictionary to insert/update
            unique_key: Column name for uniqueness check
        """
        with self.db.connection() as conn:
            cursor = conn.cursor()

            columns = list(data.keys())
            values = list(data.values())
            placeholders = ["%s"] * len(columns)

            update_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in columns if col != unique_key]
            )

            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ({unique_key}) DO UPDATE
                SET {update_clause}, updated_at = NOW()
            """

            cursor.execute(query, values)
            conn.commit()

            self.logger.debug(f"Successfully inserted/updated record in {table_name}")

    def batch_insert_or_update(
        self,
        table_name: str,
        data_list: List[Dict],
        unique_key: str = "datetime",
        batch_size: int = 100,
    ):
        """
        Batch insert or update multiple records.

        Args:
            table_name: Name of the table
            data_list: List of data dictionaries to insert/update
            unique_key: Column name for uniqueness check
            batch_size: Number of records per batch
        """
        if not data_list:
            return

        with self.db.connection() as conn:
            cursor = conn.cursor()

            for i in range(0, len(data_list), batch_size):
                batch = data_list[i : i + batch_size]

                columns = list(batch[0].keys())

                values_list = []
                for data in batch:
                    values_list.extend(data.values())

                placeholders = []
                for _ in batch:
                    placeholders.append("(" + ", ".join(["%s"] * len(columns)) + ")")

                update_clause = ", ".join(
                    [f"{col} = EXCLUDED.{col}" for col in columns if col != unique_key]
                )

                query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES {', '.join(placeholders)}
                    ON CONFLICT ({unique_key}) DO UPDATE
                    SET {update_clause}, updated_at = NOW()
                """

                cursor.execute(query, values_list)

            conn.commit()
            self.logger.info(
                f"Successfully batch inserted/updated {len(data_list)} records in {table_name}"
            )

    def get_last_processed_date(self, table_name: str) -> Optional[Any]:
        """
        Get the last processed date from the target table.

        Args:
            table_name: Name of the target table

        Returns:
            Last processed datetime or None if table is empty
        """
        with self.db.cursor() as cursor:
            query = f"""
                SELECT MAX(datetime) as last_date
                FROM {table_name}
                WHERE source = %s AND instrument = %s
            """

            cursor.execute(query, (self.source, self.instrument))
            result = cursor.fetchone()

            if result and result["last_date"]:
                return result["last_date"]

            return None

    def run(self, start_date=None, end_date=None) -> Dict:
        """
        Run the aggregation process.

        Args:
            start_date: Start date for aggregation (optional)
            end_date: End date for aggregation (optional)

        Returns:
            Dict with results
        """
        raise NotImplementedError("Subclasses must implement the run method")
