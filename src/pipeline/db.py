"""Database connection utilities."""

import logging
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from pipeline.config import get_db_config, get_settings

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection manager."""

    def __init__(self, config: Optional[dict] = None):
        """Initialize with optional config override."""
        self.config = config or get_db_config()

    def get_connection(self, cursor_factory=RealDictCursor):
        """Get a new database connection."""
        return psycopg2.connect(**self.config, cursor_factory=cursor_factory)

    @contextmanager
    def connection(self, cursor_factory=RealDictCursor) -> Generator:
        """Context manager for database connection."""
        conn = None
        try:
            conn = self.get_connection(cursor_factory)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def cursor(self, cursor_factory=RealDictCursor) -> Generator:
        """Context manager for database cursor."""
        with self.connection(cursor_factory) as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


# Global database connection instance
_db: Optional[DatabaseConnection] = None


def get_db() -> DatabaseConnection:
    """Get or create database connection instance."""
    global _db
    if _db is None:
        _db = DatabaseConnection()
    return _db


def execute_query(query: str, params: tuple = None, fetch: bool = True):
    """Execute a query and optionally fetch results."""
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(query, params)
        if fetch:
            return cursor.fetchall()
        return None


def execute_many(query: str, params_list: list):
    """Execute a query with multiple parameter sets."""
    db = get_db()
    with db.cursor() as cursor:
        cursor.executemany(query, params_list)


def test_connection() -> bool:
    """Test database connection."""
    try:
        db = get_db()
        with db.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False
