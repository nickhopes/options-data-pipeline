#!/usr/bin/env python3
"""Health check script for the pipeline worker."""

import sys
import os

def check_prefect_connection():
    """Check if we can connect to Prefect server."""
    try:
        import httpx
        api_url = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
        response = httpx.get(f"{api_url}/health", timeout=5.0)
        return response.status_code == 200
    except Exception as e:
        print(f"Prefect connection failed: {e}")
        return False

def check_database_connection():
    """Check if we can connect to the database."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "62.171.161.104"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=os.getenv("DB_NAME", "hexdb"),
            user=os.getenv("DB_USER", "hexuser"),
            password=os.getenv("DB_PASSWORD", ""),
            connect_timeout=5
        )
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

def main():
    """Run all health checks."""
    checks = [
        ("Prefect", check_prefect_connection),
        ("Database", check_database_connection),
    ]

    all_passed = True
    for name, check_fn in checks:
        try:
            result = check_fn()
            status = "OK" if result else "FAILED"
            print(f"{name}: {status}")
            if not result:
                all_passed = False
        except Exception as e:
            print(f"{name}: ERROR - {e}")
            all_passed = False

    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    main()
