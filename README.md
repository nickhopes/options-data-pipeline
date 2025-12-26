# Options Data Pipeline

Standalone data pipeline service for crypto OHLC and options data collection and aggregation.

## Features

- **Data Collection**: Binance (BTC/ETH/SOL), Hyperliquid (HYPE), Deribit (BTC Options)
- **Data Aggregation**: Daily/Weekly/Monthly session aggregation
- **Orchestration**: Prefect 2.x with Web UI
- **Monitoring**: Job status, automatic retries, notifications
- **Docker**: Ready for deployment

## Quick Start

### 1. Clone and Configure

```bash
cd options-data-pipeline
cp .env.example .env
# Edit .env with your database credentials
```

### 2. Start with Docker Compose

```bash
docker-compose up -d
```

### 3. Open Prefect UI

Navigate to http://localhost:4200 to see all scheduled jobs.

## Project Structure

```
options-data-pipeline/
├── src/pipeline/
│   ├── collectors/         # Data collection modules
│   │   ├── binance.py      # BTC/ETH/SOL OHLC sync
│   │   ├── hyperliquid.py  # HYPE OHLC sync
│   │   └── deribit.py      # BTC option trades
│   ├── aggregators/        # Data aggregation modules
│   │   ├── base.py         # Base aggregator class
│   │   └── daily_sessions.py
│   ├── flows/              # Prefect flows
│   │   ├── collection.py   # Collection flows
│   │   ├── aggregation.py  # Aggregation flows
│   │   └── main.py         # Entry point
│   ├── config.py           # Configuration
│   └── db.py               # Database utilities
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── pyproject.toml
```

## Schedules

| Flow | Schedule | Description |
|------|----------|-------------|
| Binance Sync | `*/5 * * * *` | Every 5 minutes |
| Hyperliquid Sync | `0 * * * *` | Hourly |
| Deribit Sync | `0 * * * *` | Hourly |
| Option OHLC | `5 * * * *` | Hourly at :05 |
| Daily Aggregation | `0 11 * * *` | 11:00 UTC daily |
| Weekly Aggregation | `0 11 * * 5` | Friday 11:00 UTC |
| Monthly Aggregation | `0 11 * * 5` | Last Friday 11:00 UTC |

## Environment Variables

```bash
# Database
DB_HOST=62.171.161.104
DB_PORT=5432
DB_NAME=hexdb
DB_USER=hexuser
DB_PASSWORD=your_password

# Prefect
PREFECT_API_URL=http://prefect-server:4200/api

# Logging
LOG_LEVEL=INFO
```

## Development

### Run Locally

```bash
# Install dependencies
pip install -e .

# Run a single flow manually
python -c "from pipeline.flows.collection import binance_sync_flow; binance_sync_flow()"

# Start the pipeline with all schedules
python -m pipeline.flows.main
```

### Run Tests

```bash
pip install -e ".[dev]"
pytest
```

## Docker Services

| Service | Port | Description |
|---------|------|-------------|
| prefect-server | 4200 | Prefect UI & API |
| pipeline-worker | - | Executes scheduled jobs |

## Monitoring

1. **Prefect UI** (http://localhost:4200):
   - View all flow runs
   - Check job status
   - Restart failed jobs
   - View logs

2. **Health Check**:
   ```bash
   docker exec pipeline-worker python scripts/healthcheck.py
   ```

## Migration from options_tools

This service is extracted from the `scripts/` directory of `options_tools` project.
It runs independently and connects to the same PostgreSQL database.

### Parallel Running

During transition, you can run both:
1. Keep old cron jobs in `options_tools`
2. Start this pipeline
3. Monitor both, compare data
4. Disable old cron when confident

### Cutover

1. Stop aggregator in options_tools: `docker-compose stop aggregator`
2. Verify new pipeline works
3. Remove old aggregator service from options_tools docker-compose.yml
