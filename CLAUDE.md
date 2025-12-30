# Options Data Pipeline - Claude Instructions

## Project Overview

Standalone data pipeline service for crypto OHLC and options data collection and aggregation.
Deployed on VPS at **62.171.161.104** with Prefect 3.x orchestration.

## Quick Reference

### Access
- **Prefect UI**: http://62.171.161.104:4200
- **GitHub**: https://github.com/nickhopes/options-data-pipeline

### VPS Connection
```bash
sshpass -p '0aI6Zi4M4EZh' ssh root@62.171.161.104
```

### Database
- Host: 62.171.161.104
- Port: 5432
- Database: hexdb
- User: hexuser
- Password: HexUser2024SecurePass

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    VPS (62.171.161.104)                 │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────┐  ┌─────────────────────────┐   │
│  │   prefect-server    │  │    pipeline-worker      │   │
│  │   (port 4200)       │  │                         │   │
│  │   - UI              │  │  - Runs scheduled flows │   │
│  │   - API             │◄─┤  - Collects data        │   │
│  │   - SQLite DB       │  │  - Aggregates sessions  │   │
│  └─────────────────────┘  └─────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │     PostgreSQL      │
                    │  (62.171.161.104)   │
                    │                     │
                    │  - hexdb            │
                    │  - OHLC tables      │
                    │  - Options trades   │
                    └─────────────────────┘
```

## Data Flows

### Collection (from exchanges)
| Flow | Source | Target Table | Schedule |
|------|--------|--------------|----------|
| binance-sync | Binance API | btc_ohlc_1h, eth_ohlc_1h, sol_ohlc_1h | */5 * * * * |
| hyperliquid-sync | Hyperliquid API | hype_ohlc_1h | 0 * * * * |
| deribit-sync | Deribit API | btc_deribit_option_trades | 0 * * * * |

### Aggregation (internal processing)
| Flow | Source | Target | Schedule |
|------|--------|--------|----------|
| option-ohlc | btc_deribit_option_trades | btc_option_ohlc_hourly | 5 * * * * |
| daily-aggregation | *_ohlc_1h | *_ohlc_daily | 0 11 * * * |
| weekly-aggregation | *_ohlc_daily | *_ohlc_weekly | 0 11 * * 5 |
| monthly-aggregation | *_ohlc_daily | *_ohlc_monthly | 0 11 * * 5 (last Fri) |

## Management Commands

Use the `/pipeline` skill or run these commands:

### Check Status
```bash
sshpass -p '0aI6Zi4M4EZh' ssh root@62.171.161.104 'cd /root/options-data-pipeline && docker compose ps -a'
```

### View Logs
```bash
sshpass -p '0aI6Zi4M4EZh' ssh root@62.171.161.104 'docker logs pipeline-worker --tail 50'
```

### Restart Services
```bash
sshpass -p '0aI6Zi4M4EZh' ssh root@62.171.161.104 'cd /root/options-data-pipeline && docker compose restart'
```

### Update & Redeploy
```bash
git push  # Push local changes first
sshpass -p '0aI6Zi4M4EZh' ssh root@62.171.161.104 'cd /root/options-data-pipeline && git pull && docker compose up -d --build'
```

## Project Structure

```
src/pipeline/
├── collectors/          # Data collection from exchanges
│   ├── binance.py       # BTC/ETH/SOL OHLC
│   ├── hyperliquid.py   # HYPE OHLC
│   └── deribit.py       # BTC options trades
├── aggregators/         # Data aggregation
│   ├── base.py          # Base aggregator class
│   ├── daily_sessions.py
│   ├── weekly_sessions.py
│   ├── monthly_sessions.py
│   └── option_ohlc.py   # Options OHLC from trades
├── flows/               # Prefect flows
│   ├── collection.py    # Collection flows
│   ├── aggregation.py   # Aggregation flows
│   └── main.py          # Entry point with schedules
├── config.py            # Configuration (Pydantic)
└── db.py                # Database utilities
```

## Known Issues

### 1. SQLite "Database Locked" Errors
- **Cause**: Prefect server uses SQLite which struggles with concurrent writes
- **Impact**: ~10-15% flow run failures
- **Solution**: Migrate Prefect to PostgreSQL backend (future improvement)

### 2. Deribit API 400 Errors for Future Dates
- **Cause**: Requesting trades for dates with no data yet
- **Impact**: None - flows complete successfully despite warnings
- **Status**: Expected behavior, no fix needed

## Development Workflow

1. Make changes locally in `/Users/ostmn/Desktop/AI/coding/options-data-pipeline`
2. Test if needed: `python -c "from pipeline.flows.collection import binance_sync_flow; binance_sync_flow()"`
3. Commit and push: `git add . && git commit -m "message" && git push`
4. Deploy: SSH to VPS and `git pull && docker compose up -d --build`

## Monitoring Checklist

When checking pipeline health:
1. Check container status (both should be Up)
2. Check flow run statistics (COMPLETED > 80%)
3. Review worker logs for errors
4. Check failed runs for patterns
5. Verify data in PostgreSQL if issues persist
