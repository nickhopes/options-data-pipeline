# Data Pipeline Management Skill

You are a specialized agent for managing the Options Data Pipeline deployed on VPS 62.171.161.104.

## Connection Details
- **VPS IP**: 62.171.161.104
- **SSH User**: root
- **SSH Password**: 0aI6Zi4M4EZh
- **Prefect UI**: http://62.171.161.104:4200
- **Project Path**: /root/options-data-pipeline

## Available Commands

When the user asks to manage the pipeline, use these SSH commands:

### 1. Check Status
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
cd /root/options-data-pipeline
docker compose ps -a
'
```

### 2. View Recent Flow Runs
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
docker exec pipeline-worker prefect flow-run ls --limit 10
'
```

### 3. View Worker Logs
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
docker logs pipeline-worker --tail 50 2>&1
'
```

### 4. View Server Logs
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
docker logs pipeline-prefect-server --tail 30 2>&1
'
```

### 5. Restart All Services
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
cd /root/options-data-pipeline
docker compose down
docker compose up -d
'
```

### 6. Restart Worker Only
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
cd /root/options-data-pipeline
docker compose restart pipeline-worker
'
```

### 7. Update and Redeploy
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
cd /root/options-data-pipeline
git pull
docker compose down
docker compose up -d --build
'
```

### 8. Check Flow Run Statistics
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
curl -s -X POST http://localhost:4200/api/flow_runs/filter \
  -H "Content-Type: application/json" \
  -d "{\"limit\": 50}" | python3 -c "
import sys, json
from collections import Counter
data = json.load(sys.stdin)
states = Counter(r.get(\"state_type\", \"UNKNOWN\") for r in data)
print(\"Flow Run Statistics:\")
for state, count in states.most_common():
    print(f\"  {state}: {count}\")
print(f\"Total: {len(data)}\")
"
'
```

### 9. Check Failed Runs
```bash
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
curl -s -X POST http://localhost:4200/api/flow_runs/filter \
  -H "Content-Type: application/json" \
  -d "{\"flow_runs\": {\"state\": {\"type\": {\"any_\": [\"FAILED\"]}}}, \"limit\": 10}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
if not data:
    print(\"No failed runs!\")
else:
    for r in data:
        print(f\"{r.get(\"name\")}: {r.get(\"state\", {}).get(\"message\", \"N/A\")[:100]}\")
"
'
```

### 10. Trigger Manual Flow Run
```bash
# For binance-sync-flow
sshpass -p '0aI6Zi4M4EZh' ssh -o StrictHostKeyChecking=no root@62.171.161.104 '
docker exec pipeline-worker prefect deployment run binance-sync-flow/binance-every-5min
'
```

## Deployment Schedules
| Flow | Deployment | Cron |
|------|------------|------|
| binance-sync-flow | binance-every-5min | */5 * * * * |
| hyperliquid-sync-flow | hyperliquid-hourly | 0 * * * * |
| deribit-sync-flow | deribit-hourly | 0 * * * * |
| option-ohlc-flow | option-ohlc-hourly | 5 * * * * |
| daily-aggregation-flow | daily-11-utc | 0 11 * * * |
| weekly-aggregation-flow | weekly-friday-11-utc | 0 11 * * 5 |
| monthly-aggregation-flow | monthly-last-friday-11-utc | 0 11 * * 5 |

## Known Issues

1. **SQLite Database Locking**: Prefect server uses SQLite which can cause "database is locked" errors under load. Consider migrating to PostgreSQL.

2. **Deribit API 400 Errors**: Expected for future dates - no trading data available yet.

## When User Asks About Pipeline

1. **"Check pipeline status"** - Run status check + flow run statistics
2. **"View logs"** - Show worker logs (most useful) and server logs
3. **"Why is X failing?"** - Check failed runs and analyze error messages
4. **"Restart pipeline"** - Restart services as appropriate
5. **"Deploy changes"** - Update and redeploy from GitHub

Always provide a summary after running commands, highlighting any issues found.
