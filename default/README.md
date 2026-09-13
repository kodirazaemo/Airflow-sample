# Airflow Sample Project

A containerized Apache Airflow 2.8.1 setup with PostgreSQL, Redis, and sample DAGs — including a commodity trading pipeline powered by Alpha Vantage.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- An [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key) (free)

### Configure Alpha Vantage
```bash
cp .env.example .env
# Edit .env and set ALPHA_VANTAGE_API_KEY to your key
```

`docker compose` reads `.env` automatically and passes the key into the webserver and scheduler.

### Run
```bash
docker compose up -d --pull always
```

Access Airflow at `http://localhost:8080`
- **Username:** `admin`
- **Password:** `admin`

### Stop
```bash
docker compose down
```

## Project Structure
```
.
├── Dockerfile              # Multi-stage build for Airflow
├── docker-compose.yml      # Services: webserver, scheduler, postgres, redis
├── entrypoint.sh           # Database init & admin user creation
├── requirements.txt        # Python dependencies
├── .env.example            # Alpha Vantage config template
├── dags/
│   ├── sample_dag.py       # Example DAG with Python & Bash tasks
│   └── commodity_dag.py    # Commodity trading sample pipeline
└── .dockerignore
```

## Commodity Trading DAG (`commodity_dag.py`)

Daily pipeline (`commodity_trading_dag`) that pulls live commodity data from **Alpha Vantage**:

1. **start_trading_session** — opens the session (Bash)
2. **fetch_market_prices** — calls Alpha Vantage for gold, WTI crude, wheat, copper, and natural gas
3. **validate_market_data** — checks for missing/invalid quotes
4. **compute_trading_signals** — BUY / SELL / HOLD from period-over-period momentum
5. **generate_trade_orders** — builds notional orders for actionable signals
6. **publish_daily_report** — prints an end-of-day summary
7. **close_trading_session** — closes the session (Bash)

| Symbol | Alpha Vantage function | Interval |
|--------|------------------------|----------|
| `GOLD` | `GOLD_SILVER_HISTORY` (spot fallback) | monthly by default (daily optional) |
| `CRUDE_OIL` | `WTI` | monthly by default (daily optional) |
| `NATURAL_GAS` | `NATURAL_GAS` | monthly by default (daily optional) |
| `COPPER` | `COPPER` | monthly only |
| `WHEAT` | `WHEAT` | monthly only |

Free-tier keys are limited (~5 requests/minute). The fetch task pauses between calls (`ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS`, default `15`). Set `ALPHA_VANTAGE_INTERVAL=daily` if your key supports daily series for gold/oil/gas.

Use your own free API key for full coverage (including gold). The public `demo` key only works for a subset of commodity endpoints.

## Services
- **Airflow Webserver:** `http://localhost:8080`
- **PostgreSQL:** `localhost:5432` (airflow/airflow)
- **Redis:** `localhost:6379`
- **Airflow Scheduler:** Runs DAG scheduling

## Adding Custom DAGs
1. Create a new Python file in `dags/`
2. Define your DAG using the Airflow API
3. Scheduler automatically picks it up (refresh UI)

## Environment Variables
See `docker-compose.yml` for Airflow config (database, executor, etc.).

Commodity DAG:
- `ALPHA_VANTAGE_API_KEY` — required for live prices (see `.env.example`)
- `ALPHA_VANTAGE_INTERVAL` — `monthly` (default) or `daily`
- `ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS` — delay between API calls (default `15`)

## Notes
- Uses **LocalExecutor** for single-machine setup
- PostgreSQL stores metadata and DAG state
- For production, consider AWS MWAA, Celery workers, or Kubernetes deployment
