# Airflow Sample Project

A containerized Apache Airflow 2.8.1 setup with PostgreSQL, Redis, and sample DAGs — including a commodity trading pipeline.

## Quick Start

### Prerequisites
- Docker & Docker Compose

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
├── entrypoint.sh          # Database init & admin user creation
├── requirements.txt       # Python dependencies
├── dags/
│   ├── sample_dag.py      # Example DAG with Python & Bash tasks
│   └── commodity_dag.py   # Commodity trading sample pipeline
└── .dockerignore
```

## Commodity Trading DAG (`commodity_dag.py`)

Daily demo pipeline (`commodity_trading_dag`) that:

1. **start_trading_session** — opens the session (Bash)
2. **fetch_market_prices** — simulates EOD prices for gold, crude oil, wheat, copper, and natural gas
3. **validate_market_data** — checks for missing/invalid quotes
4. **compute_trading_signals** — BUY / SELL / HOLD from simple momentum rules
5. **generate_trade_orders** — builds notional orders for actionable signals
6. **publish_daily_report** — prints an end-of-day summary
7. **close_trading_session** — closes the session (Bash)

Prices are deterministic per logical date (good for demos/replays). No external market API is required.

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
See `docker-compose.yml` for Airflow config (database, executor, etc.)

## Notes
- Uses **LocalExecutor** for single-machine setup
- PostgreSQL stores metadata and DAG state
- For production, consider AWS MWAA, Celery workers, or Kubernetes deployment
