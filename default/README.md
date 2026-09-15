# Airflow Sample Project

A containerized **Apache Airflow 3.3.1** (Python 3.12) setup with PostgreSQL, Redis, and sample DAGs — including a commodity trading pipeline that **reads Alpha Vantage data through the official MCP server**.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- An [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key) (free)
- Local Python **3.12+** recommended for VS Code / local tooling (matches the container image)

### Configure Alpha Vantage
```bash
cp .env.example .env
# Edit .env and set ALPHA_VANTAGE_API_KEY to your key (do not commit .env)
```

`docker compose` reads `.env` automatically and passes the key into Airflow services.

The sample authenticates to the [Alpha Vantage MCP server](https://mcp.alphavantage.co/#connection-examples) using that key. Interactive OAuth is for desktop MCP clients; Airflow uses the documented API-key connection patterns:

| `ALPHA_VANTAGE_TRANSPORT` | What it does |
|---------------------------|--------------|
| `http` (default) | Streamable HTTP: `https://mcp.alphavantage.co/mcp?apikey=…` |
| `sse` | Legacy HTTP+SSE: `https://mcp.alphavantage.co/sse?apikey=…` |
| `stdio` | Local `uvx marketdata-mcp-server YOUR_API_KEY` (needs `uvx` on the worker) |
| `rest` | Legacy `www.alphavantage.co/query` HTTP API (not MCP) |

The API key is never written to git. Task logs redact `apikey=` query values.

### Run
```bash
docker compose up -d --build
```

Access Airflow at `http://localhost:8080`
- **Username:** `admin`
- **Password:** `admin`

### Invoke an MCP read
1. Set `ALPHA_VANTAGE_API_KEY` in `.env` and start compose (above).
2. In the UI, enable and trigger **`alpha_vantage_mcp_read`** (manual DAG, no schedule).
3. Task `list_mcp_tools` runs MCP `tools/list`.
4. Task `read_wti_monthly` runs MCP `tools/call` for `WTI` with `interval=monthly`.

The daily **`commodity_trading_dag`** uses the same MCP client for gold, WTI, wheat, copper, and natural gas.

### Stop
```bash
docker compose down
```

## Project Structure
```
.
├── Dockerfile              # Multi-stage build (Python 3.12 + Airflow 3.3.1)
├── docker-compose.yml      # api-server, scheduler, dag-processor, postgres, redis
├── entrypoint.sh           # DB migrate + Simple Auth Manager bootstrap
├── requirements.txt        # Python dependencies (includes the MCP SDK)
├── .env.example            # Alpha Vantage / MCP / signal config template
├── dags/
│   ├── sample_dag.py                 # Example DAG with Python tasks
│   ├── alpha_vantage_mcp.py          # MCP client (http / sse / stdio)
│   ├── alpha_vantage_mcp_read_dag.py # Manual tools/list + WTI tools/call
│   └── commodity_dag.py              # Commodity trading sample pipeline
├── tests/                  # Unit tests with a mocked MCP session
└── .dockerignore
```

## Commodity Trading DAG (`commodity_dag.py`)

Daily pipeline (`commodity_trading_dag`) that pulls live commodity data from **Alpha Vantage MCP** and scores trades with a **weighted multi-indicator model**:

1. **start_trading_session** — opens the session
2. **fetch_market_prices** — MCP `tools/list` then `tools/call` for gold, WTI crude, wheat, copper, and natural gas (stores price history)
3. **validate_market_data** — checks for missing/invalid quotes
4. **compute_trading_signals** — runs separate signal functions, then combines them by weight
5. **generate_trade_orders** — builds notional orders for actionable signals
6. **publish_daily_report** — prints an end-of-day summary with per-indicator detail
7. **close_trading_session** — closes the session

Alpha Vantage functions (`WTI`, `COPPER`, `GOLD_SILVER_HISTORY`, …) are exposed as MCP tools; the client discovers them with `tools/list` and reads with `tools/call`.

### Signal functions (each independent)

| Function | Signal | Default weight |
|----------|--------|----------------|
| `compute_momentum_signal` | Period-over-period price change | 0.15 |
| `compute_moving_average_signal` | SMA(5) vs SMA(20) trend | 0.25 |
| `compute_rsi_signal` | RSI(14) oversold / overbought | 0.25 |
| `compute_macd_signal` | MACD(12,26,9) histogram | 0.25 |
| `compute_obv_signal` | OBV trend vs SMA (synthetic volume*) | 0.10 |

\*Commodity endpoints do not provide volume, so OBV uses `|price change|` as a volume proxy.

Each indicator returns `BUY (+1)`, `SELL (-1)`, or `HOLD (0)`.  
`combine_weighted_signals()` computes a weighted score:

- **BUY** if score ≥ `SIGNAL_BUY_THRESHOLD` (default `0.25`)
- **SELL** if score ≤ `SIGNAL_SELL_THRESHOLD` (default `-0.25`)
- **HOLD** otherwise

| Symbol | Alpha Vantage MCP tool | Interval |
|--------|------------------------|----------|
| `GOLD` | `GOLD_SILVER_HISTORY` (spot fallback) | monthly by default (daily optional) |
| `CRUDE_OIL` | `WTI` | monthly by default (daily optional) |
| `NATURAL_GAS` | `NATURAL_GAS` | monthly by default (daily optional) |
| `COPPER` | `COPPER` | monthly only |
| `WHEAT` | `WHEAT` | monthly only |

Free-tier keys are limited (~5 requests/minute). The fetch task pauses between calls (`ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS`, default `15`). Set `ALPHA_VANTAGE_INTERVAL=daily` if your key supports daily series for gold/oil/gas.

Use your own free API key for full coverage (including gold). The public `demo` key only works for a subset of commodity endpoints.

## Services
- **Airflow API server (UI):** `http://localhost:8080`
- **PostgreSQL:** `localhost:5432` (airflow/airflow)
- **Redis:** `localhost:6379`
- **Airflow Scheduler:** schedules Dag runs
- **Airflow Dag processor:** parses Dag files (required in Airflow 3)

### Troubleshooting task runs (Airflow 3 + Docker)

If a manual run stalls on the first task (`queued` / retry) and you never see Alpha Vantage fetch logs:

1. Rebuild so scheduler gets the Execution API URL fix:
   ```bash
   docker compose down
   docker compose up -d --build
   ```
2. Confirm compose sets:
   `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/`
   (must be the **api-server service name**, not `localhost`)
3. Inspect the failed task log in the UI, or:
   ```bash
   docker compose logs airflow-scheduler --tail=200
   ```
4. After a successful `fetch_market_prices` run you should see lines like:
   `Fetched GOLD via alpha_vantage_mcp ...`

## Adding Custom DAGs
1. Create a new Python file in `dags/`
2. Define your DAG using the Airflow 3 SDK / standard provider operators
3. Dag processor picks it up automatically (refresh UI)

Example imports for Airflow 3:

```python
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
```

MCP reads from Python tasks:

```python
from alpha_vantage_mcp import AlphaVantageMcpClient

with AlphaVantageMcpClient() as client:
    tools = client.list_tools()          # MCP tools/list
    payload = client.call_tool('WTI', {'interval': 'monthly'})  # tools/call
```

## Environment Variables
See `docker-compose.yml` for Airflow config (database, executor, auth, etc.).

Commodity / MCP:
- `ALPHA_VANTAGE_API_KEY` (or `ALPHA_VANTAGE_KEY`) — required for live prices (see `.env.example`)
- `ALPHA_VANTAGE_TRANSPORT` — `http` (default), `sse`, `stdio`, or `rest`
- `ALPHA_VANTAGE_MCP_URL` — Streamable HTTP endpoint (default `https://mcp.alphavantage.co/mcp`)
- `ALPHA_VANTAGE_MCP_SSE_URL` — SSE endpoint (default `https://mcp.alphavantage.co/sse`)
- `ALPHA_VANTAGE_MCP_STDIO_COMMAND` / `ALPHA_VANTAGE_MCP_STDIO_ARGS` — local stdio server (`uvx` + `marketdata-mcp-server`)
- `ALPHA_VANTAGE_INTERVAL` — `monthly` (default) or `daily`
- `ALPHA_VANTAGE_REQUEST_PAUSE_SECONDS` — delay between API calls (default `15`)
- `SIGNAL_WEIGHT_MOMENTUM` / `SIGNAL_WEIGHT_MOVING_AVERAGE` / `SIGNAL_WEIGHT_RSI` / `SIGNAL_WEIGHT_MACD` / `SIGNAL_WEIGHT_OBV` — optional weight overrides
- `SIGNAL_BUY_THRESHOLD` / `SIGNAL_SELL_THRESHOLD` — weighted-score cutoffs (defaults `0.25` / `-0.25`)

## Tests
From `default/`:

```bash
PYTHONPATH=dags python -m unittest discover -s tests -v
```

These tests mock the MCP session. They do not need a paid (or any live) Alpha Vantage key.

## Notes
- Uses **LocalExecutor** for single-machine setup
- PostgreSQL stores metadata and DAG state
- Auth uses Airflow 3 **Simple Auth Manager** (`admin` / `admin` for local use)
- For production, consider the official Helm chart, Celery/Kubernetes executors, and a stronger auth manager
