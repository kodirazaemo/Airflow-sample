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

`docker compose` reads `.env` automatically. On container boot `entrypoint.sh` upserts Airflow Connection `alpha_vantage_default` (`airflow connections import --overwrite`) with:

- **conn-password** — `ALPHA_VANTAGE_API_KEY` (local seed only)
- **conn extra** — `transport`, `mcp_url`, `mcp_sse_url` (compose forwarder defaults: `https://mcp.alphavantage.co:18080/mcp` and `/sse`)

Compose pins `AIRFLOW__CORE__FERNET_KEY` so every Airflow service can decrypt that connection password. Override it outside local compose.

Tasks resolve the key and MCP extras with `BaseHook.get_connection('alpha_vantage_default')`. `.env` is not the runtime source of truth; it only seeds the connection for local compose. Production should store the password in an Airflow secrets backend instead of an env file.

The sample authenticates to the [Alpha Vantage MCP server](https://mcp.alphavantage.co/#connection-examples) using that connection. Interactive OAuth is for desktop MCP clients; Airflow uses the documented API-key connection patterns:

| `ALPHA_VANTAGE_TRANSPORT` | What it does |
|---------------------------|--------------|
| `http` (default) | Streamable HTTP via compose `mcp-https-forwarder` (host TCP 18080) |
| `sse` | Legacy HTTP+SSE, same forwarder |
| `stdio` | Local `uvx marketdata-mcp-server YOUR_API_KEY` (needs `uvx` on the worker) |
| `rest` | Legacy `www.alphavantage.co/query` HTTP API (not MCP) |

Compose maps `mcp.alphavantage.co` to `host-gateway` and forwards `host:18080` → `mcp.alphavantage.co:443`. That is a **compose** workaround for Docker bridges that cannot SNAT to the public internet (nested VMs, some CI). It is separate from a host sysctl such as `net.bridge.bridge-nf-call-iptables=0`, which only affects container-to-container traffic on the same bridge.

Docker Desktop for Mac/Windows does not support `network_mode: host`. On those setups, comment out the `mcp-https-forwarder` extra_hosts if needed and set `ALPHA_VANTAGE_MCP_URL=https://mcp.alphavantage.co/mcp` (direct Cloudflare).

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

The daily **`commodity_trading_dag`** uses the same MCP client for gold, WTI, wheat, copper, and natural gas, writing **bronze / silver / gold** tables in Postgres (plus JSON snapshots under `data/medallion/`).

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
│   ├── alpha_vantage_mcp.py          # MCP client + Connection helpers
│   ├── medallion.py                  # Bronze/silver/gold warehouse helpers
│   ├── alpha_vantage_mcp_read_dag.py # Manual tools/list + WTI tools/call
│   └── commodity_dag.py              # Medallion commodity pipeline
├── tests/                  # Unit tests with a mocked MCP session
└── .dockerignore
```

## Commodity Trading DAG (`commodity_dag.py`)

Daily pipeline (`commodity_trading_dag`) that pulls live commodity data from **Alpha Vantage MCP**, lands it in a **medallion** warehouse, and scores trades with a **weighted multi-indicator model**:

1. **start_trading_session** — opens the session
2. **extract_bronze** — MCP `tools/list` / `tools/call` (or REST); writes an **immutable** raw payload per symbol (Postgres `bronze.mcp_snapshots` and `data/medallion/bronze/<ds>/<symbol>.json`). If that snapshot already exists, the task **does not re-fetch** MCP — retries after a later failure reuse bronze.
3. **transform_silver** — reads bronze, validates schema/nulls/non-positive prices, writes `silver.commodity_quotes` and `silver.commodity_price_series`
4. **compute_gold** — **SQL** aggregations into `gold.commodity_metrics` (latest/min/max/avg, observation count, period return) plus weighted Python signals
5. **score_obv_data_quality** — compares template synthetic-volume `compute_obv_signal` to a linear historical price trend line (agreement + correlation → `gold.obv_quality`)
6. **generate_trade_orders** — builds notional orders for actionable signals
7. **publish_daily_report** — end-of-day summary including gold metrics and OBV data-quality scores
8. **close_trading_session** — closes the session

A BI tool can query `gold.commodity_metrics` and `gold.obv_quality` in the same Postgres instance compose already runs for Airflow metadata. Unit tests use SQLite + local JSON when that database is not configured.

Alpha Vantage functions (`WTI`, `COPPER`, `GOLD_SILVER_HISTORY`, …) are exposed as MCP tools; the client discovers them with `tools/list` and reads with `tools/call`.

### Signal functions (each independent)

| Function | Signal | Default weight |
|----------|--------|----------------|
| `compute_momentum_signal` | Period-over-period price change | 0.15 |
| `compute_moving_average_signal` | SMA(5) vs SMA(20) trend | 0.25 |
| `compute_rsi_signal` | RSI(14) oversold / overbought | 0.25 |
| `compute_macd_signal` | MACD(12,26,9) histogram | 0.25 |
| `compute_obv_signal` | OBV trend vs SMA (synthetic volume*) | 0.10 |

\*Commodity endpoints do not provide volume, so OBV uses `|price change|` as a volume proxy. Task `score_obv_data_quality` scores that proxy against the actual price trend line.

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
4. After a successful `extract_bronze` run you should see lines like:
   `Bronze snapshot GOLD via alpha_vantage_mcp ...`
   On retry, reused bronze logs `Reusing bronze snapshot for GOLD ... (skip MCP)`.

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

MCP reads from Python tasks (client loads Connection `alpha_vantage_default`):

```python
from alpha_vantage_mcp import AlphaVantageMcpClient

with AlphaVantageMcpClient() as client:
    tools = client.list_tools()          # MCP tools/list
    payload = client.call_tool('WTI', {'interval': 'monthly'})  # tools/call
```

## Environment Variables
See `docker-compose.yml` for Airflow config (database, executor, auth, etc.).

Commodity / MCP:
- `ALPHA_VANTAGE_API_KEY` (or `ALPHA_VANTAGE_KEY`) — seeds Connection `alpha_vantage_default` on boot (see `.env.example`). Do not commit `.env`.
- `ALPHA_VANTAGE_TRANSPORT` — `http` (default), `sse`, `stdio`, or `rest` (also stored on the connection extra)
- `ALPHA_VANTAGE_MCP_URL` — default `https://mcp.alphavantage.co:18080/mcp` (host-network forwarder; connection extra `mcp_url`)
- `ALPHA_VANTAGE_MCP_SSE_URL` — default `https://mcp.alphavantage.co:18080/sse` (connection extra `mcp_sse_url`)
- `MEDALLION_DATA_DIR` — local bronze JSON directory (default `/app/data/medallion`)
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

These tests mock the MCP session and use a temp SQLite medallion warehouse. They do not need a paid (or any live) Alpha Vantage key.

## Notes
- Uses **LocalExecutor** for single-machine setup
- PostgreSQL stores Airflow metadata plus bronze/silver/gold schemas (`bronze`, `silver`, `gold`)
- Auth uses Airflow 3 **Simple Auth Manager** (`admin` / `admin` for local use)
- For production, consider the official Helm chart, Celery/Kubernetes executors, and a stronger auth manager
