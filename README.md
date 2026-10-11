Airflow Sample Project
Containerized Apache Airflow 3.3.1 (Python 3.12) with PostgreSQL, Redis, and sample DAGs. Commodity prices come from the Alpha Vantage MCP server.

This sample collects prices for gold, WTI crude oil, natural gas, copper, and wheat, then turns the data into a small analytics pipeline.
A warehouse pattern named **Medallion** is used here (not a commodity and not an Airflow feature): raw data **Bronze**, cleaned data **Silver**, business-ready data **Gold**.

commodity_trading_dag runs every 2 minutes. It fetches prices, stores the raw responses in Bronze, validates and normalizes them into Silver, and calculates Gold metrics and market signals using momentum, moving averages, RSI, MACD, and OBV.
The DAG finishes with a daily-style report and suggested trade orders. These are sample recommendations only; the app does not connect to a broker or place trades.
alpha_vantage_mcp_read is a manual test DAG that discovers available Alpha Vantage tools and reads a sample WTI series through the local MCP process.
Airflow orchestrates the tasks; PostgreSQL stores Airflow metadata and the Bronze/Silver/Gold tables. A valid Alpha Vantage API key and outbound internet access are needed for live prices.

See default/README.md for detailed description.
