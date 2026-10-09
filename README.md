Airflow sample lives in [default/](default/README.md) (Airflow 3.3.1, Compose, medallion DAG). MCP defaults to **direct** `https://mcp.alphavantage.co/mcp`. Run Compose from **Linux** or **WSL on Windows** — Docker Desktop (Windows) port forwarding has been unreliable for the UI; the host-network MCP forwarder is opt-in for Linux/CI only.

Optional AWS IaC is in [terraform/](terraform/README.md) (Bronze S3 bucket, optional RDS). Do not apply in CI; no cloud credentials in the repo.
