Airflow sample lives in [default/](default/README.md) (Airflow 3.3.1, Compose, medallion DAG). Run Compose from **Linux** or **WSL on Windows** — Docker Desktop (Windows) port forwarding has been unreliable for this stack.

Optional AWS IaC is in [terraform/](terraform/README.md) (Bronze S3 bucket, optional RDS). Do not apply in CI; no cloud credentials in the repo.
