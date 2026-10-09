#!/bin/bash
set -e

# Initialize / migrate the Airflow metadata database
airflow db migrate

# Pin a stable admin password for local development (Simple Auth Manager).
# Airflow 3's default auth manager no longer uses `airflow users create`.
PASSWORDS_FILE="${AIRFLOW_HOME:-/app}/simple_auth_manager_passwords.json.generated"
python - "$PASSWORDS_FILE" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
users = {}
if path.exists():
    try:
        users = json.loads(path.read_text())
    except Exception:
        users = {}
users["admin"] = "admin"
path.write_text(json.dumps(users, indent=2) + "\n")
print(f"Wrote Simple Auth Manager password file: {path}")
PY

# Seed Connection alpha_vantage_default from env (idempotent add-or-update).
# Password is never printed. Production should use an Airflow secrets backend.
export PYTHONPATH="${PYTHONPATH:-}:/app/dags:/app/plugins"
python - <<'PY'
from alpha_vantage_mcp import (
    ALPHA_VANTAGE_CONN_ID,
    _env_api_key,
    redact_secrets,
    upsert_alpha_vantage_connection,
)
from medallion import init_warehouse

password = _env_api_key()
try:
    result = upsert_alpha_vantage_connection()
    if result == "skipped-empty-password":
        print(
            f"Skipping {ALPHA_VANTAGE_CONN_ID} seed: ALPHA_VANTAGE_API_KEY is empty. "
            "Set it in .env for local compose."
        )
    else:
        print(
            f"Seeded Airflow connection {ALPHA_VANTAGE_CONN_ID} "
            "(conn-password from env; extra transport/mcp_url; secret not logged)"
        )
    init_warehouse()
    print("Medallion warehouse ready (bronze/silver/gold)")
except Exception as exc:
    print(redact_secrets(f"Failed to seed {ALPHA_VANTAGE_CONN_ID}: {exc}", password))
    raise
PY

# Run whatever service command was passed by docker-compose
# (api-server, scheduler, dag-processor, etc.)
if [ "$#" -eq 0 ]; then
  exec airflow api-server --port 8080
fi
exec "$@"
