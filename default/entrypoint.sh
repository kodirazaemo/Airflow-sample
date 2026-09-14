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

# Run whatever service command was passed by docker-compose
# (api-server, scheduler, dag-processor, etc.)
if [ "$#" -eq 0 ]; then
  exec airflow api-server --port 8080
fi
exec "$@"
