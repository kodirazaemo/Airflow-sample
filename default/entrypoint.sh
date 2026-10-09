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
export PYTHONPATH="${PYTHONPATH:-}:/app/dags"
python - <<'PY'
import json
import os
import subprocess
import tempfile

from alpha_vantage_mcp import ALPHA_VANTAGE_CONN_ID, connection_import_document, redact_secrets
from medallion import init_warehouse

doc = connection_import_document()
password = doc[ALPHA_VANTAGE_CONN_ID].get("password") or ""
if not password:
    print(
        f"Skipping {ALPHA_VANTAGE_CONN_ID} seed: ALPHA_VANTAGE_API_KEY is empty. "
        "Set it in .env for local compose."
    )
else:
    fd, path = tempfile.mkstemp(prefix="alpha_vantage_conn_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(doc, handle)
        subprocess.check_call(
            ["airflow", "connections", "import", "--overwrite", path],
            stdout=subprocess.DEVNULL,
        )
        print(
            f"Seeded Airflow connection {ALPHA_VANTAGE_CONN_ID} "
            "(conn-password from env; extra transport/mcp_url; secret not logged)"
        )
    except Exception as exc:
        print(redact_secrets(f"Failed to seed {ALPHA_VANTAGE_CONN_ID}: {exc}", password))
        raise
    finally:
        try:
            os.remove(path)
        except OSError:
            pass

init_warehouse()
print("Medallion warehouse ready (bronze/silver/gold)")
PY

# Run whatever service command was passed by docker-compose
# (api-server, scheduler, dag-processor, etc.)
if [ "$#" -eq 0 ]; then
  exec airflow api-server --port 8080
fi
exec "$@"
