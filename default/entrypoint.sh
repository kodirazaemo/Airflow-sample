#!/bin/bash
set -e

# Initialize the Airflow database
airflow db migrate

# Create a default user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin \
    2>/dev/null || true

# Start the scheduler and webserver
exec airflow webserver --port 8080
