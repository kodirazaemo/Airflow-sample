# Airflow Sample Project

A containerized Apache Airflow 2.8.1 setup with PostgreSQL, Redis, and a sample DAG.

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
│   └── sample_dag.py      # Example DAG with Python & Bash tasks
└── .dockerignore
```

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
