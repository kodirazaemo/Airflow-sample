FROM python:3.12-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=base /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=base /usr/local/bin /usr/local/bin

COPY dags ./dags
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
ENV AIRFLOW__API__BASE_URL=http://localhost:8080

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
