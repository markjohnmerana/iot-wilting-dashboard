# ──────────────────────────────────────────────────────────────
# Plant Monitoring ETL — Dockerfile
# Based on the official Apache Airflow image.
# Adds the SQL Server ODBC driver and Python dependencies.
# ──────────────────────────────────────────────────────────────

FROM apache/airflow:2.9.1-python3.11

USER root

# ── Install Microsoft ODBC Driver 18 for SQL Server ──────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/12/prod.list \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        mssql-tools18 \
        unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ── Copy and install Python requirements ─────────────────────
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ── Copy ETL source into the image ───────────────────────────
COPY --chown=airflow:root . /opt/airflow/etl/
COPY --chown=airflow:root dags/ /opt/airflow/dags/

ENV PYTHONPATH="/opt/airflow/etl:${PYTHONPATH}"
