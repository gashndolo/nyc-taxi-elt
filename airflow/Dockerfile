FROM apache/airflow:3.3.1

# Separate venv so pipeline/dbt dependencies never conflict with Airflow's own
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/airflow/pipeline-venv \
 && /opt/airflow/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt