FROM apache/airflow:2.10.4-python3.11

# Switch to root to install system-level build dependencies
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to install Python packages safely
USER airflow

# Copy Python dependencies file
COPY requirements.txt /requirements.txt

# Install dependencies with constraints to avoid accidental Airflow upgrades
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt" \
    -r /requirements.txt

# Isolated venv for dbt with --system-site-packages to reuse heavy Airflow packages (pyarrow, pandas, numpy, bigquery)
RUN python -m venv --system-site-packages /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --no-build-isolation \
        Logbook==1.5.3 \
        agate==1.9.1 \
        daff==1.4.2 \
        dbt-adapters==1.16.3 \
        dbt-bigquery==1.8.3 \
        dbt-common==1.27.1 \
        dbt-core==1.8.10 \
        dbt-extractor==0.6.0 \
        dbt-protos==1.0.615 \
        dbt-semantic-interfaces==0.5.1 \
        deepdiff==7.0.1 \
        isodate==0.6.1 \
        leather==0.4.1 \
        mashumaro==3.14 \
        minimal-snowplow-tracker==0.0.2 \
        msgpack==1.2.3 \
        networkx==3.6.1 \
        parsedatetime==2.6 \
        protobuf==5.29.6 \
        pytimeparse==1.1.10 \
    && ln -sf /opt/airflow/dbt_venv/bin/dbt /home/airflow/.local/bin/dbt

# Ensure project root and scripts are in PYTHONPATH
ENV PYTHONPATH="/opt/airflow:/opt/airflow/scripts"

