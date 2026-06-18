# Airflow image for the ERA5 pipeline DAGs (PLANNING.md §10, §14).
# apache-airflow is installed here (not in the root project) with the official
# constraints file, then the project's runtime deps are layered on top.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER airflow

# Project runtime dependencies, pinned against the Airflow constraints so the
# resolver cannot drift Airflow's own dependency tree.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    numpy pandas pyarrow xarray netCDF4 cfgrib cdsapi psycopg2-binary python-dotenv

# src/ and dags/ are bind-mounted at runtime (see docker-compose.yml); PYTHONPATH
# makes `import src...` resolve inside the container.
ENV PYTHONPATH=/opt/airflow
