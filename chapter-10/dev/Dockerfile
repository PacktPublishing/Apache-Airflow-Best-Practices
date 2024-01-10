# syntax=docker/dockerfile:1.4
ARG IMAGE_NAME="apache/airflow:2.5.0"
FROM ${IMAGE_NAME}



#install library requirements.
COPY packages.txt .
USER root
RUN if [[ -s packages.txt ]]; then \
    apt-get update && cat packages.txt | tr '\r\n' '\n' | xargs apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*; \
  fi

## install python requirements
# Install python packages
USER airflow
COPY requirements.txt .
RUN if grep -Eqx 'apache-airflow\s*[=~>]{1,2}.*' requirements.txt; then \
    echo >&2 "Set Apache Airflow version via the Dockerfile, not requirements!";  exit 1; \
  fi; \
  pip install --no-cache-dir -q -r requirements.txt
COPY --chown=airflow:airflow . .
