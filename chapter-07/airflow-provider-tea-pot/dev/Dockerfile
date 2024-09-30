ARG IMAGE_NAME="apache/airflow:2.5.0"
FROM ${IMAGE_NAME}

USER airflow
COPY --chown=airflow:airflow . ${AIRFLOW_HOME}/airflow-provider-tea-pot
COPY --chown=airflow:root example_dags/ /opt/airflow/dags
RUN pip install --no-cache-dir --user ${AIRFLOW_HOME}/airflow-provider-tea-pot/.
