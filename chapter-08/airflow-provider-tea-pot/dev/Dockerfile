ARG IMAGE_NAME="apache/airflow:2.5.0}"
FROM ${IMAGE_NAME}

USER airflow
COPY --chown=airflow:airflow . ${AIRFLOW_HOME}/airflow-provider-tea-pot
RUN pip install --upgrade pip
RUN pip install -e ${AIRFLOW_HOME}/airflow-provider-tea-pot/[dev]
USER ${AIRFLOW_UID}
