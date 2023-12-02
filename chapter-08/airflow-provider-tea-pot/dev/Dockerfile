ARG IMAGE_NAME="apache/airflow:2.4.3"
FROM ${IMAGE_NAME}

USER airflow
COPY --chown=airflow:airflow . ${AIRFLOW_HOME}/airflow_provider_this_provider
RUN pip install --upgrade pip
RUN pip install -e ${AIRFLOW_HOME}/airflow_provider_this_provider/[dev]
USER ${AIRFLOW_UID}
