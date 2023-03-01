FROM apache/airflow:2.5.1-python3.10

ARG AIRFLOW_HOME
ARG AIRFLOW_PROJ_DIR

USER root

RUN sudo apt update -y && sudo apt install -y build-essential libpq-dev

USER airflow

RUN python -m pip install --upgrade pip

RUN pip uninstall psycopg2-binary -y

RUN pip install psycopg2 --no-cache-dir --user

WORKDIR $AIRFLOW_HOME

COPY "$AIRFLOW_PROJ_DIR/../scripts/airflow_init.sh" .
