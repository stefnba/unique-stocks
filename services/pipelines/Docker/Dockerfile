FROM apache/airflow:2.8.0-python3.11

ARG AIRFLOW_HOME
ARG AIRFLOW_PROJ_DIR

# Setup env
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

USER root

RUN sudo apt update -y && sudo apt install -y build-essential libpq-dev

USER airflow

RUN python -m pip install --upgrade pip

# install pipenv
RUN pip install pipenv

# Fix psycopg2 bug
RUN pip uninstall psycopg2-binary -y
RUN pip install psycopg2 --no-cache-dir --user

RUN pip install apache-airflow --no-cache-dir --user

RUN pip install \
    crypto \
    celery \
    apache-airflow-providers-postgres \
    apache-airflow-providers-microsoft-azure \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon

WORKDIR $AIRFLOW_HOME

# Install python dependencies
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv requirements > requirements.txt
RUN pip install -r requirements.txt --no-cache-dir --user

# Copy init script
COPY "$AIRFLOW_PROJ_DIR/../scripts/airflow_init.sh" .
