ARG AIRFLOW_VERSION
ARG PY_MAJOR_VERSION

FROM apache/airflow:${AIRFLOW_VERSION}-python${PY_MAJOR_VERSION} as base

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

RUN sudo apt update -y && \
    sudo apt install -y \
    build-essential \
    libpq-dev \
    openssh-client \
    git

USER airflow

WORKDIR $AIRFLOW_HOME

# Install python dependencies
RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt --no-cache-dir

# Fix psycopg2 bug
RUN pip uninstall psycopg2-binary -y
RUN pip install psycopg2 --no-cache-dir --user
# fix flask-session bug https://github.com/apache/airflow/pull/36895
RUN pip install "flask-session>=0.4.0,<0.6" --no-cache-dir 

# Copy init script
COPY scripts/airflow_init.sh .


FROM base as dev
# duckdb
# we need to compile duckdb ourselves because duckdb doesnt provide
# binary extensions for 'httpfs' in platform: linux_arm64_gcc4
# this means duckdb is not working to query remote files in 
# both Mac M1 (only under docker) and Linux ARM (only under docker)
# Note: without docker, duckdb extensions autoload mechanism works.
# More info at:
# https://github.com/duckdb/duckdb/issues/8035
RUN git clone --depth 1 --branch v0.9.2 https://github.com/duckdb/duckdb && \
    cd duckdb/tools/pythonpkg && BUILD_HTTPFS=1 python -m pip install . ;


FROM base as prod

COPY dags/ ./dags
COPY include/ ./include
COPY config/ ./config
COPY plugins/ ./plugins