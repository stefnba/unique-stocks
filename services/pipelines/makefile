#!make
include .env

# Python setup with pipenv
ifeq ($(PY_VERSION),)
PY_VERSION := 3.10
endif

setup:
	mkdir ./.venv &&  \
	pipenv --python ${PY_VERSION} && \
	source .venv/bin/activate && \
	pip install --upgrade pip && \
	make install-airflow-local && \
	make setup-airflow-local && \
	make install-dev
install-dev:
	pipenv install --dev
packages-lock:
	pipenv lock
install-prod:
	pipenv install --ignore-pipfile --deploy

# Airflow local testing
install-airflow-local:
	source .venv/bin/activate && \
	pip install "apache-airflow[celery]==2.7.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.0/constraints-${PY_VERSION}.txt" && \
	pip install apache-airflow-providers-postgres apache-airflow-providers-microsoft-azure
setup-airflow-local:
	export AIRFLOW_HOME=${CURDIR}/${AIRFLOW_HOME} && \
	airflow db init && \
	airflow info

# Docker
docker-up-prod:
	docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml --project-directory . up -d --build --force-recreate --remove-orphans
docker-up:
	export AIRFLOW_HOME=${AIRFLOW_HOME_CONTAINER} && \
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans