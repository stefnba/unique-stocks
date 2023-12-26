#!make
include .env

# Python setup with pipenv
ifeq ($(PY_VERSION),)
PY_VERSION := 3.11

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
	pip install "apache-airflow==2.8.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-${PY_VERSION}.txt" && \
	pip install \
		celery \
		apache-airflow-providers-postgres \
		apache-airflow-providers-microsoft-azure \
		apache-airflow-providers-apache-spark \
		apache-airflow-providers-amazon
setup-airflow-local:
	export AIRFLOW_HOME=${CURDIR}/${AIRFLOW_HOME} && \
	airflow db init && \
	airflow info

# Docker
docker-up-prod:
	export AIRFLOW_HOME=${AIRFLOW_HOME_CONTAINER} && \
	docker compose -f docker/docker-compose.prod.yml --project-directory . up -d --build --force-recreate --remove-orphans
docker-up:
	export AIRFLOW_HOME=${AIRFLOW_HOME_CONTAINER} && \
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans
docker-down:
	docker compose -f docker/docker-compose.yml --project-directory . down --remove-orphans


# Port forwarding
# 4000:8080 -> webserver
# 4001:5418 -> meta db
# 4002:5555 -> flower
port-forward:
	ssh -L 4000:localhost:8080 -L 4001:localhost:5418 -L 4002:localhost:5555 contabo