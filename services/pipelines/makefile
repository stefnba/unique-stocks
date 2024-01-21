#!make
include .env

# Python setup with pipenv
ifeq ($(PY_VERSION),)
PY_VERSION := 3.11.7
endif
ifeq ($(PY_MAJOR_VERSION),)
PY_MAJOR_VERSION := 3.11
endif
ifeq ($(AIRFLOW_VERSION),)
AIRFLOW_VERSION := 2.8.1
endif

# poetry
setup-poetry:
	export POETRY_VIRTUALENVS_IN_PROJECT=true
	poetry init --python ^${PY_MAJOR_VERSION} -n
	pyenv local ${PY_VERSION}
	poetry env use ${PY_VERSION}
	poetry add --dev black ruff mypy 

install:
	poetry env use ${PY_VERSION}
	poetry install

packages-export:
	poetry export -f requirements.txt --without-hashes  --output requirements.txt --without dev

# Airflow local for testing
install-airflow-local:
	poetry env use ${PY_VERSION}
	source .venv/bin/activate && \
	pip install --upgrade pip && \
	pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt" && \
	poetry add -D apache-airflow==${AIRFLOW_VERSION} && \
	poetry install && \
	pip uninstall -y pyiceberg && \
	arch -arm64 python -m pip install pyiceberg --no-cache && \
	make setup-airflow-local

setup-airflow-local:
	export AIRFLOW_HOME=${CURDIR}/${AIRFLOW_HOME} && \
	airflow db init && \
	airflow info

# Docker
docker-up-prod:
	export AIRFLOW_HOME=${AIRFLOW_HOME_CONTAINER} && \
	docker compose -f docker/docker-compose.prod.yml --project-directory . up -d --build --force-recreate --remove-orphans
docker-up:
	make packages-export && \
	export AIRFLOW_HOME=${AIRFLOW_HOME_CONTAINER} && \
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans
docker-down:
	docker compose -f docker/docker-compose.yml --project-directory . down --remove-orphans


# Port forwarding
# 4000:8080 -> webserver
# 4001:5418 -> meta db
# 4002:5555 -> flower
# 4010:9047 -> flower
port-forward:
	ssh \
		-L 4000:localhost:8080 \
		-L 4001:localhost:5418 \
		-L 4002:localhost:5555 \
		-L 4010:localhost:9047 \
		contabo