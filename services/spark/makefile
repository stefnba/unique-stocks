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
	pip install --upgrade pip
install-dev:
	pipenv install --dev
packages-lock:
	pipenv lock
install-prod:
	pipenv install --ignore-pipfile --deploy

down:
	docker-compose down --volumes --remove-orphans

run:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans

run-scaled:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans --scale spark-worker=3

## param app: app name
## param packages: comma separated list of packages to install
submit:
	docker exec spark-master bash -c \
		"spark-submit $(if $(strip $(packages)),--packages $(packages)) --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)"

run-iceberg-sql:
	docker compose -f docker/docker-compose.yml -f docker/iceberg-sql/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans
run-iceberg-rest:
	docker compose -f docker/docker-compose.yml -f docker/iceberg-rest/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans
run-iceberg-local:
	docker compose -f docker/docker-compose.yml -f docker/local/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans
