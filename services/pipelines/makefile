#!make
include .env

# Python setup with pipenv
ifeq ($(PY_VERSION),)
PY_VERSION := 3.10
endif

setup:
	mkdir ./.venv && pipenv shell --python ${PY_VERSION}
install-dev:
	pipenv install --dev
packages-lock:
	pipenv lock
install-prod:
	pipenv install --ignore-pipfile --deploy

# Docker
docker-up-prod:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate
docker-up:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate