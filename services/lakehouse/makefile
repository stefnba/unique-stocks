#!make
include .env

# Python setup with pipenv
ifeq ($(PY_VERSION),)
PY_VERSION := 3.11.7
endif
ifeq ($(PY_MAJOR_VERSION),)
PY_MAJOR_VERSION := 3.11
endif

setup:
	export POETRY_VIRTUALENVS_IN_PROJECT=true
	poetry init --python ^${PY_MAJOR_VERSION} -n
	pyenv local ${PY_VERSION}
	poetry env use ${PY_VERSION}
	poetry run poetry add --dev black ruff mypy
	make core-dependencies

core-dependencies:
	poetry run poetry add pyiceberg -E sql-postgres

activate:
	poetry shell


install:
	poetry env use ${PY_VERSION}
	poetry install


# Docker
docker-up:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans