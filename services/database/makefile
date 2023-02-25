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

# alembic database migrations
migrate-up:
	alembic -c ./db/alembic.ini upgrade head
migrate-down:
	alembic -c ./db/alembic.ini downgrade base
migrate-create:
	alembic -c ./db/alembic.ini revision -m $(name) $(if $(filter auto,$(MAKECMDGOALS)),--autogenerate) ${params}
migrate-history:
	alembic -c ./db/alembic.ini history

