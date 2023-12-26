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
	alembic -c ./database/alembic.ini upgrade $(if $(step),+$(step),head)
migrate-down:
	alembic -c ./database/alembic.ini downgrade $(if $(step),-$(step),base)
migrate-history:
	alembic -c ./database/alembic.ini history
migrate-create:
	python ./database/utils/create_revision.py ./database/alembic.ini $(name)


# Docker
docker-up:
	docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml --project-directory . up -d --build --force-recreate --remove-orphans
docker-up-prod:
	docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml --project-directory . up -d --build --force-recreate --remove-orphans

