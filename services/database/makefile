# Python setup with pipenv
setup:
	mkdir ./.venv && pipenv shell --python 3.11
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

