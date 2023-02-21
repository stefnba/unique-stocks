# packages
packages:
	pip list --format=freeze > requirements.txt
install:
	pip install -r requirements.txt

# alembic
migrate-up:
	alembic -c ./alembic.ini upgrade head
migrate-down:
	alembic -c ./alembic.ini downgrade base
migrate-create:
	alembic -c ./alembic.ini revision -m $(name) $(if $(filter auto,$(MAKECMDGOALS)),--autogenerate) ${params}
migrate-history:
	alembic -c alembic.ini history

