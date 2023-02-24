#!make
include .env

ifeq ($(PY_VERSION),)
PY_VERSION := 3.10
endif

# Python setup with pipenv
setup:
	mkdir ./.venv && pipenv shell --python ${PY_VERSION}
install-dev:
	pipenv install --dev
packages-lock:
	pipenv lock
install-prod:
	pipenv install --ignore-pipfile --deploy