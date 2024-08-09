FROM python:3.12 as requirements-stage

WORKDIR /tmp

RUN pip install poetry

COPY ./pyproject.toml ./poetry.lock* /tmp/

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes


FROM python:3.12 as base

WORKDIR /app

COPY --from=requirements-stage /tmp/requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt


FROM base as server

CMD ["prefect", "server", "start"]


FROM base as flows

CMD ["python", "prefect_orchestration/main.py"]
