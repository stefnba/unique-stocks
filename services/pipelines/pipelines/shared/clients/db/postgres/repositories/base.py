from shared.clients.db.postgres.client import db_client


class PgRepositories:
    table: str | tuple[str, str]

    _query = db_client
