from shared.clients.db.postgres.client import db_client
from shared.hooks.postgres.client import PgClient


class PgRepositories:
    _query = db_client
    # _query = PgClient(
    #     {"db_name": "uniquestocks", "port": 5871, "user": "admin", "host": "localhost", "password": "password"}
    # )
