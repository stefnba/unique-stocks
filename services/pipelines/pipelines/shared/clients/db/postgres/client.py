from shared.config import config
from shared.hooks.postgres.client import PgClient

if config.app.env == "DockerDevelopment":
    db_client = PgClient(conn_id="postgres_database")
else:
    db_client = PgClient(
        {"db_name": "uniquestocks", "port": 5871, "user": "admin", "host": "localhost", "password": "password"}
    )
