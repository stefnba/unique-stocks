from shared.config import CONFIG
from shared.hooks.postgres.client import PgClient

if CONFIG.app.env == "DockerDevelopment":
    db_client = PgClient(conn_id="postgres_database")
else:
    db_client = PgClient(
        {"db_name": "stocks", "port": 5871, "user": "admin", "host": "localhost", "password": "password"}
    )
