from lib.hooks.sql.hook import SQLHook
from prefect import flow


@flow(log_prints=True)
def setup_hive():
    hook = SQLHook(connection_uri="trino://user@localhost:8080", database="hive_warehouse")

    # Create schema
    hook.execute("sql/create_ingestion_schema.sql")

    # Create tables
    hook.execute("sql/create_exchange_table.sql")


if __name__ == "__main__":

    setup_hive()
