import assets.blocks as block
from lib.hooks.sql.hook import SQLHook
from prefect import flow


@flow(log_prints=True)
def setup_hive():
    hook = SQLHook(block.trino_hive_warehouse())

    # Create schema
    hook.execute("sql/create_ingestion_schema.sql")

    # Create tables
    hook.execute("sql/create_exchange_table.sql")
    hook.execute("sql/create_exchange_security_table.sql")
    hook.execute("sql/create_security_quote_table.sql")


if __name__ == "__main__":
    setup_hive()
