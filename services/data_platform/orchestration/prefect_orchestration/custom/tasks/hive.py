from prefect import task
from prefect_sqlalchemy import SqlAlchemyConnector


@task
def update_hive_partitions(block_name: str, table_name: str):
    """Update Hive partitions for the given table."""

    connection = SqlAlchemyConnector.load(block_name)
    with connection:
        connection.execute(f"CALL system.sync_partition_metadata('ingestion', '{table_name}', 'FULL')")
