# from shared.clients.datalake.azure.azure_datalake import datalake_client

from shared.clients.datalake.azure.file_system import abfs_client
from shared.hooks.duck.hook import DuckDbHook
from shared.utils.sql.file import QueryFile


def main():
    duck = DuckDbHook(file_system=abfs_client)

    test = duck.db.read_parquet(duck.helpers.build_abfs_path("temp/20230326_043cc83e3d9d4a27ace3ea5190a55177.parquet"))

    # test = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    query = QueryFile("./sql.sql")

    result = duck.query(query, data=test)

    print(result.pl())


if __name__ == "__main__":
    main()
