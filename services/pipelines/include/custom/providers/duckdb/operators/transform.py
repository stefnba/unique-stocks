import typing as t

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from custom.providers.duckdb.hooks.query import DuckDBQueryHook


class DuckDbTransformOperator(BaseOperator):
    """
    Transform data using DuckDB and save results as parquet file on the specified `destination_path`.

    Returns:
        str: Path to saved results.
    """

    template_fields = (
        "destination_path",
        "query",
        "query_params",
    )
    template_ext = (".sql",)

    query: str
    conn_id: str
    query_params: t.Optional[t.Dict[str, str]]
    destination_path: str

    def __init__(
        self,
        query: str,
        conn_id: str,
        destination_path: str,
        task_id: str = "transform",
        query_params: t.Optional[t.Dict[str, str]] = None,
        **kwargs,
    ):
        self.query = query
        self.query_params = query_params
        self.conn_id = conn_id
        self.destination_path = destination_path

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Context):
        """Main method executed by Airflow."""

        hook = DuckDBQueryHook(conn_id=self.conn_id)

        data = hook.query(
            self.query,
            params=self.query_params,
        )

        data.write_parquet(self.destination_path, compression=None)

        return self.destination_path
