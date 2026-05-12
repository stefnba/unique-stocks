import traceback
from pathlib import Path
from typing import Optional, Type, TypeVar, overload

from pydantic import BaseModel
from sqlalchemy import CursorResult, Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql.expression import text

T = TypeVar("T", bound=BaseModel)


class SQLHook:
    engine: Engine | AsyncEngine

    def __init__(self, engine_or_connection_uri: str | Engine | AsyncEngine):

        if isinstance(engine_or_connection_uri, str):
            self.engine = create_engine(engine_or_connection_uri)
        elif isinstance(engine_or_connection_uri, (Engine, AsyncEngine)):
            self.engine = engine_or_connection_uri
        else:
            raise ValueError("Invalid engine or connection URI.")

    def _read_sql_file(self, file_path: str) -> str:
        """Read a SQL file and return the content as a string."""

        stack = traceback.extract_stack()
        base_path = stack[-3].filename

        query_file_path = Path(base_path).parent / file_path

        return query_file_path.read_text()

    def execute(self, query: str) -> CursorResult:
        """Execute a query and return the CursorResult object."""

        if query.endswith(".sql"):
            query = self._read_sql_file(query)

        if isinstance(self.engine, AsyncEngine):
            raise NotImplementedError("AsyncEngine is not supported yet.")

        with self.engine.connect() as connection:
            return connection.execute(text(query))

    @overload
    def query_data(self, query: str, data_model: Type[T]) -> T: ...

    @overload
    def query_data(self, query: str, data_model: None = ...) -> list[dict]: ...

    def query_data(self, query: str, data_model: Optional[Type[T]] = None) -> list[dict] | T:
        """Execute a SELECT query and return the results."""

        if query.endswith(".sql"):
            query = self._read_sql_file(query)

        rows = self.execute(query).all()
        data = [row._asdict() for row in rows]

        if data_model:
            return data_model.model_validate(data)

        return data
