from dataclasses import dataclass

from core.clients.lake import DataLakeClient
from core.ingestion import LandingSpec
from core.schema import BronzeTableModel


@dataclass(frozen=True, slots=True)
class IngestionDataset:
    """A dataset for ingestion in Bronze layer."""

    provider: str
    table: type[BronzeTableModel]
    landing: LandingSpec  # todo multiple via registry

    @property
    def schema(self) -> str:
        """Return the lake schema name from the table spec."""
        return self.table.schema_name

    @property
    def table_name(self) -> str:
        """Return the physical table name from the table spec."""
        return self.table.table_name

    @property
    def idempotency_columns(self) -> tuple[str, ...]:
        """Return idempotency columns from the table spec."""
        return self.table.idempotency_column_names()

    def write_bronze(self, lake: DataLakeClient, sources):
        """Insert Bronze sources into their configured lake table."""
        pass
