from typing import List

from lib.hooks.sql.hook import SQLHook
from pydantic import BaseModel, RootModel


class ExchangeModel(BaseModel):
    code: str


class ExchangeListModel(RootModel):
    root: List[ExchangeModel]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item) -> ExchangeModel:
        return self.root[item]


class ExchangeSecurityModel(BaseModel):
    code: str
    exchange: str


class ExchangeSecurityListModel(RootModel):
    root: List[ExchangeSecurityModel]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item) -> ExchangeSecurityModel:
        return self.root[item]


class IndexModel(BaseModel):
    code: str


class IndexListModel(RootModel):
    root: List[IndexModel]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item) -> IndexModel:
        return self.root[item]


class TrinoWarehouseHook(SQLHook):

    def exchanges(self):
        return self.query_data(
            "SELECT code FROM iceberg_warehouse.analytics.anlytcs__exchange", data_model=ExchangeListModel
        )

    def indexes(self):
        return self.query_data(
            """
            SELECT
                code,
                exchange_code
            FROM iceberg_warehouse.analytics.anlytcs__exchange_security
            WHERE
                type = 'Index'
            """,
            data_model=IndexListModel,
        )

    def exchange_securities(self):
        return self.query_data(
            "SELECT code FROM iceberg_warehouse.analytics.anlytcs__exchange_security",
            data_model=ExchangeSecurityListModel,
        )
