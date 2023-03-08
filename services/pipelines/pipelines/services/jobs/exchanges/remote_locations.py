from dataclasses import dataclass
from typing import Literal, Optional

from services.utils import DataLakeLocation
from services.utils.remote_location import FileExtTypes


@dataclass
class ExchangeListLocation(DataLakeLocation):
    asset_category = "exchanges"
    asset = "exchange_list"
    asset_source: Optional[str] = None
    stage: Optional[Literal["raw", "processed"]] = None

    def build_location(self):
        self.directory = [
            self.asset_category,
            self.asset,
            self.stage,
            self.asset_source,
            self.datetime_path("%Y"),
        ]
        self.file_name = [
            self.datetime_path("%Y%m%d"),
            self.asset_source,
            self.asset,
            self.stage,
        ]
        self.file_extension = "parquet"

    @classmethod
    def raw(cls, asset_source: str, file_extension: FileExtTypes = "csv"):
        location = cls(stage="raw", asset_source=asset_source)
        location.file_extension = file_extension
        return location

    @classmethod
    def processed(cls, asset_source: str):
        location = cls(stage="processed", asset_source=asset_source)
        return location


@dataclass
class ExchangeDetailLocation(DataLakeLocation):
    asset_category = "exchanges"
    asset = "exchange_detail"
    asset_source: Optional[str] = None
    exchange: Optional[str] = None
    stage: Optional[Literal["raw", "processed"]] = None

    def build_location(self):
        self.directory = [
            self.asset_category,
            self.asset,
            self.stage,
            self.asset_source,
            self.exchange,
            self.datetime_path("%Y"),
        ]
        self.file_name = [
            self.datetime_path("%Y%m%d"),
            self.asset_source,
            self.exchange,
            self.asset,
            self.stage,
        ]
        self.file_extension = "parquet"

    @classmethod
    def raw(cls, asset_source: str, exchange: str):
        location = cls(stage="raw", asset_source=asset_source, exchange=exchange)
        location.file_extension = "csv"
        return location

    @classmethod
    def processed(cls, asset_source: str, exchange: str):
        return cls(stage="processed", asset_source=asset_source, exchange=exchange)
