from shared.paths.path import directory, directory_current, file_name, file_name_current
from shared.utils.path.datalake.path import DatalakeFileTypes, DatalakePath, DatalakeZones


class ExchangeDetailsPath(DatalakePath):
    product = "exchanges"
    asset = "exchange_details"
    asset_source: str
    exchange: str
    file_type: DatalakeFileTypes = "parquet"

    file_name = file_name
    directory = directory


class ExchangeHolidaysPath(ExchangeDetailsPath):
    asset = "exchange_holidays"


class ExchangeDetailsPathCuratedCurrent(ExchangeDetailsPath):
    zone: DatalakeZones = "curated"  # type: ignore
    exchange: str = ""
    asset_source: str = ""

    directory = directory_current
    file_name = file_name_current


class ExchangeHolidaysPathCuratedCurrent(ExchangeHolidaysPath):
    zone: DatalakeZones = "curated"  # type: ignore
    exchange: str = ""
    asset_source: str = ""

    directory = directory_current
    file_name = file_name_current
