from shared.utils.path.datelake_builder import DatalakeFileTypes, DatalakePath


class ExchangesPath(DatalakePath):
    product = "exchanges"
    asset = "exchanges"
    asset_source: str

    file_name = "${year}${month}${day}_${zone}_${asset_source}_${asset}"
    file_type: DatalakeFileTypes = "parquet"
    directory = [
        "${zone}",
        "product=${product}",
        "asset=${asset}",
        "source=${asset_source}",
        "year=${year}",
        "month=${month}",
    ]


class ExchangeDetailsPath(DatalakePath):
    product = "exchanges"
    asset = "exchange_details"
    asset_source: str
    exchange: str
    file_type: DatalakeFileTypes = "parquet"

    file_name = "${year}${month}${day}_${zone}_${asset_source}_${exchange}_${asset}"
    directory = [
        "${zone}",
        "product=${product}",
        "asset=${asset}",
        "exchange=${exchange}",
        "source=${asset_source}",
        "year=${year}",
        "month=${month}",
    ]


class ExchangeSecuritiesPath(ExchangeDetailsPath):
    asset = "exchange_securities"
