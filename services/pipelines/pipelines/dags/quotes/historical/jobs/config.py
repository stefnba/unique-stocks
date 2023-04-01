from shared.utils.path.datalake.path import DatalakeFileTypes, DatalakePath


class HistoricalQuotesPath(DatalakePath):
    product = "quotes"
    asset_source: str
    security: str
    exchange: str
    file_type: DatalakeFileTypes = "parquet"

    file_name = "${year}${month}${day}_${zone}_${asset_source}_${security}_${exchange}"
    directory = [
        "${zone}",
        "product=${product}",
        "security=${security}",
        "exchange=${exchange}",
        "asset_source=${asset_source}",
        "year=${year}",
        "month=${month}",
    ]
