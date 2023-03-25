from shared.utils.path.datalake.path import DatalakeFileTypes, DatalakePath


class ExchangeDetailsPath(DatalakePath):
    product = "exchanges"
    asset = "exchange_details"
    asset_source: str
    exchange: str
    file_type: DatalakeFileTypes = "parquet"

    file_name = "${year}${month}${day}_${asset_source}_${exchange}_${asset}_${zone}"
    directory = [
        "${zone}",
        "product=${product}",
        "asset=${asset}",
        "exchange=${exchange}",
        "source=${asset_source}",
        "year=${year}",
        "month=${month}",
    ]
