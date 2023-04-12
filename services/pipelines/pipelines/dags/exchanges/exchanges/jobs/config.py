from shared.utils.path.datalake.path import DatalakeFileTypes, DatalakePath


class ExchangesPath(DatalakePath):
    product = "exchanges"
    asset = "exchanges"
    asset_source: str

    file_name = "${year}${month}${day}_${asset_source}_${asset}_${zone}"
    file_type: DatalakeFileTypes = "parquet"
    directory = [
        "${zone}",
        "product=${product}",
        "asset=${asset}",
        "source=${asset_source}",
        "year=${year}",
        "month=${month}",
    ]
