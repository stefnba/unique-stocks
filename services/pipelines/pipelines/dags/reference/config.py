from shared.utils.path.datalake.path import DatalakeFileTypes, DatalakePath, DatalakeZones


class ReferenceBasePath(DatalakePath):
    product = "reference"
    asset: str

    file_name = "${year}${month}${day}_${zone}_${asset}"
    file_type: DatalakeFileTypes = "parquet"
    directory = [
        "${zone}",
        "${product}",
        "asset=${asset}",
        "year=${year}",
        "month=${month}",
    ]


class ReferenceFinalBasePath(DatalakePath):
    product = "reference"
    zone: DatalakeZones = "curated"  # type: ignore
    asset: str

    file_name = "${asset}"
    file_type: DatalakeFileTypes = "parquet"
    directory = [
        "${zone}",
        "${product}",
        "asset=${asset}",
    ]
