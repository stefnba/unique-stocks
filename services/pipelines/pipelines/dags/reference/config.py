from shared.utils.path.datelake_builder import DatalakeFileTypes, DatalakePath


class ReferenceBasePath(DatalakePath):
    product = "reference"
    asset: str

    file_name = "${year}${month}${day}_${zone}_${asset}"
    file_type: DatalakeFileTypes = "parquet"
    directory = [
        "${zone}",
        "product=${product}",
        "asset=${asset}",
        "year=${year}",
        "month=${month}",
    ]
