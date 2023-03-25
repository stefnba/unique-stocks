from shared.utils.path.datalake.path import DatalakeFileTypes, DatalakePath


class IndexMembersPath(DatalakePath):
    product = "indices"
    asset = "index_members"
    asset_source: str
    index: str
    file_type: DatalakeFileTypes = "parquet"

    file_name = "${year}${month}${day}_${zone}_${asset_source}_${index}_${asset}"
    directory = [
        "${zone}",
        "product=${product}",
        "asset=${asset}",
        "index=${index}",
        "asset_source=${asset_source}",
        "year=${year}",
        "month=${month}",
    ]
