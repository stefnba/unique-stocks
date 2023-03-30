import uuid

from shared.utils.path.datalake.path import DatalakeFileTypes, DatalakePath, DatalakeZones

file_name_date = "${year}${month}${day}"
dir_name_date = "year=${year}/month=${month}/day=${day}"


directory_current = ["${zone}", "product=${product}", "asset=${asset}", "current"]
file_name_current = "${product}_${asset}_${zone}_current"
directory_history = ["${zone}", "product=${product}", "asset=${asset}", "history"]
file_name_history = "${product}_${asset}_${zone}_history"


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


class TempPath(DatalakePath):
    zone: DatalakeZones = "temp"  # type: ignore
    file_type: DatalakeFileTypes = "parquet"
    key = uuid.uuid4().hex

    file_name = "${year}${month}${day}_${key}"
    directory = [
        "${zone}",
    ]
