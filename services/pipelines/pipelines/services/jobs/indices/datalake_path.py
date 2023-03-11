from dataclasses import dataclass
from services.utils.path.datalake_path_builder import DatalakePathConfig


class IndexPath(DatalakePathConfig):
    product = "indices"


class IndexListPath(IndexPath):
    asset = "index_list"


@dataclass
class IndexMemberListPath(IndexPath):
    asset = "index_members"
    index: str
