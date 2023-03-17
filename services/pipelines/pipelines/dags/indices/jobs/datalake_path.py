from dataclasses import dataclass
from services.utils.path.datalake_path_builder import DatalakePathConfig
from services.utils.path.types import DatalakeFileTypes
from typing import Optional


class IndexBasePath(DatalakePathConfig):
    product = "indices"


@dataclass(kw_only=True)
class IndicesPath(IndexBasePath):
    asset = "indices"
    file_type: Optional[DatalakeFileTypes] = "parquet"


@dataclass(kw_only=True)
class IndexMembersPath(IndexBasePath):
    asset = "index_members"
    index: str
    file_type: Optional[DatalakeFileTypes] = "parquet"

    def set_directory(self):
        """
        Add folder with exchange code after /asset=index_members/
        """
        self.insert_into_dir_path({"index": self.index}, 3)
