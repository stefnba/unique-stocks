from dataclasses import dataclass
from typing import Optional

from shared.utils.path.datalake_path_builder import DatalakePathConfig
from shared.utils.path.types import DatalakeFileTypes


class ExchangesBasePath(DatalakePathConfig):
    product = "exchanges"


@dataclass(kw_only=True)
class ExchangesPath(ExchangesBasePath):
    asset = "exchanges"
    file_type: Optional[DatalakeFileTypes] = "parquet"


@dataclass(kw_only=True)
class ExchangeDetailPath(ExchangesBasePath):
    asset = "exchange_details"
    exchange: str
    file_type: Optional[DatalakeFileTypes] = "parquet"

    def set_directory(self):
        """
        Add folder with exchange code after /asset=exchange_details/
        """
        self.insert_into_dir_path({"exchange": self.exchange}, 3)


@dataclass(kw_only=True)
class ExchangeSecuritiesPath(ExchangesBasePath):
    asset = "exchange_securities"
    exchange: str
    file_type: Optional[DatalakeFileTypes] = "parquet"

    def set_directory(self):
        """
        Add folder with exchange code after /asset=exchange_details/
        """
        self.insert_into_dir_path({"exchange": self.exchange}, 3)
