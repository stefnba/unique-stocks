from dataclasses import dataclass
from services.utils.path.datalake_path_builder import DatalakePathConfig


class ExchangePath(DatalakePathConfig):
    product = "exchanges"


class ExchangeListPath(ExchangePath):
    asset = "exchange_list"


@dataclass
class ExchangeDetailPath(ExchangePath):
    asset = "exchange_details"
    exchange: str

    def set_directory(self):
        """
        add folder with exchange code after asset=exchange_details
        """
        self.insert_into_existing_path({"exchange": self.exchange}, 3)


class ExchangeSecuritiesPath(ExchangePath):
    asset = "exchange_securities"
