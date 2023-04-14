from shared.utils.path.data_lake.file_path import DataLakeFilePath


class ExchangeDetailPath(DataLakeFilePath.Generics.Bin):
    product = "exchange_detail"
    bin_name = "exchange"


class ExchangeHolidayPath(DataLakeFilePath.Generics.Bin):
    asset = "exchange_holiday"
    bin_name = "exchange"
