from shared.utils.path.data_lake.file_path import DataLakeFilePath


class ExchangeSecurityPath(DataLakeFilePath.Generics.Bin):
    product = "exchange_security"
    bin_name = "exchange"
