from shared.utils.path.data_lake.file_path import DataLakeFilePath


class ExchangePath(DataLakeFilePath.Generics.Default):
    product = "exchange"
