from shared.utils.path.data_lake.file_path import DataLakeFilePath


class HistoricalQuotePath(DataLakeFilePath.Generics.Default):
    product = "quote"
