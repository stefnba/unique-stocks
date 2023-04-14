from shared.utils.path.data_lake.file_path import DataLakeFilePath


class IndexPath(DataLakeFilePath.Generics.Default):
    product = "index"
