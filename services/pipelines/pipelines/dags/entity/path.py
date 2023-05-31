from shared.utils.path.data_lake.file_path import DataLakeFilePath


class EntityPath(DataLakeFilePath.Generics.Default):
    product = "entity"
