from shared.utils.path.data_lake.file_path import DataLakeFilePath


class SecurityPath(DataLakeFilePath.Generics.Bin):
    product = "security"
    bin_name = "exchange"
