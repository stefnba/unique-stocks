from shared.utils.path.data_lake.file_path import DataLakeFilePath


class SecurityQuotePath(DataLakeFilePath.Generics.Bin):
    product = "security_quote"
    bin_name = "security"
