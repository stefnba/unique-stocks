from shared.utils.path.data_lake.file_path import DataLakeFilePath


class IndexMemberPath(DataLakeFilePath.Generics.Bin):
    product = "index_member"
    bin_name = "index"
