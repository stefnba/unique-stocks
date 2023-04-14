from shared.utils.path.datalake.file_path import DataLakeFilePath


class ExchangePath(DataLakeFilePath.Generics.Default):
    product = "exchange"


# print(DataLakeFilePath.Builder(ExchangePath.raw(source="eod", file_type="json")))
# print(DataLakeFilePath.Builder(ExchangePath.processed(source="eod")))
# print(DataLakeFilePath.Builder(ExchangePath.curated()))
print(DataLakeFilePath.Builder(ExchangePath.curated(version="history")))
# print(DataLakeFilePath.Builder(ExchangePath.temp()))


class ExchangeMembersPath(DataLakeFilePath.Generics.Bin):
    product = "exchange_member"
    bin_name = "member"


# print(DataLakeFilePath.Builder(ExchangeMembersPath.raw(source="eod", file_type="json", bin="XETRA")))
# print(DataLakeFilePath.Builder(ExchangeMembersPath.processed(source="eod", bin="XETRA")))
# print(DataLakeFilePath.Builder(ExchangeMembersPath.curated(bin="XETRA")))
print(DataLakeFilePath.Builder(ExchangeMembersPath.curated(version="history", bin="XETRA")))
# print(DataLakeFilePath.Builder(ExchangeMembersPath.temp()))
