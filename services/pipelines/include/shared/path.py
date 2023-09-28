from shared.types import DataLakeDatasetFileTypes, DataSources, DataLakeDataFileTypes
from utils.filesystem.path import AdlsDatasetPath, PathElement
from utils.filesystem import path as PathAlias


# For re-export
Path = PathAlias.Path
AdlsPath = PathAlias.AdlsPath
LocalPath = PathAlias.LocalPath


class ExchangePath(AdlsDatasetPath):
    product = "exchange"


class EntityPath(AdlsDatasetPath):
    product = "entity"

    @classmethod
    def raw(cls, format: DataLakeDataFileTypes, source: DataSources, **kwargs):
        c = cls(format=format, source=source, **kwargs)
        return AdlsPath(container="raw", blob=c.uri, format=format)


class EntityIsinPath(AdlsDatasetPath):
    product = "entity_isin"

    @classmethod
    def raw(cls, format: DataLakeDataFileTypes, source: DataSources, **kwargs):
        c = cls(format=format, source=source, **kwargs)
        return AdlsPath(container="raw", blob=c.uri, format=format)


class SecurityPath(AdlsDatasetPath):
    product = "security"

    dir_template = [
        "product",
        "source",
        PathElement(name="exchange", hive_flavor=True),
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    filename_template = [
        "datetime",
        "product",
        "source",
        "exchange",
    ]

    @classmethod
    def raw(cls, exchange: str, format: DataLakeDatasetFileTypes, source: DataSources):
        return super().raw(format=format, source=source, security=exchange)


class SecurityQuotePath(AdlsDatasetPath):
    product = "security_quote"

    dir_template = [
        "product",
        "source",
        PathElement(name="security", hive_flavor=True),
        PathElement(name="exhange", hive_flavor=True),
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    filename_template = [
        "datetime",
        "product",
        "source",
        "security",
        "exhange",
    ]

    @classmethod
    def raw(cls, security: str, exhange: str, format: DataLakeDatasetFileTypes, source: DataSources):
        return super().raw(format=format, source=source, exhange=exhange, security=security)


class IndexMemberPath(AdlsDatasetPath):
    product = "index_member"

    dir_template = [
        "product",
        "source",
        PathElement(name="security", hive_flavor=True),
        PathElement(name="index", hive_flavor=True),
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    filename_template = [
        "datetime",
        "product",
        "source",
        "security",
        "index",
    ]

    @classmethod
    def raw(cls, security: str, index: str, format: DataLakeDatasetFileTypes, source: DataSources):
        return super().raw(format=format, source=source, index=index, security=security)


class FundamentalPath(AdlsDatasetPath):
    product = "fundamental"

    dir_template = [
        "product",
        "source",
        PathElement(name="entity", hive_flavor=True),
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    filename_template = [
        "datetime",
        "product",
        "source",
        "entity",
    ]

    @classmethod
    def raw(cls, entity: str, format: DataLakeDatasetFileTypes, source: DataSources):
        return super().raw(format=format, source=source, security=entity)