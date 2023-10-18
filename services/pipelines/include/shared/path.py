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
        return super().raw(format=format, source=source, exchange=exchange)


class SecurityQuotePerformancePath(AdlsDatasetPath):
    product = "quote_performance"


class SecurityQuotePath(AdlsDatasetPath):
    product = "security_quote"

    dir_template = [
        "product",
        "source",
        PathElement(name="security", hive_flavor=True),
        PathElement(name="exchange", hive_flavor=True),
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    filename_template = [
        "datetime",
        "product",
        "source",
        "security",
        "exchange",
    ]

    @classmethod
    def raw(cls, security: str, exchange: str, format: DataLakeDatasetFileTypes, source: DataSources):
        return super().raw(format=format, source=source, exchange=exchange, security=security)


class IndexMemberPath(AdlsDatasetPath):
    product = "index_member"

    dir_template = [
        "product",
        "source",
        PathElement(name="index", hive_flavor=True),
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    filename_template = [
        "datetime",
        "product",
        "source",
        "index",
    ]

    @classmethod
    def raw(cls, index: str, format: DataLakeDatasetFileTypes, source: DataSources):
        return super().raw(format=format, source=source, index=index)


class FundamentalPath(AdlsDatasetPath):
    product = "fundamental"

    dir_template = [
        "product",
        "source",
        PathElement(name="entity", hive_flavor=True),
        PathElement(name="exchange", hive_flavor=True),
        PathElement(name="year", hive_flavor=True),
        PathElement(name="month", hive_flavor=True),
        PathElement(name="day", hive_flavor=True),
    ]
    filename_template = [
        "datetime",
        "product",
        "source",
        "entity",
        "exchange",
    ]

    @classmethod
    def raw(cls, entity: str, exchange: str, format: DataLakeDatasetFileTypes, source: DataSources):
        return super().raw(format=format, source=source, entity=entity, exchange=exchange)
