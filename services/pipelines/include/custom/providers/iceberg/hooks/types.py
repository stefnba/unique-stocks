import typing as t

CatalogType = t.Literal["glue", "sql", "hive"]


AWSFileIOProps = t.TypedDict(
    "AWSFileIOProps",
    {
        "s3.access-key-id": str,
        "s3.secret-access-key": str,
        "s3.region": str,
    },
)


def is_aws_file_io_connection(connection: t.Mapping) -> t.TypeGuard[AWSFileIOProps]:
    if "s3.access-key-id" in connection and "s3.secret-access-key" in connection and "s3.region" in connection:
        return True
    return False


AzureFileIOProps = t.TypedDict(
    "AzureFileIOProps",
    {
        "adlfs.account-name": str,
        "adlfs.tenant-id": str,
        "adlfs.client-id": str,
        "adlfs.client-secret": str,
    },
)

FileIOConnectionProps = AWSFileIOProps | AzureFileIOProps


class GlueCatalogConnectionProps(t.TypedDict):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    type: CatalogType


def is_glue_catalog_connection(connection: dict) -> t.TypeGuard[GlueCatalogConnectionProps]:
    if "aws_access_key_id" in connection and "aws_secret_access_key" in connection and "region_name" in connection:
        return True
    return False


class SQLCatalogConnectionProps(t.TypedDict):
    uri: str
    type: CatalogType


class HiveCatalogConnectionProps(t.TypedDict):
    uri: str
    type: CatalogType


def is_sql_catalog_connection(connection: t.Mapping) -> t.TypeGuard[SQLCatalogConnectionProps]:
    if "uri" in connection:
        return True
    return False


def is_hive_catalog_connection(connection: t.Mapping) -> t.TypeGuard[HiveCatalogConnectionProps]:
    if "uri" in connection:
        return True
    return False


CatalogConnectionProps = GlueCatalogConnectionProps | SQLCatalogConnectionProps
