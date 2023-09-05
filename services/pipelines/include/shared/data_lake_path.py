from utils.filesystem.data_lake import DataLakePath, PathElement, TempDirectory, TempFile


class ExchangePath(DataLakePath):
    product = "exchange"


class EntityPath(DataLakePath):
    product = "entity"


class SecurityPath(DataLakePath):
    product = "security"


class SecurityQuotePath(DataLakePath):
    product = "security_quote"

    placeholder_pattern = [
        PathElement(key="security"),
        PathElement(key="exchange"),
    ]

    def add_element(self, security: str, exchange: str):
        return super().add_element(security=security, exchange=exchange)


class FundamentalPath(DataLakePath):
    product = "fundamental"

    placeholder_pattern = [
        PathElement(key="entity"),
    ]

    def add_element(self, entity: str):
        return super().add_element(entity=entity)
