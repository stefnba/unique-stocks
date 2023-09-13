from utils.filesystem.data_lake import DataLakePath, PathElement, TempDirectory, TempFile


class ExchangePath(DataLakePath):
    product = "exchange"


class EntityPath(DataLakePath):
    product = "entity"


class EntityIsinPath(DataLakePath):
    product = "entity_isin"


class SecurityPath(DataLakePath):
    product = "security"

    placeholder_pattern = [
        PathElement(key="exchange"),
    ]

    def add_element(self, exchange: str):
        return super().add_element(exchange=exchange)


class SecurityQuotePath(DataLakePath):
    product = "security_quote"

    placeholder_pattern = [
        PathElement(key="security"),
        PathElement(key="exchange"),
    ]

    def add_element(self, security: str, exchange: str):
        return super().add_element(security=security, exchange=exchange)


class IndexMemberPath(DataLakePath):
    product = "index_member"

    placeholder_pattern = [
        PathElement(key="security"),
        PathElement(key="index"),
    ]

    def add_element(self, index: str, exchange: str):
        return super().add_element(security=index, exchange=exchange)


class FundamentalPath(DataLakePath):
    product = "fundamental"

    placeholder_pattern = [
        PathElement(key="entity"),
    ]

    def add_element(self, entity: str):
        return super().add_element(entity=entity)
