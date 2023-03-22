from dags.reference.config import ReferenceBasePath


class CurrenciesPath(ReferenceBasePath):
    asset: str = "currencies"
