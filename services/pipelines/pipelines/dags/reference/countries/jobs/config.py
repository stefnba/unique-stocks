from dags.reference.config import ReferenceBasePath, ReferenceFinalBasePath


class CountriesPath(ReferenceBasePath):
    asset: str = "countries"


class CountriesFinalPath(ReferenceFinalBasePath):
    asset: str = "countries"
