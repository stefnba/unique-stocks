from dags.reference.config import ReferenceBasePath, ReferenceFinalBasePath


class MappingPath(ReferenceBasePath):
    asset: str = "mapping"


class MappingFinalPath(ReferenceFinalBasePath):
    asset: str = "mapping"
