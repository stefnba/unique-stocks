from pydantic import BaseModel
from shared.utils.path.types import FilePath


class DatalakeProperties(BaseModel):
    storage_account: str
    file_system: str
    storage_account_url: str


class DatalakeFile(BaseModel):
    file: FilePath
    datalake: DatalakeProperties
