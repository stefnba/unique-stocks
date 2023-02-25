from pydantic import BaseModel


class DataLakeFileUpload(BaseModel):
    file_system: str
    file_name: str
    file_path: str
    file_extension: str
    storage_account: str | None
