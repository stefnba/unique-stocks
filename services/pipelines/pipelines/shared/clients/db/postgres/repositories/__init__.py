from pydantic import BaseModel
from shared.clients.db.postgres.repositories.mappings import MappingsRepository


class RegisteredRepositories(BaseModel):
    mappings = MappingsRepository()

    class Config:
        arbitrary_types_allowed = True


DbRepositories = RegisteredRepositories()


__all__ = ["DbRepositories"]
