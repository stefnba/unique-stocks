from pydantic import BaseModel
from shared.clients.db.postgres.repositories.mappings import MappingsRepository


class RegisteredQueryRepositories(BaseModel):
    mappings = MappingsRepository()

    class Config:
        arbitrary_types_allowed = True


DbQueryRepositories = RegisteredQueryRepositories()


__all__ = ["DbQueryRepositories"]
