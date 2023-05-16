from pydantic import BaseModel
from shared.clients.db.postgres.repositories.exhange.repository import ExchangeRepository
from shared.clients.db.postgres.repositories.mapping.repository import MappingsRepository
from shared.clients.db.postgres.repositories.mapping_figi.repository import MappingFigiRepository
from shared.clients.db.postgres.repositories.mapping_surrogate_key.repository import MappingSurrogateKeyRepository
from shared.clients.db.postgres.repositories.security.repository import SecurityRepo
from shared.clients.db.postgres.repositories.security_listing.repository import SecurityListingRepo
from shared.clients.db.postgres.repositories.security_ticker.repository import SecurityTickerRepo
from shared.clients.db.postgres.repositories.security_type.repository import SecurityTypeRepo


class RegisteredQueryRepositories(BaseModel):
    mappings = MappingsRepository()
    exchange = ExchangeRepository()
    mapping_surrogate_key = MappingSurrogateKeyRepository()
    mapping_figi = MappingFigiRepository()
    security_listing = SecurityListingRepo()
    security = SecurityRepo()
    security_ticker = SecurityTickerRepo()
    security_type = SecurityTypeRepo()

    class Config:
        arbitrary_types_allowed = True


DbQueryRepositories = RegisteredQueryRepositories()


__all__ = ["DbQueryRepositories"]
