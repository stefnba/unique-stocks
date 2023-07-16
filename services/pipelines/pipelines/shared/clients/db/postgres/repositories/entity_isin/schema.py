from typing import Optional

from pydantic import BaseModel


class EntityIsin(BaseModel):
    id: int
    entity_id: Optional[int]
    isin: str
