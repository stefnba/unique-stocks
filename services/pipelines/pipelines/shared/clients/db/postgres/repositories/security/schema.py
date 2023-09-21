from typing import Optional

from pydantic import BaseModel


class Security(BaseModel):
    id: int
    country_id: Optional[str]
    figi: str
    name: Optional[str]
    entity_isin_id: Optional[int]
    security_type_id: int
    isin: Optional[str]
    name_figi_alias: Optional[list[str]]
    name_figi_count: Optional[int]
    # level_figi: int
