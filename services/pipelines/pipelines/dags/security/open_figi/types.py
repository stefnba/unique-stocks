from typing import Dict, Optional, TypedDict

from pydantic import BaseModel, Field


class FigiSecurityTypeDict(TypedDict):
    name_figi: str
    name_figi2: str
    market_sector_figi: str


FigiSecurityTypeMappingDict = Dict[int, FigiSecurityTypeDict]


class SecurityData(BaseModel):
    ticker: str
    source: str
    isin: Optional[str]
    security_type_id: int
    source_exchange_uid: str = Field(..., alias="exchange_uid")
