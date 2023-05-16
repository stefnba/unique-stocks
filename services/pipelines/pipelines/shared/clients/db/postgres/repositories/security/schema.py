from typing import Optional

from pydantic import BaseModel, Field


class Security(BaseModel):
    id: int
    country_id: Optional[str]
    figi: str = Field(..., alias="share_class_figi")
    description: Optional[str] = Field(..., alias="name_figi")
    security_type_id: int
    isin: Optional[str]
