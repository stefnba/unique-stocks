from typing import Optional

from pydantic import BaseModel, Field


class SecurityTicker(BaseModel):
    id: int
    ticker: str = Field(..., alias="ticker_figi")
    security_id: int
