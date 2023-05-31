from typing import Optional

from pydantic import BaseModel


class SecurityQuote(BaseModel):
    security_listing_id: int
    interval_id: int
    timestamp: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    adj_close: Optional[float]
    volume: Optional[float]
