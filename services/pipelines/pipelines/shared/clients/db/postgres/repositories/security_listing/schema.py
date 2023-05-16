from typing import Optional

from pydantic import BaseModel


class SecurityListing(BaseModel):
    id: int
    exchange_id: int
    figi: str
    security_ticker_id: int
    quote_source: Optional[str]
