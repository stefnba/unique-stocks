from pydantic import BaseModel


class SecurityTicker(BaseModel):
    id: int
    ticker: str
    security_id: int
