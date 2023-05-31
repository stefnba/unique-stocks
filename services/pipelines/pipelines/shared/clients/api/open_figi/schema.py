from typing import Optional

from pydantic import BaseModel, Field


class FigiResult(BaseModel):
    figi: str
    name_figi: str = Field(..., alias="name")
    ticker_figi: str = Field(..., alias="ticker")
    exchange_code_figi: Optional[str] = Field(..., alias="exchCode")
    composite_figi: Optional[str] = Field(..., alias="compositeFIGI")
    share_class_figi: Optional[str] = Field(..., alias="shareClassFIGI")
    security_type: str = Field(..., alias="securityType")
    security_type_alias: str = Field(..., alias="securityType2")
    market_sector: str = Field(..., alias="marketSector")
