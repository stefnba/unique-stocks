from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class MappingFigi(BaseModel):
    isin_source: Optional[str] = Field(..., alias="isin")
    wkn_source: Optional[int] = None
    ticker_source: Optional[str] = None
    ticker_figi: Optional[str] = None
    name_figi: Optional[str] = None
    figi: str
    share_class_figi: Optional[str] = None
    composite_figi: Optional[str] = None
    exchange_code_figi: Optional[str] = None
    security_type_figi: Optional[str] = None
    security_type2_figi: Optional[str] = None
    market_sector_figi: Optional[str] = None
    security_description_figi: Optional[str] = None


# class MappingFigi(BaseModel):
#     isin: Optional[str]
#     wkn: Optional[str]
#     ticker: Optional[str]
#     ticker_figi: Optional[str]
#     name_figi: Optional[str]
#     exchange_mic: Optional[str] = Field(..., alias="exchange_mic_figi")
#     currency: Optional[str]
#     country: Optional[str]
#     figi: Optional[str]
#     composite_figi: Optional[str]
#     share_class_figi: Optional[str]
#     security_type_id: Optional[str]
#     is_active: Optional[bool] = True
#     # created_at: Optional[datetime] = datetime.utcnow()
#     updated_at: Optional[datetime]
#     active_from: Optional[datetime]
#     active_until: Optional[datetime]
