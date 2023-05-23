from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class MappingFigi(BaseModel):
    isin: Optional[str]
    wkn: Optional[str]
    ticker: Optional[str]
    ticker_figi: Optional[str]
    name_figi: Optional[str]
    exchange_mic: Optional[str] = Field(..., alias="exchange_mic_figi")
    currency: Optional[str]
    country: Optional[str]
    figi: Optional[str]
    composite_figi: Optional[str]
    share_class_figi: Optional[str]
    security_type_id: Optional[str]
    is_active: Optional[bool] = True
    # created_at: Optional[datetime] = datetime.utcnow()
    updated_at: Optional[datetime]
    valid_from: Optional[datetime]
    valid_until: Optional[datetime]
