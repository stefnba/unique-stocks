from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class MappingFigi(BaseModel):
    isin: Optional[str]
    wkn: Optional[str]
    ticker: Optional[str]
    ticker_figi: Optional[str]
    exchange_mic: Optional[str]
    # exchange_operating_mic: Optional[str]
    # exchange_code_figi: Optional[str]
    currency: Optional[str]
    country: Optional[str]
    figi: Optional[str]
    composite_figi: Optional[str]
    share_class_figi: Optional[str]
    security_type_id: Optional[str]
    is_active: bool = True
    # created_at: Optional[datetime] = datetime.utcnow()
    updated_at: Optional[datetime]
    valid_from: Optional[datetime]
    valid_until: Optional[datetime]
