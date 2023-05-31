from typing import Optional

from pydantic import BaseModel


class Entity(BaseModel):
    id: int
    lei: Optional[str] = None
    type_id: Optional[int] = None
    name: str
    description: Optional[str] = None
    legal_address_street: Optional[str] = None
    legal_address_street_number: Optional[str] = None
    legal_address_zip_code: Optional[str] = None
    legal_address_city: Optional[str] = None
    legal_address_country: Optional[str] = None
    headquarter_address_street: Optional[str] = None
    headquarter_address_street_number: Optional[str] = None
    headquarter_address_city: Optional[str] = None
    headquarter_address_zip_code: Optional[str] = None
    headquarter_address_country: Optional[str] = None
    jurisdiction: Optional[str] = None
    industry_id: Optional[int] = None
    sector_id: Optional[int] = None
    country_id: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    telephone: Optional[str] = None
