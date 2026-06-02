"""Raw provider models for the ISO 10383 MIC registry CSV."""

from datetime import date, datetime
from typing import ClassVar, Literal

from pydantic import Field, field_validator

from core.models import ProviderModel


class ISO10383MICRaw(ProviderModel):
    """One row from the ISO 10383 MIC registry CSV release."""

    provider: ClassVar[str] = "iso10383"

    mic: str = Field(alias="MIC", min_length=4, max_length=4)
    operating_mic: str = Field(alias="OPERATING MIC", min_length=4, max_length=4)
    mic_type: Literal["OPRT", "SGMT"] = Field(alias="OPRT/SGMT")
    name: str = Field(alias="MARKET NAME-INSTITUTION DESCRIPTION")
    legal_entity_name: str | None = Field(default=None, alias="LEGAL ENTITY NAME")
    lei: str | None = Field(default=None, alias="LEI")
    market_category_code: str | None = Field(default=None, alias="MARKET CATEGORY CODE")
    acronym: str | None = Field(default=None, alias="ACRONYM")
    country_iso2: str = Field(alias="ISO COUNTRY CODE (ISO 3166)", min_length=2, max_length=2)
    city: str = Field(alias="CITY")
    website: str | None = Field(default=None, alias="WEBSITE")
    status: str = Field(alias="STATUS")
    creation_date: date = Field(alias="CREATION DATE")
    last_update_date: date | None = Field(default=None, alias="LAST UPDATE DATE")
    last_validation_date: date | None = Field(default=None, alias="LAST VALIDATION DATE")
    expiry_date: date | None = Field(default=None, alias="EXPIRY DATE")
    comments: str | None = Field(default=None, alias="COMMENTS")

    @field_validator("*", mode="before")
    @classmethod
    def _blank_to_none(cls, value: object) -> object:
        """Normalize blank optional CSV cells before typed validation."""
        if isinstance(value, str) and value.strip() == "":
            return None
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("creation_date", "last_update_date", "last_validation_date", "expiry_date", mode="before")
    @classmethod
    def _parse_iso_release_date(cls, value: object) -> object:
        """Parse ISO 10383 release dates in YYYYMMDD format."""
        if value is None or isinstance(value, date):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                return None
            return datetime.strptime(stripped, "%Y%m%d").date()
        return value
