"""ISO 10383 MIC registry HTTP client."""

from core.clients.http.base import HttpClientBase
from providers.registry import Provider

ISO10383_MIC_CSV_PATH = "/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"


class ISO10383Client(HttpClientBase):
    """HTTP client for the public ISO 10383 MIC registry files."""

    PROVIDER = Provider.ISO10383
    BASE_URL = "https://www.iso20022.org"

    def __init__(self, timeout: float = 30.0) -> None:
        """Initialize with an optional request timeout."""
        self._timeout = timeout

    async def get_mic_registry_csv(self) -> str:
        """Return the current ISO 10383 MIC registry CSV release as text."""
        return await self._request_text(ISO10383_MIC_CSV_PATH)
