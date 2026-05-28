"""Base HTTP client with shared request infrastructure.

Subclasses must define:
- BASE_URL:        the API root URL as a class variable
- _auth:           override to return an httpx.Auth instance (default: no auth)
- _default_params: override to add params on every request (default: none)

Usage:
    async with MyClient(api_key="...") as client:
        data = await client._request("/some/path", params={"foo": "bar"})
"""

from abc import ABC
from typing import Any, ClassVar, Literal, Self, TypeVar

import httpx
import structlog
from pydantic import BaseModel, TypeAdapter

log = structlog.get_logger(__name__)

type HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]

T = TypeVar("T", bound=BaseModel)


class HttpClientBase(ABC):
    """Abstract async HTTP client backed by httpx."""

    PROVIDER: ClassVar[str]
    BASE_URL: ClassVar[str]

    _timeout: float = 30.0
    _http: httpx.AsyncClient | None = None

    @property
    def _auth(self) -> httpx.Auth | None:
        """Return an httpx.Auth instance injected into every request.

        Override in subclasses to implement API key, Bearer token, Basic auth, etc.
        """
        return None

    @property
    def _default_params(self) -> dict[str, str]:
        """Params merged into every request (e.g. fmt=json, api_version=v2).

        Separate from auth — use _auth for credentials.
        """
        return {}

    async def __aenter__(self) -> Self:
        """Open the underlying httpx session."""
        self._http = httpx.AsyncClient(
            base_url=self.BASE_URL,
            auth=self._auth,
            params=self._default_params,
            timeout=httpx.Timeout(self._timeout),
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the underlying httpx session."""
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _request(
        self,
        path: str,
        *,
        method: HttpMethod = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any:
        """Make an authenticated HTTP request and return the parsed JSON body.

        Args:
            path:   URL path relative to BASE_URL.
            method: HTTP method (default: GET).
            params: Per-request query params, merged with _default_params by httpx.
            json:   Request body for POST/PATCH/PUT.

        Raises:
            httpx.HTTPStatusError:  Non-2xx response.
            httpx.TimeoutException: Request timed out.
            RuntimeError:           Called outside of async context manager.
        """
        response = await self._send(path, method=method, params=params, json=json)
        return response.json()

    async def _request_text(
        self,
        path: str,
        *,
        method: HttpMethod = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> str:
        """Make an authenticated HTTP request and return the text body."""
        response = await self._send(path, method=method, params=params, json=json)
        return response.text

    async def _send(
        self,
        path: str,
        *,
        method: HttpMethod = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Make an authenticated HTTP request and return the response object."""
        if self._http is None:
            raise RuntimeError(f"{type(self).__name__} must be used as an async context manager")

        log.debug(f"http.client.{self.PROVIDER}.request", method=method, path=path)
        try:
            response = await self._http.request(method, path, params=params, json=json)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            log.error(
                f"http.client.{self.PROVIDER}.http_error",
                path=path,
                status=exc.response.status_code,
                body=exc.response.text[:300],
            )
            raise
        except httpx.TimeoutException:
            log.error(f"http.client.{self.PROVIDER}.timeout", path=path, method=method)
            raise

        log.info(
            f"http.client.{self.PROVIDER}.response",
            path=path,
            method=method,
            status=response.status_code,
            elapsed_seconds=response.elapsed.total_seconds(),
        )
        return response

    async def _get(
        self,
        path: str,
        *,
        model: type[T],
        params: dict[str, Any] | None = None,
    ) -> T:
        """GET a single resource and validate the response against a Pydantic model.

        Args:
            path:   URL path relative to BASE_URL.
            model:  Pydantic model to validate the response against.
            params: Per-request query params, merged with _default_params by httpx.

        Returns:
            Validated response.
        """
        data = await self._request(path, params=params)
        return model.model_validate(data)

    async def _get_list(
        self,
        path: str,
        *,
        model: type[T],
        params: dict[str, Any] | None = None,
    ) -> list[T]:
        """GET a list resource and validate each item against a Pydantic model.

        Args:
            path:   URL path relative to BASE_URL.
            model:  Pydantic model to validate each item against.
            params: Per-request query params, merged with _default_params by httpx.

        Returns:
            List of validated items.
        """
        data = await self._request(path, params=params)
        return TypeAdapter(list[model]).validate_python(data)
