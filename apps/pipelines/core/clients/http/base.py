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
from collections.abc import Mapping, Sequence
from typing import ClassVar, Final, Literal, Self, TypeVar, cast

import httpx
import structlog
from pydantic import BaseModel

from core.prefect_controls import wait_for_provider_api_credit
from core.utils.redaction import redact_sensitive_query_params

log = structlog.get_logger(__name__)

type HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]
type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]
type QueryParamValue = str | int | float | bool | None
type QueryParams = Mapping[str, QueryParamValue | Sequence[QueryParamValue]]

T = TypeVar("T", bound=BaseModel)

REDACTED_QUERY_VALUE: Final[str] = "[redacted]"


def redacted_http_status_error(exc: httpx.HTTPStatusError) -> httpx.HTTPStatusError:
    """Return a status error whose message and request URL are safe for Prefect logs."""
    safe_request = httpx.Request(
        exc.request.method,
        redact_sensitive_query_params(str(exc.request.url)),
        headers=exc.request.headers,
    )
    safe_response = httpx.Response(
        exc.response.status_code,
        content=redact_sensitive_query_params(exc.response.text),
        request=safe_request,
    )
    return httpx.HTTPStatusError(
        redact_sensitive_query_params(str(exc)),
        request=safe_request,
        response=safe_response,
    )


class ProviderRateLimitError(httpx.HTTPStatusError):
    """Provider returned HTTP 429 and further immediate calls should stop."""

    def __init__(
        self,
        message: str,
        *,
        request: httpx.Request,
        response: httpx.Response,
        retry_after: str | None = None,
    ) -> None:
        """Create a redacted rate-limit error with optional provider retry guidance."""
        super().__init__(message, request=request, response=response)
        self.retry_after = retry_after


def redacted_provider_rate_limit_error(exc: httpx.HTTPStatusError) -> ProviderRateLimitError:
    """Return a redacted 429 error that orchestration code can distinguish."""
    safe_exc = redacted_http_status_error(exc)
    return ProviderRateLimitError(
        str(safe_exc),
        request=safe_exc.request,
        response=safe_exc.response,
        retry_after=exc.response.headers.get("Retry-After"),
    )


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
        params: QueryParams | None = None,
        json: JsonObject | None = None,
    ) -> JsonValue:
        """Make an authenticated HTTP request and return the parsed JSON body.

        Args:
            path:   URL path relative to BASE_URL.
            method: HTTP method (default: GET).
            params: Per-request query params, merged with _default_params by httpx.
            json:   Request body for POST/PATCH/PUT.

        Raises:
            httpx.HTTPStatusError:  Non-2xx response.
            ProviderRateLimitError: Provider returned HTTP 429.
            httpx.TimeoutException: Request timed out.
            RuntimeError:           Called outside of async context manager.
        """
        response = await self._send(path, method=method, params=params, json=json)
        return cast(JsonValue, response.json())

    async def _request_text(
        self,
        path: str,
        *,
        method: HttpMethod = "GET",
        params: QueryParams | None = None,
        json: JsonObject | None = None,
    ) -> str:
        """Make an authenticated HTTP request and return the text body."""
        response = await self._send(path, method=method, params=params, json=json)
        return response.text

    async def _send(
        self,
        path: str,
        *,
        method: HttpMethod = "GET",
        params: QueryParams | None = None,
        json: JsonObject | None = None,
    ) -> httpx.Response:
        """Make an authenticated HTTP request and return the response object."""
        if self._http is None:
            raise RuntimeError(f"{type(self).__name__} must be used as an async context manager")

        safe_path = redact_sensitive_query_params(path)
        log.debug(f"http.client.{self.PROVIDER}.request", method=method, path=safe_path)
        try:
            await wait_for_provider_api_credit(
                provider=self.PROVIDER,
                operation=f"{method} {safe_path}",
            )
            response = await self._http.request(method, path, params=params, json=json)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                log.warning(
                    "provider.rate_limit",
                    provider=self.PROVIDER,
                    path=safe_path,
                    status=exc.response.status_code,
                    retry_after=exc.response.headers.get("Retry-After"),
                    body=redact_sensitive_query_params(exc.response.text[:300]),
                )
                raise redacted_provider_rate_limit_error(exc) from None
            log.error(
                f"http.client.{self.PROVIDER}.http_error",
                path=safe_path,
                status=exc.response.status_code,
                body=redact_sensitive_query_params(exc.response.text[:300]),
            )
            raise redacted_http_status_error(exc) from None
        except httpx.TimeoutException:
            log.error(f"http.client.{self.PROVIDER}.timeout", path=safe_path, method=method)
            raise

        log.debug(
            f"http.client.{self.PROVIDER}.response",
            path=safe_path,
            method=method,
            status=response.status_code,
            elapsed_seconds=_response_elapsed_seconds(response),
        )
        return response

    async def _get(
        self,
        path: str,
        *,
        model: type[T],
        params: QueryParams | None = None,
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
        params: QueryParams | None = None,
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
        if not isinstance(data, list):
            raise TypeError(f"Expected list response from {path}, got {type(data).__name__}")
        return [model.model_validate(item) for item in data]


def _response_elapsed_seconds(response: httpx.Response) -> float | None:
    """Return response elapsed seconds when httpx has recorded it."""
    try:
        return response.elapsed.total_seconds()
    except RuntimeError:
        return None
