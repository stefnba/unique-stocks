"""Tests for shared HTTP client logging safety."""

from collections.abc import Generator
from typing import ClassVar, Self

import httpx
import pytest
from pytest import CaptureFixture
from structlog.testing import capture_logs

from config.settings import Settings
from core.clients.http.base import REDACTED_QUERY_VALUE, HttpClientBase, ProviderRateLimitError
from core.prefect.concurrency import ProviderRateLimitPolicy
from core.utils.logging import configure_logging

SECRET_TOKEN = "provider-token-123"


class _TokenQueryAuth(httpx.Auth):
    """Test auth that mirrors providers which use query-string API tokens."""

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
        """Inject the test token as an API query parameter."""
        request.url = request.url.copy_merge_params({"api_token": SECRET_TOKEN})
        yield request


class _DemoClient(HttpClientBase):
    """HTTP client with injectable transport for log-safety tests."""

    PROVIDER: ClassVar[str] = "demo"
    BASE_URL: ClassVar[str] = "https://provider.example"
    RATE_LIMIT_POLICY: ClassVar[ProviderRateLimitPolicy | None] = ProviderRateLimitPolicy(
        burst_capacity=2,
        slot_decay_per_second=1.0,
    )

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        """Create a client using an in-memory transport."""
        self._transport = transport

    @property
    def _auth(self) -> httpx.Auth:
        return _TokenQueryAuth()

    async def __aenter__(self) -> Self:
        """Open the underlying HTTP session with the test transport."""
        self._http = httpx.AsyncClient(
            base_url=self.BASE_URL,
            auth=self._auth,
            params=self._default_params,
            timeout=httpx.Timeout(self._timeout),
            transport=self._transport,
        )
        return self


@pytest.mark.asyncio
async def test_http_client_waits_for_provider_api_credit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every provider request should pass through the shared Prefect rate limit."""
    calls: list[dict[str, object]] = []

    async def wait_for_credit(**kwargs: object) -> None:
        calls.append(kwargs)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[], request=request)

    monkeypatch.setattr("core.clients.http.base.wait_for_provider_api_credit", wait_for_credit)

    async with _DemoClient(httpx.MockTransport(handler)) as client:
        await client._request("/prices", params={"symbol": "AAPL.US"})

    assert calls == [
        {
            "provider": "demo",
            "operation": "GET /prices",
            "policy": _DemoClient.RATE_LIMIT_POLICY,
        }
    ]


@pytest.mark.asyncio
async def test_http_client_redacts_sensitive_query_params_from_logged_path() -> None:
    """Sensitive query parameters passed in paths should be redacted from logs."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="provider unavailable", request=request)

    transport = httpx.MockTransport(handler)

    with capture_logs() as logs, pytest.raises(httpx.HTTPStatusError) as exc_info:
        async with _DemoClient(transport) as client:
            await client._request(f"/prices?api_token={SECRET_TOKEN}&symbol=AAPL.US")

    assert SECRET_TOKEN not in str(exc_info.value)
    assert SECRET_TOKEN not in str(exc_info.value.request.url)
    assert SECRET_TOKEN not in str(exc_info.value.response.request.url)
    log_text = repr(logs)
    assert SECRET_TOKEN not in log_text
    assert f"api_token={REDACTED_QUERY_VALUE}" in log_text


@pytest.mark.asyncio
async def test_http_client_redacts_provider_token_echoed_in_error_body() -> None:
    """Provider error bodies that echo request URLs should not leak auth tokens."""
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return httpx.Response(
            429,
            text=f"quota exceeded for {request.url}",
            request=request,
        )

    transport = httpx.MockTransport(handler)

    with capture_logs() as logs, pytest.raises(ProviderRateLimitError) as exc_info:
        async with _DemoClient(transport) as client:
            await client._request("/prices", params={"symbol": "AAPL.US"})

    assert exc_info.value.response.status_code == 429
    assert SECRET_TOKEN in seen_urls[0]
    assert SECRET_TOKEN not in str(exc_info.value)
    assert SECRET_TOKEN not in str(exc_info.value.request.url)
    assert SECRET_TOKEN not in str(exc_info.value.response.request.url)
    assert SECRET_TOKEN not in exc_info.value.response.text
    log_text = repr(logs)
    assert SECRET_TOKEN not in log_text
    assert f"api_token={REDACTED_QUERY_VALUE}" in log_text


@pytest.mark.asyncio
async def test_http_client_success_response_logs_at_debug_level(capsys: CaptureFixture[str]) -> None:
    """Successful provider responses should stay out of normal INFO operational logs."""
    configure_logging(settings=Settings(pipeline_log_format="json", pipeline_log_level="INFO"), force=True)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[], request=request)

    transport = httpx.MockTransport(handler)

    async with _DemoClient(transport) as client:
        await client._request("/prices", params={"symbol": "AAPL.US"})

    assert "http.client.demo.response" not in capsys.readouterr().out
