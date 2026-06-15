"""Tests for generic Prefect block handles."""

from uuid import uuid4

import pytest
from prefect.blocks.system import Secret
from prefect.exceptions import ObjectNotFound
from pydantic import SecretStr

from core.infrastructure import blocks as infrastructure_blocks
from core.infrastructure.blocks import define_block


def test_disabled_block_entry_guards_runtime_operations() -> None:
    """Disabled block entries should be inert until explicitly loaded."""
    entry = define_block("disabled-secret", Secret(value=SecretStr("secret")), enabled=False)

    assert not entry.exists()
    entry.save()
    with pytest.raises(ValueError, match="not configured"):
        entry.load()
    with pytest.raises(ValueError, match="not configured"):
        entry.document_id()


def test_block_entry_exists_returns_false_for_missing_block_document(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exists should return false only for Prefect's missing-block error."""
    entry = define_block("missing-secret", Secret(value=SecretStr("secret")))

    def missing_load(cls: type[Secret], name: str) -> Secret:
        raise ValueError("Unable to find block document named missing-secret") from ObjectNotFound(Exception(name))

    monkeypatch.setattr(Secret, "load", classmethod(missing_load))

    assert not entry.exists()


def test_block_entry_exists_reraises_unexpected_value_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exists should not hide validation or configuration errors."""
    entry = define_block("broken-secret", Secret(value=SecretStr("secret")))

    def broken_load(cls: type[Secret], name: str) -> Secret:
        raise ValueError(f"{name} has invalid schema")

    monkeypatch.setattr(Secret, "load", classmethod(broken_load))

    with pytest.raises(ValueError, match="invalid schema"):
        entry.exists()


@pytest.mark.asyncio
async def test_block_entry_exists_async_returns_false_for_missing_block_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """exists_async should return false only for Prefect's missing-block error."""
    entry = define_block("missing-secret", Secret(value=SecretStr("secret")))

    async def missing_load(cls: type[Secret], name: str) -> Secret:
        raise ValueError("Unable to find block document named missing-secret") from ObjectNotFound(Exception(name))

    monkeypatch.setattr(Secret, "aload", classmethod(missing_load))

    assert not await entry.exists_async()


@pytest.mark.asyncio
async def test_block_entry_exists_async_reraises_unexpected_value_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """exists_async should not hide validation or configuration errors."""
    entry = define_block("broken-secret", Secret(value=SecretStr("secret")))

    async def broken_load(cls: type[Secret], name: str) -> Secret:
        raise ValueError(f"{name} has invalid schema")

    monkeypatch.setattr(Secret, "aload", classmethod(broken_load))

    with pytest.raises(ValueError, match="invalid schema"):
        await entry.exists_async()


def test_block_entry_document_id_reads_prefect_block_document(monkeypatch: pytest.MonkeyPatch) -> None:
    """document_id should return the saved Prefect block document UUID."""
    expected_id = uuid4()
    calls: list[dict[str, object]] = []

    class FakeBlockDocument:
        """Fake Prefect block document."""

        id = expected_id

    class FakeClient:
        """Fake synchronous Prefect client."""

        def __enter__(self) -> FakeClient:
            """Enter the fake client context."""
            return self

        def __exit__(self, *_: object) -> None:
            """Exit without cleanup."""

        def read_block_document_by_name(
            self,
            *,
            name: str,
            block_type_slug: str,
            include_secrets: bool,
        ) -> FakeBlockDocument:
            """Record the block lookup and return a fake document."""
            calls.append(
                {
                    "name": name,
                    "block_type_slug": block_type_slug,
                    "include_secrets": include_secrets,
                }
            )
            return FakeBlockDocument()

    def fake_get_client(*, sync_client: bool = False) -> FakeClient:
        """Return a fake sync client."""
        assert sync_client is True
        return FakeClient()

    monkeypatch.setattr(infrastructure_blocks, "get_client", fake_get_client)
    entry = define_block("api-key", Secret(value=SecretStr("secret")))

    assert entry.document_id() == expected_id
    assert calls == [
        {
            "name": "api-key",
            "block_type_slug": Secret.get_block_type_slug(),
            "include_secrets": False,
        }
    ]


@pytest.mark.asyncio
async def test_block_entry_document_id_async_reads_prefect_block_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """document_id_async should return the saved Prefect block document UUID."""
    expected_id = uuid4()
    calls: list[dict[str, object]] = []

    class FakeBlockDocument:
        """Fake Prefect block document."""

        id = expected_id

    class FakeClient:
        """Fake asynchronous Prefect client."""

        async def __aenter__(self) -> FakeClient:
            """Enter the fake client context."""
            return self

        async def __aexit__(self, *_: object) -> None:
            """Exit without cleanup."""

        async def read_block_document_by_name(
            self,
            *,
            name: str,
            block_type_slug: str,
            include_secrets: bool,
        ) -> FakeBlockDocument:
            """Record the block lookup and return a fake document."""
            calls.append(
                {
                    "name": name,
                    "block_type_slug": block_type_slug,
                    "include_secrets": include_secrets,
                }
            )
            return FakeBlockDocument()

    def fake_get_client() -> FakeClient:
        """Return a fake async client."""
        return FakeClient()

    monkeypatch.setattr(infrastructure_blocks, "get_client", fake_get_client)
    entry = define_block("api-key", Secret(value=SecretStr("secret")))

    assert await entry.document_id_async() == expected_id
    assert calls == [
        {
            "name": "api-key",
            "block_type_slug": Secret.get_block_type_slug(),
            "include_secrets": False,
        }
    ]
