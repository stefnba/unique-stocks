"""Tests for Prefect automation registries."""

from uuid import uuid4

import pytest
from prefect.automations import Automation
from prefect.events.actions import DoNothing
from prefect.events.schemas.automations import EventTrigger, Posture

from core.orchestration import automations as orchestration_automations
from core.orchestration.automations import CustomAutomation, define_automations


def build_automation(name: str = "demo automation") -> Automation:
    """Build a minimal automation for sync tests."""
    return Automation(
        name=name,
        description="demo",
        trigger=EventTrigger(
            expect={"demo.event"},
            match={},
            posture=Posture.Reactive,
            threshold=1,
        ),
        actions=[DoNothing()],
    )


class DemoCustomAutomation(CustomAutomation):
    """Minimal custom automation for registry conversion tests."""

    name = "custom automation"

    def to_prefect_automation(self) -> Automation:
        """Return a native Prefect automation."""
        return build_automation(self.name)


class DeferredCustomAutomation(CustomAutomation):
    """Custom automation that fails if materialized too early."""

    name = "deferred automation"

    def to_prefect_automation(self) -> Automation:
        """Fail if this definition is materialized."""
        raise AssertionError("custom automations should be materialized at sync time")


class FakeClient:
    """Fake Prefect automation client."""

    def __init__(self, existing: list[Automation] | None = None) -> None:
        """Store existing server automations and deleted ids."""
        self.existing = existing or []
        self.deleted: list[object] = []

    async def read_automations(self) -> list[Automation]:
        """Return fake server automations."""
        return self.existing

    async def delete_automation(self, automation_id: object) -> None:
        """Capture deleted automation ids."""
        self.deleted.append(automation_id)


class FakeClientContext:
    """Async context manager for fake Prefect automation clients."""

    def __init__(self, client: FakeClient) -> None:
        """Store the fake client."""
        self.client = client

    async def __aenter__(self) -> FakeClient:
        """Return the fake client."""
        return self.client

    async def __aexit__(self, *_: object) -> None:
        """Exit without cleanup."""


def test_define_automations_stores_definitions_immutably() -> None:
    """Automation registries should keep a stable desired definition set."""
    automation = build_automation()

    registry = define_automations([automation])

    assert registry.definitions == (automation,)
    assert registry.automations == (automation,)


def test_define_automations_accepts_custom_definitions() -> None:
    """Custom automation presets should resolve to native Prefect automations."""
    definition = DemoCustomAutomation()

    registry = define_automations([definition])
    automation = registry.automations[0]

    assert registry.definitions == (definition,)
    assert isinstance(automation, Automation)
    assert automation.name == "custom automation"


def test_define_automations_defers_custom_materialization() -> None:
    """Custom automations should not resolve external state during declaration."""
    definition = DeferredCustomAutomation()

    registry = define_automations([definition])

    assert registry.definitions == (definition,)


def test_define_automations_rejects_duplicate_names() -> None:
    """Desired automation names should be unique before touching Prefect."""
    automation = build_automation()

    with pytest.raises(ValueError, match="Duplicate Prefect automation"):
        define_automations([automation, automation])


@pytest.mark.asyncio
async def test_automation_registry_sync_creates_when_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Missing automations should be created through the current Prefect API."""
    desired = build_automation()
    registry = define_automations([desired])
    created: list[Automation] = []

    async def fake_read(cls: type[Automation], id: object | None = None, name: str | None = None) -> Automation:
        raise ValueError(f"Automation with name {name!r} not found")

    async def fake_create(self: Automation) -> Automation:
        created.append(self)
        self.id = uuid4()
        return self

    async def fail_update(self: Automation) -> None:
        raise AssertionError(f"unexpected update for {self.name}")

    monkeypatch.setattr(Automation, "aread", classmethod(fake_read))
    monkeypatch.setattr(Automation, "acreate", fake_create)
    monkeypatch.setattr(Automation, "aupdate", fail_update)

    exit_code = await registry.sync(plan=False)

    assert exit_code == 0
    assert "Created automation: demo automation" in capsys.readouterr().out
    assert created[0].name == desired.name
    assert created[0] is not desired
    assert desired.id is None


@pytest.mark.asyncio
async def test_automation_registry_sync_updates_existing_by_name(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Existing automations should copy the server id before updating."""
    desired = build_automation()
    existing = build_automation()
    existing.id = uuid4()
    registry = define_automations([desired])
    updated: list[Automation] = []

    async def fake_read(cls: type[Automation], id: object | None = None, name: str | None = None) -> Automation:
        return existing

    async def fail_create(self: Automation) -> Automation:
        raise AssertionError(f"unexpected create for {self.name}")

    async def fake_update(self: Automation) -> None:
        updated.append(self)

    monkeypatch.setattr(Automation, "aread", classmethod(fake_read))
    monkeypatch.setattr(Automation, "acreate", fail_create)
    monkeypatch.setattr(Automation, "aupdate", fake_update)

    exit_code = await registry.sync(plan=False)

    assert exit_code == 0
    assert "Updated automation: demo automation" in capsys.readouterr().out
    assert updated[0].id == existing.id
    assert updated[0].name == desired.name
    assert updated[0] is not desired
    assert desired.id is None


@pytest.mark.asyncio
async def test_automation_registry_deletes_all_visible_automations(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cleanup should remove every automation visible to the configured Prefect API."""
    desired = build_automation()
    desired.tags = ["unique-stocks"]
    existing = build_automation()
    existing.tags = ["unique-stocks"]
    existing.id = uuid4()
    unrelated = build_automation("manual automation")
    unrelated.id = uuid4()
    client = FakeClient(existing=[existing, unrelated])
    registry = define_automations([desired])
    monkeypatch.setattr(orchestration_automations, "get_client", lambda: FakeClientContext(client))

    await registry.delete_automations()

    assert client.deleted == [existing.id, unrelated.id]
    assert "Deleted automation: demo automation" in capsys.readouterr().out
