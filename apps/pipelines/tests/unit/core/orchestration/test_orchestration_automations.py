"""Tests for Prefect automation registries."""

from uuid import uuid4

import pytest
from prefect.automations import Automation
from prefect.events.actions import DoNothing
from prefect.events.schemas.automations import EventTrigger, Posture

from core.orchestration.automations import define_automations


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


def test_define_automations_stores_definitions_immutably() -> None:
    """Automation registries should keep a stable desired definition set."""
    automation = build_automation()

    registry = define_automations([automation])

    assert registry.automations == (automation,)


@pytest.mark.asyncio
async def test_automation_registry_sync_creates_when_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Missing automations should be created through the current Prefect API."""
    desired = build_automation()
    registry = define_automations([desired])
    created: list[Automation] = []

    async def read_missing(*, name: str) -> Automation:
        assert name == desired.name
        raise ValueError(f"Automation with name {name!r} not found")

    async def fake_create(self: Automation) -> Automation:
        created.append(self)
        self.id = uuid4()
        return self

    async def fail_update(self: Automation) -> None:
        raise AssertionError(f"unexpected update for {self.name}")

    monkeypatch.setattr(Automation, "aread", read_missing)
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

    async def read_existing(*, name: str) -> Automation:
        assert name == desired.name
        return existing

    async def fail_create(self: Automation) -> Automation:
        raise AssertionError(f"unexpected create for {self.name}")

    async def fake_update(self: Automation) -> None:
        updated.append(self)

    monkeypatch.setattr(Automation, "aread", read_existing)
    monkeypatch.setattr(Automation, "acreate", fail_create)
    monkeypatch.setattr(Automation, "aupdate", fake_update)

    exit_code = await registry.sync(plan=False)

    assert exit_code == 0
    assert "Updated automation: demo automation" in capsys.readouterr().out
    assert updated[0].id == existing.id
    assert updated[0].name == desired.name
    assert updated[0] is not desired
    assert desired.id is None
