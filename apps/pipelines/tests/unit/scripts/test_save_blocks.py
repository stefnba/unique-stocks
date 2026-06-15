"""Tests for the Prefect block save CLI adapter."""

import pytest

from scripts.infrastructure import save_blocks


def test_save_blocks_main_uses_if_exists_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """The blocks CLI should pass the existence policy to the app registry."""
    calls: list[str] = []

    class FakeBlockRegistry:
        @classmethod
        def save_all(cls, *, if_exists: str) -> None:
            calls.append(if_exists)

    monkeypatch.setattr(save_blocks, "BlockRegistry", FakeBlockRegistry)

    exit_code = save_blocks.main(["--if-exists", "skip"])

    assert exit_code == 0
    assert calls == ["skip"]
