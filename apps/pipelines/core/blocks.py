"""Generic infrastructure for typed Prefect block handles.

This module is app-agnostic.  It provides ``BlockEntry``, ``define_block``,
and ``ExistsMode`` — the building blocks used by ``config/blocks.py`` to
define the project's concrete block registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from prefect.blocks.core import Block

type ExistsMode = Literal["skip", "throw", "overwrite"]
"""
Controls behaviour when a block with the same name already exists in the registry.

- ``"skip"``       leave the existing block untouched and return without error.
- ``"throw"``      raise a ``ValueError`` so the caller is aware of the conflict.
- ``"overwrite"``  replace the existing block with the new value.
"""


@dataclass
class BlockEntry[T: Block]:
    """A typed handle for a named Prefect block.

    Pairs a registry name with a concrete block instance so that the block can
    be saved, loaded, and existence-checked without losing static type
    information about the underlying ``Block`` subclass.

    Type parameter ``T`` is inferred from the ``block`` argument, giving callers
    full IDE completion on the returned value of ``load`` / ``load_async``.
    """

    name: str
    """Registry name used to identify the block inside Prefect."""

    block: T
    """The concrete block instance (e.g. ``Secret``, ``AwsCredentials``)."""

    def __repr__(self) -> str:
        """Return the registry name (same as :meth:`__str__`)."""
        return self.name

    def __str__(self) -> str:
        """Return the registry name for display and string coercion."""
        return self.name

    def __hash__(self) -> int:
        """Return a hash based on the registry name."""
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        """Return ``True`` if *other* is a ``BlockEntry`` with the same name."""
        if not isinstance(other, BlockEntry):
            return False
        return self.name == other.name

    # ------------------------------------------------------------------
    # Existence checks
    # ------------------------------------------------------------------

    def exists(self) -> bool:
        """Return ``True`` if a block named ``self.name`` exists in the registry."""
        try:
            type(self.block).load(name=self.name)
            return True
        except Exception:
            return False

    async def exists_async(self) -> bool:
        """Async variant of :meth:`exists`."""
        try:
            await self.block.aload(name=self.name)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> T:
        """Load and return the block from the Prefect registry (sync)."""
        return cast(T, type(self.block).load(name=self.name))

    async def load_async(self) -> T:
        """Load and return the block from the Prefect registry (async)."""
        return await self.block.aload(name=self.name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _handle_exists(self, if_exists: ExistsMode) -> bool:
        """Evaluate the ``if_exists`` policy synchronously.

        Returns ``True`` when the caller should abort the save (block already
        exists and mode is ``"skip"``).  Raises ``ValueError`` for
        ``"throw"``.  Returns ``False`` when the save should proceed.
        """
        if if_exists == "overwrite" or not self.exists():
            return False
        if if_exists == "throw":
            raise ValueError(f"Block '{self.name}' already exists")
        print(f"Block '{self.name}' already exists, skipping...")
        return True

    async def _handle_exists_async(self, if_exists: ExistsMode) -> bool:
        """Async variant of :meth:`_handle_exists`."""
        if if_exists == "overwrite" or not await self.exists_async():
            return False
        if if_exists == "throw":
            raise ValueError(f"Block '{self.name}' already exists")
        print(f"Block '{self.name}' already exists, skipping...")
        return True

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def save(self, if_exists: ExistsMode = "skip") -> None:
        """Persist the block to the Prefect registry (sync).

        Args:
            if_exists: How to handle a name collision. Defaults to ``"skip"``.
        """
        if self._handle_exists(if_exists):
            return
        self.block.save(name=self.name, overwrite=(if_exists == "overwrite"))
        print(f"Block '{self.name}' of type '{type(self.block).__name__}' saved successfully")

    async def save_async(self, if_exists: ExistsMode = "skip") -> None:
        """Persist the block to the Prefect registry (async).

        Uses :meth:`exists_async` to probe for an existing block, then falls
        back to the synchronous ``Block.save`` call (Prefect does not expose an
        async save).

        Args:
            if_exists: How to handle a name collision. Defaults to ``"skip"``.
        """
        if await self._handle_exists_async(if_exists):
            return
        self.block.save(name=self.name, overwrite=(if_exists == "overwrite"))
        print(f"Block '{self.name}' of type '{type(self.block).__name__}' saved successfully")


def define_block[T: Block](name: str, block: T) -> BlockEntry[T]:
    """Create a typed ``BlockEntry`` without saving to the Prefect registry.

    Call ``BlockRegistry.save_all()`` explicitly (or run ``config/blocks.py``
    directly) to persist blocks to the Prefect server.
    """
    return BlockEntry(name=name, block=block)


class BlockRegistryBase:
    """Base class for Prefect block registries.

    Subclass this in ``config/blocks.py`` and declare block entries as class
    attributes using :func:`define_block`.  Call :meth:`save_all` once during
    environment setup to persist all entries to the Prefect server.
    """

    @classmethod
    def _entries(cls) -> list[BlockEntry]:
        """Return a list of all block entries in the registry."""
        return [v for v in vars(cls).values() if isinstance(v, BlockEntry)]

    @classmethod
    def save_all(cls, if_exists: ExistsMode = "skip") -> None:
        """Persist all block entries to the Prefect server (sync)."""
        for entry in cls._entries():
            entry.save(if_exists=if_exists)

    @classmethod
    async def save_all_async(cls, if_exists: ExistsMode = "skip") -> None:
        """Persist all block entries to the Prefect server (async)."""
        for entry in cls._entries():
            await entry.save_async(if_exists=if_exists)
