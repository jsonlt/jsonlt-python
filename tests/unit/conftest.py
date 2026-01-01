"""Shared fixtures for unit tests."""

from typing import TYPE_CHECKING

import pytest

from jsonlt import Table

from tests.fakes.fake_filesystem import FakeFileSystem

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from jsonlt._json import JSONObject


@pytest.fixture
def fake_fs() -> FakeFileSystem:
    """Provide a fresh FakeFileSystem instance for each test.

    The FakeFileSystem provides an in-memory filesystem implementation
    that can be injected into Table for isolated testing without disk I/O.

    Returns:
        A new FakeFileSystem instance with no files and no failure modes.

    Example:
        def test_table_with_fake_fs(fake_fs: FakeFileSystem, tmp_path: Path) -> None:
            table = Table(tmp_path / "test.jsonlt", key="id", _fs=fake_fs)
            fake_fs.set_content(tmp_path / "test.jsonlt", b'{"id":"alice"}\\n')
            assert table.count() == 1
    """
    return FakeFileSystem()


@pytest.fixture
def fake_fs_with_file(
    fake_fs: FakeFileSystem,
    tmp_path: "Path",
) -> "Callable[[bytes], Path]":
    """Factory fixture to create a fake file with content.

    Returns a callable that creates a file in the fake filesystem
    and returns its path.

    Args:
        fake_fs: The FakeFileSystem fixture.
        tmp_path: Pytest's temporary directory fixture.

    Returns:
        A callable that takes bytes content and returns the file path.

    Example:
        def test_with_content(fake_fs_with_file, fake_fs) -> None:
            path = fake_fs_with_file(b'{"id":"alice"}\\n')
            content = fake_fs.get_content(path)
            assert b"alice" in content
    """
    counter = 0

    def create_file(content: bytes) -> "Path":
        nonlocal counter
        counter += 1
        path = tmp_path / f"test_{counter}.jsonlt"
        fake_fs.set_content(path, content)
        return path

    return create_file


@pytest.fixture
def make_table(
    tmp_path: "Path",
) -> "Callable[..., Table]":
    """Factory fixture for creating Table instances.

    Provides a convenient way to create tables with sensible defaults
    while allowing full customization through keyword arguments.

    Returns:
        A callable that creates Table instances.

    Example:
        def test_table_operations(make_table) -> None:
            table = make_table()  # Creates table with key="id"
            table.put({"id": "alice", "role": "admin"})
            assert table.get("alice") is not None

        def test_with_custom_key(make_table) -> None:
            table = make_table(key=("org", "id"))
            table.put({"org": "acme", "id": 1, "name": "alice"})
    """
    counter = 0

    def create_table(
        *,
        key: str | tuple[str, ...] = "id",
        content: str | None = None,
        auto_reload: bool = True,
        max_file_size: int | None = None,
        lock_timeout: float | None = None,
        _fs: FakeFileSystem | None = None,
    ) -> Table:
        nonlocal counter
        counter += 1
        path = tmp_path / f"table_{counter}.jsonlt"

        if content is not None:
            _ = path.write_text(content)

        return Table(
            path,
            key=key,
            auto_reload=auto_reload,
            max_file_size=max_file_size,
            lock_timeout=lock_timeout,
            _fs=_fs,
        )

    return create_table


@pytest.fixture
def make_table_with_records(
    make_table: "Callable[..., Table]",
) -> "Callable[..., Table]":
    """Factory fixture for creating pre-populated Table instances.

    Builds on make_table to provide convenient record seeding.

    Returns:
        A callable that creates Table instances with initial records.

    Example:
        def test_populated_table(make_table_with_records) -> None:
            table = make_table_with_records([
                {"id": "alice", "role": "admin"},
                {"id": "bob", "role": "user"},
            ])
            assert table.count() == 2
    """

    def create_table_with_records(
        records: "list[JSONObject]",
        *,
        key: str | tuple[str, ...] = "id",
        auto_reload: bool = True,
        max_file_size: int | None = None,
        lock_timeout: float | None = None,
        _fs: FakeFileSystem | None = None,
    ) -> Table:
        table = make_table(
            key=key,
            auto_reload=auto_reload,
            max_file_size=max_file_size,
            lock_timeout=lock_timeout,
            _fs=_fs,
        )
        for record in records:
            table.put(record)
        return table

    return create_table_with_records
