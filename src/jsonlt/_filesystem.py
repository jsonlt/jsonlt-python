"""Filesystem abstraction for JSONLT Table operations.

This module provides a filesystem protocol and implementation used by the Table
class for file operations, enabling testability through dependency injection.
"""

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Protocol, cast, runtime_checkable

from ._exceptions import FileError, LimitError
from ._lock import exclusive_lock
from ._writer import atomic_replace as _atomic_replace

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from contextlib import AbstractContextManager
    from pathlib import Path
    from typing import BinaryIO


@dataclass(frozen=True, slots=True)
class FileStats:
    """Immutable container for file stat results."""

    mtime: float
    size: int
    exists: bool


@runtime_checkable
class LockedFile(Protocol):
    """Protocol for a file handle with exclusive lock held."""

    def read(self) -> bytes:  # pragma: no cover
        """Read all remaining bytes from the file."""
        ...

    def write(self, data: bytes) -> int:  # pragma: no cover
        """Write bytes to the file."""
        ...

    def seek(self, offset: int, whence: int = 0) -> int:  # pragma: no cover
        """Seek to a position in the file."""
        ...

    def sync(self) -> None:  # pragma: no cover
        """Flush and fsync the file."""
        ...


@runtime_checkable
class FileSystem(Protocol):
    """Protocol for filesystem operations needed by Table."""

    def stat(self, path: "Path") -> FileStats:  # pragma: no cover
        """Get file stats. Returns FileStats with exists=False if not found."""
        ...

    def read_bytes(
        self, path: "Path", *, max_size: int | None = None
    ) -> bytes:  # pragma: no cover
        """Read entire file contents. Raises FileError if not readable."""
        ...

    def ensure_parent_dir(self, path: "Path") -> None:  # pragma: no cover
        """Create parent directories if needed."""
        ...

    def open_locked(  # pragma: no cover
        self,
        path: "Path",
        mode: str,
        timeout: float | None,
    ) -> "AbstractContextManager[LockedFile]":
        """Open file with exclusive lock."""
        ...

    def atomic_replace(
        self, path: "Path", lines: "Sequence[str]"
    ) -> None:  # pragma: no cover
        """Atomically replace file contents with lines."""
        ...


class _LockedFileHandle:
    """Wrapper around file handle satisfying LockedFile protocol."""

    __slots__: ClassVar[tuple[str, ...]] = ("_file",)

    _file: "BinaryIO"

    def __init__(self, file: "BinaryIO") -> None:
        self._file = file

    def read(self) -> bytes:
        """Read all remaining bytes from the file."""
        return self._file.read()

    def write(self, data: bytes) -> int:
        """Write bytes to the file."""
        return self._file.write(data)

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to a position in the file."""
        return self._file.seek(offset, whence)

    def sync(self) -> None:
        """Flush and fsync the file."""
        self._file.flush()
        os.fsync(self._file.fileno())


class RealFileSystem:
    """Real filesystem implementation using standard library."""

    __slots__: ClassVar[tuple[str, ...]] = ()

    def stat(self, path: "Path") -> FileStats:
        """Get file stats. Returns FileStats with exists=False if not found.

        Args:
            path: Path to the file.

        Returns:
            FileStats with file metadata, or exists=False if not found.

        Raises:
            FileError: If stat fails for reasons other than file not found.
        """
        try:
            st = path.stat()
            return FileStats(mtime=st.st_mtime, size=st.st_size, exists=True)
        except FileNotFoundError:
            return FileStats(mtime=0.0, size=0, exists=False)
        except OSError as e:
            msg = f"cannot stat file: {e}"
            raise FileError(msg) from e

    def read_bytes(self, path: "Path", *, max_size: int | None = None) -> bytes:
        """Read entire file contents.

        Args:
            path: Path to the file.
            max_size: Optional maximum file size to allow. If the file exceeds
                this size, LimitError is raised.

        Returns:
            The file contents as bytes.

        Raises:
            FileError: If the file cannot be read.
            LimitError: If the file size exceeds max_size.
        """
        if max_size is not None:
            try:
                st = path.stat()
            except OSError as e:
                msg = f"cannot read file: {e}"
                raise FileError(msg) from e
            if st.st_size > max_size:
                msg = f"file size {st.st_size} exceeds maximum {max_size}"
                raise LimitError(msg)
        try:
            return path.read_bytes()
        except OSError as e:
            msg = f"cannot read file: {e}"
            raise FileError(msg) from e

    def ensure_parent_dir(self, path: "Path") -> None:
        """Create parent directories if needed.

        Args:
            path: Path whose parent directory should exist.

        Raises:
            FileError: If directory creation fails.
        """
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            msg = f"cannot create directory: {e}"
            raise FileError(msg) from e

    @contextmanager
    def open_locked(
        self,
        path: "Path",
        mode: str,
        timeout: float | None,
    ) -> "Iterator[LockedFile]":
        """Open file with exclusive lock.

        Args:
            path: Path to the file.
            mode: File mode ("r+b" or "xb").
            timeout: Lock acquisition timeout in seconds, or None for no timeout.

        Yields:
            A LockedFile handle for reading/writing.

        Raises:
            FileNotFoundError: If mode is "r+b" and file doesn't exist.
            FileExistsError: If mode is "xb" and file already exists.
            LockError: If lock cannot be acquired within timeout.
            FileError: For other OS-level errors.
        """
        try:
            file = path.open(mode)
        except (FileNotFoundError, FileExistsError):
            # Let these propagate for control flow in Table
            raise
        except OSError as e:
            msg = f"cannot open file: {e}"
            raise FileError(msg) from e

        try:
            with exclusive_lock(cast("BinaryIO", file), timeout=timeout):
                yield _LockedFileHandle(cast("BinaryIO", file))
        finally:
            file.close()

    def atomic_replace(self, path: "Path", lines: "Sequence[str]") -> None:
        """Atomically replace file contents with lines.

        Args:
            path: Target file path.
            lines: Lines to write (newlines added automatically).

        Raises:
            FileError: If write, sync, or rename fails.
        """
        _atomic_replace(path, lines)
