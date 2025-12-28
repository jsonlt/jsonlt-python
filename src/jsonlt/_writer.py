"""File writing operations for JSONLT.

This module provides low-level file writing with durability guarantees.
"""

import contextlib
import os
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from ._exceptions import FileError

if TYPE_CHECKING:
    from collections.abc import Sequence


def append_line(path: Path, line: str) -> None:
    """Append a single line to file with fsync.

    Opens file in append mode, writes line + newline, fsyncs.
    Caller must hold exclusive lock.

    Args:
        path: Path to the file.
        line: JSON line to append (without trailing newline).

    Raises:
        FileError: If append or sync fails.
    """
    try:
        with path.open("a", encoding="utf-8") as f:
            _ = f.write(line)
            _ = f.write("\n")
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        msg = f"cannot append to file: {e}"
        raise FileError(msg) from e


def append_lines(path: Path, lines: "Sequence[str]") -> None:
    """Append multiple lines to file with single fsync.

    Opens file in append mode, writes all lines, single flush + fsync.
    Caller must hold exclusive lock.

    Args:
        path: Path to the file.
        lines: JSON lines to append (without trailing newlines).

    Raises:
        FileError: If append or sync fails.
    """
    if not lines:
        return
    try:
        with path.open("a", encoding="utf-8") as f:
            for line in lines:
                _ = f.write(line)
                _ = f.write("\n")
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        msg = f"cannot append to file: {e}"
        raise FileError(msg) from e


def atomic_replace(path: Path, lines: "Sequence[str]") -> None:
    """Atomically replace file contents with lines.

    Writes to temp file in same directory, fsyncs, renames.
    Used by clear() and future compact().

    Args:
        path: Target file path.
        lines: Lines to write (newlines added automatically).

    Raises:
        FileError: If write, sync, or rename fails.
    """
    # Create temp file in same directory to ensure atomic rename works
    parent_dir = path.parent
    temp_fd = -1
    temp_path: Path | None = None

    try:
        # Create temp file
        temp_fd, temp_path_str = tempfile.mkstemp(
            suffix=".tmp",
            prefix=".jsonlt_",
            dir=parent_dir,
        )
        temp_path = Path(temp_path_str)

        # Write content
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            temp_fd = -1  # Ownership transferred to fdopen
            for line in lines:
                _ = f.write(line)
                _ = f.write("\n")
            f.flush()
            os.fsync(f.fileno())

        # Atomic rename
        _ = temp_path.replace(path)
        temp_path = None  # Successfully moved, don't delete

        # fsync the directory to ensure the rename is durable
        # This is a POSIX-specific operation - Windows doesn't support
        # opening directories and NTFS handles atomic renames differently
        if sys.platform != "win32":
            dir_fd = os.open(str(parent_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

    except OSError as e:
        msg = f"cannot write file atomically: {e}"
        raise FileError(msg) from e
    finally:
        # Clean up temp file if it still exists (defensive cleanup)
        if temp_fd != -1:  # pragma: no cover
            with contextlib.suppress(OSError):
                os.close(temp_fd)
        if temp_path is not None:  # pragma: no cover
            with contextlib.suppress(OSError):
                temp_path.unlink()
