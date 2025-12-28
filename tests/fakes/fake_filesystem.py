"""Fake filesystem for testing."""

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

from jsonlt._exceptions import FileError
from jsonlt._filesystem import FileStats, LockedFile

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path


@dataclass
class FakeFile:
    """In-memory file."""

    content: bytes = b""
    mtime: float = 0.0


class FakeLockedFile:
    """Fake locked file handle."""

    __slots__: ClassVar[tuple[str, ...]] = ("_file", "_position")

    _file: FakeFile
    _position: int

    def __init__(self, fake_file: FakeFile) -> None:
        self._file = fake_file
        self._position = 0

    def read(self) -> bytes:
        data = self._file.content[self._position :]
        self._position = len(self._file.content)
        return data

    def write(self, data: bytes) -> int:
        content = self._file.content
        self._file.content = content[: self._position] + data
        self._position += len(data)
        return len(data)

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:  # SEEK_SET
            self._position = offset
        elif whence == 1:  # SEEK_CUR
            self._position += offset
        elif whence == 2:  # SEEK_END
            self._position = len(self._file.content) + offset
        return self._position

    def sync(self) -> None:
        # Update mtime on sync
        self._file.mtime = time.time()


# Verify FakeLockedFile satisfies LockedFile protocol at module load time
_: type[LockedFile] = FakeLockedFile  # type: ignore[type-abstract]


@dataclass
class FakeFileSystem:
    """In-memory filesystem for testing."""

    files: dict["Path", FakeFile] = field(default_factory=dict)
    fail_stat: set["Path"] = field(default_factory=set)
    fail_open: set["Path"] = field(default_factory=set)
    fail_ensure_parent: set["Path"] = field(default_factory=set)

    def stat(self, path: "Path") -> FileStats:
        if path in self.fail_stat:
            msg = "simulated stat error"
            raise FileError(msg)
        if path not in self.files:
            return FileStats(mtime=0.0, size=0, exists=False)
        f = self.files[path]
        return FileStats(mtime=f.mtime, size=len(f.content), exists=True)

    def read_bytes(self, path: "Path", *, max_size: int | None = None) -> bytes:
        if path not in self.files:
            msg = "file not found"
            raise FileError(msg)
        content = self.files[path].content
        if max_size is not None and len(content) > max_size:
            msg = f"file size {len(content)} exceeds maximum {max_size}"
            raise FileError(msg)
        return content

    def ensure_parent_dir(self, path: "Path") -> None:
        if path in self.fail_ensure_parent:
            msg = "cannot create directory"
            raise FileError(msg)
        # No-op in fake filesystem

    @contextmanager
    def open_locked(
        self,
        path: "Path",
        mode: str,
        timeout: float | None,
    ) -> "Iterator[LockedFile]":
        # timeout is intentionally unused in this fake implementation
        del timeout
        if path in self.fail_open:
            msg = "simulated open error"
            raise FileError(msg)

        if mode == "r+b":
            if path not in self.files:
                raise FileNotFoundError
            fake_file = self.files[path]
        elif mode == "xb":
            if path in self.files:
                raise FileExistsError
            fake_file = FakeFile()
            self.files[path] = fake_file
        else:
            msg = f"unsupported mode: {mode}"
            raise ValueError(msg)

        yield FakeLockedFile(fake_file)

    def atomic_replace(self, path: "Path", lines: "Sequence[str]") -> None:
        content = "".join(line + "\n" for line in lines).encode("utf-8")
        self.files[path] = FakeFile(content=content, mtime=time.time())

    # Test helpers
    def set_content(self, path: "Path", content: bytes) -> None:
        self.files[path] = FakeFile(content=content, mtime=time.time())

    def get_content(self, path: "Path") -> bytes:
        if path not in self.files:
            raise KeyError(path)
        return self.files[path].content
