"""File locking for JSONLT write operations.

This module provides cross-platform exclusive file locking with timeout support.
Uses fcntl on Unix systems and LockFileEx via ctypes on Windows.
"""

import contextlib
import ctypes
import importlib
import sys
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Final, Protocol

from ._exceptions import LockError

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType
    from typing import BinaryIO

# Lock polling constants
_LOCK_POLL_INTERVAL: Final[float] = 0.01  # 10ms initial
_LOCK_MAX_POLL_INTERVAL: Final[float] = 0.1  # 100ms max with backoff


class _LockModule(Protocol):
    """Protocol for platform-specific lock module."""

    def acquire(self, fd: int) -> bool:  # pragma: no cover
        """Try to acquire exclusive lock."""
        ...

    def release(self, fd: int) -> None:  # pragma: no cover
        """Release exclusive lock."""
        ...


class _UnixLock:
    """Unix file locking using fcntl."""

    _fcntl: "ModuleType"
    _lock_ex_nb: int
    _lock_un: int

    def __init__(self) -> None:
        self._fcntl = importlib.import_module("fcntl")
        self._lock_ex_nb = self._fcntl.LOCK_EX | self._fcntl.LOCK_NB  # pyright: ignore[reportAny]
        self._lock_un = self._fcntl.LOCK_UN  # pyright: ignore[reportAny]

    def acquire(self, fd: int) -> bool:
        """Try to acquire exclusive lock on Unix."""
        try:
            self._fcntl.flock(fd, self._lock_ex_nb)  # pyright: ignore[reportAny]
        except OSError:
            return False
        else:
            return True

    def release(self, fd: int) -> None:
        """Release exclusive lock on Unix."""
        self._fcntl.flock(fd, self._lock_un)  # pyright: ignore[reportAny]


class _WindowsLock:
    """Windows file locking using LockFileEx.

    Uses the Windows LockFileEx API via ctypes for proper file locking.
    This provides semantics similar to Unix flock:
    - Locks the entire file (not just a byte range)
    - Works correctly on empty files
    - Interoperates with other Windows applications using LockFileEx
    """

    # Windows API constants
    _LOCKFILE_EXCLUSIVE_LOCK: Final[int] = 0x0002
    _LOCKFILE_FAIL_IMMEDIATELY: Final[int] = 0x0001

    _msvcrt: "ModuleType"
    _kernel32: object  # WinDLL from ctypes
    _overlapped_class: type[ctypes.Structure]

    def __init__(self) -> None:
        # Get handle to kernel32.dll
        self._kernel32 = ctypes.windll.kernel32

        # Define OVERLAPPED structure for LockFileEx
        # (ctypes _fields_ must be plain list, not ClassVar - standard ctypes pattern)
        class _Overlapped(ctypes.Structure):
            _fields_ = [  # noqa: RUF012  # pyright: ignore[reportUnannotatedClassAttribute]
                ("Internal", ctypes.c_void_p),
                ("InternalHigh", ctypes.c_void_p),
                ("Offset", ctypes.c_ulong),
                ("OffsetHigh", ctypes.c_ulong),
                ("hEvent", ctypes.c_void_p),
            ]

        self._overlapped_class = _Overlapped

        # Get msvcrt for get_osfhandle
        self._msvcrt = importlib.import_module("msvcrt")

    def _get_handle(self, fd: int) -> int:
        """Convert file descriptor to Windows HANDLE."""
        handle: int = self._msvcrt.get_osfhandle(fd)  # pyright: ignore[reportAny]
        return handle

    def acquire(self, fd: int) -> bool:
        """Try to acquire exclusive lock on Windows using LockFileEx."""
        try:
            handle = self._get_handle(fd)
            overlapped = self._overlapped_class()

            # Lock entire file using maximum range (0xFFFFFFFF for both low/high)
            # LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
            flags = self._LOCKFILE_EXCLUSIVE_LOCK | self._LOCKFILE_FAIL_IMMEDIATELY
            # ctypes Windows API call - suppress type warnings for dynamic ctypes access
            result = self._kernel32.LockFileEx(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportAttributeAccessIssue]
                handle,
                flags,
                0,  # dwReserved
                0xFFFFFFFF,  # nNumberOfBytesToLockLow
                0xFFFFFFFF,  # nNumberOfBytesToLockHigh
                ctypes.byref(overlapped),
            )
            return bool(result)  # pyright: ignore[reportUnknownArgumentType]
        except OSError:
            return False

    def release(self, fd: int) -> None:
        """Release exclusive lock on Windows using UnlockFileEx."""
        with contextlib.suppress(OSError):
            handle = self._get_handle(fd)
            overlapped = self._overlapped_class()

            # Unlock the same range we locked
            self._kernel32.UnlockFileEx(  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
                handle,
                0,  # dwReserved
                0xFFFFFFFF,  # nNumberOfBytesToUnlockLow
                0xFFFFFFFF,  # nNumberOfBytesToUnlockHigh
                ctypes.byref(overlapped),
            )


# Initialize platform-specific lock implementation
_lock_impl: _LockModule = _WindowsLock() if sys.platform == "win32" else _UnixLock()


@contextmanager
def exclusive_lock(
    file: "BinaryIO",
    timeout: float | None = None,
) -> "Iterator[None]":
    """Acquire exclusive lock on file, yield, then release.

    Uses platform-specific locking (fcntl on Unix, msvcrt on Windows).
    Implements polling with exponential backoff up to 100ms.

    Args:
        file: Open file object to lock.
        timeout: Maximum seconds to wait. None means wait indefinitely.

    Yields:
        None when lock is acquired.

    Raises:
        LockError: If lock cannot be acquired within timeout.
    """
    fd = file.fileno()
    start_time = time.monotonic()
    poll_interval = _LOCK_POLL_INTERVAL

    while True:
        if _lock_impl.acquire(fd):
            try:
                yield
            finally:
                _lock_impl.release(fd)
            return

        # Check timeout
        if timeout is not None:
            elapsed = time.monotonic() - start_time
            if elapsed >= timeout:
                msg = f"could not acquire file lock within {timeout}s"
                raise LockError(msg)

        # Sleep with exponential backoff
        time.sleep(poll_interval)
        poll_interval = min(poll_interval * 2, _LOCK_MAX_POLL_INTERVAL)
