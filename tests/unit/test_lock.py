import threading
import time
from typing import TYPE_CHECKING

import pytest

from jsonlt._exceptions import LockError
from jsonlt._lock import exclusive_lock

if TYPE_CHECKING:
    from pathlib import Path


class TestExclusiveLock:
    def test_lock_acquires_and_releases(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        with path.open("r+b") as f, exclusive_lock(f):
            # Lock is held - we got here without error
            pass
        # Lock released after context exit

    def test_lock_timeout_zero_succeeds_when_available(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        with path.open("r+b") as f, exclusive_lock(f, timeout=0):
            pass  # Should succeed when not locked

    def test_lock_allows_operations_inside_context(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("initial")

        with path.open("r+b") as f, exclusive_lock(f):
            # Can read while holding lock
            content = f.read()
            assert content == b"initial"

    def test_lock_released_on_exception(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        def raise_inside_lock() -> None:
            msg = "test error"
            with path.open("r+b") as f, exclusive_lock(f):
                raise ValueError(msg)

        with pytest.raises(ValueError, match="test error"):
            raise_inside_lock()

        # Should be able to acquire lock again after exception
        with path.open("r+b") as f, exclusive_lock(f):
            pass

    def test_lock_can_be_reacquired_after_release(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        # First acquisition
        with path.open("r+b") as f, exclusive_lock(f):
            pass

        # Second acquisition
        with path.open("r+b") as f, exclusive_lock(f):
            pass

    def test_lock_with_explicit_timeout(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        with path.open("r+b") as f, exclusive_lock(f, timeout=1.0):
            pass

    def test_lock_timeout_with_contention(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        # Track thread execution
        lock_acquired = threading.Event()
        can_release = threading.Event()
        timeout_occurred = threading.Event()
        error_message: list[str] = []

        def holder_thread() -> None:
            with path.open("r+b") as f, exclusive_lock(f):
                _ = lock_acquired.set()
                _ = can_release.wait(timeout=5.0)

        def waiter_thread() -> None:
            _ = lock_acquired.wait(timeout=5.0)
            try:
                with path.open("r+b") as f, exclusive_lock(f, timeout=0.05):
                    pass
            except LockError as e:
                _ = timeout_occurred.set()
                error_message.append(str(e))

        holder = threading.Thread(target=holder_thread)
        waiter = threading.Thread(target=waiter_thread)

        holder.start()
        waiter.start()

        # Wait for waiter to timeout
        waiter.join(timeout=2.0)

        # Clean up holder thread
        _ = can_release.set()
        holder.join(timeout=2.0)

        assert timeout_occurred.is_set()
        assert len(error_message) == 1
        assert "could not acquire file lock" in error_message[0]

    def test_lock_blocks_until_available(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        # Track thread execution
        lock_acquired = threading.Event()
        waiter_succeeded = threading.Event()
        hold_duration = 0.05  # 50ms

        def holder_thread() -> None:
            with path.open("r+b") as f, exclusive_lock(f):
                _ = lock_acquired.set()
                time.sleep(hold_duration)

        def waiter_thread() -> None:
            _ = lock_acquired.wait(timeout=5.0)
            with path.open("r+b") as f, exclusive_lock(f, timeout=1.0):
                _ = waiter_succeeded.set()

        holder = threading.Thread(target=holder_thread)
        waiter = threading.Thread(target=waiter_thread)

        holder.start()
        waiter.start()

        holder.join(timeout=2.0)
        waiter.join(timeout=2.0)

        assert waiter_succeeded.is_set()

    def test_lock_with_none_timeout_waits_indefinitely(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        lock_acquired = threading.Event()
        waiter_succeeded = threading.Event()

        def holder_thread() -> None:
            with path.open("r+b") as f, exclusive_lock(f):
                _ = lock_acquired.set()
                time.sleep(0.05)

        def waiter_thread() -> None:
            _ = lock_acquired.wait(timeout=5.0)
            with path.open("r+b") as f, exclusive_lock(f, timeout=None):
                _ = waiter_succeeded.set()

        holder = threading.Thread(target=holder_thread)
        waiter = threading.Thread(target=waiter_thread)

        holder.start()
        waiter.start()

        holder.join(timeout=2.0)
        waiter.join(timeout=2.0)

        assert waiter_succeeded.is_set()
