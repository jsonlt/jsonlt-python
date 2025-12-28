"""Table class for JSONLT file operations.

This module provides the Table class, the primary interface for working
with JSONLT files. It handles file loading, auto-reload, and read/write operations.
"""

# pyright: reportImportCycles=false

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar
from typing_extensions import override

from ._constants import MAX_KEY_LENGTH, MAX_RECORD_SIZE
from ._encoding import validate_no_surrogates
from ._exceptions import (
    ConflictError,
    FileError,
    InvalidKeyError,
    LimitError,
    TransactionError,
)
from ._filesystem import FileSystem, RealFileSystem
from ._header import serialize_header
from ._json import serialize_json, utf8_byte_length
from ._keys import (
    Key,
    KeySpecifier,
    key_length,
    key_specifiers_match,
    normalize_key_specifier,
    validate_key_arity,
)
from ._readable import ReadableMixin
from ._reader import parse_table_content, read_table_file
from ._records import build_tombstone, extract_key, validate_record
from ._state import compute_logical_state

if TYPE_CHECKING:
    from ._header import Header
    from ._json import JSONObject
    from ._transaction import Transaction

__all__ = ["Table"]


class Table(ReadableMixin):
    """A JSONLT table backed by a file.

    The Table class provides the primary interface for working with JSONLT
    files. It loads the file, computes the logical state, and provides
    methods for reading records.

    The table supports auto-reload: before each read operation, it checks
    if the underlying file has changed (by mtime and size) and reloads
    the state if necessary. This can be disabled via `auto_reload=False`.

    Example:
        >>> table = Table("users.jsonlt", key="id")
        >>> table.get("alice")
        {'id': 'alice', 'role': 'admin'}
        >>> table.has("bob")
        False
        >>> table.count()
        1
    """

    __slots__: ClassVar[tuple[str, ...]] = (
        "_active_transaction",
        "_auto_reload",
        "_cached_sorted_keys",
        "_file_mtime",
        "_file_size",
        "_fs",
        "_header",
        "_key_specifier",
        "_lock_timeout",
        "_max_file_size",
        "_path",
        "_state",
    )

    _path: Path
    _key_specifier: "KeySpecifier | None"
    _auto_reload: bool
    _lock_timeout: float | None
    _max_file_size: int | None
    _fs: FileSystem
    _header: "Header | None"
    _state: "dict[Key, JSONObject]"
    _file_mtime: float
    _file_size: int
    _active_transaction: "Transaction | None"
    _cached_sorted_keys: list[Key] | None

    def __init__(
        self,
        path: "Path | str",
        key: "KeySpecifier | None" = None,
        *,
        auto_reload: bool = True,
        lock_timeout: float | None = None,
        max_file_size: int | None = None,
        _fs: "FileSystem | None" = None,
    ) -> None:
        """Open or create a table at the given path.

        Args:
            path: Path to the JSONLT file.
            key: Key specifier for the table. If the file has a header with
                a key specifier, this must match or be omitted. If the file
                has operations but no header, this is required.
            auto_reload: If True (default), check for file changes before
                each read operation and reload if necessary.
            lock_timeout: Maximum seconds to wait for file lock on write
                operations. None means wait indefinitely.
            max_file_size: Maximum allowed file size in bytes when loading
                the file. If the file exceeds this limit, LimitError is raised.
                If None (default), no limit is enforced.
            _fs: Internal filesystem abstraction for testing. Do not use.

        Raises:
            FileError: If the file cannot be read.
            LimitError: If the file size exceeds max_file_size.
            ParseError: If the file contains invalid content.
            InvalidKeyError: If the key specifier is invalid or mismatches
                the header, or if the file has operations but no key specifier
                can be determined.
        """
        self._path = Path(path) if isinstance(path, str) else path
        self._auto_reload = auto_reload
        self._lock_timeout = lock_timeout
        self._max_file_size = max_file_size
        self._fs = RealFileSystem() if _fs is None else _fs

        # These will be set by _load()
        self._header = None
        self._state = {}
        self._file_mtime = 0.0
        self._file_size = 0
        self._key_specifier = None
        self._active_transaction = None
        self._cached_sorted_keys = None

        # Initial load
        self._load(key)

    def _load(self, caller_key: "KeySpecifier | None" = None) -> None:
        """Load or reload the table from disk.

        This method reads the file, parses it, validates the key specifier,
        and computes the logical state.

        Args:
            caller_key: Key specifier provided by caller (only used on
                initial load, not on auto-reload).

        Raises:
            FileError: If the file cannot be read.
            ParseError: If the file contains invalid content.
            InvalidKeyError: If the key specifier is invalid or mismatches
                the header, or if the file has operations but no key specifier.
        """
        # Check if file exists - if not, treat as empty table
        if not self._path.exists():
            self._load_empty_table(caller_key)
            return

        # Read and parse the file
        header, operations = read_table_file(
            self._path, max_file_size=self._max_file_size
        )
        self._header = header

        # Track file stats for auto-reload
        self._update_file_stats()

        # Resolve which key specifier to use
        resolved_key = self._resolve_key_specifier(caller_key, header, operations)
        if resolved_key is None:
            # Empty file with no key specifier - OK for now
            self._key_specifier = None
            self._state = {}
            self._cached_sorted_keys = None
            return

        self._key_specifier = resolved_key

        # Compute logical state if we have operations
        if operations:
            self._state = compute_logical_state(operations, self._key_specifier)
        else:
            self._state = {}
        self._cached_sorted_keys = None

    def _load_from_content(self, content: bytes) -> None:
        """Load table state from bytes content.

        This is used when we already have the file content in memory
        (e.g., read via a locked file handle on Windows).

        Args:
            content: Raw bytes of the file content.

        Raises:
            ParseError: If the content contains invalid data.
        """
        if not content:
            # Empty content - nothing to load
            self._state = {}
            self._cached_sorted_keys = None
            return

        # Parse the content
        header, operations = parse_table_content(content)
        self._header = header

        # Resolve key specifier (use existing since this is a reload)
        resolved_key = self._resolve_key_specifier(None, header, operations)
        if resolved_key is None:
            self._state = {}
            self._cached_sorted_keys = None
            return

        self._key_specifier = resolved_key

        # Compute logical state if we have operations
        if operations:
            self._state = compute_logical_state(operations, self._key_specifier)
        else:
            self._state = {}
        self._cached_sorted_keys = None

    def _load_empty_table(self, caller_key: "KeySpecifier | None") -> None:
        """Initialize state for a non-existent file."""
        self._header = None
        self._state = {}
        self._cached_sorted_keys = None
        self._file_mtime = 0.0
        self._file_size = 0

        # For new files, use caller's key specifier if provided
        if caller_key is not None:
            self._key_specifier = normalize_key_specifier(caller_key)
        # Otherwise keep existing key specifier (may be None)

    def _update_file_stats(self) -> None:
        """Update cached file mtime and size for auto-reload detection."""
        stats = self._fs.stat(self._path)
        if not stats.exists:
            msg = "cannot stat file: file does not exist"
            raise FileError(msg)
        self._file_mtime = stats.mtime
        self._file_size = stats.size

    def _try_update_stats(self) -> None:
        """Update cached file stats, ignoring errors."""
        try:
            stats = self._fs.stat(self._path)
            if stats.exists:
                self._file_mtime = stats.mtime
                self._file_size = stats.size
        except FileError:
            # Ignore stat failures - stats will refresh on next read
            pass

    def _reload_if_changed(self, cached_mtime: float, cached_size: int) -> None:
        """Reload file if stats differ from cached values.

        Called inside lock during transaction commit. If the file's mtime
        and size match the cached values, the file is unchanged and we
        skip the expensive reload.

        Args:
            cached_mtime: File mtime when transaction started.
            cached_size: File size when transaction started.
        """
        stats = self._fs.stat(self._path)
        if not stats.exists:
            self._load()
            return

        if stats.mtime != cached_mtime or stats.size != cached_size:
            # File changed - full reload required
            self._load()
        # else: file unchanged, state is already current

    def _resolve_key_specifier(
        self,
        caller_key: "KeySpecifier | None",
        header: "Header | None",
        operations: "list[JSONObject]",
    ) -> "KeySpecifier | None":
        """Resolve which key specifier to use.

        Returns the resolved key specifier, or None if the file is empty
        and no key specifier is available.

        Raises:
            InvalidKeyError: If caller and header key specifiers conflict,
                or if operations exist but no key specifier is available.
        """
        header_key = header.key if header is not None else None

        if caller_key is not None and header_key is not None:
            # Both provided - must match
            caller_normalized = normalize_key_specifier(caller_key)
            header_normalized = normalize_key_specifier(header_key)
            if not key_specifiers_match(caller_normalized, header_normalized):
                msg = (
                    f"key specifier mismatch: caller provided {caller_key!r} "
                    f"but header specifies {header_key!r}"
                )
                raise InvalidKeyError(msg)
            return header_normalized

        if header_key is not None:
            return normalize_key_specifier(header_key)

        if caller_key is not None:
            return normalize_key_specifier(caller_key)

        if self._key_specifier is not None:
            # Keep existing key specifier (from initial load)
            return self._key_specifier

        if operations:
            # File has operations but no key specifier - error
            msg = (
                "file has operations but no key specifier: "
                "provide key parameter or add header with key field"
            )
            raise InvalidKeyError(msg)

        # Empty file with no key specifier
        return None

    def _maybe_reload(self) -> None:
        """Check if file changed and reload if necessary.

        This is called before read operations when auto_reload is enabled.
        """
        if not self._auto_reload:
            return

        stats = self._fs.stat(self._path)
        if not stats.exists:
            # File was deleted - clear state
            if self._file_size != 0 or self._file_mtime != 0.0:
                self._header = None
                self._state = {}
                self._cached_sorted_keys = None
                self._file_mtime = 0.0
                self._file_size = 0
            return

        if stats.mtime != self._file_mtime or stats.size != self._file_size:
            self._load()

    @override
    def _get_state(self) -> "dict[Key, JSONObject]":
        """Return the table state dictionary."""
        return self._state

    @override
    def _prepare_read(self) -> None:
        """Check for file changes and reload if necessary."""
        self._maybe_reload()

    @property
    def path(self) -> Path:
        """The path to the table file."""
        return self._path

    @property
    def key_specifier(self) -> "KeySpecifier | None":
        """The key specifier for this table."""
        return self._key_specifier

    @property
    def header(self) -> "Header | None":
        """The header of the table file, if present."""
        self._maybe_reload()
        return self._header

    def reload(self) -> None:
        """Force a reload of the table from disk.

        This method is useful when `auto_reload=False` and you want to
        manually refresh the table state after external changes.

        Raises:
            FileError: If the file cannot be read.
            ParseError: If the file contains invalid content.
        """
        self._load()
        self._cached_sorted_keys = None

    # --- Write Operations ---

    def _require_key_specifier(self) -> KeySpecifier:
        """Return key specifier or raise InvalidKeyError if not set."""
        if self._key_specifier is None:
            msg = "key specifier is required for write operations"
            raise InvalidKeyError(msg)
        return self._key_specifier

    def put(self, record: "JSONObject") -> None:
        """Insert or update a record.

        Validates the record, serializes it deterministically, and appends
        to the file under exclusive lock.

        Args:
            record: The record to insert/update. Must contain key fields.

        Raises:
            InvalidKeyError: If key specifier not set, record missing key fields,
                has invalid key values, contains $-prefixed fields, or contains
                unpaired surrogates.
            LimitError: If key length > 1024 bytes or record size > 1 MiB.
            LockError: If file lock cannot be acquired within timeout.
            FileError: If file write fails.
        """
        key_specifier = self._require_key_specifier()

        # Check for unpaired surrogates in all strings
        validate_no_surrogates(record)

        # Validate record structure (missing fields, invalid key types, $ fields)
        validate_record(record, key_specifier)

        # Extract and validate key
        key = extract_key(record, key_specifier)
        key_len = key_length(key)
        if key_len > MAX_KEY_LENGTH:
            msg = f"key length {key_len} bytes exceeds maximum {MAX_KEY_LENGTH}"
            raise LimitError(msg)

        # Serialize record
        serialized = serialize_json(record)
        record_bytes = utf8_byte_length(serialized)
        if record_bytes > MAX_RECORD_SIZE:
            msg = f"record size {record_bytes} bytes exceeds maximum {MAX_RECORD_SIZE}"
            raise LimitError(msg)

        # Write under lock (put doesn't return whether key existed)
        _ = self._write_with_lock(serialized, key, record)

    def _finalize_write(self, key: Key, record: "JSONObject | None") -> bool:
        """Update state after successful write. Returns whether key existed."""
        existed = key in self._state
        if record is not None:
            self._state[key] = record
        else:
            _ = self._state.pop(key, None)
        self._cached_sorted_keys = None
        self._update_file_stats()
        return existed

    _MAX_WRITE_RETRIES: ClassVar[int] = 3

    def _write_with_lock(
        self,
        line: str,
        key: Key,
        record: "JSONObject | None",
        *,
        _retries: int = 0,
    ) -> bool:
        """Write a line to the file under exclusive lock.

        On Windows, LockFileEx prevents other handles (even from the same process)
        from accessing the locked file. So we must read and write using the same
        file handle that holds the lock.

        Args:
            line: The JSON line to write.
            key: The key for state update.
            record: The record to add to state, or None for delete.
            _retries: Internal retry counter (do not pass externally).

        Returns:
            True if the key existed in the table (after reload), False otherwise.
        """
        self._fs.ensure_parent_dir(self._path)

        try:
            with self._fs.open_locked(self._path, "r+b", self._lock_timeout) as f:
                content = f.read()
                self._load_from_content(content)
                _ = f.seek(0, 2)
                encoded = (line + "\n").encode("utf-8")
                _ = f.write(encoded)
                f.sync()
                return self._finalize_write(key, record)
        except FileNotFoundError:
            try:
                with self._fs.open_locked(self._path, "xb", self._lock_timeout) as f:
                    encoded = (line + "\n").encode("utf-8")
                    _ = f.write(encoded)
                    f.sync()
                    return self._finalize_write(key, record)
            except FileExistsError:
                if _retries >= self._MAX_WRITE_RETRIES:
                    msg = "cannot acquire stable file handle after multiple retries"
                    raise FileError(msg) from None
                return self._write_with_lock(line, key, record, _retries=_retries + 1)

    def delete(self, key: Key) -> bool:
        """Delete a record by key.

        Writes a tombstone to the file. Returns whether the record existed.

        Args:
            key: The key to delete. Must match key specifier arity.

        Returns:
            True if record existed, False otherwise.

        Raises:
            InvalidKeyError: If key specifier not set, key is invalid,
                or key arity doesn't match specifier.
            LockError: If file lock cannot be acquired within timeout.
            FileError: If file write fails.
        """
        key_specifier = self._require_key_specifier()

        # Validate key arity matches specifier
        validate_key_arity(key, key_specifier)

        # Build tombstone
        tombstone = build_tombstone(key, key_specifier)
        serialized = serialize_json(tombstone)

        # Write under lock - returns whether key existed (checked after reload)
        return self._write_with_lock(serialized, key, None)

    def clear(self) -> None:
        """Remove all records from the table.

        Atomically replaces file with header only (if present).

        Raises:
            LockError: If file lock cannot be acquired within timeout.
            FileError: If file operations fail.
        """
        # Build lines: header only if present
        lines: list[str] = []
        if self._header is not None:
            lines.append(serialize_header(self._header))

        self._fs.ensure_parent_dir(self._path)

        # Atomically replace file
        # On Windows, we must release the lock before atomic_replace because
        # you can't rename to a locked file. We accept the small race window.
        stats = self._fs.stat(self._path)
        if stats.exists:
            # Read current content under lock
            with self._fs.open_locked(self._path, "r+b", self._lock_timeout) as f:
                content = f.read()
                self._load_from_content(content)
                lines = []
                if self._header is not None:
                    lines.append(serialize_header(self._header))
            # Lock released, now do atomic replace
            self._fs.atomic_replace(self._path, lines)
            self._state = {}
            self._cached_sorted_keys = None
            self._try_update_stats()
        elif lines:
            # File doesn't exist - create with header
            # No lock needed since atomic_replace handles races via temp file
            self._fs.atomic_replace(self._path, lines)
            self._state = {}
            self._cached_sorted_keys = None
            self._try_update_stats()
        else:
            # No header, nothing to write for empty table
            self._state = {}
            self._cached_sorted_keys = None

    def compact(self) -> None:
        """Compact the table to its minimal representation.

        Rewrites the file atomically with only the header (if present) and
        current records in key order. Removes all tombstones and superseded
        record versions.

        Raises:
            LockError: If file lock cannot be acquired within timeout.
            FileError: If file operations fail.
        """
        self._fs.ensure_parent_dir(self._path)

        # Atomically replace file with compacted content
        stats = self._fs.stat(self._path)
        if stats.exists:
            with self._fs.open_locked(self._path, "r+b", self._lock_timeout) as f:
                # Reload state using locked handle (Windows-compatible)
                content = f.read()
                self._load_from_content(content)
                # Build lines from fresh state
                lines: list[str] = []
                if self._header is not None:
                    lines.append(serialize_header(self._header))
                lines.extend(serialize_json(r) for r in self._sorted_records())
            # Lock released, now do atomic replace (Windows can't rename locked)
            self._fs.atomic_replace(self._path, lines)
            self._try_update_stats()
        elif self._header is not None or self._state:
            # File doesn't exist but we have in-memory content - create it
            # No lock needed since atomic_replace handles races via temp file
            lines = []
            if self._header is not None:
                lines.append(serialize_header(self._header))
            lines.extend(serialize_json(r) for r in self._sorted_records())
            self._fs.atomic_replace(self._path, lines)
            self._try_update_stats()
        else:
            # No header, no records - nothing to write
            self._state = {}

    # --- Transaction Operations ---

    def transaction(self) -> "Transaction":
        """Start a new transaction.

        Returns a Transaction object that provides snapshot isolation for reads
        and buffered writes. Use as a context manager for automatic commit/abort.

        Example:
            >>> with table.transaction() as tx:
            ...     tx.put({"id": "alice", "v": 1})
            ...     tx.delete("bob")
            ... # Commits on successful exit

        Returns:
            A new Transaction.

        Raises:
            InvalidKeyError: If no key specifier is set.
            TransactionError: If a transaction is already active.
        """
        # Import here to avoid circular import at runtime
        from ._transaction import Transaction as TransactionImpl  # noqa: PLC0415

        key_specifier = self._require_key_specifier()

        if self._active_transaction is not None:
            msg = "a transaction is already active on this table"
            raise TransactionError(msg)

        # Reload to get fresh state
        self._maybe_reload()

        # Create transaction with current state
        tx: Transaction = TransactionImpl(self, key_specifier, self._state)
        self._active_transaction = tx
        return tx

    def _end_transaction(self) -> None:
        """Clear the active transaction.

        Called by Transaction.commit() and Transaction.abort().
        """
        self._active_transaction = None

    def _commit_transaction_buffer(  # noqa: PLR0913
        self,
        lines: list[str],
        start_state: "dict[Key, JSONObject]",
        written_keys: set[Key],
        buffer_updates: "dict[Key, JSONObject | None]",
        start_mtime: float,
        start_size: int,
        *,
        _retries: int = 0,
    ) -> None:
        """Commit a transaction's buffered writes.

        Called by Transaction.commit() to write buffered changes to the file.
        Performs conflict detection and writes all lines under exclusive lock.

        Args:
            lines: Serialized JSON lines to append.
            start_state: Snapshot of table state when transaction started.
            written_keys: Keys that were modified in the transaction.
            buffer_updates: Map of key -> record (or None for delete).
            start_mtime: File mtime when transaction started.
            start_size: File size when transaction started.
            _retries: Internal retry counter (do not pass externally).

        Raises:
            ConflictError: If a write-write conflict is detected.
            LockError: If file lock cannot be acquired within timeout.
            FileError: If file write fails.
        """
        self._fs.ensure_parent_dir(self._path)

        stats = self._fs.stat(self._path)
        if stats.exists:
            with self._fs.open_locked(self._path, "r+b", self._lock_timeout) as f:
                # Read and reload if file changed (Windows-compatible)
                content = f.read()
                current_size = len(content)
                if current_size != start_size:
                    # File changed - reload from content
                    self._load_from_content(content)
                # Check for conflicts
                self._detect_conflicts(start_state, written_keys)
                # Write all buffered lines using same handle
                _ = f.seek(0, 2)  # Seek to end
                for line in lines:
                    encoded = (line + "\n").encode("utf-8")
                    _ = f.write(encoded)
                f.sync()
                # Update state from buffer
                self._apply_buffer_updates(buffer_updates)
                self._try_update_stats()
        else:
            # File doesn't exist - create it
            try:
                with self._fs.open_locked(self._path, "xb", self._lock_timeout) as f:
                    # Check for conflicts (should be none since start_state
                    # was empty)
                    self._detect_conflicts(start_state, written_keys)
                    # Write all buffered lines
                    for line in lines:
                        encoded = (line + "\n").encode("utf-8")
                        _ = f.write(encoded)
                    f.sync()
                    # Update state from buffer
                    self._apply_buffer_updates(buffer_updates)
                    self._try_update_stats()
            except FileExistsError:
                # File was created between our check and open - retry
                if _retries >= self._MAX_WRITE_RETRIES:
                    msg = "cannot acquire stable file handle after multiple retries"
                    raise FileError(msg) from None
                self._commit_transaction_buffer(
                    lines,
                    start_state,
                    written_keys,
                    buffer_updates,
                    start_mtime,
                    start_size,
                    _retries=_retries + 1,
                )

    def _detect_conflicts(
        self,
        start_state: "dict[Key, JSONObject]",
        written_keys: set[Key],
    ) -> None:
        """Detect write-write conflicts.

        For each key in written_keys, compare the current state (after reload)
        with the start_state. If they differ, another process modified the
        key since the transaction started.

        Args:
            start_state: Snapshot of table state when transaction started.
            written_keys: Keys that were modified in the transaction.

        Raises:
            ConflictError: If a write-write conflict is detected.
        """
        for key in written_keys:
            key_in_current = key in self._state
            key_in_start = key in start_state

            # Check if key presence differs
            if key_in_current != key_in_start:
                msg = f"conflict detected: key {key!r} was modified externally"
                expected = start_state.get(key)
                actual = self._state.get(key)
                raise ConflictError(msg, key, expected, actual)

            # If neither has the key, no conflict
            if not key_in_current:
                continue

            # Both have the key - compare records
            if self._state[key] != start_state[key]:
                msg = f"conflict detected: key {key!r} was modified externally"
                expected = start_state.get(key)
                actual = self._state.get(key)
                raise ConflictError(msg, key, expected, actual)

    def _apply_buffer_updates(
        self,
        buffer_updates: "dict[Key, JSONObject | None]",
    ) -> None:
        """Apply buffered updates to the table state.

        Args:
            buffer_updates: Map of key -> record (or None for delete).
        """
        for key, record in buffer_updates.items():
            if record is not None:
                self._state[key] = record
            else:
                _ = self._state.pop(key, None)
        self._cached_sorted_keys = None

    @override
    def __repr__(self) -> str:
        """Return a string representation of the table."""
        return f"Table({self._path!r}, key={self._key_specifier!r})"
