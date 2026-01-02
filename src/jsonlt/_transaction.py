"""Transaction class for JSONLT atomic operations.

This module provides the Transaction class, which enables snapshot isolation
for reads and buffered writes until commit. Conflicts are detected at commit
time using optimistic concurrency control.
"""

import copy
from typing import TYPE_CHECKING, ClassVar
from typing_extensions import override

from ._constants import MAX_RECORD_SIZE
from ._encoding import validate_no_surrogates
from ._exceptions import LimitError, TransactionError
from ._json import serialize_json, utf8_byte_length
from ._keys import Key, KeySpecifier, validate_key_arity, validate_key_length
from ._mixin import TableMixin
from ._records import build_tombstone, extract_key, validate_record

if TYPE_CHECKING:
    from ._json import JSONObject
    from ._table import Table


class Transaction(TableMixin):
    """A transaction for atomic operations on a JSONLT table.

    Transactions provide snapshot isolation: reads see a consistent snapshot
    taken when the transaction started, plus any writes made within the
    transaction. Writes are buffered until commit.

    At commit time, the transaction detects conflicts by checking whether
    any key it modified has been changed externally since the transaction
    started. If a conflict is detected, the transaction raises ConflictError
    and the table retains the externally modified state.

    Example:
        >>> table = Table("users.jsonlt", key="id")
        >>> with table.transaction() as tx:
        ...     user = tx.get("alice")
        ...     if user:
        ...         tx.put({"id": "alice", "visits": user["visits"] + 1})

    Transactions can also be managed manually:
        >>> tx = table.transaction()
        >>> try:
        ...     tx.put({"id": "bob", "role": "admin"})
        ...     tx.commit()
        ... except:
        ...     tx.abort()
        ...     raise
    """

    __slots__: ClassVar[tuple[str, ...]] = (
        "_buffer_serialized",
        "_buffer_updates",
        "_cached_sorted_keys",
        "_file_mtime",
        "_file_size",
        "_finalized",
        "_key_specifier",
        "_snapshot",
        "_start_state",
        "_table",
        "_written_keys",
    )

    _table: "Table"
    _key_specifier: KeySpecifier
    _snapshot: "dict[Key, JSONObject]"
    _start_state: "dict[Key, JSONObject]"
    _buffer_updates: "dict[Key, JSONObject | None]"
    _buffer_serialized: "dict[Key, str]"
    _written_keys: set[Key]
    _finalized: bool
    _file_mtime: float
    _file_size: int
    _cached_sorted_keys: list[Key] | None

    def __init__(
        self,
        table: "Table",
        key_specifier: KeySpecifier,
        state: "dict[Key, JSONObject]",
    ) -> None:
        """Initialize a transaction.

        This is an internal constructor. Use Table.transaction() to create
        transactions.

        Args:
            table: The parent table.
            key_specifier: The key specifier for this table.
            state: The current table state (will be deep copied).
        """
        self._table = table
        self._key_specifier = key_specifier
        # Deep copy state for snapshot isolation
        self._snapshot = copy.deepcopy(state)
        # Shallow copy for conflict detection - values compared with == against
        # reloaded state. Safe because _start_state values are never modified.
        self._start_state = state.copy()
        self._buffer_updates = {}
        self._buffer_serialized = {}
        self._written_keys = set()
        self._finalized = False
        # Cache file stats for skip-reload optimization at commit time
        # Access to table's private attributes is intentional (friend class pattern)
        self._file_mtime = table._file_mtime  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        self._file_size = table._file_size  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        self._cached_sorted_keys = None

    def _require_active(self) -> None:
        """Ensure the transaction is still active.

        Raises:
            TransactionError: If the transaction has already been committed
                or aborted.
        """
        if self._finalized:
            msg = "transaction has already been committed or aborted"
            raise TransactionError(msg)

    @override
    def _get_state(self) -> "dict[Key, JSONObject]":
        """Return the transaction snapshot dictionary."""
        return self._snapshot

    @override
    def _prepare_read(self) -> None:
        """Ensure the transaction is still active."""
        self._require_active()

    @override
    def _get_key_specifier(self) -> KeySpecifier:
        """Return the key specifier for this transaction.

        Returns:
            The key specifier.
        """
        return self._key_specifier

    @override
    def put(self, record: "JSONObject") -> None:
        """Insert or update a record in the transaction.

        The record is validated and serialized, then buffered for commit.
        The transaction snapshot is updated immediately.

        Args:
            record: The record to insert/update. Must contain key fields.

        Raises:
            TransactionError: If the transaction is no longer active.
            InvalidKeyError: If record missing key fields, has invalid key
                values, contains $-prefixed fields, or contains unpaired
                surrogates.
            LimitError: If key length > 1024 bytes or record size > 1 MiB.
        """
        self._require_active()

        validate_no_surrogates(record)
        validate_record(record, self._key_specifier)

        key = extract_key(record, self._key_specifier)
        validate_key_length(key)

        # Serialize record to check size limit and cache for commit
        serialized = serialize_json(record)
        record_bytes = utf8_byte_length(serialized)
        if record_bytes > MAX_RECORD_SIZE:
            msg = f"record size {record_bytes} bytes exceeds maximum {MAX_RECORD_SIZE}"
            raise LimitError(msg)

        # Cache serialized form before deep copy (record hasn't been modified)
        self._buffer_serialized[key] = serialized

        record_copy = copy.deepcopy(record)
        self._buffer_updates[key] = record_copy
        self._written_keys.add(key)

        self._snapshot[key] = record_copy
        self._cached_sorted_keys = None

    @override
    def delete(self, key: Key) -> bool:
        """Delete a record by key in the transaction.

        Buffers a tombstone for commit. The transaction snapshot is updated
        immediately.

        Args:
            key: The key to delete. Must match key specifier arity.

        Returns:
            True if record existed in snapshot, False otherwise.

        Raises:
            TransactionError: If the transaction is no longer active.
            InvalidKeyError: If key arity doesn't match specifier.
        """
        self._require_active()

        validate_key_arity(key, self._key_specifier)
        validate_key_length(key)

        existed = key in self._snapshot

        self._buffer_updates[key] = None
        _ = self._buffer_serialized.pop(key, None)
        self._written_keys.add(key)

        if existed:
            del self._snapshot[key]
        self._cached_sorted_keys = None

        return existed

    def commit(self) -> None:
        """Commit the transaction.

        Writes all buffered changes to the file under exclusive lock.
        Detects conflicts by checking if any key modified by this transaction
        was also modified externally since the transaction started.

        Raises:
            TransactionError: If the transaction is no longer active.
            ConflictError: If a write-write conflict is detected.
            LockError: If file lock cannot be acquired within timeout.
            FileError: If file write fails.
        """
        self._require_active()

        try:
            # If no updates, just mark as committed
            if not self._buffer_updates:
                return

            # Build deduplicated buffer from _buffer_updates at commit time
            # Dict preserves insertion order in Python 3.7+, so each key appears once
            lines: list[str] = []
            for key, value in self._buffer_updates.items():
                if value is None:
                    # Tombstone (delete)
                    tombstone = build_tombstone(key, self._key_specifier)
                    lines.append(serialize_json(tombstone))
                else:
                    # Record (put) - use cached serialization from put()
                    lines.append(self._buffer_serialized[key])

            # Commit via table (handles locking and conflict detection)
            # Transaction is a friend class of Table - protected access is intentional
            self._table._commit_transaction_buffer(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                lines,
                self._start_state,
                self._written_keys,
                self._buffer_updates,
                self._file_size,
            )
        finally:
            self._finalized = True
            # Transaction is a friend class of Table - protected access is intentional
            self._table._end_transaction()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def abort(self) -> None:
        """Abort the transaction.

        Discards all buffered changes. The table state is unchanged.

        Raises:
            TransactionError: If the transaction is no longer active.
        """
        self._require_active()
        self._finalized = True
        # Transaction is a friend class of Table - protected access is intentional
        self._table._end_transaction()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def __enter__(self) -> "Transaction":
        """Enter the transaction context.

        Returns:
            This transaction.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool:
        """Exit the transaction context.

        If no exception occurred, commits the transaction. Otherwise,
        aborts the transaction. Exceptions are not suppressed.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.

        Returns:
            False (exceptions are not suppressed).
        """
        if self._finalized:
            return False
        if exc_type is None:
            self.commit()
        else:
            self.abort()
        return False

    @override
    def __repr__(self) -> str:
        """Return a string representation of the transaction."""
        status = "finalized" if self._finalized else "active"
        return (
            f"Transaction({self._table._path!r}, key={self._key_specifier!r}, {status})"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        )

    @override
    def __eq__(self, other: object) -> bool:
        """Equality based on parent table identity, finalized status, snapshot.

        Two transactions are equal if they reference the same parent Table
        instance, have the same finalized status, and have identical snapshot
        state (which includes any buffered writes).

        Args:
            other: Object to compare with.

        Returns:
            True if equal, False otherwise. Returns NotImplemented for others.
        """
        if not isinstance(other, Transaction):
            return NotImplemented
        return (
            self._table is other._table
            and self._finalized == other._finalized
            and self._snapshot == other._snapshot
        )

    @override
    def __hash__(self) -> int:
        """Transaction is mutable and not hashable."""
        msg = f"unhashable type: '{type(self).__name__}'"
        raise TypeError(msg)
