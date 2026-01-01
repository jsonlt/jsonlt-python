"""Exception hierarchy for JSONLT operations.

This module defines the exception types used throughout the JSONLT library,
following the specification's error categories.
"""

from typing_extensions import override

from ._types import JSONObject, Key


class JSONLTError(Exception):
    """Base exception for all JSONLT operations.

    All exceptions raised by JSONLT inherit from this class, allowing
    callers to catch all JSONLT-related errors with a single handler.
    """


class ParseError(JSONLTError):
    """Error during parsing of JSONLT content.

    Raised for:
    - Invalid UTF-8 encoding
    - Invalid JSON syntax
    - Non-object JSON values where objects are expected
    - Duplicate keys in JSON objects
    - Invalid $deleted values (not boolean true)
    - Header appearing on non-first line
    - Invalid header structure
    - Missing or invalid version field

    See specification section "Parse errors (ParseError)".
    """


class InvalidKeyError(JSONLTError):
    """Error related to keys or key specifiers.

    Raised for:
    - Missing required key fields in records
    - Invalid key field values (null, boolean, object, array)
    - Numbers outside the valid integer key range
    - Fractional numbers in key fields
    - Records containing $-prefixed fields
    - Key specifier mismatch between header and caller
    - Duplicate field names in key specifier tuples
    - Empty key specifier or key tuples

    See specification section "Key errors (KeyError)".
    """


class FileError(JSONLTError):
    """Error during file system operations.

    Raised for:
    - File read failures (permissions, I/O errors)
    - File write failures (permissions, I/O errors)
    - Atomic file replacement failures

    See specification section "File errors (IOError)".
    """


class LockError(JSONLTError):
    """Error during file locking.

    Raised when a file lock cannot be acquired within the configured timeout.

    See specification section "Lock errors (LockError)".
    """


class LimitError(JSONLTError):
    """Error when content exceeds implementation limits.

    Raised for:
    - Key length exceeding maximum
    - Record size exceeding maximum
    - JSON nesting depth exceeding maximum
    - Tuple key exceeding maximum element count

    See specification section "Limit errors (LimitError)".
    """


class TransactionError(JSONLTError):
    """Error related to transaction operations.

    Raised for:
    - Attempting to start a nested transaction

    See specification section "Transaction errors (TransactionError)".
    """


class ConflictError(TransactionError):
    """Error when a transaction commit detects a write-write conflict.

    Raised when another process has modified a key that the transaction
    also modified since the transaction started.

    See specification section "Transaction errors (TransactionError)".

    Attributes:
        key: The conflicting key.
        expected: The value that was expected (from transaction start snapshot).
        actual: The actual current value (after reload).
    """

    def __init__(
        self,
        message: str,
        key: "Key",
        expected: "JSONObject | None",
        actual: "JSONObject | None",
    ) -> None:
        """Initialize a ConflictError.

        Args:
            message: The error message.
            key: The conflicting key.
            expected: The value that was expected (from transaction start).
            actual: The actual current value (after reload).
        """
        super().__init__(message)
        self._key: Key = key
        self._expected: JSONObject | None = expected
        self._actual: JSONObject | None = actual

    @property
    def key(self) -> "Key":
        """The conflicting key."""
        return self._key

    @property
    def expected(self) -> "JSONObject | None":
        """The value that was expected (from transaction start snapshot)."""
        return self._expected

    @property
    def actual(self) -> "JSONObject | None":
        """The actual current value (after reload)."""
        return self._actual

    @override
    def __repr__(self) -> str:
        """Return a string representation of the conflict error."""
        return f"ConflictError({self.args[0]!r}, key={self._key!r})"
