"""Mixin class providing full table interface.

This module provides TableMixin, an abstract base class that implements
all read operations, write operation signatures, and MutableMapping
interface for both Table and Transaction classes.
"""

from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from functools import cmp_to_key
from typing import TYPE_CHECKING, ClassVar, TypeGuard, cast, overload

from ._exceptions import InvalidKeyError
from ._keys import Key, compare_keys
from ._records import extract_key

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._json import JSONObject
    from ._keys import KeySpecifier

__all__ = ["TableMixin"]


class TableMixin(ABC):
    """Mixin providing full table interface including MutableMapping.

    Subclasses must implement:
    - _get_state(): Returns the dict[Key, JSONObject] to read from
    - _prepare_read(): Called before each public read operation
    - _get_key_specifier(): Return the key specifier
    - put(record): Insert or update a record
    - delete(key): Delete a record by key

    Subclasses must also have a `_cached_sorted_keys: list[Key] | None` slot.
    """

    __slots__: ClassVar[tuple[str, ...]] = ()

    # Subclasses must have this as a slot attribute
    _cached_sorted_keys: list[Key] | None

    @abstractmethod
    def _get_state(self) -> "dict[Key, JSONObject]":
        """Return the state dictionary to read from."""
        ...

    @abstractmethod
    def _prepare_read(self) -> None:
        """Perform any required setup before a read operation."""
        ...

    @abstractmethod
    def _get_key_specifier(self) -> "KeySpecifier":
        """Return the key specifier for this table.

        Returns:
            The key specifier.

        Raises:
            InvalidKeyError: If no key specifier is set.
        """
        ...

    @abstractmethod
    def put(self, record: "JSONObject") -> None:
        """Insert or update a record.

        Args:
            record: The record to insert/update. Must contain key fields.

        Raises:
            InvalidKeyError: If record is missing key fields, has invalid key
                values, or contains $-prefixed fields.
            LimitError: If key or record size exceeds limits.
        """
        ...

    @abstractmethod
    def delete(self, key: Key) -> bool:
        """Delete a record by key. Returns whether it existed.

        Args:
            key: The key to delete.

        Returns:
            True if the key existed, False otherwise.

        Raises:
            InvalidKeyError: If key arity doesn't match specifier.
        """
        ...

    def _sorted_keys(self) -> list[Key]:
        """Return keys sorted by JSONLT key ordering."""
        if self._cached_sorted_keys is None:
            self._cached_sorted_keys = sorted(
                self._get_state().keys(), key=cmp_to_key(compare_keys)
            )
        return self._cached_sorted_keys

    def _sorted_records(self) -> "list[JSONObject]":
        """Return records sorted by key order."""
        state = self._get_state()
        return [state[k] for k in self._sorted_keys()]

    @staticmethod
    def _validate_key(key: Key) -> None:
        """Validate that a key is not an empty tuple.

        Args:
            key: The key to validate.

        Raises:
            InvalidKeyError: If the key is an empty tuple.
        """
        if isinstance(key, tuple) and len(key) == 0:
            msg = "empty tuple is not a valid key"
            raise InvalidKeyError(msg)

    @staticmethod
    def _is_valid_tuple_key(
        key: tuple[object, ...],
    ) -> "TypeGuard[tuple[str | int, ...]]":
        """Check if a tuple is a valid Key tuple (all elements are str or int)."""
        return all(isinstance(k, (str, int)) for k in key)

    @overload
    def get(self, key: Key) -> "JSONObject | None": ...  # pragma: no cover

    @overload
    def get(
        self, key: Key, default: "JSONObject"
    ) -> "JSONObject": ...  # pragma: no cover

    @overload
    def get(
        self, key: Key, default: None
    ) -> "JSONObject | None": ...  # pragma: no cover

    def get(self, key: Key, default: "JSONObject | None" = None) -> "JSONObject | None":
        """Get a record by key.

        Args:
            key: The key to look up.
            default: Value to return if key not found. Defaults to None.

        Returns:
            The record if found, otherwise the default value.

        Raises:
            InvalidKeyError: If the key is an empty tuple.
        """
        self._validate_key(key)
        self._prepare_read()
        result = self._get_state().get(key)
        if result is None:
            return default
        return result

    def has(self, key: Key) -> bool:
        """Check if a key exists.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.

        Raises:
            InvalidKeyError: If the key is an empty tuple.
        """
        self._validate_key(key)
        self._prepare_read()
        return key in self._get_state()

    def all(self) -> "list[JSONObject]":
        """Get all records in key order.

        Returns:
            A list of all records, sorted by key.
        """
        self._prepare_read()
        return self._sorted_records()

    def keys(self) -> list[Key]:
        """Get all keys in key order.

        Returns:
            A list of all keys, sorted.
        """
        self._prepare_read()
        return self._sorted_keys()

    def values(self) -> "list[JSONObject]":
        """Get all records in key order.

        Returns:
            A list of all records, sorted by key.
        """
        self._prepare_read()
        return self._sorted_records()

    def items(self) -> "list[tuple[Key, JSONObject]]":
        """Get all key-value pairs in key order.

        Returns:
            A list of (key, record) tuples, sorted by key.
        """
        self._prepare_read()
        state = self._get_state()
        return [(k, state[k]) for k in self._sorted_keys()]

    def count(self) -> int:
        """Get the number of records.

        Returns:
            The number of records.
        """
        self._prepare_read()
        return len(self._get_state())

    @overload
    def find(
        self,
        predicate: "Callable[[JSONObject], bool]",
    ) -> "list[JSONObject]": ...  # pragma: no cover

    @overload
    def find(
        self,
        predicate: "Callable[[JSONObject], bool]",
        *,
        limit: int,
    ) -> "list[JSONObject]": ...  # pragma: no cover

    def find(
        self,
        predicate: "Callable[[JSONObject], bool]",
        *,
        limit: "int | None" = None,
    ) -> "list[JSONObject]":
        """Find records matching a predicate.

        Records are returned in key order.

        Args:
            predicate: A function that takes a record and returns True if
                it should be included.
            limit: Maximum number of records to return.

        Returns:
            A list of matching records, in key order.
        """
        self._prepare_read()
        results: list[JSONObject] = []
        for record in self._sorted_records():
            if predicate(record):
                results.append(record)
                if limit is not None and len(results) >= limit:
                    break
        return results

    def find_one(
        self,
        predicate: "Callable[[JSONObject], bool]",
    ) -> "JSONObject | None":
        """Find the first record matching a predicate.

        Records are checked in key order.

        Args:
            predicate: A function that takes a record and returns True.

        Returns:
            The first matching record, or None if no match.
        """
        self._prepare_read()
        for record in self._sorted_records():
            if predicate(record):
                return record
        return None

    def __getitem__(self, key: Key) -> "JSONObject":
        """Get a record by key.

        Args:
            key: The key to look up.

        Returns:
            The record associated with the key.

        Raises:
            KeyError: If the key does not exist.
            InvalidKeyError: If the key is an empty tuple.
        """
        self._validate_key(key)
        self._prepare_read()
        state = self._get_state()
        if key not in state:
            raise KeyError(key)
        return state[key]

    def __contains__(self, key: object) -> bool:
        """Check if a key exists.

        Args:
            key: The key to check. Must be a valid Key type.

        Returns:
            True if the key exists, False otherwise.
        """
        self._prepare_read()
        state = self._get_state()
        if isinstance(key, str):
            return key in state
        if isinstance(key, int):
            return key in state
        if isinstance(key, tuple):
            tuple_key = cast("tuple[object, ...]", key)
            if self._is_valid_tuple_key(tuple_key):
                return tuple_key in state
        return False

    def __setitem__(self, key: Key, value: "JSONObject") -> None:
        """Store a record.

        The key parameter must match the key extracted from the record.
        This ensures consistency between the lookup key and the record's
        actual key fields.

        Args:
            key: The key for the record.
            value: The record to store. Must contain key fields.

        Raises:
            InvalidKeyError: If the key does not match the record's key,
                or if the record is invalid.
        """
        key_specifier = self._get_key_specifier()
        extracted_key = extract_key(value, key_specifier)
        if extracted_key != key:
            msg = (
                f"key mismatch: provided key {key!r} does not match "
                f"record key {extracted_key!r}"
            )
            raise InvalidKeyError(msg)
        self.put(value)

    def __delitem__(self, key: Key) -> None:
        """Delete a record by key.

        Args:
            key: The key to delete.

        Raises:
            KeyError: If the key does not exist.
        """
        existed = self.delete(key)
        if not existed:
            raise KeyError(key)

    def __iter__(self) -> "Iterator[Key]":
        """Iterate over keys in sorted order.

        Returns:
            An iterator over all keys.
        """
        return iter(self._sorted_keys())

    def __len__(self) -> int:
        """Return the number of records.

        Returns:
            The count of records.
        """
        return len(self._get_state())

    def pop(self, key: Key, *args: "JSONObject") -> "JSONObject":
        """Remove and return record for key.

        Args:
            key: The key to remove.
            *args: Optional default value if key not found.

        Returns:
            The removed record, or default if provided and key not found.

        Raises:
            KeyError: If key not found and no default provided.
        """
        if len(args) > 1:
            msg = f"pop expected at most 2 arguments, got {1 + len(args)}"
            raise TypeError(msg)
        try:
            value = self[key]
        except KeyError:
            if args:
                return args[0]
            raise
        del self[key]
        return value

    def popitem(self) -> "tuple[Key, JSONObject]":
        """Remove and return an arbitrary (key, record) pair.

        Returns:
            A (key, record) tuple.

        Raises:
            KeyError: If the table is empty.
        """
        try:
            key = next(iter(self))
        except StopIteration:
            msg = "popitem(): table is empty"
            raise KeyError(msg) from None
        value = self[key]
        del self[key]
        return key, value

    def setdefault(self, key: Key, default: "JSONObject") -> "JSONObject":
        """Get record for key, setting it to default if not present.

        Note: Unlike dict.setdefault(), a default value is required because
        JSONLT records must contain key fields.

        Args:
            key: The key to look up.
            default: The record to insert if key not found. Must contain key fields.

        Returns:
            The existing record if found, otherwise the default (after insertion).

        Raises:
            InvalidKeyError: If default record's key doesn't match the provided key.
        """
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def update(
        self,
        other: (
            "Mapping[Key, JSONObject] | Iterable[tuple[Key, JSONObject]] | None"
        ) = None,
        /,
        **kwargs: "JSONObject",
    ) -> None:
        """Update table from mapping/iterable and/or keyword arguments.

        Args:
            other: A mapping or iterable of (key, record) pairs.
            **kwargs: Additional key=record pairs (keys must be strings).
        """
        if other is not None:
            if hasattr(other, "keys"):
                mapping = cast("Mapping[Key, JSONObject]", other)
                for key in mapping:
                    self[key] = mapping[key]
            else:
                iterable = cast("Iterable[tuple[Key, JSONObject]]", other)
                for key, value in iterable:
                    self[key] = value
        for str_key, value in kwargs.items():
            self[str_key] = value


_ = cast("ABCMeta", cast("object", MutableMapping)).register(TableMixin)
