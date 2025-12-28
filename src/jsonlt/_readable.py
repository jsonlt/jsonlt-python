"""Mixin class for readable table-like objects.

This module provides ReadableMixin, an abstract base class that implements
read operations for both Table and Transaction classes.
"""

from abc import ABC, abstractmethod
from functools import cmp_to_key
from typing import TYPE_CHECKING, ClassVar, TypeGuard, cast, overload

from ._keys import Key, compare_keys

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from ._json import JSONObject


class ReadableMixin(ABC):
    """Abstract mixin providing read operations for table-like objects.

    Subclasses must implement:
    - _get_state(): Returns the dict[Key, JSONObject] to read from
    - _prepare_read(): Called before each public read operation

    Subclasses must also have a `_cached_sorted_keys: list[Key] | None` slot.
    """

    __slots__: ClassVar[tuple[str, ...]] = ()

    # --- Abstract methods for subclasses ---

    @abstractmethod
    def _get_state(self) -> "dict[Key, JSONObject]":
        """Return the state dictionary to read from."""
        ...

    @abstractmethod
    def _prepare_read(self) -> None:
        """Perform any required setup before a read operation."""
        ...

    # Subclasses must have this as a slot attribute
    _cached_sorted_keys: list[Key] | None

    # --- Private helpers ---

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
    def _is_valid_tuple_key(
        key: tuple[object, ...],
    ) -> "TypeGuard[tuple[str | int, ...]]":
        """Check if a tuple is a valid Key tuple (all elements are str or int)."""
        return all(isinstance(k, (str, int)) for k in key)

    @staticmethod
    def _validate_key(key: Key) -> None:
        """Validate that a key is not an empty tuple.

        Args:
            key: The key to validate.

        Raises:
            InvalidKeyError: If the key is an empty tuple.
        """
        if isinstance(key, tuple) and len(key) == 0:
            from ._exceptions import InvalidKeyError  # noqa: PLC0415

            msg = "empty tuple is not a valid key"
            raise InvalidKeyError(msg)

    # --- Read methods ---

    def get(self, key: Key) -> "JSONObject | None":
        """Get a record by key.

        Args:
            key: The key to look up.

        Returns:
            The record if found, None otherwise.

        Raises:
            InvalidKeyError: If the key is an empty tuple.
        """
        self._validate_key(key)
        self._prepare_read()
        return self._get_state().get(key)

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

    def __len__(self) -> int:
        """Return the number of records."""
        return self.count()

    def __contains__(self, key: object) -> bool:
        """Check if a key exists.

        Args:
            key: The key to check. Must be a valid Key type.

        Returns:
            True if the key exists, False otherwise.
        """
        if isinstance(key, str):
            return self.has(key)
        if isinstance(key, int):
            return self.has(key)
        if isinstance(key, tuple):
            tuple_key = cast("tuple[object, ...]", key)
            if self._is_valid_tuple_key(tuple_key):
                return self.has(tuple_key)
        return False

    def __iter__(self) -> "Iterator[JSONObject]":
        """Iterate over all records in key order."""
        yield from self.all()

    @overload
    def find(
        self,
        predicate: "Callable[[JSONObject], bool]",
    ) -> "list[JSONObject]": ...

    @overload
    def find(
        self,
        predicate: "Callable[[JSONObject], bool]",
        *,
        limit: int,
    ) -> "list[JSONObject]": ...

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
