"""Key types and operations for JSONLT.

This module defines the key types (Key, KeyElement, KeySpecifier) and
operations for working with keys per the JSONLT specification.
"""

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, TypeAlias

from ._constants import (
    MAX_INTEGER_KEY,
    MAX_KEY_LENGTH,
    MAX_TUPLE_ELEMENTS,
    MIN_INTEGER_KEY,
)
from ._exceptions import InvalidKeyError, LimitError
from ._json import utf8_byte_length

if TYPE_CHECKING:
    from typing import TypeGuard
    from typing_extensions import TypeIs

KeyElement: TypeAlias = "str | int"
"""A key element is a string or integer that may appear in a tuple key."""

Key: TypeAlias = "str | int | tuple[str | int, ...]"
"""A key identifies a record within a table.

A key is one of:
- A string
- An integer in the range [-(2^53)+1, (2^53)-1]
- A tuple of key elements (non-empty, max 16 elements)
"""

KeySpecifier: TypeAlias = "str | tuple[str, ...]"
"""A key specifier defines how to extract a key from a record.

A key specifier is one of:
- A string naming a single field
- A tuple of strings naming multiple fields (for compound keys)
"""


def is_valid_key_element(value: object) -> "TypeIs[str | int]":
    """Check if a value is a valid key element.

    A valid key element is a string or an integer within the range
    [-(2^53)+1, (2^53)-1].

    Args:
        value: The value to check.

    Returns:
        True if the value is a valid key element, False otherwise.
    """
    if isinstance(value, str):
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return MIN_INTEGER_KEY <= value <= MAX_INTEGER_KEY
    return False


def is_valid_key(value: object) -> "TypeIs[str | int | tuple[str | int, ...]]":
    """Check if a value is a valid key.

    A valid key is:
    - A string
    - An integer within the range [-(2^53)+1, (2^53)-1]
    - A non-empty tuple of valid key elements (max 16 elements)

    Args:
        value: The value to check.

    Returns:
        True if the value is a valid key, False otherwise.
    """
    if isinstance(value, str):
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return MIN_INTEGER_KEY <= value <= MAX_INTEGER_KEY
    if not isinstance(value, tuple):
        return False
    # Type narrowing from isinstance(value, tuple) gives tuple[Unknown, ...]
    # which is unavoidable when validating arbitrary objects
    tuple_value: tuple[object, ...] = value  # pyright: ignore[reportUnknownVariableType]
    if len(tuple_value) == 0:
        return False
    if len(tuple_value) > MAX_TUPLE_ELEMENTS:
        return False
    return all(is_valid_key_element(elem) for elem in tuple_value)


def is_valid_key_specifier(specifier: object) -> "TypeGuard[str | tuple[str, ...]]":
    """Check if a value is a valid key specifier.

    A valid key specifier is:
    - A string (naming a single field)
    - A non-empty tuple of strings with no duplicates

    Args:
        specifier: The value to check.

    Returns:
        True if the value is a valid key specifier, False otherwise.
    """
    if isinstance(specifier, str):
        return True
    if not isinstance(specifier, tuple):
        return False
    # Type narrowing from isinstance(specifier, tuple) gives tuple[Unknown, ...]
    tuple_spec: tuple[object, ...] = specifier  # pyright: ignore[reportUnknownVariableType]
    if len(tuple_spec) == 0:
        return False
    if not all(isinstance(field, str) for field in tuple_spec):
        return False
    # After the isinstance check above, all elements are strings
    str_tuple: tuple[str, ...] = tuple_spec  # pyright: ignore[reportAssignmentType]
    return len(str_tuple) == len(set(str_tuple))


def normalize_key_specifier(specifier: KeySpecifier) -> KeySpecifier:
    """Normalize a key specifier.

    Single-element tuples are normalized to strings. Other specifiers
    are returned unchanged.

    Args:
        specifier: A valid key specifier.

    Returns:
        The normalized key specifier.
    """
    if isinstance(specifier, tuple) and len(specifier) == 1:
        return specifier[0]
    return specifier


def key_specifiers_match(a: KeySpecifier, b: KeySpecifier) -> bool:
    """Check if two key specifiers match.

    Key specifiers match if, after normalizing single-element tuples to
    strings, they are structurally identical and each field name consists
    of the same sequence of Unicode code points.

    Args:
        a: First key specifier.
        b: Second key specifier.

    Returns:
        True if the key specifiers match, False otherwise.
    """
    return normalize_key_specifier(a) == normalize_key_specifier(b)


def _compare_elements(a: str | int, b: str | int) -> int:
    """Compare two key elements.

    Args:
        a: First key element.
        b: Second key element.

    Returns:
        -1 if a < b, 0 if a == b, 1 if a > b.
    """
    # Integers are ordered before strings
    a_is_int = isinstance(a, int)
    b_is_int = isinstance(b, int)

    if a_is_int and not b_is_int:
        return -1
    if not a_is_int and b_is_int:
        return 1

    # Same type: compare directly
    if a < b:  # pyright: ignore[reportOperatorIssue]
        return -1
    if a > b:  # pyright: ignore[reportOperatorIssue]
        return 1
    return 0


def _cmp(a: int, b: int) -> int:
    """Return -1, 0, or 1 based on comparison."""
    return -1 if a < b else (1 if a > b else 0)


def _compare_ints(a: int, b: int) -> int:
    """Compare two integers."""
    return _cmp(a, b)


def _compare_strs(a: str, b: str) -> int:
    """Compare two strings lexicographically."""
    return -1 if a < b else (1 if a > b else 0)


def _compare_tuples(a: tuple[str | int, ...], b: tuple[str | int, ...]) -> int:
    """Compare two tuples lexicographically by element."""
    for elem_a, elem_b in zip(a, b, strict=False):
        cmp = _compare_elements(elem_a, elem_b)
        if cmp != 0:
            return cmp
    return _cmp(len(a), len(b))


def _type_rank(k: Key) -> int:
    """Return type rank for ordering: int=0, str=1, tuple=2."""
    if isinstance(k, int):
        return 0
    if isinstance(k, str):
        return 1
    return 2


def compare_keys(a: Key, b: Key) -> int:
    """Compare two keys according to JSONLT ordering.

    Key ordering:
    - Integers are ordered numerically
    - Strings are ordered lexicographically by Unicode code point
    - Tuples are ordered lexicographically by element
    - Across types: integers < strings < tuples

    Args:
        a: First key.
        b: Second key.

    Returns:
        -1 if a < b, 0 if a == b, 1 if a > b.
    """
    rank_a = _type_rank(a)
    rank_b = _type_rank(b)

    if rank_a != rank_b:
        return _cmp(rank_a, rank_b)

    # Same type - dispatch to type-specific comparison
    if isinstance(a, int) and isinstance(b, int):
        return _compare_ints(a, b)
    if isinstance(a, str) and isinstance(b, str):
        return _compare_strs(a, b)
    # Both are tuples (type narrowed after int/str checks)
    return _compare_tuples(a, b)  # pyright: ignore[reportArgumentType]


def serialize_key(key: Key) -> str:
    """Serialize a key to its JSON representation.

    Uses ensure_ascii=False per the specification requirement that generators
    SHOULD NOT escape characters that do not require escaping.

    Args:
        key: A valid key.

    Returns:
        The JSON string representation of the key.
    """
    if isinstance(key, tuple):
        return json.dumps(list(key), separators=(",", ":"), ensure_ascii=False)
    return json.dumps(key, separators=(",", ":"), ensure_ascii=False)


def key_length(key: Key) -> int:
    """Compute the byte length of a key per the specification.

    The key length is the number of bytes in its JSON representation
    when encoded as UTF-8:
    - For a string key: byte length including quotes and escapes
    - For an integer key: byte length of decimal representation
    - For a tuple key: byte length of the complete JSON array

    Args:
        key: A valid key.

    Returns:
        The key length in bytes.
    """
    return utf8_byte_length(serialize_key(key))


def validate_key_arity(key: Key, key_specifier: KeySpecifier) -> None:
    """Validate that key arity matches specifier arity.

    A scalar key specifier (string) requires a scalar key (string or int).
    A tuple key specifier requires a tuple key of the same length.

    Args:
        key: The key to validate.
        key_specifier: The key specifier to match against.

    Raises:
        InvalidKeyError: If the key arity doesn't match the specifier.
    """
    if isinstance(key_specifier, str):
        # Scalar specifier - key must be scalar
        if isinstance(key, tuple):
            msg = (
                f"key arity mismatch: expected scalar key, "
                f"got tuple of {len(key)} elements"
            )
            raise InvalidKeyError(msg)
    else:
        # Tuple specifier - key must be tuple of same length
        if not isinstance(key, tuple):
            msg = (
                f"key arity mismatch: expected tuple of {len(key_specifier)} "
                f"elements, got scalar"
            )
            raise InvalidKeyError(msg)
        if len(key) != len(key_specifier):
            msg = (
                f"key arity mismatch: expected tuple of {len(key_specifier)} "
                f"elements, got {len(key)}"
            )
            raise InvalidKeyError(msg)


def key_from_json(value: object) -> Key:
    """Convert a JSON-parsed value to a Key.

    This is used when extracting keys from parsed JSON objects.
    Lists are converted to tuples.

    Args:
        value: A value from JSON parsing.

    Returns:
        The value as a Key type.

    Raises:
        TypeError: If the value cannot be converted to a valid key.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, Sequence) and not isinstance(value, str):
        elements: list[str | int] = []
        for item in value:
            if isinstance(item, str) or (
                isinstance(item, int) and not isinstance(item, bool)
            ):
                elements.append(item)
            else:
                msg = f"Cannot convert {type(item).__name__} to key element"
                raise TypeError(msg)
        return tuple(elements)
    msg = f"Cannot convert {type(value).__name__} to key"
    raise TypeError(msg)


def validate_key_length(key: Key) -> None:
    """Validate that key length does not exceed the maximum.

    Args:
        key: The key to validate.

    Raises:
        LimitError: If key length exceeds MAX_KEY_LENGTH (1024 bytes).
    """
    key_len = key_length(key)
    if key_len > MAX_KEY_LENGTH:
        msg = f"key length {key_len} bytes exceeds maximum {MAX_KEY_LENGTH}"
        raise LimitError(msg)
