"""Record operations for JSONLT.

This module provides functions for validating records, extracting keys,
and handling tombstones per the JSONLT specification.
"""

import math
from typing import TYPE_CHECKING

from ._constants import MAX_INTEGER_KEY, MIN_INTEGER_KEY
from ._exceptions import InvalidKeyError, ParseError
from ._json import serialize_json, utf8_byte_length

if TYPE_CHECKING:
    from ._json import JSONObject

# Key and KeySpecifier are TypeAlias definitions needed at runtime for type hints
from ._keys import Key, KeySpecifier, validate_key_arity


def _validate_key_field_value(value: object, field: str) -> str | int:
    """Validate that a value is valid for a key field.

    Args:
        value: The value from the key field.
        field: The field name (for error messages).

    Returns:
        The validated value as a string or int.

    Raises:
        InvalidKeyError: If the value is not a valid key element.
    """
    if value is None:
        msg = f"key field '{field}' value is null"
        raise InvalidKeyError(msg)

    if isinstance(value, bool):
        msg = f"key field '{field}' value is boolean"
        raise InvalidKeyError(msg)

    if isinstance(value, dict):
        msg = f"key field '{field}' value is an object"
        raise InvalidKeyError(msg)

    if isinstance(value, list):
        msg = f"key field '{field}' value is an array"
        raise InvalidKeyError(msg)

    if isinstance(value, float):
        if math.isinf(value) or math.isnan(value):
            msg = f"key field '{field}' value is Infinity or NaN"
            raise InvalidKeyError(msg)
        if not value.is_integer():
            msg = f"key field '{field}' value is not an integer"
            raise InvalidKeyError(msg)

    if isinstance(value, (int, float)):
        int_value = int(value)
        if int_value < MIN_INTEGER_KEY or int_value > MAX_INTEGER_KEY:
            msg = (
                f"key field '{field}' value {int_value} is outside valid integer range"
            )
            raise InvalidKeyError(msg)
        return int_value

    if isinstance(value, str):
        return value

    # Defensive fallback - unreachable with valid JSON input
    type_name = type(value).__name__  # pragma: no cover
    msg = f"key field '{field}' has invalid type {type_name}"  # pragma: no cover
    raise InvalidKeyError(msg)  # pragma: no cover


def validate_record(record: "JSONObject", key_specifier: KeySpecifier) -> None:
    """Validate that a record contains required key fields and no $-prefixed fields.

    Args:
        record: The record to validate.
        key_specifier: The key specifier defining required fields.

    Raises:
        InvalidKeyError: If the record is missing required key fields,
            has invalid key field values, or contains $-prefixed field names.
    """
    # Check for $-prefixed fields (reserved for protocol use)
    for field_name in record:
        if field_name.startswith("$"):
            msg = f"record contains reserved field name '{field_name}'"
            raise InvalidKeyError(msg)

    # Get the list of required key fields
    if isinstance(key_specifier, str):
        key_fields = [key_specifier]
    else:
        key_fields = list(key_specifier)

    # Validate each key field exists and has a valid value
    for field in key_fields:
        if field not in record:
            msg = f"record missing required key field '{field}'"
            raise InvalidKeyError(msg)
        _ = _validate_key_field_value(record[field], field)


def is_tombstone(obj: "JSONObject") -> bool:
    """Check if a JSON object is a tombstone (delete marker).

    A tombstone contains `$deleted` with value `true`.

    Args:
        obj: A parsed JSON object.

    Returns:
        True if the object contains `$deleted: true`, False otherwise.
    """
    return obj.get("$deleted") is True


def validate_tombstone(obj: "JSONObject", key_specifier: KeySpecifier) -> None:
    """Validate that a tombstone has the correct structure.

    A valid tombstone contains:
    - The field `$deleted` with value `true`
    - The required key fields per the key specifier

    Args:
        obj: A parsed JSON object that is expected to be a tombstone.
        key_specifier: The key specifier defining required key fields.

    Raises:
        ParseError: If `$deleted` has an invalid value.
        InvalidKeyError: If required key fields are missing or invalid.
    """
    # Validate $deleted field
    if "$deleted" not in obj:
        msg = "tombstone missing $deleted field"
        raise ParseError(msg)

    deleted_value = obj["$deleted"]
    if deleted_value is not True:
        if deleted_value is False:
            msg = "$deleted must be true, got false"
        elif deleted_value is None:
            msg = "$deleted must be true, got null"
        elif isinstance(deleted_value, str):
            msg = "$deleted must be true, got string"
        elif isinstance(deleted_value, (int, float)):
            msg = "$deleted must be true, got number"
        else:
            msg = f"$deleted must be true, got {type(deleted_value).__name__}"
        raise ParseError(msg)

    # Get the list of required key fields
    if isinstance(key_specifier, str):
        key_fields = [key_specifier]
    else:
        key_fields = list(key_specifier)

    # Validate each key field exists and has a valid value
    for field in key_fields:
        if field not in obj:
            msg = f"tombstone missing required key field '{field}'"
            raise InvalidKeyError(msg)
        _ = _validate_key_field_value(obj[field], field)


def extract_key(record: "JSONObject", key_specifier: KeySpecifier) -> Key:
    """Extract a key from a record using the given key specifier.

    Per the specification's "extract a key" algorithm:
    - For a string key specifier, extract the single field value
    - For a tuple key specifier, extract each field and return as a tuple
    - Single-element tuple key specifiers return a scalar key (not a tuple)

    Args:
        record: The record to extract the key from.
        key_specifier: The key specifier defining which fields form the key.

    Returns:
        The extracted key (string, int, or tuple).

    Raises:
        InvalidKeyError: If a required key field is missing or has an invalid value.
    """
    if isinstance(key_specifier, str):
        # Single field key specifier
        if key_specifier not in record:
            msg = f"record missing required key field '{key_specifier}'"
            raise InvalidKeyError(msg)
        return _validate_key_field_value(record[key_specifier], key_specifier)

    # Tuple key specifier
    if len(key_specifier) == 0:
        msg = "key specifier cannot be empty"
        raise InvalidKeyError(msg)

    elements: list[str | int] = []
    for field in key_specifier:
        if field not in record:
            msg = f"record missing required key field '{field}'"
            raise InvalidKeyError(msg)
        value = _validate_key_field_value(record[field], field)
        elements.append(value)

    # Single-element tuple key specifiers return a scalar key
    if len(elements) == 1:
        return elements[0]

    return tuple(elements)


def build_tombstone(key: Key, key_specifier: KeySpecifier) -> "JSONObject":
    """Build a tombstone object for the given key.

    A tombstone is a delete marker containing `$deleted: true` and the
    key field(s) to identify the record being deleted.

    Args:
        key: The key identifying the record to delete.
        key_specifier: The key specifier defining key field names.

    Returns:
        A tombstone JSONObject with $deleted: true and key fields.

    Raises:
        InvalidKeyError: If key arity doesn't match specifier.
    """
    validate_key_arity(key, key_specifier)

    tombstone: dict[str, str | int | bool] = {"$deleted": True}

    if isinstance(key_specifier, str):
        # Scalar key specifier - key is scalar
        tombstone[key_specifier] = key  # pyright: ignore[reportArgumentType]
    else:
        # Tuple key specifier - key is tuple of same length
        key_tuple: tuple[str | int, ...] = key  # pyright: ignore[reportAssignmentType]
        tombstone.update(dict(zip(key_specifier, key_tuple, strict=True)))

    return tombstone  # pyright: ignore[reportReturnType]


def record_size(record: "JSONObject") -> int:
    """Compute the serialized byte size of a record.

    The record size is the number of bytes in the record's JSON serialization
    using deterministic serialization, encoded as UTF-8.

    Args:
        record: The record to measure.

    Returns:
        The size in bytes of the deterministically serialized record.
    """
    serialized = serialize_json(record)
    return utf8_byte_length(serialized)
