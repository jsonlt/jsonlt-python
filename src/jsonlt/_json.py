"""JSON parsing and serialization for JSONLT.

This module provides functions for parsing and serializing JSON with
JSONLT-specific requirements:
- Duplicate key detection (ParseError)
- Nesting depth enforcement (LimitError at depth > 64)
- Deterministic serialization (sorted keys, no whitespace, ensure_ascii=False)
"""

import json
from json import JSONDecodeError
from typing import TYPE_CHECKING, cast

from ._constants import MIN_NESTING_DEPTH
from ._exceptions import LimitError, ParseError
from ._types import JSONArray, JSONObject, JSONPrimitive, JSONValue

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Re-export type aliases for backwards compatibility
__all__ = ["JSONArray", "JSONObject", "JSONPrimitive", "JSONValue"]


def json_nesting_depth(value: object) -> int:
    """Compute the nesting depth of a JSON value.

    Per specification: "The nesting depth of a JSON value is the maximum
    number of nested JSON objects and arrays at any point within that value,
    where the outermost value is at depth 1."

    - A primitive value (null, boolean, number, or string) has nesting depth 1.
    - An empty object or array has nesting depth 1.
    - An object or array containing only primitive values has nesting depth 2.

    Args:
        value: A JSON-compatible value (dict, list, str, int, float, bool, None).

    Returns:
        The nesting depth of the value.
    """
    if isinstance(value, dict):
        if not value:
            return 1
        # Cast to JSONObject after isinstance check
        obj = cast("JSONObject", value)
        return 1 + max(json_nesting_depth(v) for v in obj.values())
    if isinstance(value, list):
        if not value:
            return 1
        # Cast to JSONArray after isinstance check
        arr = cast("JSONArray", value)
        return 1 + max(json_nesting_depth(item) for item in arr)
    # Primitives: str, int, float, bool, None
    return 1


class _DuplicateKeyDetector(dict[str, JSONValue]):
    """A dict subclass that detects duplicate keys during JSON parsing.

    Used as object_pairs_hook in json.loads to detect duplicate keys,
    which are prohibited by the JSONLT specification.
    """

    def __init__(self, pairs: "Sequence[tuple[str, JSONValue]]") -> None:
        """Initialize from key-value pairs, checking for duplicates.

        Args:
            pairs: List of (key, value) pairs from JSON parsing.

        Raises:
            ParseError: If duplicate keys are detected.
        """
        super().__init__()
        for key, value in pairs:
            if key in self:
                msg = f"duplicate key: {key!r}"
                raise ParseError(msg)
            self[key] = value


def parse_json_line(
    line: str,
    *,
    max_depth: int = MIN_NESTING_DEPTH,
) -> JSONObject:
    """Parse a single JSON line with JSONLT-specific validation.

    This function parses JSON with additional checks required by JSONLT:
    - Duplicate key detection (raises ParseError)
    - Nesting depth enforcement (raises LimitError if depth > max_depth)

    Args:
        line: A single line of JSON text to parse.
        max_depth: Maximum allowed nesting depth (default: 64 per spec).

    Returns:
        The parsed JSON object as a dict.

    Raises:
        ParseError: If the line contains invalid JSON, is not a JSON object,
            or contains duplicate keys.
        LimitError: If the JSON nesting depth exceeds max_depth.
    """
    try:
        result: JSONValue = cast(
            "JSONValue", json.loads(line, object_pairs_hook=_DuplicateKeyDetector)
        )
    except JSONDecodeError as e:
        msg = f"invalid JSON: {e.msg}"
        raise ParseError(msg) from e
    except RecursionError:
        # Deeply nested JSON exhausted the Python call stack during parsing.
        # Convert to LimitError since this represents excessive nesting depth.
        msg = f"nesting depth exceeds maximum {max_depth}"
        raise LimitError(msg) from None
    except ParseError:
        # Re-raise ParseError from duplicate key detection
        raise

    if not isinstance(result, dict):
        msg = f"expected JSON object, got {type(result).__name__}"
        raise ParseError(msg)

    # Check nesting depth
    try:
        depth = json_nesting_depth(result)
    except RecursionError:
        # Deeply nested JSON exhausted the Python call stack during depth check.
        msg = f"nesting depth exceeds maximum {max_depth}"
        raise LimitError(msg) from None
    if depth > max_depth:
        msg = f"nesting depth {depth} exceeds maximum {max_depth}"
        raise LimitError(msg)

    return result


def _sort_keys_recursive(value: JSONValue) -> JSONValue:
    """Recursively sort dictionary keys for deterministic serialization.

    Args:
        value: A JSON-compatible value.

    Returns:
        The value with all dictionary keys sorted.
    """
    if isinstance(value, dict):
        return {k: _sort_keys_recursive(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_sort_keys_recursive(item) for item in value]
    return value


def serialize_json(value: "Mapping[str, object]") -> str:
    """Serialize a JSON object using deterministic serialization.

    Per specification: "Deterministic serialization is a JSON serialization
    that produces consistent output for identical logical data."

    - Keys are sorted lexicographically by Unicode code point, recursively
    - No whitespace except within string values
    - ensure_ascii=False (SHOULD NOT escape characters that don't require it)

    Args:
        value: A JSON object (Mapping) to serialize.

    Returns:
        The JSON string with sorted keys and no extraneous whitespace.
    """
    # Convert Mapping to JSONObject and sort recursively
    dict_value: JSONObject = cast("JSONObject", dict(value))
    sorted_value = _sort_keys_recursive(dict_value)
    return json.dumps(
        sorted_value,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def utf8_byte_length(s: str) -> int:
    """Compute the UTF-8 byte length of a string.

    Uses a fast path for ASCII-only strings where len(s) == UTF-8 byte length.
    Falls back to encoding for strings containing non-ASCII characters.

    Args:
        s: The string to measure.

    Returns:
        The number of bytes when encoded as UTF-8.
    """
    if s.isascii():
        return len(s)
    return len(s.encode("utf-8"))
