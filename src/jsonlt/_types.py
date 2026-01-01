"""Type aliases for JSONLT.

This module contains ONLY TypeAlias definitions for JSON and key types.
It exists to break circular imports between _exceptions.py, _json.py, and _keys.py:
- _exceptions.py needs JSONObject and Key for ConflictError
- _json.py needs ParseError, LimitError from _exceptions.py
- _keys.py needs InvalidKeyError, LimitError from _exceptions.py

By placing the type aliases here with no dependencies on other JSONLT modules,
all modules can safely import from _types.py.
"""

from typing import TypeAlias

# JSON type definitions per RFC 8259
# Using string annotations for forward references to avoid runtime | issues
JSONPrimitive: TypeAlias = "str | int | float | bool | None"
"""A JSON primitive value: string, number, boolean, or null."""

JSONArray: TypeAlias = "list[JSONValue]"
"""A JSON array containing any JSON values."""

JSONObject: TypeAlias = "dict[str, JSONValue]"
"""A JSON object mapping string keys to JSON values."""

JSONValue: TypeAlias = "JSONPrimitive | JSONArray | JSONObject"
"""Any JSON value: primitive, array, or object."""

# Key type definitions per JSONLT specification
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
