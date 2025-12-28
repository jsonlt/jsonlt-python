"""Constants defining implementation limits and protocol version.

These constants define the minimum required limits per the JSONLT specification.
Implementations may support larger values.
"""

from typing import Final

# JSONLT specification version
JSONLT_VERSION: Final[int] = 1
"""The JSONLT specification version implemented."""

# Minimum required limits per specification
MAX_KEY_LENGTH: Final[int] = 1024
"""Maximum supported key length in bytes.

The key length is the number of bytes in the key's JSON representation
when encoded as UTF-8. For example, "alice" is 7 bytes (including quotes).
"""

MAX_RECORD_SIZE: Final[int] = 1_048_576
"""Maximum supported record size in bytes (1 MiB).

The record size is the number of bytes in the record's JSON serialization
using deterministic serialization, encoded as UTF-8.
"""

MIN_NESTING_DEPTH: Final[int] = 64
"""Minimum supported JSON nesting depth.

Nesting depth is the maximum number of nested JSON objects and arrays
at any point within a value, where the outermost value is at depth 1.
"""

MAX_TUPLE_ELEMENTS: Final[int] = 16
"""Maximum number of elements in a tuple key.

Tuple keys may contain at most 16 elements. Key specifiers with more
than 16 field names are invalid.
"""

# Valid integer key range (IEEE 754 double-precision safe integers)
MAX_INTEGER_KEY: Final[int] = 2**53 - 1
"""Maximum valid integer key value (9007199254740991).

This is the maximum integer that IEEE 754 double-precision floating-point
can represent exactly, ensuring interoperability across languages.
"""

MIN_INTEGER_KEY: Final[int] = -(2**53) + 1
"""Minimum valid integer key value (-9007199254740991).

This is the minimum integer that IEEE 754 double-precision floating-point
can represent exactly, ensuring interoperability across languages.
"""
