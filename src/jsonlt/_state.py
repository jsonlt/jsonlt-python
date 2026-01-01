"""Logical state computation for JSONLT.

This module provides functions for computing the logical state of a table
by replaying a sequence of operations (upserts and deletes).
"""

from typing import TYPE_CHECKING

from ._records import extract_key, is_tombstone

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ._json import JSONObject
    from ._keys import Key, KeySpecifier


def compute_logical_state(
    operations: "Sequence[JSONObject]",
    key_specifier: "KeySpecifier",
) -> dict["Key", "JSONObject"]:
    """Compute the logical state by replaying operations.

    This function processes a sequence of operations in order and
    computes the resulting logical state of the table:

    - Upserts (non-tombstone records) add or update entries
    - Deletes (tombstones) remove entries

    The key for each operation is extracted using the given key specifier.

    Per specification, the logical state is the result of applying all
    operations in sequence, where later operations overwrite earlier ones
    for the same key.

    Args:
        operations: List of parsed JSON objects (records and tombstones).
        key_specifier: The key specifier defining which fields form the key.

    Returns:
        A dictionary mapping keys to their final record values.
        Deleted records are not present in the result.

    Raises:
        InvalidKeyError: If an operation is missing required key fields
            or has invalid key field values.
    """
    state: dict[Key, JSONObject] = {}

    for obj in operations:
        key = extract_key(obj, key_specifier)

        # Determine operation type and apply
        if is_tombstone(obj):
            # Delete: remove from state if present
            _ = state.pop(key, None)
        else:
            # Upsert: add or update
            state[key] = obj

    return state
