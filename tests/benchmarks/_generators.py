"""Benchmark data generators for JSONLT benchmarks.

This module provides deterministic data generation functions for benchmark
tests. All generators use seeded random instances for reproducibility.
"""

import random
from typing import TYPE_CHECKING, Literal

from jsonlt import Table
from jsonlt._header import Header, serialize_header
from jsonlt._json import JSONObject, serialize_json

if TYPE_CHECKING:
    from pathlib import Path

    from jsonlt._keys import Key, KeySpecifier


def generate_key(
    key_type: Literal["string", "integer", "tuple"],
    index: int,
) -> "Key":
    """Generate a deterministic key for a given index.

    Args:
        key_type: The type of key to generate.
        index: The index used to generate the key value.

    Returns:
        A key of the specified type.
    """
    if key_type == "string":
        return f"key_{index:08d}"
    if key_type == "integer":
        return index
    # tuple: distribute across 10 orgs
    org_index = index % 10
    return (f"org_{org_index}", index)


def get_key_specifier(
    key_type: Literal["string", "integer", "tuple"],
) -> "KeySpecifier":
    """Get the key specifier for a given key type.

    Args:
        key_type: The type of key.

    Returns:
        The corresponding key specifier.
    """
    if key_type == "tuple":
        return ("org", "seq")
    return "id"


def _generate_small_record(
    key_type: Literal["string", "integer", "tuple"],
    index: int,
    rng: random.Random,
) -> JSONObject:
    """Generate a small record (~5 fields).

    Args:
        key_type: The type of key to use.
        index: The index for deterministic key generation.
        rng: Random instance for deterministic value generation.

    Returns:
        A JSONObject with ~5 fields.
    """
    record: JSONObject = {}

    # Add key field(s)
    if key_type == "tuple":
        org_index = index % 10
        record["org"] = f"org_{org_index}"
        record["seq"] = index
    elif key_type == "integer":
        record["id"] = index
    else:
        record["id"] = f"key_{index:08d}"

    # Add additional fields
    record["name"] = f"Record {index}"
    record["active"] = rng.choice([True, False])
    record["count"] = rng.randint(0, 10000)
    record["score"] = round(rng.uniform(0.0, 100.0), 2)

    return record


def _generate_medium_record(
    key_type: Literal["string", "integer", "tuple"],
    index: int,
    rng: random.Random,
) -> JSONObject:
    """Generate a medium record (~20 fields).

    Args:
        key_type: The type of key to use.
        index: The index for deterministic key generation.
        rng: Random instance for deterministic value generation.

    Returns:
        A JSONObject with ~20 fields.
    """
    record = _generate_small_record(key_type, index, rng)

    # Add more fields to reach ~20 total
    record["description"] = f"This is a detailed description for record {index}."
    record["tags"] = [f"tag_{rng.randint(1, 100)}" for _ in range(5)]
    record["address"] = {
        "street": f"{rng.randint(1, 9999)} Main St",
        "city": rng.choice(
            ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
        ),
        "state": rng.choice(["NY", "CA", "IL", "TX", "AZ"]),
        "zip": f"{rng.randint(10000, 99999)}",
    }
    record["created_at"] = f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
    record["updated_at"] = f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
    record["priority"] = rng.randint(1, 5)
    record["category"] = rng.choice(["A", "B", "C", "D", "E"])
    record["status"] = rng.choice(["pending", "active", "completed", "archived"])
    record["version"] = rng.randint(1, 100)
    record["weight"] = round(rng.uniform(0.1, 100.0), 3)
    record["rating"] = round(rng.uniform(1.0, 5.0), 1)
    record["views"] = rng.randint(0, 1000000)
    record["likes"] = rng.randint(0, 100000)

    return record


def _generate_large_record(
    key_type: Literal["string", "integer", "tuple"],
    index: int,
    rng: random.Random,
) -> JSONObject:
    """Generate a large record (~100 fields with 1KB+ text blobs).

    Args:
        key_type: The type of key to use.
        index: The index for deterministic key generation.
        rng: Random instance for deterministic value generation.

    Returns:
        A JSONObject with ~100 fields including large text blobs.
    """
    record = _generate_medium_record(key_type, index, rng)

    # Generate large text blobs (1KB+ each)
    words = [
        "lorem",
        "ipsum",
        "dolor",
        "sit",
        "amet",
        "consectetur",
        "adipiscing",
        "elit",
        "sed",
        "do",
        "eiusmod",
        "tempor",
        "incididunt",
        "ut",
        "labore",
        "et",
        "dolore",
        "magna",
        "aliqua",
        "enim",
    ]

    def generate_blob(min_chars: int) -> str:
        result: list[str] = []
        current_len = 0
        while current_len < min_chars:
            word = rng.choice(words)
            result.append(word)
            current_len += len(word) + 1
        return " ".join(result)

    # Add large text blobs (these make up most of the record size)
    record["long_description"] = generate_blob(1024)
    record["notes"] = generate_blob(1024)
    record["content"] = generate_blob(2048)

    # Add many additional fields to reach ~100 total
    for i in range(80):
        field_name = f"field_{i:02d}"
        field_type = i % 5
        if field_type == 0:
            record[field_name] = f"value_{rng.randint(1, 10000)}"
        elif field_type == 1:
            record[field_name] = rng.randint(0, 1000000)
        elif field_type == 2:
            record[field_name] = round(rng.uniform(0.0, 1000.0), 4)
        elif field_type == 3:
            record[field_name] = rng.choice([True, False])
        else:
            record[field_name] = [rng.randint(1, 100) for _ in range(3)]

    return record


def generate_record(
    key_type: Literal["string", "integer", "tuple"],
    size: Literal["small", "medium", "large"],
    index: int,
    *,
    seed: int = 42,
) -> JSONObject:
    """Generate a single deterministic record.

    Args:
        key_type: The type of key to use ("string", "integer", or "tuple").
        size: The size of record to generate ("small", "medium", or "large").
        index: The index for deterministic generation.
        seed: Random seed for reproducibility.

    Returns:
        A JSONObject of the specified size and key type.
    """
    rng = random.Random(seed + index)  # noqa: S311

    if size == "small":
        return _generate_small_record(key_type, index, rng)
    if size == "medium":
        return _generate_medium_record(key_type, index, rng)
    return _generate_large_record(key_type, index, rng)


def generate_records(
    key_type: Literal["string", "integer", "tuple"],
    size: Literal["small", "medium", "large"],
    count: int,
    *,
    seed: int = 42,
) -> list[JSONObject]:
    """Generate a list of deterministic records.

    Args:
        key_type: The type of key to use ("string", "integer", or "tuple").
        size: The size of records to generate ("small", "medium", or "large").
        count: Number of records to generate.
        seed: Random seed for reproducibility.

    Returns:
        A list of JSONObjects of the specified size and key type.
    """
    return [generate_record(key_type, size, i, seed=seed) for i in range(count)]


def write_table_file(
    path: "Path",
    records: list[JSONObject],
    key_specifier: "KeySpecifier",
) -> None:
    """Write records to a JSONLT file.

    This creates a new file with a header and all records serialized.

    Args:
        path: Path to write the file to.
        records: List of records to write.
        key_specifier: The key specifier for the table.
    """
    lines: list[str] = []

    # Add header
    header = Header(version=1, key=key_specifier)
    lines.append(serialize_header(header))

    # Add records
    lines.extend(serialize_json(record) for record in records)

    # Write to file
    content = "\n".join(lines) + "\n"
    _ = path.write_text(content, encoding="utf-8")


def create_test_table(
    tmp_path: "Path",
    key_type: Literal["string", "integer", "tuple"],
    record_size: Literal["small", "medium", "large"],
    scale: int,
    *,
    auto_reload: bool = False,
) -> Table:
    """Create a test table with generated records.

    Args:
        tmp_path: pytest tmp_path fixture.
        key_type: Type of keys to generate.
        record_size: Size of records to generate.
        scale: Number of records to generate.
        auto_reload: Whether to enable auto-reload on the table.

    Returns:
        A Table instance with the generated records.
    """
    records = generate_records(key_type, record_size, scale)
    key_spec = get_key_specifier(key_type)
    file_path = tmp_path / "bench.jsonlt"
    write_table_file(file_path, records, key_spec)
    return Table(file_path, key=key_spec, auto_reload=auto_reload)


def add_history_to_table(
    table: Table,
    key_type: Literal["string", "integer", "tuple"],
    record_size: Literal["small", "medium", "large"],
    count: int,
) -> None:
    """Add update history to a table.

    Args:
        table: The table to add history to.
        key_type: Type of keys.
        record_size: Size of records.
        count: Number of updates to add.
    """
    for i in range(count):
        updated_record = generate_record(key_type, record_size, i, seed=99)
        table.put(updated_record)


def create_extended_test_table(  # noqa: PLR0913
    tmp_path: "Path",
    key_type: Literal["string", "integer", "tuple"],
    record_size: Literal["small", "medium", "large"],
    base_scale: int,
    extra_keys: int,
    *,
    auto_reload: bool = False,
) -> Table:
    """Create a test table with additional keys beyond base scale.

    The table contains keys from index 0 to (base_scale + extra_keys - 1).
    Use this for delete benchmarks where each iteration needs a unique key.

    Args:
        tmp_path: pytest tmp_path fixture.
        key_type: Type of keys to generate.
        record_size: Size of records to generate.
        base_scale: Base number of records.
        extra_keys: Additional keys for benchmark iterations.
        auto_reload: Whether to enable auto-reload on the table.

    Returns:
        A Table instance with the generated records.
    """
    total_count = base_scale + extra_keys
    records = generate_records(key_type, record_size, total_count)
    key_spec = get_key_specifier(key_type)
    file_path = tmp_path / "bench.jsonlt"
    write_table_file(file_path, records, key_spec)
    return Table(file_path, key=key_spec, auto_reload=auto_reload)


def create_table_with_history(  # noqa: PLR0913
    tmp_path: "Path",
    key_type: Literal["string", "integer", "tuple"],
    record_size: Literal["small", "medium", "large"],
    scale: int,
    history_count: int,
    *,
    auto_reload: bool = False,
) -> Table:
    """Create a table pre-populated with update history.

    Creates a table with `scale` records, then applies `history_count`
    updates to existing records. Useful for compact benchmarks.

    Args:
        tmp_path: pytest tmp_path fixture.
        key_type: Type of keys to generate.
        record_size: Size of records to generate.
        scale: Number of records to generate.
        history_count: Number of updates to apply.
        auto_reload: Whether to enable auto-reload on the table.

    Returns:
        A Table instance with the generated records and history.
    """
    table = create_test_table(
        tmp_path, key_type, record_size, scale, auto_reload=auto_reload
    )
    add_history_to_table(table, key_type, record_size, history_count)
    return table


def create_table_with_tombstones(  # noqa: PLR0913
    tmp_path: "Path",
    key_type: Literal["string", "integer", "tuple"],
    record_size: Literal["small", "medium", "large"],
    scale: int,
    tombstone_count: int,
    *,
    auto_reload: bool = False,
) -> Table:
    """Create a table pre-populated with tombstones.

    Creates a table with `scale` records, then deletes `tombstone_count`
    records starting from index 0. Useful for compact benchmarks.

    Args:
        tmp_path: pytest tmp_path fixture.
        key_type: Type of keys to generate.
        record_size: Size of records to generate.
        scale: Number of records to generate.
        tombstone_count: Number of records to delete.
        auto_reload: Whether to enable auto-reload on the table.

    Returns:
        A Table instance with tombstones.
    """
    table = create_test_table(
        tmp_path, key_type, record_size, scale, auto_reload=auto_reload
    )
    for i in range(tombstone_count):
        key = generate_key(key_type, i)
        _ = table.delete(key)
    return table
