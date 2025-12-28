"""Memory profiling benchmarks for JSONLT.

This module contains memory usage benchmarks using pytest-memray to ensure
memory consumption stays within expected bounds.
"""

import sys
from typing import TYPE_CHECKING

import pytest

from jsonlt import Table

from ._generators import generate_records, get_key_specifier, write_table_file

if TYPE_CHECKING:
    from pathlib import Path

    from jsonlt._json import JSONObject

# Skip entire module on Windows (memray not available)
pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="pytest-memray not available on Windows",
)


class TestMemoryLoad:
    """Memory benchmarks for loading tables."""

    @pytest.mark.limit_memory("10 MB")
    def test_load_1k_small_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("50 MB")
    @pytest.mark.slow
    def test_load_10k_small_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 10000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("500 MB")
    @pytest.mark.slow
    def test_load_100k_small_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 100000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("20 MB")
    def test_load_1k_medium_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "medium", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("100 MB")
    @pytest.mark.slow
    def test_load_10k_medium_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "medium", 10000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("100 MB")
    @pytest.mark.slow
    def test_load_1k_large_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "large", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)


class TestMemoryLoadKeyTypes:
    """Memory benchmarks for loading with different key types."""

    @pytest.mark.limit_memory("10 MB")
    def test_load_1k_integer_keys(self, tmp_path: "Path") -> None:
        records = generate_records("integer", "small", 1000)
        key_spec = get_key_specifier("integer")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("10 MB")
    def test_load_1k_tuple_keys(self, tmp_path: "Path") -> None:
        records = generate_records("tuple", "small", 1000)
        key_spec = get_key_specifier("tuple")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("50 MB")
    @pytest.mark.slow
    def test_load_10k_integer_keys(self, tmp_path: "Path") -> None:
        records = generate_records("integer", "small", 10000)
        key_spec = get_key_specifier("integer")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)

    @pytest.mark.limit_memory("50 MB")
    @pytest.mark.slow
    def test_load_10k_tuple_keys(self, tmp_path: "Path") -> None:
        records = generate_records("tuple", "small", 10000)
        key_spec = get_key_specifier("tuple")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        _ = Table(file_path, key=key_spec, auto_reload=False)


class TestMemoryRead:
    """Memory benchmarks for read operations."""

    @pytest.mark.limit_memory("15 MB")
    def test_all_1k_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        _ = table.all()

    @pytest.mark.limit_memory("75 MB")
    @pytest.mark.slow
    def test_all_10k_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 10000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        _ = table.all()

    @pytest.mark.limit_memory("15 MB")
    def test_find_1k_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        _ = table.find(lambda r: r.get("active") is True)

    @pytest.mark.limit_memory("15 MB")
    def test_keys_1k_records(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        _ = table.keys()


class TestMemoryWrite:
    """Memory benchmarks for write operations."""

    @pytest.mark.limit_memory("15 MB")
    def test_put_to_1k_table(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        new_record: JSONObject = {
            "id": "new_key",
            "name": "New Record",
            "active": True,
            "count": 1,
            "score": 1.0,
        }
        table.put(new_record)

    @pytest.mark.limit_memory("15 MB")
    def test_delete_from_1k_table(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        _ = table.delete("key_00000000")

    @pytest.mark.limit_memory("20 MB")
    def test_compact_1k_table(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 1000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        # Add some updates to create history
        for i in range(100):
            updated: JSONObject = {
                "id": f"key_{i:08d}",
                "name": f"Updated {i}",
                "active": True,
                "count": i,
                "score": float(i),
            }
            table.put(updated)
        table.compact()

    @pytest.mark.limit_memory("100 MB")
    @pytest.mark.slow
    def test_compact_10k_table(self, tmp_path: "Path") -> None:
        records = generate_records("string", "small", 10000)
        key_spec = get_key_specifier("string")
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        table = Table(file_path, key=key_spec, auto_reload=False)
        # Add some updates to create history
        for i in range(1000):
            updated: JSONObject = {
                "id": f"key_{i:08d}",
                "name": f"Updated {i}",
                "active": True,
                "count": i,
                "score": float(i),
            }
            table.put(updated)
        table.compact()
