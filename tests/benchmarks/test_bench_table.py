from typing import TYPE_CHECKING, Literal

import pytest

from jsonlt import Table

from ._generators import (
    create_extended_test_table,
    create_table_with_history,
    create_table_with_tombstones,
    create_test_table,
    generate_key,
    generate_record,
    generate_records,
    get_key_specifier,
    write_table_file,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_codspeed.plugin import BenchmarkFixture

    from jsonlt._json import JSONObject


# Type aliases for parametrization
RecordSize = Literal["small", "medium", "large"]
KeyType = Literal["string", "integer", "tuple"]

# Scale and size parameters for CI (fast benchmarks)
CI_PARAMS: list[object] = [
    pytest.param("small", "string", 100, id="small-str-100"),
    pytest.param("small", "string", 1000, id="small-str-1k"),
    pytest.param("small", "integer", 100, id="small-int-100"),
    pytest.param("small", "integer", 1000, id="small-int-1k"),
    pytest.param("small", "tuple", 100, id="small-tuple-100"),
    pytest.param("small", "tuple", 1000, id="small-tuple-1k"),
]

# Larger scale parameters (marked slow)
SLOW_PARAMS: list[object] = [
    pytest.param("small", "string", 10000, id="small-str-10k", marks=pytest.mark.slow),
    pytest.param(
        "small", "string", 100000, id="small-str-100k", marks=pytest.mark.slow
    ),
    pytest.param("medium", "string", 1000, id="med-str-1k", marks=pytest.mark.slow),
    pytest.param("medium", "string", 10000, id="med-str-10k", marks=pytest.mark.slow),
    pytest.param("large", "string", 1000, id="large-str-1k", marks=pytest.mark.slow),
    pytest.param("small", "integer", 10000, id="small-int-10k", marks=pytest.mark.slow),
    pytest.param(
        "small", "integer", 100000, id="small-int-100k", marks=pytest.mark.slow
    ),
    pytest.param("small", "tuple", 10000, id="small-tuple-10k", marks=pytest.mark.slow),
    pytest.param(
        "small", "tuple", 100000, id="small-tuple-100k", marks=pytest.mark.slow
    ),
    pytest.param("medium", "integer", 1000, id="med-int-1k", marks=pytest.mark.slow),
    pytest.param("medium", "tuple", 1000, id="med-tuple-1k", marks=pytest.mark.slow),
    pytest.param("large", "integer", 1000, id="large-int-1k", marks=pytest.mark.slow),
    pytest.param("large", "tuple", 1000, id="large-tuple-1k", marks=pytest.mark.slow),
]

# Edge case parameters for boundary testing
EDGE_PARAMS: list[object] = [
    pytest.param("small", "string", 0, id="small-str-0"),
    pytest.param("small", "string", 1, id="small-str-1"),
    pytest.param("small", "integer", 0, id="small-int-0"),
    pytest.param("small", "integer", 1, id="small-int-1"),
    pytest.param("small", "tuple", 0, id="small-tuple-0"),
    pytest.param("small", "tuple", 1, id="small-tuple-1"),
]

ALL_PARAMS: list[object] = CI_PARAMS + SLOW_PARAMS
ALL_WITH_EDGE_PARAMS: list[object] = ALL_PARAMS + EDGE_PARAMS

# Buffer size for delete benchmarks to ensure unique keys per iteration
DELETE_ITERATION_BUFFER: int = 10000


class TestBenchLoad:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_PARAMS)
    def test_load(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        records = generate_records(key_type, record_size, scale)
        key_spec = get_key_specifier(key_type)
        file_path = tmp_path / "bench.jsonlt"
        write_table_file(file_path, records, key_spec)

        def load_table() -> None:
            _ = Table(file_path, key=key_spec, auto_reload=False)

        benchmark(load_table)


class TestBenchReload:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_PARAMS)
    def test_reload(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def reload_table() -> None:
            table.reload()

        benchmark(reload_table)


class TestBenchGet:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_PARAMS)
    def test_get_existing_key(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Get key from middle of dataset
        middle_index = scale // 2
        target_key = generate_key(key_type, middle_index)

        def get_record() -> None:
            _ = table.get(target_key)

        benchmark(get_record)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_get_nonexistent_key(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Generate a key that doesn't exist
        nonexistent_key = generate_key(key_type, scale + 1000)

        def get_missing() -> None:
            _ = table.get(nonexistent_key)

        benchmark(get_missing)


class TestBenchAll:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_WITH_EDGE_PARAMS)
    def test_all(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def get_all() -> None:
            # Invalidate cache to measure full sort
            table._cached_sorted_keys = None  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
            _ = table.all()

        benchmark(get_all)


class TestBenchFind:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_PARAMS)
    def test_find_high_selectivity(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # High selectivity: match ~10% of records (count > 9000)
        def predicate_high_count(r: "JSONObject") -> bool:
            count = r.get("count", 0)
            return isinstance(count, int) and count > 9000

        def find_high_count() -> None:
            _ = table.find(predicate_high_count)

        benchmark(find_high_count)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_find_low_selectivity(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Low selectivity: match ~90% of records (count < 9000)
        def predicate_low_count(r: "JSONObject") -> bool:
            count = r.get("count", 0)
            return isinstance(count, int) and count < 9000

        def find_low_count() -> None:
            _ = table.find(predicate_low_count)

        benchmark(find_low_count)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_find_very_high_selectivity(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Very high selectivity: match ~1% of records (count > 9900)
        def predicate_very_selective(r: "JSONObject") -> bool:
            count = r.get("count", 0)
            return isinstance(count, int) and count > 9900

        def find_very_selective() -> None:
            _ = table.find(predicate_very_selective)

        benchmark(find_very_selective)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_find_all_records(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def predicate_all(_r: "JSONObject") -> bool:
            return True

        def find_all() -> None:
            _ = table.find(predicate_all)

        benchmark(find_all)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_find_with_limit(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Find with limit - should short-circuit early
        def predicate_active(r: "JSONObject") -> bool:
            return r.get("active") is True

        def find_limited() -> None:
            _ = table.find(predicate_active, limit=10)

        benchmark(find_limited)


class TestBenchFindOne:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_find_one_match_early(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def predicate_any(_r: "JSONObject") -> bool:
            return True

        def find_first() -> None:
            _ = table.find_one(predicate_any)

        benchmark(find_first)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_find_one_match_late(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Match only high count values (~1% of records)
        def predicate_high_count(r: "JSONObject") -> bool:
            count = r.get("count", 0)
            return isinstance(count, int) and count > 9900

        def find_late() -> None:
            _ = table.find_one(predicate_high_count)

        benchmark(find_late)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_find_one_no_match(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def predicate_never(_r: "JSONObject") -> bool:
            return False

        def find_none() -> None:
            _ = table.find_one(predicate_never)

        benchmark(find_none)


class TestBenchPut:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_put_new_record(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Generate new records to put (beyond existing range)
        new_record_index = scale + 1
        counter = [new_record_index]

        def put_new() -> None:
            new_record = generate_record(key_type, record_size, counter[0], seed=42)
            table.put(new_record)
            counter[0] += 1

        benchmark(put_new)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_put_update_record(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        # Update existing records (cycling through them)
        counter = [0]

        def put_update() -> None:
            index = counter[0] % scale
            updated_record = generate_record(key_type, record_size, index, seed=99)
            table.put(updated_record)
            counter[0] += 1

        benchmark(put_update)


class TestBenchBatchWrite:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_batch_put_10(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        batch_size = 10
        counter = [scale + 1]

        def batch_put() -> None:
            start = counter[0]
            for i in range(batch_size):
                new_record = generate_record(key_type, record_size, start + i, seed=42)
                table.put(new_record)
            counter[0] += batch_size

        benchmark(batch_put)

    @pytest.mark.parametrize(
        ("record_size", "key_type", "scale"),
        [
            pytest.param("small", "string", 100, id="small-str-100"),
            pytest.param("small", "integer", 100, id="small-int-100"),
        ],
    )
    def test_batch_put_100(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        batch_size = 100
        counter = [scale + 1]

        def batch_put() -> None:
            start = counter[0]
            for i in range(batch_size):
                new_record = generate_record(key_type, record_size, start + i, seed=42)
                table.put(new_record)
            counter[0] += batch_size

        benchmark(batch_put)


class TestBenchCompact:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_compact_with_history(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        history_count = max(scale // 10, 1)
        table = create_table_with_history(
            tmp_path, key_type, record_size, scale, history_count
        )

        def compact_only() -> None:
            table.compact()

        benchmark(compact_only)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_compact_with_tombstones(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        tombstone_count = max(scale // 10, 1)
        table = create_table_with_tombstones(
            tmp_path, key_type, record_size, scale, tombstone_count
        )

        def compact_only() -> None:
            table.compact()

        benchmark(compact_only)


class TestBenchKeys:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_WITH_EDGE_PARAMS)
    def test_keys(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def get_keys() -> None:
            # Invalidate cache to measure full sort
            table._cached_sorted_keys = None  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
            _ = table.keys()

        benchmark(get_keys)


class TestBenchItems:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_WITH_EDGE_PARAMS)
    def test_items(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def get_items() -> None:
            # Invalidate cache to measure full sort
            table._cached_sorted_keys = None  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
            _ = table.items()

        benchmark(get_items)


class TestBenchCount:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_WITH_EDGE_PARAMS)
    def test_count(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        def count() -> None:
            _ = table.count()

        benchmark(count)


class TestBenchHas:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), ALL_WITH_EDGE_PARAMS)
    def test_has_existing(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        if scale == 0:
            pytest.skip("Cannot test has_existing with scale=0 (no records)")
        table = create_test_table(tmp_path, key_type, record_size, scale)

        middle_key = generate_key(key_type, scale // 2)

        def has_key() -> None:
            _ = table.has(middle_key)

        benchmark(has_key)

    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_has_nonexistent(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        table = create_test_table(tmp_path, key_type, record_size, scale)

        missing_key = generate_key(key_type, scale + 1000)

        def has_missing() -> None:
            _ = table.has(missing_key)

        benchmark(has_missing)


class TestBenchDelete:
    @pytest.mark.parametrize(("record_size", "key_type", "scale"), CI_PARAMS)
    def test_delete_existing(
        self,
        benchmark: "BenchmarkFixture",
        tmp_path: "Path",
        record_size: RecordSize,
        key_type: KeyType,
        scale: int,
    ) -> None:
        # Create table with extra keys for benchmark iterations
        table = create_extended_test_table(
            tmp_path, key_type, record_size, scale, DELETE_ITERATION_BUFFER
        )

        # Counter starts at base scale (first extra key)
        counter = [scale]

        def delete_unique() -> None:
            key = generate_key(key_type, counter[0])
            _ = table.delete(key)
            counter[0] += 1

        benchmark(delete_unique)
