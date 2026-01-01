import time
from typing import TYPE_CHECKING

import pytest

from jsonlt import FileError, InvalidKeyError, LimitError, Table

from tests.fakes.fake_filesystem import FakeFileSystem

if TYPE_CHECKING:
    from pathlib import Path


class TestTableConstruction:
    def test_new_file_with_key_specifier(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        assert table.path == table_path
        assert table.key_specifier == "id"
        assert table.count() == 0
        assert table.header is None

    def test_new_file_without_key_specifier(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path)

        assert table.path == table_path
        assert table.key_specifier is None
        assert table.count() == 0

    def test_existing_file_with_header_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"$jsonlt": {"version": 1, "key": "id"}}\n')

        table = Table(table_path)

        assert table.key_specifier == "id"
        assert table.header is not None
        assert table.header.key == "id"
        assert table.count() == 0

    def test_existing_file_with_matching_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"$jsonlt": {"version": 1, "key": "id"}}\n')

        table = Table(table_path, key="id")

        assert table.key_specifier == "id"

    def test_existing_file_with_mismatched_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"$jsonlt": {"version": 1, "key": "id"}}\n')

        with pytest.raises(InvalidKeyError, match="key specifier mismatch"):
            _ = Table(table_path, key="name")

    def test_existing_file_with_operations_no_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1, "name": "alice"}\n')

        with pytest.raises(InvalidKeyError, match="no key specifier"):
            _ = Table(table_path)

    def test_existing_file_with_operations_and_caller_key(
        self, tmp_path: "Path"
    ) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1, "name": "alice"}\n')

        table = Table(table_path, key="id")

        assert table.key_specifier == "id"
        assert table.count() == 1

    def test_compound_key_specifier(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"org": "acme", "id": 1, "name": "alice"}\n')

        table = Table(table_path, key=("org", "id"))

        assert table.key_specifier == ("org", "id")
        assert table.count() == 1

    def test_single_element_tuple_normalized(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("id",))

        assert table.key_specifier == "id"

    def test_repr(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        assert "Table" in repr(table)
        # Check path is present (use name to avoid Windows path sep issues)
        assert table_path.name in repr(table)
        assert "'id'" in repr(table)


class TestTableGet:
    def test_get_existing_record(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')

        table = Table(table_path, key="id")

        assert table.get("alice") == {"id": "alice", "role": "admin"}

    def test_get_nonexistent_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')

        table = Table(table_path, key="id")

        assert table.get("bob") is None

    def test_get_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"

        table = Table(table_path, key="id")

        assert table.get("alice") is None

    def test_get_integer_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1, "name": "first"}\n')

        table = Table(table_path, key="id")

        assert table.get(1) == {"id": 1, "name": "first"}

    def test_get_compound_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"org": "acme", "id": 1, "name": "alice"}\n')

        table = Table(table_path, key=("org", "id"))

        assert table.get(("acme", 1)) == {"org": "acme", "id": 1, "name": "alice"}


class TestTableHas:
    def test_has_existing_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')

        table = Table(table_path, key="id")

        assert table.has("alice") is True

    def test_has_nonexistent_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')

        table = Table(table_path, key="id")

        assert table.has("bob") is False

    def test_has_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"

        table = Table(table_path, key="id")

        assert table.has("alice") is False


class TestTableAll:
    def test_all_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"

        table = Table(table_path, key="id")

        assert table.all() == []

    def test_all_single_record(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id")

        assert table.all() == [{"id": "alice", "v": 1}]

    def test_all_sorted_by_key_strings(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "bob", "v": 2}\n{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id")

        records = table.all()
        assert len(records) == 2
        assert records[0] == {"id": "alice", "v": 1}
        assert records[1] == {"id": "bob", "v": 2}

    def test_all_sorted_by_key_integers(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text(
            '{"id": 10, "v": 1}\n{"id": 2, "v": 2}\n{"id": 1, "v": 3}\n'
        )

        table = Table(table_path, key="id")

        records = table.all()
        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[1]["id"] == 2
        assert records[2]["id"] == 10

    def test_all_sorted_mixed_types(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": "b", "v": 1}\n{"id": 2, "v": 2}\n'
        content += '{"id": "a", "v": 3}\n{"id": 1, "v": 4}\n'
        _ = table_path.write_text(content)

        table = Table(table_path, key="id")

        records = table.all()
        keys = [r["id"] for r in records]
        assert keys == [1, 2, "a", "b"]


class TestTableKeys:
    def test_keys_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"

        table = Table(table_path, key="id")

        assert table.keys() == []

    def test_keys_sorted(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "bob", "v": 2}\n{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id")

        assert table.keys() == ["alice", "bob"]


class TestTableCount:
    def test_count_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"

        table = Table(table_path, key="id")

        assert table.count() == 0

    def test_count_single_record(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id")

        assert table.count() == 1

    def test_count_multiple_records(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n')

        table = Table(table_path, key="id")

        assert table.count() == 3


class TestTableFind:
    def test_find_matches(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": 1, "role": "admin"}\n'
        content += '{"id": 2, "role": "user"}\n'
        content += '{"id": 3, "role": "admin"}\n'
        _ = table_path.write_text(content)

        table = Table(table_path, key="id")

        results = table.find(lambda r: r["role"] == "admin")
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 3

    def test_find_no_matches(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1, "role": "user"}\n')

        table = Table(table_path, key="id")

        results = table.find(lambda r: r["role"] == "admin")
        assert results == []

    def test_find_with_limit(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": 1, "role": "admin"}\n'
        content += '{"id": 2, "role": "admin"}\n'
        content += '{"id": 3, "role": "admin"}\n'
        _ = table_path.write_text(content)

        table = Table(table_path, key="id")

        results = table.find(lambda r: r["role"] == "admin", limit=2)
        assert len(results) == 2

    def test_find_in_key_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text(
            '{"id": "c", "v": 1}\n{"id": "a", "v": 2}\n{"id": "b", "v": 3}\n'
        )

        table = Table(table_path, key="id")

        results = table.find(lambda _: True)
        keys = [r["id"] for r in results]
        assert keys == ["a", "b", "c"]

    def test_find_one_matches(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": 1, "role": "user"}\n'
        content += '{"id": 2, "role": "admin"}\n'
        content += '{"id": 3, "role": "admin"}\n'
        _ = table_path.write_text(content)

        table = Table(table_path, key="id")

        result = table.find_one(lambda r: r["role"] == "admin")
        assert result is not None
        assert result["id"] == 2

    def test_find_one_no_match(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1, "role": "user"}\n')

        table = Table(table_path, key="id")

        result = table.find_one(lambda r: r["role"] == "admin")
        assert result is None


class TestTableLogicalState:
    def test_upsert_overwrites(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text(
            '{"id": "alice", "role": "user"}\n{"id": "alice", "role": "admin"}\n'
        )

        table = Table(table_path, key="id")

        assert table.get("alice") == {"id": "alice", "role": "admin"}
        assert table.count() == 1

    def test_tombstone_deletes(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text(
            '{"id": "alice", "role": "admin"}\n{"id": "alice", "$deleted": true}\n'
        )

        table = Table(table_path, key="id")

        assert table.get("alice") is None
        assert table.count() == 0

    def test_upsert_after_tombstone(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": "alice", "role": "admin"}\n'
        content += '{"id": "alice", "$deleted": true}\n'
        content += '{"id": "alice", "role": "user"}\n'
        _ = table_path.write_text(content)

        table = Table(table_path, key="id")

        assert table.get("alice") == {"id": "alice", "role": "user"}


class TestTableAutoReload:
    def test_auto_reload_detects_changes(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id")
        assert table.get("alice") == {"id": "alice", "v": 1}

        # Wait a bit to ensure mtime changes
        time.sleep(0.01)
        _ = table_path.write_text('{"id": "alice", "v": 2}\n')

        assert table.get("alice") == {"id": "alice", "v": 2}

    def test_auto_reload_disabled(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id", auto_reload=False)
        assert table.get("alice") == {"id": "alice", "v": 1}

        # Wait a bit to ensure mtime changes
        time.sleep(0.01)
        _ = table_path.write_text('{"id": "alice", "v": 2}\n')

        # Should still return old value
        assert table.get("alice") == {"id": "alice", "v": 1}

    def test_auto_reload_file_deleted(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id")
        assert table.count() == 1

        table_path.unlink()

        assert table.count() == 0
        assert table.get("alice") is None

    def test_auto_reload_new_records(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')

        table = Table(table_path, key="id")
        assert table.count() == 1

        # Wait a bit to ensure mtime changes
        time.sleep(0.01)
        _ = table_path.write_text('{"id": "alice", "v": 1}\n{"id": "bob", "v": 2}\n')

        assert table.count() == 2
        assert table.has("bob") is True


class TestTablePut:
    def test_put_creates_file(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.put({"id": "alice", "name": "Alice"})

        assert table_path.exists()
        assert table.count() == 1
        assert table.get("alice") == {"id": "alice", "name": "Alice"}

    def test_put_appends_to_file(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "name": "Alice"}\n')
        table = Table(table_path, key="id")

        table.put({"id": "bob", "name": "Bob"})

        assert table.count() == 2
        assert table.get("bob") == {"id": "bob", "name": "Bob"}
        # Check file has both lines
        content = table_path.read_text()
        assert content.count("\n") == 2

    def test_put_updates_existing_record(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.put({"id": "alice", "role": "user"})
        table.put({"id": "alice", "role": "admin"})

        assert table.count() == 1
        assert table.get("alice") == {"id": "alice", "role": "admin"}

    def test_put_with_integer_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.put({"id": 1, "name": "First"})

        assert table.get(1) == {"id": 1, "name": "First"}

    def test_put_with_compound_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("org", "id"))

        table.put({"org": "acme", "id": 1, "name": "Alice"})

        assert table.get(("acme", 1)) == {"org": "acme", "id": 1, "name": "Alice"}

    def test_put_without_key_specifier_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path)

        with pytest.raises(InvalidKeyError, match="key specifier is required"):
            table.put({"id": "alice"})

    def test_put_missing_key_field_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        with pytest.raises(InvalidKeyError, match="missing required key field"):
            table.put({"name": "Alice"})

    def test_put_dollar_field_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        with pytest.raises(InvalidKeyError, match="reserved field name"):
            table.put({"id": "alice", "$custom": "value"})

    def test_put_invalid_key_type_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        with pytest.raises(InvalidKeyError, match="boolean"):
            table.put({"id": True, "name": "Alice"})

    def test_put_key_length_limit(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        # Key with > 1024 bytes when serialized (string with quotes)
        long_key = "x" * 1030
        with pytest.raises(LimitError, match="key length"):
            table.put({"id": long_key})

    def test_put_deterministic_serialization(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.put({"z": 1, "id": "test", "a": 2})

        content = table_path.read_text()
        # Keys should be sorted: a, id, z
        assert content.strip() == '{"a":2,"id":"test","z":1}'


class TestTableDelete:
    def test_delete_existing_record(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "name": "Alice"}\n')
        table = Table(table_path, key="id")

        result = table.delete("alice")

        assert result is True
        assert table.get("alice") is None
        assert table.count() == 0

    def test_delete_nonexistent_record(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        result = table.delete("bob")

        assert result is False

    def test_delete_writes_tombstone(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "name": "Alice"}\n')
        table = Table(table_path, key="id")

        _ = table.delete("alice")

        content = table_path.read_text()
        assert "$deleted" in content
        assert content.count("\n") == 2

    def test_delete_with_integer_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1, "name": "First"}\n')
        table = Table(table_path, key="id")

        result = table.delete(1)

        assert result is True
        assert table.get(1) is None

    def test_delete_with_compound_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"org": "acme", "id": 1, "name": "Alice"}\n')
        table = Table(table_path, key=("org", "id"))

        result = table.delete(("acme", 1))

        assert result is True
        assert table.get(("acme", 1)) is None

    def test_delete_without_key_specifier_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path)

        with pytest.raises(InvalidKeyError, match="key specifier is required"):
            _ = table.delete("alice")

    def test_delete_key_arity_mismatch_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("org", "id"))

        with pytest.raises(InvalidKeyError, match="key arity mismatch"):
            _ = table.delete("alice")  # scalar key, tuple specifier

    def test_delete_tuple_key_arity_mismatch_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("org", "id"))

        with pytest.raises(InvalidKeyError, match="key arity mismatch"):
            _ = table.delete(("acme", 1, "extra"))  # 3 elements, specifier has 2

    def test_delete_key_length_limit_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        # 1030 characters + quotes = 1032 bytes > 1024
        long_key = "x" * 1030

        with pytest.raises(LimitError, match="key length"):
            _ = table.delete(long_key)


class TestTableClear:
    def test_clear_removes_all_records(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "a"}\n{"id": "b"}\n{"id": "c"}\n')
        table = Table(table_path, key="id")

        table.clear()

        assert table.count() == 0
        assert table.get("a") is None

    def test_clear_preserves_header(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        content += '{"id": "alice"}\n'
        _ = table_path.write_text(content)
        table = Table(table_path)

        table.clear()

        assert table.count() == 0
        assert table.header is not None
        assert table.header.key == "id"
        # File should only have header
        file_content = table_path.read_text()
        lines = [line for line in file_content.strip().split("\n") if line]
        assert len(lines) == 1
        assert "$jsonlt" in lines[0]

    def test_clear_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.clear()  # Should not raise

        assert table.count() == 0

    def test_clear_creates_file_with_header(self, tmp_path: "Path") -> None:
        # First create table with header, then delete file, then clear
        table_path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        _ = table_path.write_text(content)
        table = Table(table_path)

        table_path.unlink()
        table.clear()

        # Should recreate file with header
        assert table_path.exists()

    def test_clear_reloads_header_inside_lock(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = (
            '{"$jsonlt": {"version": 1, "key": "id", "meta": {"tag": "initial"}}}\n'
        )
        content += '{"id": "alice"}\n'
        _ = table_path.write_text(content)

        # Open table without auto_reload
        table = Table(table_path, auto_reload=False)
        assert table.count() == 1

        # External modification: change header meta tag
        time.sleep(0.01)  # Ensure mtime differs
        new_content = (
            '{"$jsonlt": {"version": 1, "key": "id", "meta": {"tag": "updated"}}}\n'
        )
        new_content += '{"id": "alice"}\n'
        new_content += '{"id": "bob"}\n'
        _ = table_path.write_text(new_content)

        # Call clear - it should reload inside the lock and use the new header
        table.clear()

        # Verify the file has the updated header, not the initial one
        file_content = table_path.read_text()
        assert "updated" in file_content
        assert "initial" not in file_content


class TestTableCompact:
    def test_compact_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.compact()  # Should not raise

        assert table.count() == 0

    def test_compact_removes_tombstones(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": "alice", "name": "Alice"}\n'
        content += '{"id": "alice", "$deleted": true}\n'
        content += '{"id": "bob", "name": "Bob"}\n'
        _ = table_path.write_text(content)
        table = Table(table_path, key="id")

        table.compact()

        # Tombstone should be removed
        file_content = table_path.read_text()
        assert "$deleted" not in file_content
        # Only bob remains
        assert table.count() == 1
        assert table.get("bob") == {"id": "bob", "name": "Bob"}

    def test_compact_preserves_header(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        content += '{"id": "alice"}\n'
        _ = table_path.write_text(content)
        table = Table(table_path)

        table.compact()

        assert table.header is not None
        assert table.header.key == "id"
        # File should have header
        file_content = table_path.read_text()
        assert "$jsonlt" in file_content

    def test_compact_preserves_records(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "a"}\n{"id": "b"}\n{"id": "c"}\n')
        table = Table(table_path, key="id")

        table.compact()

        assert table.count() == 3
        assert table.get("a") == {"id": "a"}
        assert table.get("b") == {"id": "b"}
        assert table.get("c") == {"id": "c"}

    def test_compact_records_in_key_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        # Write records in reverse order
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        table.compact()

        # Read file and verify order
        file_content = table_path.read_text()
        lines = [line for line in file_content.strip().split("\n") if line]
        assert len(lines) == 3
        assert '"id":"a"' in lines[0]
        assert '"id":"b"' in lines[1]
        assert '"id":"c"' in lines[2]

    def test_compact_removes_history(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        # Multiple versions of same key
        content = '{"id": "alice", "v": 1}\n'
        content += '{"id": "alice", "v": 2}\n'
        content += '{"id": "alice", "v": 3}\n'
        _ = table_path.write_text(content)
        table = Table(table_path, key="id")

        table.compact()

        # Only one record should remain
        file_content = table_path.read_text()
        lines = [line for line in file_content.strip().split("\n") if line]
        assert len(lines) == 1
        assert '"v":3' in lines[0]
        assert table.get("alice") == {"id": "alice", "v": 3}

    def test_compact_only_tombstones(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": "alice", "name": "Alice"}\n'
        content += '{"id": "alice", "$deleted": true}\n'
        _ = table_path.write_text(content)
        table = Table(table_path, key="id")

        table.compact()

        # Table should be empty
        assert table.count() == 0
        # File should be empty
        file_content = table_path.read_text()
        assert file_content.strip() == ""

    def test_compact_returns_none(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        result = table.compact()

        assert result is None

    def test_compact_creates_file(self, tmp_path: "Path") -> None:
        # Create table with header, add records, delete file, then compact
        table_path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        content += '{"id": "alice"}\n'
        _ = table_path.write_text(content)
        table = Table(table_path)

        # Delete file but keep in-memory state
        table_path.unlink()
        table.compact()

        # Should recreate file with header and record
        assert table_path.exists()
        file_content = table_path.read_text()
        assert "$jsonlt" in file_content
        assert '"id":"alice"' in file_content

    def test_compact_with_compound_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("org", "id"))

        table.put({"org": "acme", "id": 1, "name": "alice"})
        table.put({"org": "acme", "id": 2, "name": "bob"})
        table.put({"org": "globex", "id": 1, "name": "carol"})
        _ = table.delete(("acme", 2))
        table.compact()

        assert table.count() == 2
        assert table.keys() == [("acme", 1), ("globex", 1)]

    def test_compact_integer_keys_sorted(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.put({"id": 10, "v": 1})
        table.put({"id": 2, "v": 2})
        table.put({"id": 100, "v": 3})
        table.compact()

        assert table.keys() == [2, 10, 100]

    def test_compact_mixed_key_types_sorted(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        table.put({"id": "z", "v": 1})
        table.put({"id": 1, "v": 2})
        table.put({"id": "a", "v": 3})
        table.put({"id": 10, "v": 4})
        table.compact()

        assert table.keys() == [1, 10, "a", "z"]


class TestTableWriteReload:
    def test_put_updates_state_immediately(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id", auto_reload=False)

        table.put({"id": "alice", "name": "Alice"})

        # Should be immediately visible without reload
        assert table.get("alice") == {"id": "alice", "name": "Alice"}

    def test_delete_updates_state_immediately(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id", auto_reload=False)

        _ = table.delete("alice")

        # Should be immediately gone without reload
        assert table.get("alice") is None

    def test_clear_updates_state_immediately(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n{"id": "bob"}\n')
        table = Table(table_path, key="id", auto_reload=False)

        table.clear()

        # Should be immediately empty without reload
        assert table.count() == 0


class TestTableMaxFileSize:
    def test_file_within_limit_loads_successfully(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        file_size = table_path.stat().st_size

        table = Table(table_path, key="id", max_file_size=file_size + 100)

        assert table.get("alice") == {"id": "alice", "v": 1}

    def test_file_exceeds_limit_raises_limit_error(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        file_size = table_path.stat().st_size

        with pytest.raises(LimitError, match=r"file size .* exceeds maximum"):
            _ = Table(table_path, key="id", max_file_size=file_size - 1)

    def test_none_limit_means_no_limit(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        # Create a reasonably sized file
        _ = table_path.write_text('{"id": "alice", "data": "' + "x" * 1000 + '"}\n')

        # None (default) means no limit - should succeed
        table = Table(table_path, key="id", max_file_size=None)

        assert table.count() == 1

    def test_new_file_with_max_file_size_succeeds(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"

        # New file (doesn't exist) should not check size limit
        table = Table(table_path, key="id", max_file_size=10)

        assert table.count() == 0


class TestTableMagicMethods:
    def test_len_returns_count(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "a"}\n{"id": "b"}\n{"id": "c"}\n')
        table = Table(table_path, key="id")

        assert len(table) == 3

    def test_contains_with_existing_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        assert "alice" in table

    def test_contains_with_missing_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        assert "bob" not in table

    def test_contains_with_int_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1}\n')
        table = Table(table_path, key="id")

        assert 1 in table
        assert 2 not in table

    def test_contains_with_tuple_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"a": 1, "b": "x"}\n')
        table = Table(table_path, key=("a", "b"))

        assert (1, "x") in table
        assert (1, "y") not in table

    def test_contains_with_invalid_type_returns_false(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        # Non-key types should return False, not raise
        assert 3.14 not in table
        assert None not in table
        assert ["list"] not in table
        assert {"dict": "value"} not in table

    def test_contains_with_invalid_tuple_returns_false(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"a": 1, "b": "x"}\n')
        table = Table(table_path, key=("a", "b"))

        # Tuple with invalid element types should return False
        assert (1, 3.14) not in table
        assert (None, "x") not in table

    def test_iter_yields_records_in_key_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        # Write in reverse order
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        records = list(table)

        assert len(records) == 3
        assert records[0] == {"id": "a"}
        assert records[1] == {"id": "b"}
        assert records[2] == {"id": "c"}

    def test_iter_on_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        records = list(table)

        assert records == []


class TestTableEmptyTupleKeyRejection:
    def test_get_empty_tuple_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with pytest.raises(InvalidKeyError, match="empty tuple"):
            _ = table.get(())

    def test_has_empty_tuple_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with pytest.raises(InvalidKeyError, match="empty tuple"):
            _ = table.has(())

    def test_delete_empty_tuple_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with pytest.raises(InvalidKeyError, match="arity mismatch"):
            _ = table.delete(())


class TestTableItems:
    def test_items_returns_key_value_pairs(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n{"id": "bob", "v": 2}\n')
        table = Table(table_path, key="id")

        items = table.items()

        assert len(items) == 2
        assert items[0] == ("alice", {"id": "alice", "v": 1})
        assert items[1] == ("bob", {"id": "bob", "v": 2})

    def test_items_in_key_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        # Write in reverse order
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        items = table.items()

        assert [k for k, _ in items] == ["a", "b", "c"]

    def test_items_empty_table(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        items = table.items()

        assert items == []


class TestTableReload:
    def test_reload_updates_state(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id", auto_reload=False)

        assert table.get("alice") == {"id": "alice", "v": 1}

        # Modify file externally
        _ = table_path.write_text('{"id": "alice", "v": 2}\n')

        # Before reload, still sees old value
        assert table.get("alice") == {"id": "alice", "v": 1}

        # After reload, sees new value
        table.reload()
        assert table.get("alice") == {"id": "alice", "v": 2}

    def test_reload_adds_new_records(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id", auto_reload=False)

        assert table.count() == 1

        # Add record externally
        with table_path.open("a") as f:
            _ = f.write('{"id": "bob"}\n')

        table.reload()

        assert table.count() == 2
        assert "bob" in table

    def test_reload_clears_sorted_keys_cache(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n')
        table = Table(table_path, key="id", auto_reload=False)

        # Access keys to populate cache
        keys1 = table.keys()
        assert keys1 == ["a", "c"]

        # Add record externally
        with table_path.open("a") as f:
            _ = f.write('{"id": "b"}\n')

        table.reload()

        keys2 = table.keys()
        assert keys2 == ["a", "b", "c"]


class TestFileSystemEdgeCases:
    def test_load_empty_file_with_header_but_no_ops(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"
        # Create real file on disk for _load()
        _ = table_path.write_bytes(b'{"$jsonlt":{"version":1,"key":"id"}}\n')
        # Also set in fake_fs for stat operations
        fake_fs.set_content(table_path, b'{"$jsonlt":{"version":1,"key":"id"}}\n')

        table = Table(table_path, _fs=fake_fs)

        assert table.count() == 0
        assert table.keys() == []

    def test_load_from_content_empty(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"
        # Create real file on disk for _load()
        _ = table_path.write_bytes(b'{"id":"alice"}\n')
        # Also set in fake_fs for stat operations
        fake_fs.set_content(table_path, b'{"id":"alice"}\n')

        table = Table(table_path, key="id", _fs=fake_fs)
        assert table.count() == 1

        # Simulate reload with empty content (testing internal method)
        table._load_from_content(b"")  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert table._state == {}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    def test_resolve_key_specifier_empty_no_key(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"
        # File does not exist - table should be empty

        table = Table(table_path, _fs=fake_fs)

        assert table.key_specifier is None
        assert table.count() == 0

    def test_reload_if_changed_stat_fails(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"
        # Create real file on disk for _load()
        _ = table_path.write_bytes(b'{"id":"alice"}\n')
        # Also set in fake_fs for stat operations
        fake_fs.set_content(table_path, b'{"id":"alice"}\n')

        table = Table(table_path, key="id", _fs=fake_fs)
        assert table.count() == 1

        # Make stat fail
        fake_fs.fail_stat.add(table_path)

        # _reload_if_changed should raise FileError since stat fails
        # _load() uses path.exists() + read_table_file() but _update_file_stats() fails
        with pytest.raises(FileError, match="simulated stat error"):
            # Testing internal method
            table._reload_if_changed(0.0, 0)  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    def test_write_file_not_found_then_exists(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"

        table = Table(table_path, key="id", _fs=fake_fs)

        # First put should create the file in the fake filesystem
        table.put({"id": "alice"})

        assert table.get("alice") == {"id": "alice"}

    def test_try_update_stats_ignores_file_error(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"
        # Create real file on disk for _load()
        _ = table_path.write_bytes(b'{"id":"alice"}\n')
        # Also set in fake_fs for stat operations
        fake_fs.set_content(table_path, b'{"id":"alice"}\n')

        table = Table(table_path, key="id", _fs=fake_fs)
        # Capture stats via internal attributes (testing implementation detail)
        old_mtime = table._file_mtime  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        old_size = table._file_size  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        # Make stat fail
        fake_fs.fail_stat.add(table_path)

        # _try_update_stats should not raise
        table._try_update_stats()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

        # Stats should remain unchanged
        assert table._file_mtime == old_mtime  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert table._file_size == old_size  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    def test_auto_reload_disabled_uses_cache(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"
        # Create real file on disk for _load()
        _ = table_path.write_bytes(b'{"id":"alice"}\n')
        # Also set in fake_fs for stat operations
        fake_fs.set_content(table_path, b'{"id":"alice"}\n')

        table = Table(table_path, key="id", auto_reload=False, _fs=fake_fs)
        assert table.get("alice") == {"id": "alice"}

        # Make stat fail - with auto_reload=False, this won't be called on get
        fake_fs.fail_stat.add(table_path)

        # Should still be able to read from cache since auto_reload is disabled
        assert table.get("alice") == {"id": "alice"}

    def test_clear_on_file_with_header(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"
        # Create real file on disk for _load()
        _ = table_path.write_bytes(
            b'{"$jsonlt":{"version":1,"key":"id"}}\n{"id":"alice"}\n'
        )
        # Also set in fake_fs for stat operations
        fake_fs.set_content(
            table_path, b'{"$jsonlt":{"version":1,"key":"id"}}\n{"id":"alice"}\n'
        )

        table = Table(table_path, _fs=fake_fs)
        assert table.count() == 1

        # clear() uses _fs.stat to check existence, then _fs.atomic_replace
        table.clear()

        # Count should be 0 after clear
        assert table.count() == 0

        # File should exist with just header via atomic_replace
        assert table_path in fake_fs.files
        content = fake_fs.get_content(table_path)
        assert b'"$jsonlt"' in content
        assert b'"alice"' not in content

    def test_compact_recreates_deleted_file(self, tmp_path: "Path") -> None:
        fake_fs = FakeFileSystem()
        table_path = tmp_path / "test.jsonlt"

        # Create table and add record (uses fake_fs for write)
        table = Table(table_path, key="id", _fs=fake_fs)
        table.put({"id": "alice"})

        # Delete the file from fake_fs
        del fake_fs.files[table_path]

        # Compact should recreate the file via atomic_replace
        table.compact()

        assert table_path in fake_fs.files
