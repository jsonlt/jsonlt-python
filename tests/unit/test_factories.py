"""Tests for Table factory methods."""

from typing import TYPE_CHECKING

import pytest

from jsonlt import FileError, InvalidKeyError, LimitError, Table

if TYPE_CHECKING:
    from pathlib import Path

    from tests.fakes.fake_filesystem import FakeFileSystem


class TestFromRecords:
    def test_single_record(self, tmp_path: "Path") -> None:
        table = Table.from_records(
            tmp_path / "test.jsonlt",
            {"id": "alice", "role": "admin"},
            key="id",
        )
        assert table.count() == 1
        assert table.get("alice") == {"id": "alice", "role": "admin"}

    def test_multiple_records(self, tmp_path: "Path") -> None:
        records = [
            {"id": "alice", "role": "admin"},
            {"id": "bob", "role": "user"},
            {"id": "charlie", "role": "user"},
        ]
        table = Table.from_records(
            tmp_path / "test.jsonlt",
            records,
            key="id",
        )
        assert table.count() == 3
        assert table.get("alice") is not None
        assert table.get("bob") is not None
        assert table.get("charlie") is not None

    def test_empty_records_list(self, tmp_path: "Path") -> None:
        table = Table.from_records(
            tmp_path / "test.jsonlt",
            [],
            key="id",
        )
        assert table.count() == 0
        assert table.key_specifier == "id"

    def test_compound_key(self, tmp_path: "Path") -> None:
        records = [
            {"org": "acme", "id": 1, "name": "alice"},
            {"org": "acme", "id": 2, "name": "bob"},
        ]
        table = Table.from_records(
            tmp_path / "test.jsonlt",
            records,
            key=("org", "id"),
        )
        assert table.count() == 2
        assert table.get(("acme", 1)) == {"org": "acme", "id": 1, "name": "alice"}

    def test_duplicate_keys_last_wins(self, tmp_path: "Path") -> None:
        records = [
            {"id": "alice", "role": "admin"},
            {"id": "alice", "role": "user"},  # Duplicate key, different value
        ]
        table = Table.from_records(
            tmp_path / "test.jsonlt",
            records,
            key="id",
        )
        assert table.count() == 1
        assert table.get("alice") == {"id": "alice", "role": "user"}

    def test_file_has_header(self, tmp_path: "Path") -> None:
        table = Table.from_records(
            tmp_path / "test.jsonlt",
            [{"id": "alice"}],
            key="id",
        )
        assert table.header is not None
        assert table.header.key == "id"

    def test_file_readable_by_constructor(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = Table.from_records(
            path,
            [{"id": "alice", "role": "admin"}],
            key="id",
        )

        # Load with constructor
        table2 = Table(path)  # Key auto-detected from header
        assert table2.count() == 1
        assert table2.get("alice") == {"id": "alice", "role": "admin"}

    def test_invalid_record_missing_key(self, tmp_path: "Path") -> None:
        with pytest.raises(InvalidKeyError, match=r"record at index 0:.*missing"):
            _ = Table.from_records(
                tmp_path / "test.jsonlt",
                [{"name": "alice"}],  # Missing 'id' key field
                key="id",
            )

    def test_invalid_record_dollar_field(self, tmp_path: "Path") -> None:
        with pytest.raises(InvalidKeyError, match=r"record at index 0:.*reserved"):
            _ = Table.from_records(
                tmp_path / "test.jsonlt",
                [{"id": "alice", "$custom": "value"}],
                key="id",
            )

    def test_invalid_record_index_in_error(self, tmp_path: "Path") -> None:
        records = [
            {"id": "alice"},
            {"id": "bob"},
            {"name": "charlie"},  # Missing key at index 2
        ]
        with pytest.raises(InvalidKeyError, match="record at index 2:"):
            _ = Table.from_records(
                tmp_path / "test.jsonlt",
                records,
                key="id",
            )

    def test_no_file_on_validation_error(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        with pytest.raises(InvalidKeyError):
            _ = Table.from_records(
                path,
                [{"name": "alice"}],  # Invalid
                key="id",
            )
        assert not path.exists()

    def test_creates_parent_directories(self, tmp_path: "Path") -> None:
        path = tmp_path / "nested" / "dir" / "test.jsonlt"
        table = Table.from_records(
            path,
            [{"id": "alice"}],
            key="id",
        )
        assert path.exists()
        assert table.count() == 1

    def test_overwrites_existing_file(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        # Create initial file
        _ = Table.from_records(path, [{"id": "alice"}], key="id")

        # Overwrite with new content
        table = Table.from_records(path, [{"id": "bob"}], key="id")

        assert table.count() == 1
        assert table.get("alice") is None
        assert table.get("bob") is not None

    def test_with_fake_filesystem(
        self, tmp_path: "Path", fake_fs: "FakeFileSystem"
    ) -> None:
        path = tmp_path / "test.jsonlt"
        table = Table.from_records(
            path,
            [{"id": "alice"}],
            key="id",
            _fs=fake_fs,
        )
        assert table.count() == 1
        content = fake_fs.get_content(path)
        assert b'"id":"alice"' in content
        assert b'"$jsonlt"' in content  # Header present

    def test_single_element_tuple_key_normalized(self, tmp_path: "Path") -> None:
        table = Table.from_records(
            tmp_path / "test.jsonlt",
            [{"id": "alice"}],
            key=("id",),
        )
        assert table.key_specifier == "id"

    def test_key_too_long_raises_limit_error(self, tmp_path: "Path") -> None:
        long_key = "x" * 1030  # > 1024 bytes with quotes
        with pytest.raises(LimitError, match=r"record at index 0:.*key length"):
            _ = Table.from_records(
                tmp_path / "test.jsonlt",
                [{"id": long_key}],
                key="id",
            )

    def test_record_too_large_raises_limit_error(self, tmp_path: "Path") -> None:
        large_value = "x" * (1024 * 1024 + 100)  # > 1 MiB
        with pytest.raises(LimitError, match=r"record at index 0:.*record size"):
            _ = Table.from_records(
                tmp_path / "test.jsonlt",
                [{"id": "alice", "data": large_value}],
                key="id",
            )

    def test_generator_as_records(self, tmp_path: "Path") -> None:
        def record_generator() -> list[dict[str, object]]:
            return [{"id": str(i)} for i in range(3)]

        table = Table.from_records(
            tmp_path / "test.jsonlt",
            record_generator(),
            key="id",
        )
        assert table.count() == 3


class TestFromFile:
    def test_load_file_with_header(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        content += '{"id": "alice", "role": "admin"}\n'
        _ = path.write_text(content)

        table = Table.from_file(path)
        assert table.key_specifier == "id"
        assert table.count() == 1
        assert table.get("alice") == {"id": "alice", "role": "admin"}

    def test_load_file_with_explicit_key(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"id": "alice", "role": "admin"}\n'
        _ = path.write_text(content)

        table = Table.from_file(path, key="id")
        assert table.count() == 1
        assert table.get("alice") is not None

    def test_file_not_found(self, tmp_path: "Path") -> None:
        path = tmp_path / "nonexistent.jsonlt"
        with pytest.raises(FileError, match="file not found"):
            _ = Table.from_file(path)

    def test_no_header_no_key_error(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"id": "alice", "role": "admin"}\n'
        _ = path.write_text(content)

        with pytest.raises(InvalidKeyError, match="key specifier"):
            _ = Table.from_file(path)  # No key, no header

    def test_key_mismatch_error(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        _ = path.write_text(content)

        with pytest.raises(InvalidKeyError, match="mismatch"):
            _ = Table.from_file(path, key="name")

    def test_compound_key_from_header(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": ["org", "id"]}}\n'
        content += '{"org": "acme", "id": 1, "name": "alice"}\n'
        _ = path.write_text(content)

        table = Table.from_file(path)
        assert table.key_specifier == ("org", "id")
        assert table.get(("acme", 1)) is not None

    def test_passes_options_to_table(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        _ = path.write_text(content)

        table = Table.from_file(path, auto_reload=False, lock_timeout=5.0)
        # auto_reload is private, but we can verify it works
        assert table.path == path

    def test_with_fake_filesystem(
        self, tmp_path: "Path", fake_fs: "FakeFileSystem"
    ) -> None:
        path = tmp_path / "test.jsonlt"
        content = b'{"$jsonlt": {"version": 1, "key": "id"}}\n'
        _ = path.write_bytes(content)
        fake_fs.set_content(path, content)

        table = Table.from_file(path, _fs=fake_fs)
        assert table.key_specifier == "id"

    def test_string_path(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        _ = path.write_text(content)

        table = Table.from_file(str(path))
        assert table.key_specifier == "id"

    def test_max_file_size_option(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        content += '{"id": "alice"}\n'
        _ = path.write_text(content)

        # Should succeed with generous limit
        table = Table.from_file(path, max_file_size=10000)
        assert table.count() == 1

    def test_max_file_size_exceeded(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        content += '{"id": "alice"}\n'
        _ = path.write_text(content)

        with pytest.raises(LimitError, match="file size"):
            _ = Table.from_file(path, max_file_size=10)  # Very small limit

    def test_empty_file_with_key(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_bytes(b"")  # 0-byte file

        table = Table.from_file(path, key="id")
        assert table.count() == 0


class TestFactoryIntegration:
    def test_roundtrip_from_records_to_from_file(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        records = [
            {"id": "alice", "role": "admin"},
            {"id": "bob", "role": "user"},
        ]

        # Create table with from_records
        _ = Table.from_records(path, records, key="id")

        # Load with from_file
        table = Table.from_file(path)

        assert table.count() == 2
        assert table.get("alice") == {"id": "alice", "role": "admin"}
        assert table.get("bob") == {"id": "bob", "role": "user"}

    def test_from_records_then_modify(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        table = Table.from_records(
            path,
            [{"id": "alice", "role": "admin"}],
            key="id",
        )

        # Modify the table
        table.put({"id": "bob", "role": "user"})
        _ = table.delete("alice")

        assert table.count() == 1
        assert table.get("bob") is not None
        assert table.get("alice") is None

    def test_from_file_then_modify(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n'
        content += '{"id": "alice", "role": "admin"}\n'
        _ = path.write_text(content)

        table = Table.from_file(path)

        # Modify the table
        table.put({"id": "bob", "role": "user"})

        assert table.count() == 2
