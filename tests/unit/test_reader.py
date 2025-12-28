from typing import TYPE_CHECKING

import pytest

from jsonlt import FileError, LimitError, ParseError
from jsonlt._reader import parse_table_content, parse_table_text, read_table_file

if TYPE_CHECKING:
    from pathlib import Path


class TestParseTableContent:
    def test_empty_bytes_returns_empty(self) -> None:
        header, operations = parse_table_content(b"")
        assert header is None
        assert operations == []

    def test_single_record(self) -> None:
        content = b'{"id": 1, "name": "test"}\n'
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 1
        assert operations[0] == {"id": 1, "name": "test"}

    def test_multiple_records(self) -> None:
        content = b'{"id": 1}\n{"id": 2}\n{"id": 3}\n'
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 3
        assert operations[0] == {"id": 1}
        assert operations[1] == {"id": 2}
        assert operations[2] == {"id": 3}

    def test_missing_trailing_newline_accepted(self) -> None:
        content = b'{"id": 1}'
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 1
        assert operations[0] == {"id": 1}

    def test_bom_stripped(self) -> None:
        bom = b"\xef\xbb\xbf"
        content = bom + b'{"id": 1}\n'
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 1
        assert operations[0] == {"id": 1}

    def test_bom_only_returns_empty(self) -> None:
        """File containing only BOM returns empty result."""
        bom = b"\xef\xbb\xbf"
        header, operations = parse_table_content(bom)
        assert header is None
        assert operations == []

    def test_crlf_normalized(self) -> None:
        content = b'{"id": 1}\r\n{"id": 2}\r\n'
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 2

    def test_header_detected_on_first_line(self) -> None:
        content = b'{"$jsonlt": {"version": 1, "key": "id"}}\n{"id": "alice"}\n'
        header, operations = parse_table_content(content)
        assert header is not None
        assert header.version == 1
        assert header.key == "id"
        assert len(operations) == 1
        assert operations[0] == {"id": "alice"}

    def test_header_only_file(self) -> None:
        content = b'{"$jsonlt": {"version": 1}}\n'
        header, operations = parse_table_content(content)
        assert header is not None
        assert header.version == 1
        assert operations == []

    def test_header_not_on_first_line_rejected(self) -> None:
        content = b'{"id": 1}\n{"$jsonlt": {"version": 1}}\n'
        with pytest.raises(ParseError, match="header must be on first line"):
            _ = parse_table_content(content)

    @pytest.mark.parametrize(
        ("content", "match"),
        [
            pytest.param(
                b'{"id": 1, "name": "\xff\xfe"}\n', "invalid UTF-8", id="invalid_utf8"
            ),
            pytest.param(b'{"id": 1, "name": }\n', "invalid JSON", id="invalid_json"),
            pytest.param(b"[1, 2, 3]\n", "expected JSON object", id="non_object"),
            pytest.param(b'{"id": 1, "id": 2}\n', "duplicate key", id="duplicate_keys"),
        ],
    )
    def test_parse_error_rejected(self, content: bytes, match: str) -> None:
        with pytest.raises(ParseError, match=match):
            _ = parse_table_content(content)

    def test_tombstone_parsed(self) -> None:
        content = b'{"id": 1}\n{"$deleted": true, "id": 1}\n'
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 2
        assert operations[0] == {"id": 1}
        assert operations[1] == {"$deleted": True, "id": 1}

    def test_unicode_content_preserved(self) -> None:
        content = '{"id": "test", "emoji": "👍"}\n'.encode()
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 1
        assert operations[0] == {"id": "test", "emoji": "👍"}

    def test_escaped_newline_in_string(self) -> None:
        content = b'{"id": 1, "text": "line1\\nline2"}\n'
        header, operations = parse_table_content(content)
        assert header is None
        assert len(operations) == 1
        assert operations[0] == {"id": 1, "text": "line1\nline2"}

    @pytest.mark.parametrize(
        "bad_bytes",
        [
            pytest.param(b"\xc0\x80", id="2byte_overlong_nul"),
            pytest.param(b"\xc1\xbf", id="2byte_overlong_del"),
            pytest.param(b"\xe0\x80\x80", id="3byte_overlong_nul"),
            pytest.param(b"\xe0\x80\xaf", id="3byte_overlong_slash"),
        ],
    )
    def test_overlong_utf8_rejected(self, bad_bytes: bytes) -> None:
        content = b'{"id": 1, "name": "abc' + bad_bytes + b'xyz"}\n'
        with pytest.raises(ParseError, match="invalid UTF-8"):
            _ = parse_table_content(content)


class TestParseTableText:
    def test_empty_string_returns_empty(self) -> None:
        header, operations = parse_table_text("")
        assert header is None
        assert operations == []

    def test_single_record(self) -> None:
        header, operations = parse_table_text('{"id": 1}\n')
        assert header is None
        assert len(operations) == 1

    def test_skips_empty_lines(self) -> None:
        header, operations = parse_table_text('{"id": 1}\n\n{"id": 2}\n')
        assert header is None
        assert len(operations) == 2


class TestReadTableFile:
    def test_reads_file(self, tmp_path: "Path") -> None:
        file = tmp_path / "test.jsonlt"
        _ = file.write_text('{"id": 1}\n{"id": 2}\n')
        header, operations = read_table_file(file)
        assert header is None
        assert len(operations) == 2

    def test_reads_file_from_string_path(self, tmp_path: "Path") -> None:
        file = tmp_path / "test.jsonlt"
        _ = file.write_text('{"id": 1}\n')
        header, operations = read_table_file(str(file))
        assert header is None
        assert len(operations) == 1

    def test_reads_empty_file(self, tmp_path: "Path") -> None:
        file = tmp_path / "empty.jsonlt"
        _ = file.write_text("")
        header, operations = read_table_file(file)
        assert header is None
        assert operations == []

    def test_file_not_found_raises_file_error(self, tmp_path: "Path") -> None:
        file = tmp_path / "nonexistent.jsonlt"
        with pytest.raises(FileError, match="cannot read file"):
            _ = read_table_file(file)

    def test_file_with_header(self, tmp_path: "Path") -> None:
        file = tmp_path / "test.jsonlt"
        content = '{"$jsonlt": {"version": 1, "key": "id"}}\n{"id": "alice"}\n'
        _ = file.write_text(content)
        header, operations = read_table_file(file)
        assert header is not None
        assert header.version == 1
        assert header.key == "id"
        assert len(operations) == 1


class TestMaxFileSize:
    def test_file_within_limit_succeeds(self, tmp_path: "Path") -> None:
        file = tmp_path / "test.jsonlt"
        content = '{"id": 1}\n'
        _ = file.write_text(content)
        file_size = file.stat().st_size
        # Set limit higher than file size
        header, operations = read_table_file(file, max_file_size=file_size + 100)
        assert header is None
        assert len(operations) == 1

    def test_file_at_exact_limit_succeeds(self, tmp_path: "Path") -> None:
        file = tmp_path / "test.jsonlt"
        content = '{"id": 1}\n'
        _ = file.write_text(content)
        file_size = file.stat().st_size
        # Set limit exactly at file size
        header, operations = read_table_file(file, max_file_size=file_size)
        assert header is None
        assert len(operations) == 1

    def test_file_exceeds_limit_raises_limit_error(self, tmp_path: "Path") -> None:
        file = tmp_path / "test.jsonlt"
        content = '{"id": 1, "data": "some longer content here"}\n'
        _ = file.write_text(content)
        file_size = file.stat().st_size
        # Set limit lower than file size
        with pytest.raises(LimitError, match=r"file size .* exceeds maximum"):
            _ = read_table_file(file, max_file_size=file_size - 1)

    def test_none_limit_means_no_limit(self, tmp_path: "Path") -> None:
        file = tmp_path / "test.jsonlt"
        # Create a reasonably sized file
        content = '{"id": 1, "data": "' + "x" * 1000 + '"}\n'
        _ = file.write_text(content)
        # None (default) means no limit - should succeed
        header, operations = read_table_file(file, max_file_size=None)
        assert header is None
        assert len(operations) == 1

    def test_stat_failure_raises_file_error(self, tmp_path: "Path") -> None:
        file = tmp_path / "nonexistent.jsonlt"
        # File doesn't exist - stat should fail and raise FileError
        with pytest.raises(FileError, match="cannot read file"):
            _ = read_table_file(file, max_file_size=1000)
