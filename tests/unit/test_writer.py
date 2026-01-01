from typing import TYPE_CHECKING

import pytest

from jsonlt._exceptions import FileError
from jsonlt._writer import append_line, append_lines, atomic_replace

if TYPE_CHECKING:
    from pathlib import Path


class TestAppendLine:
    def test_appends_line_with_newline(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        append_line(path, '{"id":"alice"}')

        assert path.read_text() == '{"id":"alice"}\n'

    def test_appends_multiple_lines(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        append_line(path, '{"id":"alice"}')
        append_line(path, '{"id":"bob"}')

        assert path.read_text() == '{"id":"alice"}\n{"id":"bob"}\n'

    def test_appends_to_existing_content(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text('{"id":"existing"}\n')

        append_line(path, '{"id":"new"}')

        assert path.read_text() == '{"id":"existing"}\n{"id":"new"}\n'

    def test_creates_file_if_not_exists(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        append_line(path, '{"id":"first"}')

        assert path.read_text() == '{"id":"first"}\n'

    def test_raises_file_error_on_failure(self, tmp_path: "Path") -> None:
        path = tmp_path / "nonexistent" / "dir" / "test.jsonlt"

        with pytest.raises(FileError, match="cannot append to file"):
            append_line(path, '{"id":"test"}')

    def test_appends_unicode_content(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        append_line(path, '{"name":"cafe\\u0301"}')

        assert path.read_text() == '{"name":"cafe\\u0301"}\n'

    def test_appends_empty_object(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        append_line(path, "{}")

        assert path.read_text() == "{}\n"

    def test_appends_header_line(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        append_line(path, '{"$jsonlt":{"version":1,"key":"id"}}')

        assert path.read_text() == '{"$jsonlt":{"version":1,"key":"id"}}\n'


class TestAppendLines:
    def test_appends_multiple_lines(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        append_lines(path, ['{"id":"a"}', '{"id":"b"}', '{"id":"c"}'])

        assert path.read_text() == '{"id":"a"}\n{"id":"b"}\n{"id":"c"}\n'

    def test_empty_lines_is_noop(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("existing\n")

        append_lines(path, [])

        assert path.read_text() == "existing\n"

    def test_appends_to_existing_content(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text('{"id":"existing"}\n')

        append_lines(path, ['{"id":"new1"}', '{"id":"new2"}'])

        expected = '{"id":"existing"}\n{"id":"new1"}\n{"id":"new2"}\n'
        assert path.read_text() == expected

    def test_raises_file_error_on_failure(self, tmp_path: "Path") -> None:
        path = tmp_path / "nonexistent" / "dir" / "test.jsonlt"

        with pytest.raises(FileError, match="cannot append to file"):
            append_lines(path, ['{"id":"test"}'])


class TestAtomicReplace:
    def test_replaces_file_contents(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text('{"id":"old"}\n')

        atomic_replace(path, ['{"id":"new"}'])

        assert path.read_text() == '{"id":"new"}\n'

    def test_writes_multiple_lines(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text("")

        atomic_replace(path, ['{"$jsonlt":{"version":1}}', '{"id":"alice"}'])

        assert path.read_text() == '{"$jsonlt":{"version":1}}\n{"id":"alice"}\n'

    def test_creates_file_if_not_exists(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        atomic_replace(path, ['{"id":"new"}'])

        assert path.read_text() == '{"id":"new"}\n'

    def test_empty_lines_creates_empty_file(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text('{"id":"old"}\n')

        atomic_replace(path, [])

        assert path.read_text() == ""

    def test_raises_file_error_on_failure(self, tmp_path: "Path") -> None:
        path = tmp_path / "nonexistent" / "dir" / "test.jsonlt"

        with pytest.raises(FileError, match="cannot write file atomically"):
            atomic_replace(path, ['{"id":"test"}'])

    def test_no_temp_file_left_on_success(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        atomic_replace(path, ['{"id":"test"}'])

        # Check no .tmp files exist
        tmp_files = list(tmp_path.glob(".jsonlt_*.tmp"))
        assert tmp_files == []

    def test_overwrites_larger_file(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        # Write many lines
        original_lines = [f'{{"id":"{i}"}}' for i in range(100)]
        _ = path.write_text("\n".join(original_lines) + "\n")

        atomic_replace(path, ['{"id":"only"}'])

        assert path.read_text() == '{"id":"only"}\n'

    def test_replaces_with_larger_content(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text('{"id":"small"}\n')

        new_lines = [f'{{"id":"{i}"}}' for i in range(100)]
        atomic_replace(path, new_lines)

        content = path.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 100

    def test_writes_unicode_content(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        atomic_replace(path, ['{"name":"cafe\\u0301"}', '{"emoji":"\\ud83d\\ude00"}'])

        content = path.read_text()
        assert '{"name":"cafe\\u0301"}' in content
        assert '{"emoji":"\\ud83d\\ude00"}' in content

    def test_file_remains_accessible_after_atomic_replace(
        self, tmp_path: "Path"
    ) -> None:
        path = tmp_path / "test.jsonlt"
        _ = path.write_text('{"id":"old"}\n')

        atomic_replace(path, ['{"id":"new"}'])

        # Verify file is still readable/writable
        assert path.read_text() == '{"id":"new"}\n'
        _ = path.write_text('{"id":"verify"}\n')
        assert path.read_text() == '{"id":"verify"}\n'

    def test_single_line_write(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        atomic_replace(path, ['{"single":"line"}'])

        assert path.read_text() == '{"single":"line"}\n'

    def test_header_only_write(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"

        atomic_replace(path, ['{"$jsonlt":{"version":1,"key":"id"}}'])

        assert path.read_text() == '{"$jsonlt":{"version":1,"key":"id"}}\n'

    def test_no_partial_write_on_failure(self, tmp_path: "Path") -> None:
        path = tmp_path / "test.jsonlt"
        original_content = '{"id":"original"}\n'
        _ = path.write_text(original_content)

        # Try to write to a path where the directory doesn't exist
        # This will fail when trying to create the temp file
        bad_path = tmp_path / "nonexistent_subdir" / "test.jsonlt"

        with pytest.raises(FileError):
            atomic_replace(bad_path, ['{"id":"new"}'])

        # Original file should be unchanged
        assert path.read_text() == original_content
