"""File reading operations for JSONLT.

This module provides functions for reading and parsing JSONLT files,
handling encoding normalization, line splitting, and header detection.
"""

from pathlib import Path
from typing import TYPE_CHECKING

from ._encoding import prepare_input
from ._exceptions import FileError, LimitError, ParseError
from ._header import Header, is_header_line, parse_header
from ._json import parse_json_line

if TYPE_CHECKING:
    from ._json import JSONObject


def read_table_file(
    path: Path | str,
    *,
    max_file_size: int | None = None,
) -> tuple[Header | None, list["JSONObject"]]:
    """Read and parse a JSONLT file.

    This function reads a file from disk and parses it according to the
    JSONLT specification:

    1. Read file contents as bytes
    2. Apply encoding normalization (BOM stripping, CRLF→LF, UTF-8 validation)
    3. Split by newlines into lines
    4. Parse each non-empty line as JSON
    5. Detect and parse header if present on first line
    6. Return header (if present) and list of operation objects

    Per specification:
    - Empty files are valid (return None, [])
    - Missing trailing newline is accepted
    - Empty lines are skipped (though not expected in valid files)
    - Header must be on the first line if present

    Args:
        path: Path to the JSONLT file to read.
        max_file_size: Maximum allowed file size in bytes. If the file exceeds
            this limit, LimitError is raised. If None (default), no limit is
            enforced.

    Returns:
        A tuple of (header, operations) where:
        - header is the parsed Header if the first line was a header, else None
        - operations is a list of parsed JSON objects (records and tombstones)

    Raises:
        FileError: If the file cannot be read (permissions, I/O errors).
        LimitError: If the file size exceeds max_file_size.
        ParseError: If the file contains invalid UTF-8, invalid JSON,
            non-object JSON values, duplicate keys, or invalid header structure.
    """
    file_path = Path(path) if isinstance(path, str) else path

    # Check file size before reading if limit is specified
    if max_file_size is not None:
        try:
            file_size = file_path.stat().st_size
        except OSError as e:
            msg = f"cannot read file: {e}"
            raise FileError(msg) from e
        if file_size > max_file_size:
            msg = f"file size {file_size} bytes exceeds maximum {max_file_size}"
            raise LimitError(msg)

    try:
        raw_bytes = file_path.read_bytes()
    except OSError as e:
        msg = f"cannot read file: {e}"
        raise FileError(msg) from e

    return parse_table_content(raw_bytes)


def parse_table_content(
    data: bytes,
) -> tuple[Header | None, list["JSONObject"]]:
    """Parse JSONLT content from bytes.

    This is the core parsing function that processes raw bytes according
    to the JSONLT specification. It handles:
    - BOM stripping
    - CRLF→LF normalization
    - UTF-8 validation
    - Line splitting
    - JSON parsing with duplicate key detection
    - Header detection and parsing

    This function is useful for parsing content that doesn't come from
    a file (e.g., from network, in-memory buffers).

    Args:
        data: Raw bytes containing JSONLT content.

    Returns:
        A tuple of (header, operations) where:
        - header is the parsed Header if the first line was a header, else None
        - operations is a list of parsed JSON objects (records and tombstones)

    Raises:
        ParseError: If the content contains invalid UTF-8, invalid JSON,
            non-object JSON values, duplicate keys, or invalid header structure.
    """
    # Handle empty content
    if not data:
        return (None, [])

    # Apply encoding normalization
    try:
        text = prepare_input(data)
    except UnicodeDecodeError as e:
        msg = f"invalid UTF-8: {e}"
        raise ParseError(msg) from e

    # Handle empty string (e.g., file was just BOM)
    if not text:
        return (None, [])

    return parse_table_text(text)


def parse_table_text(
    text: str,
) -> tuple[Header | None, list["JSONObject"]]:
    """Parse JSONLT content from a decoded string.

    This function handles the line-by-line parsing of JSONLT content
    after encoding normalization has been applied.

    Args:
        text: Decoded and normalized text content.

    Returns:
        A tuple of (header, operations) where:
        - header is the parsed Header if the first line was a header, else None
        - operations is a list of parsed JSON objects (records and tombstones)

    Raises:
        ParseError: If the content contains invalid JSON, non-object JSON values,
            duplicate keys, or invalid header structure.
    """
    # Handle empty text
    if not text:
        return (None, [])

    # Split into lines
    lines = text.split("\n")

    header: Header | None = None
    operations: list[JSONObject] = []

    for i, line in enumerate(lines):
        # Skip empty lines (handles missing trailing newline and empty lines)
        if not line:
            continue

        # Parse the JSON object
        obj = parse_json_line(line)

        # Check if first non-empty line is a header
        if i == 0 and is_header_line(obj):
            header = parse_header(obj)
            continue

        # If header appears after first line, reject it
        if is_header_line(obj):
            msg = "header must be on first line"
            raise ParseError(msg)

        # This is a record or tombstone
        operations.append(obj)

    return (header, operations)
