"""UTF-8 encoding validation for JSONLT.

This module provides functions to validate and prepare UTF-8 encoded bytes
for JSONLT processing, per specification section "Encoding".
"""

from typing import TYPE_CHECKING, Final

from ._exceptions import ParseError

if TYPE_CHECKING:
    from ._json import JSONValue

# Unicode surrogate code point ranges
_HIGH_SURROGATE_START: Final[int] = 0xD800
_HIGH_SURROGATE_END: Final[int] = 0xDBFF
_LOW_SURROGATE_START: Final[int] = 0xDC00
_LOW_SURROGATE_END: Final[int] = 0xDFFF


def strip_bom(data: bytes) -> bytes:
    """Strip UTF-8 BOM from the start of byte data if present.

    Per specification: "A conforming parser SHOULD strip any BOM encountered
    at the start of the file."

    Args:
        data: Raw bytes that may start with a BOM.

    Returns:
        The bytes with BOM stripped if present, otherwise unchanged.
    """
    bom = b"\xef\xbb\xbf"
    if data.startswith(bom):
        return data[len(bom) :]
    return data


def strip_cr_before_lf(data: bytes) -> bytes:
    r"""Strip CR characters that precede LF characters.

    Per specification: "A conforming parser SHOULD strip CR characters
    preceding LF."

    This ensures consistent LF-only line endings regardless of whether
    input was created on Windows (CRLF) or Unix (LF) systems.

    Args:
        data: Raw bytes that may contain CRLF sequences.

    Returns:
        The bytes with all CR-LF sequences replaced with just LF.
    """
    return data.replace(b"\r\n", b"\n")


def validate_utf8(data: bytes) -> str:
    """Validate and decode UTF-8 bytes with strict security requirements.

    Per specification:
    - "A conforming parser SHALL reject byte sequences that are overlong
      encodings" (RFC 3629, Unicode 16.0 Section 3.9)
    - Surrogate code points (U+D800-U+DFFF) are not valid in UTF-8

    Python's built-in UTF-8 codec with 'strict' error handling correctly
    rejects both overlong encodings and surrogate code points, as these
    are invalid UTF-8 per RFC 3629.

    Args:
        data: Raw bytes to validate and decode.

    Returns:
        The decoded string.

    Raises:
        UnicodeDecodeError: If the bytes contain invalid UTF-8, including
            overlong encodings or surrogate code points.
    """
    return data.decode("utf-8", errors="strict")


def prepare_input(data: bytes) -> str:
    """Prepare raw byte input for JSONLT parsing.

    This function applies all input preprocessing required by the specification:
    1. Strip BOM if present
    2. Strip CR before LF (normalize line endings)
    3. Validate UTF-8 encoding (reject overlong and surrogate encodings)

    Args:
        data: Raw bytes from a JSONLT file.

    Returns:
        The prepared string ready for line-by-line parsing.

    Raises:
        UnicodeDecodeError: If the bytes contain invalid UTF-8.
    """
    data = strip_bom(data)
    data = strip_cr_before_lf(data)
    return validate_utf8(data)


def has_unpaired_surrogates(text: str) -> bool:
    """Check if string contains unpaired Unicode surrogates.

    Surrogates are in the range U+D800-U+DFFF. High surrogates (U+D800-U+DBFF)
    must be followed by low surrogates (U+DC00-U+DFFF) to form valid pairs.

    Note: In Python 3, strings normally cannot contain unpaired surrogates
    because the string type requires valid Unicode. However, the surrogatepass
    error handler or certain APIs may produce strings with lone surrogates.

    Args:
        text: String to check.

    Returns:
        True if unpaired surrogates found, False otherwise.
    """
    i = 0
    length = len(text)

    while i < length:
        code_point = ord(text[i])

        # Check if this is a high surrogate (U+D800-U+DBFF)
        if _HIGH_SURROGATE_START <= code_point <= _HIGH_SURROGATE_END:
            # High surrogate must be followed by low surrogate
            if i + 1 < length:
                next_code_point = ord(text[i + 1])
                if _LOW_SURROGATE_START <= next_code_point <= _LOW_SURROGATE_END:
                    # Valid surrogate pair, skip both
                    i += 2
                    continue
            # Unpaired high surrogate
            return True

        # Check if this is a lone low surrogate (U+DC00-U+DFFF)
        if _LOW_SURROGATE_START <= code_point <= _LOW_SURROGATE_END:
            # Low surrogate without preceding high surrogate
            return True

        i += 1

    return False


def validate_no_surrogates(value: "JSONValue") -> None:
    """Recursively validate no unpaired surrogates in any string.

    Checks strings for unpaired Unicode surrogates and recursively descends
    into dicts and lists to validate all nested string values.

    Args:
        value: A JSON value to check.

    Raises:
        ParseError: If any string contains unpaired surrogates.
    """
    if isinstance(value, str):
        if has_unpaired_surrogates(value):
            msg = "record contains unpaired Unicode surrogates"
            raise ParseError(msg)
    elif isinstance(value, dict):
        for k, v in value.items():
            if has_unpaired_surrogates(k):
                msg = "record contains unpaired Unicode surrogates in field name"
                raise ParseError(msg)
            validate_no_surrogates(v)
    elif isinstance(value, list):
        for item in value:
            validate_no_surrogates(item)
