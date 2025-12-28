import pytest

from jsonlt._encoding import (
    has_unpaired_surrogates,
    prepare_input,
    strip_bom,
    strip_cr_before_lf,
    validate_utf8,
)


class TestStripBom:
    def test_strips_utf8_bom(self) -> None:
        data = b'\xef\xbb\xbf{"id": 1}'
        result = strip_bom(data)
        assert result == b'{"id": 1}'

    def test_preserves_data_without_bom(self) -> None:
        data = b'{"id": 1}'
        result = strip_bom(data)
        assert result == data

    def test_empty_input(self) -> None:
        assert strip_bom(b"") == b""

    def test_only_bom(self) -> None:
        assert strip_bom(b"\xef\xbb\xbf") == b""

    def test_bom_in_middle_not_stripped(self) -> None:
        data = b'{"id": "\xef\xbb\xbf"}'
        result = strip_bom(data)
        assert result == data


class TestStripCrBeforeLf:
    def test_strips_crlf_to_lf(self) -> None:
        data = b'{"id": 1}\r\n{"id": 2}\r\n'
        result = strip_cr_before_lf(data)
        assert result == b'{"id": 1}\n{"id": 2}\n'

    def test_preserves_lf_only(self) -> None:
        data = b'{"id": 1}\n{"id": 2}\n'
        result = strip_cr_before_lf(data)
        assert result == data

    def test_preserves_standalone_cr(self) -> None:
        # CR not followed by LF should be preserved
        data = b'{"id": 1}\r{"id": 2}'
        result = strip_cr_before_lf(data)
        assert result == data

    def test_mixed_line_endings(self) -> None:
        data = b'{"id": 1}\r\n{"id": 2}\n{"id": 3}\r\n'
        result = strip_cr_before_lf(data)
        assert result == b'{"id": 1}\n{"id": 2}\n{"id": 3}\n'

    def test_empty_input(self) -> None:
        assert strip_cr_before_lf(b"") == b""


class TestValidateUtf8:
    def test_valid_ascii(self) -> None:
        result = validate_utf8(b'{"id": 1}')
        assert result == '{"id": 1}'

    def test_valid_multibyte_utf8(self) -> None:
        # "café" with é as 2-byte UTF-8 (0xC3 0xA9)
        result = validate_utf8("café".encode())
        assert result == "café"

    def test_valid_emoji(self) -> None:
        # Emoji 😀 (U+1F600) as 4-byte UTF-8
        result = validate_utf8("😀".encode())
        assert result == "😀"

    def test_valid_chinese(self) -> None:
        # Chinese characters as 3-byte UTF-8
        result = validate_utf8("中文".encode())
        assert result == "中文"

    @pytest.mark.parametrize(
        "data",
        [
            # 2-byte overlong NUL (0xC0 0x80)
            b"\xc0\x80",
            # 2-byte overlong DEL (0xC1 0xBF)
            b"\xc1\xbf",
            # 3-byte overlong NUL (0xE0 0x80 0x80)
            b"\xe0\x80\x80",
            # 3-byte overlong slash (0xE0 0x80 0xAF)
            b"\xe0\x80\xaf",
            # 4-byte overlong NUL (0xF0 0x80 0x80 0x80)
            b"\xf0\x80\x80\x80",
        ],
        ids=[
            "2-byte-overlong-nul",
            "2-byte-overlong-del",
            "3-byte-overlong-nul",
            "3-byte-overlong-slash",
            "4-byte-overlong-nul",
        ],
    )
    def test_rejects_overlong_encodings(self, data: bytes) -> None:
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(data)

    @pytest.mark.parametrize(
        "data",
        [
            # High surrogate U+D800 (0xED 0xA0 0x80)
            b"\xed\xa0\x80",
            # High surrogate mid-range U+DB00 (0xED 0xAC 0x80)
            b"\xed\xac\x80",
            # High surrogate max U+DBFF (0xED 0xAF 0xBF)
            b"\xed\xaf\xbf",
            # Low surrogate U+DC00 (0xED 0xB0 0x80)
            b"\xed\xb0\x80",
            # Low surrogate mid-range U+DE00 (0xED 0xB8 0x80)
            b"\xed\xb8\x80",
            # Low surrogate max U+DFFF (0xED 0xBF 0xBF)
            b"\xed\xbf\xbf",
        ],
        ids=[
            "high-surrogate-min",
            "high-surrogate-mid",
            "high-surrogate-max",
            "low-surrogate-min",
            "low-surrogate-mid",
            "low-surrogate-max",
        ],
    )
    def test_rejects_surrogate_codepoints(self, data: bytes) -> None:
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(data)

    def test_rejects_invalid_lead_byte_ff(self) -> None:
        # 0xFF cannot start any valid UTF-8 sequence
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(b"\xff")

    def test_rejects_invalid_lead_byte_fe(self) -> None:
        # 0xFE cannot start any valid UTF-8 sequence
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(b"\xfe")

    def test_rejects_truncated_2byte_sequence(self) -> None:
        # 0xC2 expects a continuation byte
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(b"\xc2")

    def test_rejects_truncated_3byte_sequence(self) -> None:
        # 0xE2 expects two continuation bytes
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(b"\xe2\x80")

    def test_rejects_truncated_4byte_sequence(self) -> None:
        # 0xF0 expects three continuation bytes
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(b"\xf0\x9f\x98")

    def test_rejects_standalone_continuation_byte(self) -> None:
        # 0x80-0xBF are continuation bytes, invalid as lead bytes
        with pytest.raises(UnicodeDecodeError):
            _ = validate_utf8(b"\x80")


class TestPrepareInput:
    def test_combines_all_preprocessing(self) -> None:
        # BOM + CRLF + valid UTF-8
        data = b'\xef\xbb\xbf{"id": 1}\r\n{"id": 2}\r\n'
        result = prepare_input(data)
        assert result == '{"id": 1}\n{"id": 2}\n'

    def test_handles_unicode_content(self) -> None:
        data = '{"name": "café"}'.encode()
        result = prepare_input(data)
        assert result == '{"name": "café"}'

    def test_rejects_invalid_utf8(self) -> None:
        # Contains overlong encoding (0xC0 0x80 is overlong NUL)
        data = b'{"name": "bad\xc0\x80data"}'
        with pytest.raises(UnicodeDecodeError):
            _ = prepare_input(data)

    def test_empty_input(self) -> None:
        assert prepare_input(b"") == ""


class TestHasUnpairedSurrogates:
    @pytest.mark.parametrize(
        "text",
        [
            "hello world",
            "",
            "hello \U0001f600 world",
            "\U0001f600\U0001f601\U0001f602",
        ],
        ids=[
            "plain-ascii",
            "empty-string",
            "emoji-in-middle",
            "multiple-emojis",
        ],
    )
    def test_valid_text_returns_false(self, text: str) -> None:
        assert has_unpaired_surrogates(text) is False

    @pytest.mark.parametrize(
        "text",
        [
            "hello " + chr(0xD800) + " world",
            "hello " + chr(0xDC00) + " world",
            "hello" + chr(0xD800),
            chr(0xDC00) + "hello",
            chr(0xD800),
            chr(0xDFFF),
        ],
        ids=[
            "lone-high-surrogate-middle",
            "lone-low-surrogate-middle",
            "high-surrogate-at-end",
            "low-surrogate-at-start",
            "only-high-surrogate",
            "only-low-surrogate",
        ],
    )
    def test_unpaired_surrogate_returns_true(self, text: str) -> None:
        assert has_unpaired_surrogates(text) is True
