from typing import TYPE_CHECKING

import pytest

from jsonlt._exceptions import ParseError
from jsonlt._header import Header, is_header_line, parse_header, serialize_header

if TYPE_CHECKING:
    from jsonlt._json import JSONObject
    from jsonlt._keys import KeySpecifier


class TestIsHeaderLine:
    @pytest.mark.parametrize(
        ("record", "expected"),
        [
            ({"$jsonlt": {"version": 1}}, True),
            ({"id": "alice", "name": "Alice"}, False),
            ({"jsonlt": {"version": 1}}, False),  # Missing $
            ({}, False),
        ],
        ids=["with_jsonlt_field", "regular_record", "missing_dollar", "empty_object"],
    )
    def test_is_header_line(self, record: "JSONObject", *, expected: bool) -> None:
        assert is_header_line(record) is expected


class TestParseHeaderMinimal:
    def test_version_only(self) -> None:
        header = parse_header({"$jsonlt": {"version": 1}})

        assert header.version == 1
        assert header.key is None
        assert header.schema_url is None
        assert header.schema is None
        assert header.meta is None


class TestParseHeaderWithKey:
    @pytest.mark.parametrize(
        ("header", "expected"),
        [
            ({"$jsonlt": {"version": 1, "key": "id"}}, "id"),
            ({"$jsonlt": {"version": 1, "key": ""}}, ""),
            ({"$jsonlt": {"version": 1, "key": ["id"]}}, ("id",)),
            ({"$jsonlt": {"version": 1, "key": ["org", "id"]}}, ("org", "id")),
            (
                {"$jsonlt": {"version": 1, "key": ["region", "org", "id"]}},
                ("region", "org", "id"),
            ),
        ],
        ids=[
            "string_key",
            "empty_string_key",
            "single_element_array",
            "compound_key",
            "triple_compound_key",
        ],
    )
    def test_key_parsing(self, header: "JSONObject", expected: "KeySpecifier") -> None:
        result = parse_header(header)
        assert result.key == expected


class TestParseHeaderWithSchema:
    def test_schema_url(self) -> None:
        header = parse_header(
            {"$jsonlt": {"version": 1, "$schema": "https://example.com/schema.json"}}
        )
        assert header.schema_url == "https://example.com/schema.json"
        assert header.schema is None

    def test_inline_schema(self) -> None:
        header = parse_header(
            {
                "$jsonlt": {
                    "version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                    },
                }
            }
        )
        assert header.schema_url is None
        assert header.schema == {
            "type": "object",
            "properties": {"id": {"type": "string"}},
        }

    def test_schema_url_and_inline_mutually_exclusive(self) -> None:
        with pytest.raises(ParseError, match="mutually exclusive"):
            _ = parse_header(
                {
                    "$jsonlt": {
                        "version": 1,
                        "$schema": "https://example.com/schema.json",
                        "schema": {"type": "object"},
                    }
                }
            )


class TestParseHeaderWithMeta:
    @pytest.mark.parametrize(
        ("header", "expected"),
        [
            (
                {"$jsonlt": {"version": 1, "meta": {"created": "2025-01-15"}}},
                {"created": "2025-01-15"},
            ),
            (
                {
                    "$jsonlt": {
                        "version": 1,
                        "meta": {
                            "author": "Alice",
                            "tags": ["test"],
                            "nested": {"a": 1},
                        },
                    }
                },
                {"author": "Alice", "tags": ["test"], "nested": {"a": 1}},
            ),
        ],
        ids=["simple_meta", "complex_meta"],
    )
    def test_meta_parsing(self, header: "JSONObject", expected: "JSONObject") -> None:
        result = parse_header(header)
        assert result.meta == expected


class TestParseHeaderVersionErrors:
    @pytest.mark.parametrize(
        ("header", "match"),
        [
            ({"$jsonlt": {"key": "id"}}, "missing required 'version' field"),
            ({"$jsonlt": {"version": "1"}}, "version must be an integer"),
            ({"$jsonlt": {"version": 1.0}}, "version must be an integer"),
            ({"$jsonlt": {"version": True}}, "version must be an integer"),
            ({"$jsonlt": {"version": 0}}, "unsupported version 0"),
            ({"$jsonlt": {"version": 2}}, "unsupported version 2"),
            ({"$jsonlt": {"version": -1}}, "unsupported version -1"),
        ],
        ids=[
            "missing_version",
            "version_string",
            "version_float",
            "version_boolean",
            "version_zero",
            "version_two",
            "version_negative",
        ],
    )
    def test_version_errors(self, header: "JSONObject", match: str) -> None:
        with pytest.raises(ParseError, match=match):
            _ = parse_header(header)


class TestParseHeaderStructureErrors:
    @pytest.mark.parametrize(
        ("header", "match"),
        [
            ({"$jsonlt": "not an object"}, r"\$jsonlt value must be an object"),
            ({"$jsonlt": [1, 2, 3]}, r"\$jsonlt value must be an object"),
            ({"$jsonlt": None}, r"\$jsonlt value must be an object"),
        ],
        ids=["string", "array", "null"],
    )
    def test_jsonlt_structure_errors(self, header: "JSONObject", match: str) -> None:
        with pytest.raises(ParseError, match=match):
            _ = parse_header(header)


class TestParseHeaderKeyErrors:
    @pytest.mark.parametrize(
        ("header", "match"),
        [
            (
                {"$jsonlt": {"version": 1, "key": 42}},
                "key specifier must be a string or array",
            ),
            (
                {"$jsonlt": {"version": 1, "key": []}},
                "key specifier cannot be an empty array",
            ),
            (
                {"$jsonlt": {"version": 1, "key": ["id", 42]}},
                "must contain only strings",
            ),
            (
                {"$jsonlt": {"version": 1, "key": ["id", "name", "id"]}},
                "duplicate field names",
            ),
        ],
        ids=["integer_key", "empty_array", "array_with_non_string", "duplicate_fields"],
    )
    def test_key_errors(self, header: "JSONObject", match: str) -> None:
        with pytest.raises(ParseError, match=match):
            _ = parse_header(header)


class TestParseHeaderKeyTupleLimit:
    def test_max_tuple_elements_accepted(self) -> None:
        # 16 elements is the maximum allowed
        fields = [f"field{i}" for i in range(16)]
        header = parse_header(
            {"$jsonlt": {"version": 1, "key": fields}}  # pyright: ignore[reportArgumentType]
        )
        assert header.key == tuple(fields)

    def test_exceeds_max_tuple_elements_rejected(self) -> None:
        # 17 elements exceeds the limit
        fields = [f"field{i}" for i in range(17)]
        with pytest.raises(ParseError, match="exceeds maximum of 16 elements"):
            _ = parse_header(
                {"$jsonlt": {"version": 1, "key": fields}}  # pyright: ignore[reportArgumentType]
            )


class TestParseHeaderSchemaErrors:
    @pytest.mark.parametrize(
        ("header", "match"),
        [
            ({"$jsonlt": {"version": 1, "$schema": 123}}, r"\$schema must be a string"),
            (
                {"$jsonlt": {"version": 1, "schema": "not an object"}},
                "schema must be an object",
            ),
            (
                {"$jsonlt": {"version": 1, "schema": [1, 2, 3]}},
                "schema must be an object",
            ),
        ],
        ids=["schema_url_not_string", "inline_schema_string", "inline_schema_array"],
    )
    def test_schema_errors(self, header: "JSONObject", match: str) -> None:
        with pytest.raises(ParseError, match=match):
            _ = parse_header(header)


class TestParseHeaderMetaErrors:
    @pytest.mark.parametrize(
        ("header", "match"),
        [
            (
                {"$jsonlt": {"version": 1, "meta": "not an object"}},
                "meta must be an object",
            ),
            ({"$jsonlt": {"version": 1, "meta": ["a", "b"]}}, "meta must be an object"),
            ({"$jsonlt": {"version": 1, "meta": 42}}, "meta must be an object"),
        ],
        ids=["string", "array", "number"],
    )
    def test_meta_errors(self, header: "JSONObject", match: str) -> None:
        with pytest.raises(ParseError, match=match):
            _ = parse_header(header)


class TestHeaderEquality:
    @pytest.mark.parametrize(
        ("h1", "h2", "expected"),
        [
            (Header(version=1, key="id"), Header(version=1, key="id"), True),
            (Header(version=1), Header(version=1, key="id"), False),
            (Header(version=1, key="id"), Header(version=1, key="name"), False),
            (Header(version=1, key="id"), Header(version=1, key=("id",)), False),
        ],
        ids=[
            "equal_headers",
            "different_key_presence",
            "different_keys",
            "tuple_vs_string",
        ],
    )
    def test_header_equality(self, h1: Header, h2: Header, *, expected: bool) -> None:
        assert (h1 == h2) is expected


class TestSerializeHeader:
    def test_minimal_header(self) -> None:
        """Header with only version."""
        header = Header(version=1)
        result = serialize_header(header)
        assert result == '{"$jsonlt":{"version":1}}'

    def test_header_with_string_key(self) -> None:
        """Header with string key specifier."""
        header = Header(version=1, key="id")
        result = serialize_header(header)
        assert '"key":"id"' in result

    def test_header_with_tuple_key(self) -> None:
        """Header with tuple key specifier (becomes array)."""
        header = Header(version=1, key=("org", "id"))
        result = serialize_header(header)
        assert '"key":["org","id"]' in result

    def test_header_with_schema_url(self) -> None:
        """Header with $schema URL."""
        header = Header(version=1, schema_url="https://example.com/schema.json")
        result = serialize_header(header)
        assert '"$schema":"https://example.com/schema.json"' in result

    def test_header_with_inline_schema(self) -> None:
        """Header with inline schema object."""
        header = Header(version=1, schema={"type": "object"})
        result = serialize_header(header)
        assert '"schema":{"type":"object"}' in result

    def test_header_with_meta(self) -> None:
        """Header with meta object."""
        header = Header(version=1, meta={"author": "test"})
        result = serialize_header(header)
        assert '"meta":{"author":"test"}' in result
