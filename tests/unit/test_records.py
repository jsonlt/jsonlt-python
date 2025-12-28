from typing import TYPE_CHECKING

import pytest

from jsonlt._constants import MAX_INTEGER_KEY, MIN_INTEGER_KEY
from jsonlt._exceptions import InvalidKeyError, ParseError
from jsonlt._records import (
    build_tombstone,
    extract_key,
    is_tombstone,
    record_size,
    validate_record,
    validate_tombstone,
)

if TYPE_CHECKING:
    from jsonlt._json import JSONObject
    from jsonlt._keys import Key, KeySpecifier


class TestValidateRecordValid:
    @pytest.mark.parametrize(
        ("record", "key_specifier"),
        [
            ({"id": "alice", "name": "Alice"}, "id"),
            ({"id": 42, "name": "Item"}, "id"),
            ({"id": "", "name": "Default"}, "id"),
            ({"id": MAX_INTEGER_KEY, "data": "max"}, "id"),
            ({"id": MIN_INTEGER_KEY, "data": "min"}, "id"),
            ({"id": 0, "data": "zero"}, "id"),
            ({"id": -1, "data": "negative"}, "id"),
        ],
        ids=[
            "string_key",
            "integer_key",
            "empty_string_key",
            "max_integer_key",
            "min_integer_key",
            "zero_key",
            "negative_key",
        ],
    )
    def test_valid_records(
        self, record: "JSONObject", key_specifier: "KeySpecifier"
    ) -> None:
        validate_record(record, key_specifier)  # Should not raise


class TestValidateRecordInvalid:
    @pytest.mark.parametrize(
        ("record", "key_specifier", "match"),
        [
            ({"name": "Alice"}, "id", "missing required key field 'id'"),
            ({"id": None, "name": "Alice"}, "id", "key field 'id' value is null"),
            ({"id": True, "name": "Alice"}, "id", "key field 'id' value is boolean"),
            (
                {"id": {"nested": "value"}, "name": "Alice"},
                "id",
                "key field 'id' value is an object",
            ),
            (
                {"id": [1, 2, 3], "name": "Alice"},
                "id",
                "key field 'id' value is an array",
            ),
            ({"id": 3.14, "name": "Alice"}, "id", "value is not an integer"),
            (
                {"id": MAX_INTEGER_KEY + 1, "name": "Alice"},
                "id",
                "outside valid integer range",
            ),
            (
                {"id": MIN_INTEGER_KEY - 1, "name": "Alice"},
                "id",
                "outside valid integer range",
            ),
            (
                {"id": "alice", "$custom": "value"},
                "id",
                "record contains reserved field",
            ),
            (
                {"id": "alice", "$field1": 1, "$field2": 2},
                "id",
                "record contains reserved",
            ),
        ],
        ids=[
            "missing_key_field",
            "null_key_value",
            "boolean_key_value",
            "object_key_value",
            "array_key_value",
            "fractional_number",
            "integer_above_max",
            "integer_below_min",
            "dollar_prefixed_field",
            "multiple_dollar_prefixed",
        ],
    )
    def test_invalid_records(
        self, record: "JSONObject", key_specifier: "KeySpecifier", match: str
    ) -> None:
        with pytest.raises(InvalidKeyError, match=match):
            validate_record(record, key_specifier)


class TestValidateRecordCompoundKey:
    @pytest.mark.parametrize(
        ("record", "key_specifier"),
        [
            ({"org": "acme", "id": "alice", "name": "Alice"}, ("org", "id")),
            (
                {"region": "us-east", "org": "acme", "id": 1, "name": "Alice"},
                ("region", "org", "id"),
            ),
            ({"org": "acme", "id": 42}, ("org", "id")),
        ],
        ids=["two_field_key", "three_field_key", "mixed_types"],
    )
    def test_valid_compound_keys(
        self, record: "JSONObject", key_specifier: "KeySpecifier"
    ) -> None:
        validate_record(record, key_specifier)  # Should not raise

    @pytest.mark.parametrize(
        ("record", "key_specifier", "match"),
        [
            (
                {"id": "alice", "name": "Alice"},
                ("org", "id"),
                "missing required key field 'org'",
            ),
            (
                {"org": "acme", "name": "Alice"},
                ("org", "id"),
                "missing required key field 'id'",
            ),
            (
                {"org": "acme", "id": None, "name": "Alice"},
                ("org", "id"),
                "key field 'id' value is null",
            ),
        ],
        ids=["missing_first_field", "missing_second_field", "invalid_value"],
    )
    def test_invalid_compound_keys(
        self, record: "JSONObject", key_specifier: "KeySpecifier", match: str
    ) -> None:
        with pytest.raises(InvalidKeyError, match=match):
            validate_record(record, key_specifier)


class TestIsTombstone:
    @pytest.mark.parametrize(
        ("record", "expected"),
        [
            ({"$deleted": True, "id": "alice"}, True),
            ({"$deleted": False, "id": "alice"}, False),
            ({"$deleted": None, "id": "alice"}, False),
            ({"$deleted": "true", "id": "alice"}, False),
            ({"$deleted": 1, "id": "alice"}, False),
            ({"id": "alice", "name": "Alice"}, False),
        ],
        ids=[
            "deleted_true",
            "deleted_false",
            "deleted_null",
            "deleted_string",
            "deleted_one",
            "no_deleted_field",
        ],
    )
    def test_is_tombstone(self, record: "JSONObject", *, expected: bool) -> None:
        assert is_tombstone(record) is expected


class TestValidateTombstoneValid:
    @pytest.mark.parametrize(
        ("record", "key_specifier"),
        [
            ({"$deleted": True, "id": "alice"}, "id"),
            ({"$deleted": True, "org": "acme", "id": "alice"}, ("org", "id")),
            ({"$deleted": True, "id": 42}, "id"),
        ],
        ids=["simple_tombstone", "compound_key_tombstone", "integer_key_tombstone"],
    )
    def test_valid_tombstones(
        self, record: "JSONObject", key_specifier: "KeySpecifier"
    ) -> None:
        validate_tombstone(record, key_specifier)  # Should not raise


class TestValidateTombstoneInvalid:
    @pytest.mark.parametrize(
        ("record", "key_specifier", "error_type", "match"),
        [
            ({"id": "alice"}, "id", ParseError, "tombstone missing \\$deleted field"),
            (
                {"$deleted": False, "id": "alice"},
                "id",
                ParseError,
                r"\$deleted must be true, got false",
            ),
            (
                {"$deleted": None, "id": "alice"},
                "id",
                ParseError,
                r"\$deleted must be true, got null",
            ),
            (
                {"$deleted": "true", "id": "alice"},
                "id",
                ParseError,
                r"\$deleted must be true, got string",
            ),
            (
                {"$deleted": 1, "id": "alice"},
                "id",
                ParseError,
                r"\$deleted must be true, got number",
            ),
            (
                {"$deleted": True},
                "id",
                InvalidKeyError,
                "tombstone missing required key field 'id'",
            ),
            (
                {"$deleted": True, "id": None},
                "id",
                InvalidKeyError,
                "key field 'id' value is null",
            ),
            (
                {"$deleted": {"nested": True}, "id": "alice"},
                "id",
                ParseError,
                r"\$deleted must be true, got dict",
            ),
        ],
        ids=[
            "missing_deleted_field",
            "deleted_false",
            "deleted_null",
            "deleted_string",
            "deleted_number",
            "missing_key_field",
            "invalid_key_value",
            "deleted_dict_type",
        ],
    )
    def test_invalid_tombstones(
        self,
        record: "JSONObject",
        key_specifier: "KeySpecifier",
        error_type: type[Exception],
        match: str,
    ) -> None:
        with pytest.raises(error_type, match=match):
            validate_tombstone(record, key_specifier)


class TestExtractKeyScalar:
    @pytest.mark.parametrize(
        ("record", "key_specifier", "expected"),
        [
            ({"id": "alice", "name": "Alice"}, "id", "alice"),
            ({"id": "", "name": "Default"}, "id", ""),
            ({"id": 42, "data": "value"}, "id", 42),
            ({"id": -100, "data": "value"}, "id", -100),
            ({"id": 0, "data": "value"}, "id", 0),
            ({"id": MAX_INTEGER_KEY, "data": "max"}, "id", MAX_INTEGER_KEY),
            ({"id": MIN_INTEGER_KEY, "data": "min"}, "id", MIN_INTEGER_KEY),
        ],
        ids=[
            "string_key",
            "empty_string_key",
            "positive_integer_key",
            "negative_integer_key",
            "zero_key",
            "max_integer_key",
            "min_integer_key",
        ],
    )
    def test_extract_scalar_key(
        self, record: "JSONObject", key_specifier: str, expected: "Key"
    ) -> None:
        assert extract_key(record, key_specifier) == expected


class TestExtractKeyCompound:
    @pytest.mark.parametrize(
        ("record", "key_specifier", "expected", "expected_type"),
        [
            (
                {"org": "acme", "id": "alice", "name": "Alice"},
                ("org", "id"),
                ("acme", "alice"),
                tuple,
            ),
            (
                {"region": "us-east", "org": "acme", "id": 1, "name": "Alice"},
                ("region", "org", "id"),
                ("us-east", "acme", 1),
                tuple,
            ),
            (
                {"org": "acme", "id": 42, "name": "Widget"},
                ("org", "id"),
                ("acme", 42),
                tuple,
            ),
            # Single-element tuple key specifiers return scalar keys
            ({"id": "alice", "name": "Alice"}, ("id",), "alice", str),
            ({"id": 42, "name": "Widget"}, ("id",), 42, int),
        ],
        ids=[
            "two_element_tuple",
            "three_element_tuple",
            "mixed_type_tuple",
            "single_element_returns_string",
            "single_element_returns_int",
        ],
    )
    def test_extract_compound_key(
        self,
        record: "JSONObject",
        key_specifier: tuple[str, ...],
        expected: "Key",
        expected_type: type,
    ) -> None:
        key = extract_key(record, key_specifier)
        assert key == expected
        assert isinstance(key, expected_type)


class TestExtractKeyErrors:
    @pytest.mark.parametrize(
        ("record", "key_specifier", "match"),
        [
            ({"name": "Alice"}, "id", "missing required key field"),
            ({"id": None, "name": "Alice"}, "id", "key field 'id' value is null"),
            (
                {"org": "acme", "name": "Alice"},
                ("org", "id"),
                "missing required key field 'id'",
            ),
            (
                {"org": "acme", "id": True, "name": "Alice"},
                ("org", "id"),
                "key field 'id' value is boolean",
            ),
            ({"id": "alice"}, (), "key specifier cannot be empty"),
        ],
        ids=[
            "missing_key_field",
            "invalid_key_value",
            "compound_missing_field",
            "compound_invalid_field",
            "empty_key_specifier",
        ],
    )
    def test_extract_key_errors(
        self, record: "JSONObject", key_specifier: "KeySpecifier", match: str
    ) -> None:
        with pytest.raises(InvalidKeyError, match=match):
            _ = extract_key(record, key_specifier)


class TestExtractKeyFloatHandling:
    @pytest.mark.parametrize(
        ("record", "key_specifier", "expected"),
        [
            ({"id": 1.0, "name": "Widget"}, "id", 1),
            ({"id": 1e2, "name": "Widget"}, "id", 100),
            ({"id": -5.0, "name": "Widget"}, "id", -5),
        ],
        ids=["float_whole_number", "float_with_exponent", "negative_float_whole"],
    )
    def test_float_whole_numbers_accepted(
        self, record: "JSONObject", key_specifier: str, expected: int
    ) -> None:
        key = extract_key(record, key_specifier)
        assert key == expected
        assert isinstance(key, int)

    def test_float_fractional_rejected(self) -> None:
        record: JSONObject = {"id": 1.5, "name": "Widget"}
        with pytest.raises(InvalidKeyError, match="value is not an integer"):
            _ = extract_key(record, "id")


class TestExtractKeyInfinityNaN:
    @pytest.mark.parametrize(
        ("record", "key_specifier", "match"),
        [
            ({"id": float("inf"), "name": "Widget"}, "id", "Infinity or NaN"),
            ({"id": float("-inf"), "name": "Widget"}, "id", "Infinity or NaN"),
            ({"id": float("nan"), "name": "Widget"}, "id", "Infinity or NaN"),
        ],
        ids=["positive_infinity", "negative_infinity", "nan"],
    )
    def test_infinity_nan_rejected(
        self, record: "JSONObject", key_specifier: str, match: str
    ) -> None:
        with pytest.raises(InvalidKeyError, match=match):
            _ = extract_key(record, key_specifier)

    @pytest.mark.parametrize(
        ("record", "key_specifier", "match"),
        [
            ({"org": float("inf"), "id": "alice"}, ("org", "id"), "Infinity or NaN"),
            ({"org": "acme", "id": float("nan")}, ("org", "id"), "Infinity or NaN"),
        ],
        ids=["infinity_in_compound_key", "nan_in_compound_key"],
    )
    def test_infinity_nan_rejected_compound(
        self, record: "JSONObject", key_specifier: tuple[str, ...], match: str
    ) -> None:
        with pytest.raises(InvalidKeyError, match=match):
            _ = extract_key(record, key_specifier)


class TestRecordSize:
    @pytest.mark.parametrize(
        ("record", "expected"),
        [
            ({"id": "a"}, 10),  # {"id":"a"}
            ({"id": "alice", "name": "Alice"}, 29),  # {"id":"alice","name":"Alice"}
            ({"id": 42}, 9),  # {"id":42}
            ({"id": "a", "data": {"x": 1}}, 25),  # {"data":{"x":1},"id":"a"}
            ({"id": "café"}, 14),  # {"id":"café"} - é is 2 UTF-8 bytes
            ({}, 2),  # {}
            ({"id": "a", "tags": ["x", "y"]}, 27),  # {"id":"a","tags":["x","y"]}
            ({"id": "a", "active": True}, 24),  # {"active":true,"id":"a"}
            ({"id": "a", "data": None}, 22),  # {"data":null,"id":"a"}
        ],
        ids=[
            "simple",
            "multiple_fields",
            "integer_value",
            "nested_object",
            "unicode",
            "empty",
            "array",
            "boolean",
            "null",
        ],
    )
    def test_record_size(self, record: "JSONObject", expected: int) -> None:
        assert record_size(record) == expected

    def test_record_size_deterministic_key_order(self) -> None:
        record1: JSONObject = {"zebra": 1, "apple": 2}
        record2: JSONObject = {"apple": 2, "zebra": 1}
        assert record_size(record1) == record_size(record2)


class TestBuildTombstoneValid:
    @pytest.mark.parametrize(
        ("key", "key_specifier", "expected"),
        [
            ("alice", "id", {"$deleted": True, "id": "alice"}),
            (42, "id", {"$deleted": True, "id": 42}),
            (
                ("acme", "alice"),
                ("org", "id"),
                {"$deleted": True, "org": "acme", "id": "alice"},
            ),
        ],
        ids=[
            "scalar_string_key",
            "scalar_integer_key",
            "tuple_key_with_tuple_specifier",
        ],
    )
    def test_valid_tombstones(
        self, key: "Key", key_specifier: "KeySpecifier", expected: "JSONObject"
    ) -> None:
        result = build_tombstone(key, key_specifier)
        assert result == expected


class TestBuildTombstoneArityMismatch:
    @pytest.mark.parametrize(
        ("key", "key_specifier", "match"),
        [
            (("a", "b"), "id", "expected scalar key"),
            ("alice", ("org", "id"), "expected tuple"),
            (("a", "b", "c"), ("org", "id"), "expected tuple of 2"),
        ],
        ids=[
            "tuple_key_with_scalar_specifier",
            "scalar_key_with_tuple_specifier",
            "wrong_length_tuple",
        ],
    )
    def test_arity_mismatch(
        self, key: "Key", key_specifier: "KeySpecifier", match: str
    ) -> None:
        with pytest.raises(InvalidKeyError, match=match):
            _ = build_tombstone(key, key_specifier)
