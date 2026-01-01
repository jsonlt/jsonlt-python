import pytest

from jsonlt._constants import MAX_INTEGER_KEY, MAX_TUPLE_ELEMENTS, MIN_INTEGER_KEY
from jsonlt._keys import (
    compare_keys,
    is_valid_key,
    is_valid_key_element,
    is_valid_key_specifier,
    key_from_json,
    key_length,
    key_specifiers_match,
    normalize_key_specifier,
    serialize_key,
)


class TestIsValidKeyElement:
    @pytest.mark.parametrize(
        "value",
        [
            "alice",
            "",
            42,
            0,
            -100,
            MAX_INTEGER_KEY,
            MIN_INTEGER_KEY,
        ],
        ids=[
            "string",
            "empty_string",
            "positive_int",
            "zero",
            "negative_int",
            "max_int",
            "min_int",
        ],
    )
    def test_valid_key_elements(self, value: str | int) -> None:
        assert is_valid_key_element(value)

    @pytest.mark.parametrize(
        "value",
        [
            MAX_INTEGER_KEY + 1,
            MIN_INTEGER_KEY - 1,
            3.14,
            None,
            True,
            False,
            [1, 2, 3],
            {"a": 1},
        ],
        ids=[
            "above_max_int",
            "below_min_int",
            "float",
            "none",
            "true",
            "false",
            "list",
            "dict",
        ],
    )
    def test_invalid_key_elements(self, value: object) -> None:
        assert not is_valid_key_element(value)


class TestIsValidKey:
    @pytest.mark.parametrize(
        "value",
        [
            "alice",
            "",
            42,
            MAX_INTEGER_KEY,
            MIN_INTEGER_KEY,
            ("a", 1),
            ("a",),
            tuple(f"field{i}" for i in range(MAX_TUPLE_ELEMENTS)),
        ],
        ids=[
            "string",
            "empty_string",
            "integer",
            "max_integer",
            "min_integer",
            "tuple",
            "single_element_tuple",
            "max_element_tuple",
        ],
    )
    def test_valid_keys(self, value: str | int | tuple[str | int, ...]) -> None:
        assert is_valid_key(value)

    @pytest.mark.parametrize(
        "value",
        [
            MAX_INTEGER_KEY + 1,
            MIN_INTEGER_KEY - 1,
            tuple(f"field{i}" for i in range(MAX_TUPLE_ELEMENTS + 1)),
            (),
            ("a", None),
            ("a", True),
            ("a", [1, 2]),
            True,
            False,
            None,
            3.14,
            [1, 2],
            {"a": 1},
        ],
        ids=[
            "int_above_max",
            "int_below_min",
            "tuple_too_many_elements",
            "empty_tuple",
            "tuple_with_none",
            "tuple_with_bool",
            "tuple_with_list",
            "true",
            "false",
            "none",
            "float",
            "list",
            "dict",
        ],
    )
    def test_invalid_keys(self, value: object) -> None:
        assert not is_valid_key(value)


class TestIsValidKeySpecifier:
    @pytest.mark.parametrize(
        "specifier",
        [
            "id",
            "",
            ("org", "id"),
            ("id",),
        ],
        ids=[
            "string",
            "empty_string",
            "tuple_of_strings",
            "single_element_tuple",
        ],
    )
    def test_valid_key_specifiers(self, specifier: str | tuple[str, ...]) -> None:
        assert is_valid_key_specifier(specifier)

    @pytest.mark.parametrize(
        "specifier",
        [
            (),
            ("id", 42),
            (1, 2),
            ("id", "id"),
            ("a", "b", "a"),
            ["id", "name"],
            42,
            None,
        ],
        ids=[
            "empty_tuple",
            "tuple_with_int",
            "tuple_of_ints",
            "duplicate_fields",
            "duplicate_non_adjacent",
            "list",
            "integer",
            "none",
        ],
    )
    def test_invalid_key_specifiers(self, specifier: object) -> None:
        assert not is_valid_key_specifier(specifier)


class TestNormalizeKeySpecifier:
    @pytest.mark.parametrize(
        ("specifier", "expected"),
        [
            ("id", "id"),
            (("id",), "id"),
            (("org", "id"), ("org", "id")),
        ],
        ids=["string_unchanged", "single_tuple_to_string", "multi_tuple_unchanged"],
    )
    def test_normalization(
        self,
        specifier: str | tuple[str, ...],
        expected: str | tuple[str, ...],
    ) -> None:
        assert normalize_key_specifier(specifier) == expected


class TestKeySpecifiersMatch:
    @pytest.mark.parametrize(
        ("a", "b"),
        [
            ("id", "id"),
            ("id", ("id",)),
            (("id",), "id"),
            (("org", "id"), ("org", "id")),
        ],
        ids=[
            "identical_strings",
            "string_matches_single_tuple",
            "single_tuple_matches_string",
            "identical_tuples",
        ],
    )
    def test_matching_specifiers(
        self, a: str | tuple[str, ...], b: str | tuple[str, ...]
    ) -> None:
        assert key_specifiers_match(a, b)

    @pytest.mark.parametrize(
        ("a", "b"),
        [
            ("id", "name"),
            (("org", "id"), ("id", "org")),
            (("id",), ("org", "id")),
            ("id", ("org", "id")),
        ],
        ids=[
            "different_strings",
            "different_order",
            "different_length",
            "string_vs_multi_tuple",
        ],
    )
    def test_non_matching_specifiers(
        self, a: str | tuple[str, ...], b: str | tuple[str, ...]
    ) -> None:
        assert not key_specifiers_match(a, b)


class TestCompareKeys:
    @pytest.mark.parametrize(
        ("a", "b", "expected"),
        [
            # Integer comparisons
            (1, 2, -1),
            (2, 1, 1),
            (-10, -5, -1),
            (-5, -10, 1),
            # String comparisons
            ("alice", "bob", -1),
            ("bob", "alice", 1),
            # Unicode code point ordering: uppercase before lowercase
            ("Alice", "alice", -1),
            ("Zebra", "apple", -1),
            # Tuple element ordering
            (("a", 1), ("a", 2), -1),
            (("a", 2), ("b", 1), -1),
            (("a",), ("a", 1), -1),
            (("a", 1), ("a",), 1),
            # Tuple element type ordering: int < str
            ((1, "a"), ("a", 1), -1),
        ],
        ids=[
            "less_integer",
            "greater_integer",
            "negative_less",
            "negative_greater",
            "less_string",
            "greater_string",
            "uppercase_before_lowercase",
            "code_point_ordering",
            "tuple_element_ordering",
            "tuple_first_element_wins",
            "shorter_tuple_first",
            "longer_tuple_second",
            "tuple_int_before_string",
        ],
    )
    def test_comparison(
        self,
        a: str | int | tuple[str | int, ...],
        b: str | int | tuple[str | int, ...],
        expected: int,
    ) -> None:
        assert compare_keys(a, b) == expected


class TestSerializeKey:
    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("alice", '"alice"'),
            ("", '""'),
            ("hello\nworld", '"hello\\nworld"'),
            ("tab\there", '"tab\\there"'),
            (42, "42"),
            (-100, "-100"),
            (0, "0"),
            (("a", 1), '["a",1]'),
            (("alice", "bob", 123), '["alice","bob",123]'),
        ],
        ids=[
            "string",
            "empty_string",
            "newline_escape",
            "tab_escape",
            "positive_int",
            "negative_int",
            "zero",
            "tuple",
            "tuple_no_spaces",
        ],
    )
    def test_serialization(
        self, key: str | int | tuple[str | int, ...], expected: str
    ) -> None:
        assert serialize_key(key) == expected


class TestKeyLength:
    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            # "alice" = 7 bytes (5 chars + 2 quotes)
            ("alice", 7),
            # "" = 2 bytes (just quotes)
            ("", 2),
            # Integer lengths
            (42, 2),
            (12345, 5),
            (-1, 2),
            (-100, 4),
            # ["a",1] = 7 bytes
            (("a", 1), 7),
            # "café" = 4 chars, é is 2 UTF-8 bytes, + 2 quotes = 7 bytes
            ("café", 7),
            # "\n" in JSON is "\\n" = 4 bytes: quote, backslash, n, quote
            ("\n", 4),
        ],
        ids=[
            "string",
            "empty_string",
            "small_int",
            "larger_int",
            "negative_one",
            "negative_three_digit",
            "tuple",
            "unicode",
            "escaped_char",
        ],
    )
    def test_key_length(
        self, key: str | int | tuple[str | int, ...], expected: int
    ) -> None:
        assert key_length(key) == expected


class TestKeyFromJson:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("alice", "alice"),
            (42, 42),
            (["a", 1], ("a", 1)),
            ([], ()),
        ],
        ids=["string", "integer", "list_to_tuple", "empty_list"],
    )
    def test_conversion(
        self, value: object, expected: str | int | tuple[str | int, ...]
    ) -> None:
        assert key_from_json(value) == expected

    @pytest.mark.parametrize(
        ("value", "match"),
        [
            (True, "bool"),
            (None, "NoneType"),
            ({"a": 1}, "dict"),
            (3.14, "float"),
        ],
        ids=["bool", "none", "dict", "float"],
    )
    def test_invalid_conversion_raises(self, value: object, match: str) -> None:
        with pytest.raises(TypeError, match=match):
            _ = key_from_json(value)

    def test_invalid_list_element_float_raises(self) -> None:
        with pytest.raises(TypeError, match=r"Cannot convert.*key element"):
            _ = key_from_json(["valid", 3.14])

    def test_invalid_list_with_none_element_raises(self) -> None:
        with pytest.raises(TypeError, match=r"Cannot convert.*key element"):
            _ = key_from_json(["valid", None])

    def test_invalid_list_with_bool_element_raises(self) -> None:
        with pytest.raises(TypeError, match=r"Cannot convert.*key element"):
            _ = key_from_json(["valid", True])
