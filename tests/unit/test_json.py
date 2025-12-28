import pytest

from jsonlt._exceptions import LimitError, ParseError
from jsonlt._json import (
    json_nesting_depth,
    parse_json_line,
    serialize_json,
)


class TestJsonNestingDepth:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            # Primitives have depth 1
            (None, 1),
            (True, 1),
            (False, 1),
            (42, 1),
            (3.14, 1),
            ("hello", 1),
            # Empty containers have depth 1
            ({}, 1),
            ([], 1),
            # Containers with primitives have depth 2
            ({"a": 1}, 2),
            ([1, 2, 3], 2),
            # Nested containers
            ({"a": {"b": 1}}, 3),
            ({"a": {"b": {"c": 1}}}, 4),
            ([[1]], 3),
            ([[[1]]], 4),
            # Mixed nesting
            ({"a": [1, 2, 3]}, 3),
            ([{"a": 1}], 3),
            ({"a": [{"b": 1}]}, 4),
        ],
        ids=[
            "null",
            "true",
            "false",
            "int",
            "float",
            "string",
            "empty_dict",
            "empty_list",
            "flat_dict",
            "flat_list",
            "nested_dict_2",
            "nested_dict_3",
            "nested_list_2",
            "nested_list_3",
            "dict_with_list",
            "list_with_dict",
            "mixed_nesting",
        ],
    )
    def test_nesting_depth(self, value: object, expected: int) -> None:
        assert json_nesting_depth(value) == expected

    def test_depth_64_exactly(self) -> None:
        # Create a structure with exactly 64 levels of nesting
        # Start with innermost value
        value: object = 1
        for _ in range(63):  # 63 arrays + 1 inner value = 64 levels
            value = [value]
        assert json_nesting_depth(value) == 64

    def test_depth_65(self) -> None:
        # Create a structure with 65 levels
        value: object = 1
        for _ in range(64):  # 64 arrays + 1 inner value = 65 levels
            value = [value]
        assert json_nesting_depth(value) == 65


class TestParseJsonLine:
    def test_parses_simple_object(self) -> None:
        result = parse_json_line('{"id": 1, "name": "alice"}')
        assert result == {"id": 1, "name": "alice"}

    def test_parses_empty_object(self) -> None:
        result = parse_json_line("{}")
        assert result == {}

    def test_parses_nested_object(self) -> None:
        result = parse_json_line('{"id": 1, "data": {"value": 42}}')
        assert result == {"id": 1, "data": {"value": 42}}

    def test_parses_object_with_array(self) -> None:
        result = parse_json_line('{"id": 1, "items": [1, 2, 3]}')
        assert result == {"id": 1, "items": [1, 2, 3]}

    def test_parses_unicode_content(self) -> None:
        result = parse_json_line('{"name": "café", "emoji": "😀"}')
        assert result == {"name": "café", "emoji": "😀"}

    def test_rejects_invalid_json(self) -> None:
        with pytest.raises(ParseError, match="invalid JSON"):
            _ = parse_json_line('{"id": 1')  # missing closing brace

    def test_rejects_json_array(self) -> None:
        with pytest.raises(ParseError, match="expected JSON object, got list"):
            _ = parse_json_line("[1, 2, 3]")

    def test_rejects_json_string(self) -> None:
        with pytest.raises(ParseError, match="expected JSON object, got str"):
            _ = parse_json_line('"hello"')

    def test_rejects_json_number(self) -> None:
        with pytest.raises(ParseError, match="expected JSON object, got int"):
            _ = parse_json_line("42")

    def test_rejects_json_null(self) -> None:
        with pytest.raises(ParseError, match="expected JSON object, got NoneType"):
            _ = parse_json_line("null")

    def test_rejects_json_boolean(self) -> None:
        with pytest.raises(ParseError, match="expected JSON object, got bool"):
            _ = parse_json_line("true")


class TestDuplicateKeyDetection:
    def test_rejects_duplicate_keys(self) -> None:
        with pytest.raises(ParseError, match="duplicate key: 'id'"):
            _ = parse_json_line('{"id": 1, "id": 2}')

    def test_rejects_duplicate_keys_different_values(self) -> None:
        with pytest.raises(ParseError, match="duplicate key: 'name'"):
            _ = parse_json_line('{"name": "alice", "name": "bob"}')

    def test_rejects_duplicate_keys_same_value(self) -> None:
        # Even if values are the same, duplicate keys are rejected
        with pytest.raises(ParseError, match="duplicate key: 'id'"):
            _ = parse_json_line('{"id": 1, "id": 1}')

    def test_accepts_unique_keys(self) -> None:
        result = parse_json_line('{"id": 1, "name": "alice", "value": 42}')
        assert result == {"id": 1, "name": "alice", "value": 42}

    def test_rejects_duplicate_keys_in_nested_object(self) -> None:
        with pytest.raises(ParseError, match="duplicate key: 'a'"):
            _ = parse_json_line('{"outer": {"a": 1, "a": 2}}')


class TestNestingDepthEnforcement:
    def test_accepts_depth_64(self) -> None:
        # 64 levels: root object (1) + 62 nested arrays (2-63) + innermost value (64)
        # This matches the conformance test format-nesting-depth-64-valid
        json_str = '{"id": 1, "d": ' + "[" * 62 + "1" + "]" * 62 + "}"
        result = parse_json_line(json_str)
        assert result["id"] == 1

    def test_rejects_depth_65(self) -> None:
        # 65 levels: root object (1) + 63 nested arrays (2-64) + innermost value (65)
        json_str = '{"id": 1, "d": ' + "[" * 63 + "1" + "]" * 63 + "}"
        with pytest.raises(LimitError, match="nesting depth 65 exceeds maximum 64"):
            _ = parse_json_line(json_str)

    def test_custom_max_depth(self) -> None:
        json_str = '{"a": {"b": {"c": 1}}}'  # depth 4
        # Should accept with max_depth=4
        result = parse_json_line(json_str, max_depth=4)
        assert result == {"a": {"b": {"c": 1}}}
        # Should reject with max_depth=3
        with pytest.raises(LimitError, match="nesting depth 4 exceeds maximum 3"):
            _ = parse_json_line(json_str, max_depth=3)

    def test_extremely_deep_nesting_raises_limit_error(self) -> None:
        # Create JSON that would cause RecursionError during parsing.
        # Python's default recursion limit is ~1000, so 2000 nested arrays
        # will trigger RecursionError in json.loads before our depth check.
        # This verifies the DoS protection from M7 - we catch RecursionError
        # and convert it to LimitError.
        json_str = '{"d": ' + "[" * 2000 + "1" + "]" * 2000 + "}"
        with pytest.raises(LimitError, match="nesting depth exceeds maximum"):
            _ = parse_json_line(json_str)


class TestSerializeJson:
    def test_sorts_keys_alphabetically(self) -> None:
        value = {"zebra": 1, "apple": 2, "Banana": 3}
        result = serialize_json(value)
        # Unicode code point order: uppercase before lowercase
        assert result == '{"Banana":3,"apple":2,"zebra":1}'

    def test_no_whitespace(self) -> None:
        value = {"id": 1, "name": "alice"}
        result = serialize_json(value)
        assert " " not in result
        assert "\n" not in result
        assert "\t" not in result

    def test_unicode_preserved(self) -> None:
        value = {"name": "café", "emoji": "😀"}
        result = serialize_json(value)
        # ensure_ascii=False means Unicode is preserved
        assert "café" in result
        assert "😀" in result
        # Should not have escaped sequences for these characters
        assert "\\u" not in result

    def test_sorts_nested_keys(self) -> None:
        value = {"outer": {"z": 1, "a": 2}}
        result = serialize_json(value)
        assert result == '{"outer":{"a":2,"z":1}}'

    def test_sorts_keys_in_arrays_of_objects(self) -> None:
        value = {"items": [{"z": 1, "a": 2}]}
        result = serialize_json(value)
        assert result == '{"items":[{"a":2,"z":1}]}'

    def test_empty_object(self) -> None:
        result = serialize_json({})
        assert result == "{}"

    def test_nested_empty_containers(self) -> None:
        value: dict[str, list[object] | dict[str, object]] = {"a": [], "b": {}}
        result = serialize_json(value)
        assert result == '{"a":[],"b":{}}'

    def test_complex_nested_structure(self) -> None:
        value = {
            "z": {"b": 2, "a": 1},
            "a": [{"y": 3, "x": 4}],
        }
        result = serialize_json(value)
        assert result == '{"a":[{"x":4,"y":3}],"z":{"a":1,"b":2}}'


class TestSerializationDeterminism:
    def test_consistent_output_across_calls(self) -> None:
        value = {"zebra": 1, "apple": 2, "Banana": 3}
        result1 = serialize_json(value)
        result2 = serialize_json(value)
        assert result1 == result2

    def test_consistent_for_identical_data(self) -> None:
        # Same data constructed differently should serialize identically
        value1 = {"b": 2, "a": 1}
        value2 = {"a": 1, "b": 2}
        result1 = serialize_json(value1)
        result2 = serialize_json(value2)
        assert result1 == result2

    def test_preserves_value_types(self) -> None:
        value = {
            "null": None,
            "bool": True,
            "int": 42,
            "float": 3.14,
            "string": "hello",
            "array": [1, 2, 3],
            "object": {"nested": True},
        }
        result = serialize_json(value)
        expected = (
            '{"array":[1,2,3],"bool":true,"float":3.14,'
            '"int":42,"null":null,"object":{"nested":true},"string":"hello"}'
        )
        assert result == expected

    def test_escapes_control_characters(self) -> None:
        value = {"text": "hello\nworld\ttab"}
        result = serialize_json(value)
        assert result == '{"text":"hello\\nworld\\ttab"}'

    def test_escapes_backslash(self) -> None:
        value = {"path": "c:\\users\\test"}
        result = serialize_json(value)
        assert result == '{"path":"c:\\\\users\\\\test"}'

    def test_escapes_quotes(self) -> None:
        value = {"quote": 'say "hello"'}
        result = serialize_json(value)
        assert result == '{"quote":"say \\"hello\\""}'
