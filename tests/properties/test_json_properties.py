import json
from typing import TYPE_CHECKING

from hypothesis import given

from jsonlt._json import parse_json_line, serialize_json

from .strategies import json_object_strategy

if TYPE_CHECKING:
    from jsonlt._json import JSONObject


class TestSerializationRoundtrip:
    @given(json_object_strategy)
    def test_roundtrip_preserves_data(self, obj: "JSONObject") -> None:
        serialized = serialize_json(obj)
        parsed = parse_json_line(serialized)
        assert parsed == obj

    @given(json_object_strategy)
    def test_serialize_is_deterministic(self, obj: "JSONObject") -> None:
        result1 = serialize_json(obj)
        result2 = serialize_json(obj)
        assert result1 == result2


class TestSerializationProperties:
    @given(json_object_strategy)
    def test_no_extraneous_whitespace(self, obj: "JSONObject") -> None:
        serialized = serialize_json(obj)
        # Parse to check it's valid JSON
        parsed = parse_json_line(serialized)
        # Re-serialize and check for equality (no whitespace variation)
        reserialized = serialize_json(parsed)
        assert serialized == reserialized

    @given(json_object_strategy)
    def test_valid_json_output(self, obj: "JSONObject") -> None:
        serialized = serialize_json(obj)
        # Should not raise
        json.loads(serialized)
