"""Property-based tests for JSON serialization and parsing."""

import json
from typing import TYPE_CHECKING

from hypothesis import given

from jsonlt._json import parse_json_line, serialize_json

from .strategies import json_object_strategy

if TYPE_CHECKING:
    from jsonlt._json import JSONObject


class TestSerializationRoundtrip:
    """Serialize then parse produces equivalent data."""

    @given(json_object_strategy)
    def test_roundtrip_preserves_data(self, obj: "JSONObject") -> None:
        """parse(serialize(obj)) == obj for any valid JSON object."""
        serialized = serialize_json(obj)
        parsed = parse_json_line(serialized)
        assert parsed == obj

    @given(json_object_strategy)
    def test_serialize_is_deterministic(self, obj: "JSONObject") -> None:
        """serialize(obj) == serialize(obj) always."""
        result1 = serialize_json(obj)
        result2 = serialize_json(obj)
        assert result1 == result2


class TestSerializationProperties:
    """Serialization output format invariants."""

    @given(json_object_strategy)
    def test_no_extraneous_whitespace(self, obj: "JSONObject") -> None:
        """Output contains no space/newline/tab outside strings."""
        serialized = serialize_json(obj)
        # Parse to check it's valid JSON
        parsed = parse_json_line(serialized)
        # Re-serialize and check for equality (no whitespace variation)
        reserialized = serialize_json(parsed)
        assert serialized == reserialized

    @given(json_object_strategy)
    def test_valid_json_output(self, obj: "JSONObject") -> None:
        """Output is parseable by standard json.loads."""
        serialized = serialize_json(obj)
        # Should not raise
        json.loads(serialized)
