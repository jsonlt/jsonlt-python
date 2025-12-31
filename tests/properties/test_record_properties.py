"""Property-based tests for record validation."""

from typing import TYPE_CHECKING

from hypothesis import given, strategies as st

from jsonlt._records import build_tombstone, extract_key, is_tombstone, validate_record

from .strategies import (
    field_name_strategy,
    json_value_strategy,
    key_element_strategy,
    key_specifier_strategy,
    scalar_key_specifier_strategy,
    tuple_key_specifier_strategy,
)

if TYPE_CHECKING:
    from jsonlt._json import JSONObject


class TestValidRecordProperties:
    """Valid records pass validation without exception."""

    @given(
        scalar_key_specifier_strategy,
        key_element_strategy,
        st.dictionaries(field_name_strategy, json_value_strategy, max_size=5),
    )
    def test_valid_scalar_key_record(
        self, key_field: str, key_value: str | int, extra_fields: "JSONObject"
    ) -> None:
        """Records with valid scalar keys pass validation."""
        # Build record with key field and extra data
        record: JSONObject = {
            key_field: key_value,
            **{k: v for k, v in extra_fields.items() if k != key_field},
        }
        validate_record(record, key_field)  # Should not raise

    @given(tuple_key_specifier_strategy, st.data())
    def test_valid_compound_key_record(
        self, key_specifier: tuple[str, ...], data: st.DataObject
    ) -> None:
        """Records with valid compound keys pass validation."""
        # Generate a key value for each field in the specifier
        record: JSONObject = {}
        for field in key_specifier:
            record[field] = data.draw(key_element_strategy)
        validate_record(record, key_specifier)  # Should not raise


class TestExtractKeyProperties:
    """Key extraction invariants."""

    @given(scalar_key_specifier_strategy, key_element_strategy)
    def test_extracted_scalar_key_matches_field(
        self, key_field: str, key_value: str | int
    ) -> None:
        """Extracted key equals the key field value."""
        record: JSONObject = {key_field: key_value}
        extracted = extract_key(record, key_field)
        assert extracted == key_value

    @given(tuple_key_specifier_strategy, st.data())
    def test_extracted_compound_key_matches_fields(
        self, key_specifier: tuple[str, ...], data: st.DataObject
    ) -> None:
        """Extracted compound key is tuple of field values."""
        record: JSONObject = {}
        expected_elements: list[str | int] = []
        for field in key_specifier:
            value: str | int = data.draw(key_element_strategy)
            record[field] = value
            expected_elements.append(value)

        extracted = extract_key(record, key_specifier)
        assert extracted == tuple(expected_elements)


class TestTombstoneProperties:
    """Tombstone detection and construction."""

    @given(key_specifier_strategy, st.data())
    def test_tombstone_detected(
        self, key_specifier: str | tuple[str, ...], data: st.DataObject
    ) -> None:
        """is_tombstone returns True for tombstones."""
        # Build a valid key
        if isinstance(key_specifier, str):
            key: str | int | tuple[str | int, ...] = data.draw(key_element_strategy)
        else:
            key = tuple(data.draw(key_element_strategy) for _ in key_specifier)

        tombstone = build_tombstone(key, key_specifier)
        assert is_tombstone(tombstone) is True

    @given(key_specifier_strategy, st.data())
    def test_build_tombstone_roundtrip(
        self, key_specifier: str | tuple[str, ...], data: st.DataObject
    ) -> None:
        """extract_key(build_tombstone(key, specifier), specifier) == key."""
        # Build a valid key
        if isinstance(key_specifier, str):
            key: str | int | tuple[str | int, ...] = data.draw(key_element_strategy)
        else:
            key = tuple(data.draw(key_element_strategy) for _ in key_specifier)

        tombstone = build_tombstone(key, key_specifier)
        extracted = extract_key(tombstone, key_specifier)
        assert extracted == key
