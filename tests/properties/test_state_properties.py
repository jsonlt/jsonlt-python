"""Property-based tests for state computation."""

from typing import TYPE_CHECKING

from hypothesis import given, strategies as st

from jsonlt._records import extract_key, is_tombstone
from jsonlt._state import compute_logical_state

from .strategies import field_name_strategy, json_value_strategy, key_element_strategy

if TYPE_CHECKING:
    from jsonlt._json import JSONObject


class TestStateComputationBasics:
    """Fundamental state computation properties."""

    @given(field_name_strategy)
    def test_empty_sequence_yields_empty_state(self, key_field: str) -> None:
        """Empty operation sequence produces empty state."""
        state = compute_logical_state([], key_field)
        assert state == {}

    @given(
        field_name_strategy,
        key_element_strategy,
        st.dictionaries(field_name_strategy, json_value_strategy, max_size=3),
    )
    def test_single_upsert_yields_single_entry(
        self, key_field: str, key_value: str | int, extra: "JSONObject"
    ) -> None:
        """Single record produces state with one entry."""
        record: JSONObject = {
            key_field: key_value,
            **{k: v for k, v in extra.items() if k != key_field},
        }
        state = compute_logical_state([record], key_field)
        assert len(state) == 1
        assert state[key_value] == record


class TestUpsertProperties:
    """Upsert operation semantics."""

    @given(field_name_strategy, key_element_strategy)
    def test_upsert_idempotent(self, key_field: str, key_value: str | int) -> None:
        """Applying same record twice leaves state unchanged."""
        record: JSONObject = {key_field: key_value, "data": "value"}
        state = compute_logical_state([record, record], key_field)
        assert len(state) == 1
        assert state[key_value] == record

    @given(field_name_strategy, key_element_strategy)
    def test_last_upsert_wins(self, key_field: str, key_value: str | int) -> None:
        """Last record for a key determines final state."""
        record1: JSONObject = {key_field: key_value, "version": 1}
        record2: JSONObject = {key_field: key_value, "version": 2}
        state = compute_logical_state([record1, record2], key_field)
        assert state[key_value] == record2


class TestDeleteProperties:
    """Delete operation semantics."""

    @given(field_name_strategy, key_element_strategy)
    def test_delete_removes_existing(
        self, key_field: str, key_value: str | int
    ) -> None:
        """Tombstone removes key from state."""
        record: JSONObject = {key_field: key_value, "data": "value"}
        tombstone: JSONObject = {"$deleted": True, key_field: key_value}
        state = compute_logical_state([record, tombstone], key_field)
        assert key_value not in state

    @given(field_name_strategy, key_element_strategy)
    def test_delete_nonexistent_is_noop(
        self, key_field: str, key_value: str | int
    ) -> None:
        """Deleting nonexistent key has no effect."""
        tombstone: JSONObject = {"$deleted": True, key_field: key_value}
        state = compute_logical_state([tombstone], key_field)
        assert state == {}

    @given(field_name_strategy, key_element_strategy)
    def test_reinsert_after_delete(self, key_field: str, key_value: str | int) -> None:
        """Record after tombstone re-adds key."""
        record1: JSONObject = {key_field: key_value, "version": 1}
        tombstone: JSONObject = {"$deleted": True, key_field: key_value}
        record2: JSONObject = {key_field: key_value, "version": 2}
        state = compute_logical_state([record1, tombstone, record2], key_field)
        assert state[key_value] == record2


class TestStateInvariants:
    """Invariants that hold for all operation sequences."""

    @given(
        field_name_strategy,
        st.lists(
            st.tuples(
                key_element_strategy,
                st.booleans(),  # is_delete
            ),
            min_size=0,
            max_size=20,
        ),
    )
    def test_state_values_are_not_tombstones(
        self, key_field: str, operations: list[tuple[str | int, bool]]
    ) -> None:
        """All values in state are records, not tombstones."""
        ops: list[JSONObject] = []
        for key_value, is_delete in operations:
            if is_delete:
                ops.append({"$deleted": True, key_field: key_value})
            else:
                ops.append({key_field: key_value, "data": "value"})

        state = compute_logical_state(ops, key_field)

        for value in state.values():
            assert not is_tombstone(value)

    @given(
        field_name_strategy,
        st.lists(
            st.tuples(
                key_element_strategy,
                st.booleans(),  # is_delete
            ),
            min_size=0,
            max_size=20,
        ),
    )
    def test_state_keys_match_record_keys(
        self, key_field: str, operations: list[tuple[str | int, bool]]
    ) -> None:
        """For all (k, v) in state: extract_key(v, ks) == k."""
        ops: list[JSONObject] = []
        for key_value, is_delete in operations:
            if is_delete:
                ops.append({"$deleted": True, key_field: key_value})
            else:
                ops.append({key_field: key_value, "data": "value"})

        state = compute_logical_state(ops, key_field)

        for key, record in state.items():
            assert extract_key(record, key_field) == key
