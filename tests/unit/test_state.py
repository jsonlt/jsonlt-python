from typing import TYPE_CHECKING

import pytest

from jsonlt import InvalidKeyError
from jsonlt._state import compute_logical_state

if TYPE_CHECKING:
    from jsonlt._json import JSONObject


class TestComputeLogicalState:
    def test_multiple_records_distinct_keys(self) -> None:
        operations: list[JSONObject] = [
            {"id": "alice", "role": "admin"},
            {"id": "bob", "role": "user"},
            {"id": "carol", "role": "user"},
        ]
        state = compute_logical_state(operations, "id")
        assert len(state) == 3
        assert state["alice"] == {"id": "alice", "role": "admin"}
        assert state["bob"] == {"id": "bob", "role": "user"}
        assert state["carol"] == {"id": "carol", "role": "user"}

    def test_integer_key(self) -> None:
        operations: list[JSONObject] = [
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second"},
        ]
        state = compute_logical_state(operations, "id")
        assert len(state) == 2
        assert state[1] == {"id": 1, "name": "first"}
        assert state[2] == {"id": 2, "name": "second"}

    def test_integer_float_equivalent(self) -> None:
        operations: list[JSONObject] = [
            {"id": 1, "v": 1},
            {"id": 1.0, "v": 2},
        ]
        state = compute_logical_state(operations, "id")
        assert len(state) == 1
        assert state[1] == {"id": 1.0, "v": 2}

    def test_compound_key(self) -> None:
        operations: list[JSONObject] = [
            {"org": "acme", "id": 1, "name": "alice"},
            {"org": "acme", "id": 2, "name": "bob"},
            {"org": "globex", "id": 1, "name": "carol"},
        ]
        state = compute_logical_state(operations, ("org", "id"))
        assert len(state) == 3
        assert state[("acme", 1)] == {"org": "acme", "id": 1, "name": "alice"}
        assert state[("acme", 2)] == {"org": "acme", "id": 2, "name": "bob"}
        assert state[("globex", 1)] == {"org": "globex", "id": 1, "name": "carol"}

    def test_compound_key_delete(self) -> None:
        operations: list[JSONObject] = [
            {"org": "acme", "id": 1, "name": "alice"},
            {"org": "acme", "id": 2, "name": "bob"},
            {"$deleted": True, "org": "acme", "id": 1},
        ]
        state = compute_logical_state(operations, ("org", "id"))
        assert len(state) == 1
        assert state[("acme", 2)] == {"org": "acme", "id": 2, "name": "bob"}

    def test_extra_fields_preserved(self) -> None:
        operations: list[JSONObject] = [
            {
                "id": "alice",
                "role": "admin",
                "email": "alice@example.com",
                "active": True,
            }
        ]
        state = compute_logical_state(operations, "id")
        assert state["alice"] == {
            "id": "alice",
            "role": "admin",
            "email": "alice@example.com",
            "active": True,
        }

    def test_nested_values_preserved(self) -> None:
        operations: list[JSONObject] = [
            {
                "id": "alice",
                "meta": {"tags": ["admin", "active"], "created": "2025-01-01"},
            }
        ]
        state = compute_logical_state(operations, "id")
        assert state["alice"]["meta"] == {
            "tags": ["admin", "active"],
            "created": "2025-01-01",
        }

    def test_missing_key_field_raises(self) -> None:
        operations: list[JSONObject] = [{"name": "alice"}]
        with pytest.raises(InvalidKeyError, match="missing required key field"):
            _ = compute_logical_state(operations, "id")

    def test_single_element_tuple_key(self) -> None:
        operations: list[JSONObject] = [{"id": "alice", "role": "admin"}]
        state = compute_logical_state(operations, ("id",))
        assert state["alice"] == {"id": "alice", "role": "admin"}
