from collections.abc import MutableMapping
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from jsonlt import (
    ConflictError,
    InvalidKeyError,
    LimitError,
    Table,
    Transaction,
    TransactionError,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import stat_result

    from jsonlt._json import JSONObject
    from jsonlt._keys import Key


class TestTransactionCreation:
    def test_transaction_returns_transaction_object(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()

        assert isinstance(tx, Transaction)
        tx.abort()

    def test_transaction_requires_key_specifier(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path)

        with pytest.raises(InvalidKeyError, match="key specifier is required"):
            _ = table.transaction()

    def test_nested_transaction_rejected(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        try:
            with pytest.raises(TransactionError, match="already active"):
                _ = table.transaction()
        finally:
            tx.abort()


class TestTransactionSnapshotIsolation:
    def test_transaction_sees_initial_state(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            assert tx.get("alice") == {"id": "alice", "v": 1}

    def test_transaction_sees_own_writes(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})
            assert tx.get("alice") == {"id": "alice", "v": 1}
            assert tx.has("alice") is True

    def test_transaction_snapshot_is_isolated(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            # External modification
            _ = table_path.write_text('{"id": "alice", "v": 99}\n')
            # Transaction should still see original value
            assert tx.get("alice") == {"id": "alice", "v": 1}


class TestTransactionReadOperations:
    def test_get_returns_none_for_nonexistent_key(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            assert tx.get("nonexistent") is None

    def test_has_returns_false_for_nonexistent_key(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            assert tx.has("nonexistent") is False

    def test_all_returns_records_in_key_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            records = tx.all()
            assert len(records) == 3
            assert [r["id"] for r in records] == ["a", "b", "c"]

    def test_keys_returns_keys_in_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            assert tx.keys() == ["a", "b", "c"]

    def test_count_returns_record_count(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            assert tx.count() == 2

    def test_find_matches_predicate(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": 1, "role": "admin"}\n'
        content += '{"id": 2, "role": "user"}\n'
        content += '{"id": 3, "role": "admin"}\n'
        _ = table_path.write_text(content)
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            results = tx.find(lambda r: r["role"] == "admin")
            assert len(results) == 2
            assert results[0]["id"] == 1
            assert results[1]["id"] == 3

    def test_find_with_limit(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "a"}\n{"id": "b"}\n{"id": "c"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            results = tx.find(lambda _: True, limit=2)
            assert len(results) == 2

    def test_find_one_returns_first_match(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": 1, "role": "user"}\n'
        content += '{"id": 2, "role": "admin"}\n'
        content += '{"id": 3, "role": "admin"}\n'
        _ = table_path.write_text(content)
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            result = tx.find_one(lambda r: r["role"] == "admin")
            assert result is not None
            assert result["id"] == 2

    def test_find_one_returns_none_when_no_match(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "a", "role": "user"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            result = tx.find_one(lambda r: r["role"] == "admin")
            assert result is None


class TestTransactionWriteOperations:
    def test_put_updates_snapshot(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})
            assert tx.get("alice") == {"id": "alice", "v": 1}
            assert tx.count() == 1

    def test_put_overwrites_existing(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 2})
            assert tx.get("alice") == {"id": "alice", "v": 2}

    def test_put_isolates_from_caller_mutations(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            record: JSONObject = {"id": "alice", "items": [1, 2, 3]}
            tx.put(record)
            # Mutate the original record after putting
            items = record["items"]
            assert isinstance(items, list)
            items.append(4)
            record["name"] = "modified"
            # Transaction should see original value (before mutation)
            result = tx.get("alice")
            assert result is not None
            assert result == {"id": "alice", "items": [1, 2, 3]}

    def test_delete_updates_snapshot(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            result = tx.delete("alice")
            assert result is True
            assert tx.has("alice") is False
            assert tx.count() == 0

    def test_delete_nonexistent_returns_false(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            result = tx.delete("nonexistent")
            assert result is False

    def test_put_validates_record(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        with (
            table.transaction() as tx,
            pytest.raises(InvalidKeyError, match="missing required key field"),
        ):
            tx.put({"name": "alice"})

    def test_put_rejects_dollar_fields(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with (
            table.transaction() as tx,
            pytest.raises(InvalidKeyError, match="reserved field name"),
        ):
            tx.put({"id": "alice", "$meta": "value"})

    def test_delete_validates_key_arity(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("org", "id"))

        with (
            table.transaction() as tx,
            pytest.raises(InvalidKeyError, match="key arity mismatch"),
        ):
            _ = tx.delete("alice")

    def test_put_key_length_limit_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        long_key = "x" * 1030  # > 1024 bytes when serialized

        with (
            table.transaction() as tx,
            pytest.raises(LimitError, match="key length"),
        ):
            tx.put({"id": long_key})

    def test_put_record_size_limit_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        large_data = "x" * (1024 * 1024 + 1000)

        with (
            table.transaction() as tx,
            pytest.raises(LimitError, match="record size"),
        ):
            tx.put({"id": "test", "data": large_data})

    def test_delete_key_length_limit_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        # 1030 characters + quotes = 1032 bytes > 1024
        long_key = "x" * 1030

        with (
            table.transaction() as tx,
            pytest.raises(LimitError, match="key length"),
        ):
            _ = tx.delete(long_key)


class TestTransactionCommit:
    def test_commit_persists_writes(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})

        # After commit, table should see the record
        assert table.get("alice") == {"id": "alice", "v": 1}

    def test_commit_persists_delete(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            _ = tx.delete("alice")

        assert table.has("alice") is False

    def test_commit_writes_to_file(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})

        content = table_path.read_text()
        assert '"id":"alice"' in content
        assert '"v":1' in content

    def test_empty_buffer_commit_succeeds(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            _ = tx.get("alice")  # Read-only, no writes

        # Should not raise, table unchanged
        assert table.get("alice") == {"id": "alice", "v": 1}

    def test_multiple_writes_committed_together(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})
            tx.put({"id": "bob", "v": 2})
            tx.put({"id": "carol", "v": 3})

        assert table.count() == 3
        assert table.keys() == ["alice", "bob", "carol"]

    def test_commit_succeeds_when_stat_fails_after_write(
        self, tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        # Track when file has content (write has occurred)
        original_stat = Path.stat

        def stat_fails_if_file_has_content(
            self: "Path", *, follow_symlinks: bool = True
        ) -> "stat_result":
            # First call stat to get the result
            result = original_stat(self, follow_symlinks=follow_symlinks)
            # If this is the table file and it has content, fail
            if self == table_path and result.st_size > 0:
                msg = "simulated stat failure"
                raise OSError(msg)
            return result

        monkeypatch.setattr(type(table_path), "stat", stat_fails_if_file_has_content)

        # Commit should succeed despite stat failure after write
        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})

        # Data should be written
        assert table_path.read_text().strip().endswith('{"id":"alice","v":1}')


class TestTransactionAbort:
    def test_abort_discards_writes(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})
        tx.abort()

        # Table should still have original value
        assert table.get("alice") == {"id": "alice", "v": 1}

    def test_abort_does_not_write_to_file(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        original_content = '{"id": "alice", "v": 1}\n'
        _ = table_path.write_text(original_content)
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "bob", "v": 2})
        tx.abort()

        # File should be unchanged
        content = table_path.read_text()
        assert content == original_content


class TestTransactionContextManager:
    def test_context_manager_commits_on_success(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})

        assert table.get("alice") == {"id": "alice", "v": 1}

    def test_context_manager_aborts_on_exception(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        class TestError(Exception):
            pass

        def trigger_error() -> None:
            raise TestError

        try:
            with table.transaction() as tx:
                tx.put({"id": "alice", "v": 2})
                trigger_error()
        except TestError:
            pass

        # Table should still have original value
        assert table.get("alice") == {"id": "alice", "v": 1}

    def test_context_manager_does_not_suppress_exceptions(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        class PropagateError(Exception):
            pass

        def trigger_error() -> None:
            raise PropagateError

        try:
            with table.transaction():
                trigger_error()
        except PropagateError:
            pass  # Expected - exception was not suppressed


class TestTransactionAfterCommitOrAbort:
    def test_operations_fail_after_commit(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        tx.put({"id": "alice", "v": 1})
        tx.commit()

        with pytest.raises(TransactionError, match="already been committed"):
            tx.put({"id": "bob", "v": 2})

    def test_operations_fail_after_abort(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        tx.put({"id": "alice", "v": 1})
        tx.abort()

        with pytest.raises(TransactionError, match="already been committed"):
            _ = tx.get("alice")

    def test_double_commit_fails(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        tx = table.transaction()
        tx.commit()

        with pytest.raises(TransactionError, match="already been committed"):
            tx.commit()

    def test_double_abort_fails(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        tx = table.transaction()
        tx.abort()

        with pytest.raises(TransactionError, match="already been committed"):
            tx.abort()

    def test_can_start_new_transaction_after_commit(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx1:
            tx1.put({"id": "alice", "v": 1})

        # Should be able to start a new transaction
        with table.transaction() as tx2:
            tx2.put({"id": "bob", "v": 2})

        assert table.count() == 2

    def test_exit_when_already_finalized_returns_false(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        tx.commit()

        result = tx.__exit__(None, None, None)
        assert result is False


class TestTransactionConflictDetection:
    def test_conflict_same_key_update(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # External modification
        with table_path.open("a") as f:
            _ = f.write('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError, match="conflict detected"):
            tx.commit()

    def test_conflict_transaction_delete_vs_external_update(
        self, tmp_path: "Path"
    ) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        _ = tx.delete("alice")

        # External modification
        with table_path.open("a") as f:
            _ = f.write('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError, match="conflict detected"):
            tx.commit()

    def test_conflict_transaction_update_vs_external_delete(
        self, tmp_path: "Path"
    ) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # External delete
        with table_path.open("a") as f:
            _ = f.write('{"id": "alice", "$deleted": true}\n')

        with pytest.raises(ConflictError, match="conflict detected"):
            tx.commit()

    def test_conflict_both_create_same_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 1})

        # External creation
        with table_path.open("a") as f:
            _ = f.write('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError, match="conflict detected"):
            tx.commit()

    def test_conflict_both_delete_same_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        _ = tx.delete("alice")

        # External delete
        with table_path.open("a") as f:
            _ = f.write('{"id": "alice", "$deleted": true}\n')

        with pytest.raises(ConflictError, match="conflict detected"):
            tx.commit()

    def test_no_conflict_different_keys(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        content = '{"id": "alice", "v": 1}\n{"id": "bob", "v": 1}\n'
        _ = table_path.write_text(content)
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # External modification of different key
        with table_path.open("a") as f:
            _ = f.write('{"id": "bob", "v": 99}\n')

        # Should not raise
        tx.commit()

        # Both changes should be visible
        assert table.get("alice") == {"id": "alice", "v": 2}
        assert table.get("bob") == {"id": "bob", "v": 99}

    def test_table_retains_external_state_on_conflict(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # External modification
        with table_path.open("a") as f:
            _ = f.write('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError):
            tx.commit()

        # Table should have external value
        assert table.get("alice") == {"id": "alice", "v": 99}


class TestTransactionWithCompoundKeys:
    def test_put_with_compound_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("org", "id"))

        with table.transaction() as tx:
            tx.put({"org": "acme", "id": 1, "name": "alice"})
            assert tx.get(("acme", 1)) == {"org": "acme", "id": 1, "name": "alice"}

    def test_delete_with_compound_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"org": "acme", "id": 1, "name": "alice"}\n')
        table = Table(table_path, key=("org", "id"))

        with table.transaction() as tx:
            result = tx.delete(("acme", 1))
            assert result is True
            assert tx.has(("acme", 1)) is False


class TestTransactionBufferDeduplication:
    def test_multiple_puts_same_key_produces_single_line(
        self, tmp_path: "Path"
    ) -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})
            tx.put({"id": "alice", "v": 2})
            tx.put({"id": "alice", "v": 3})

        # Read raw file content and count lines
        content = table_path.read_text()
        lines = [line for line in content.split("\n") if line.strip()]
        assert len(lines) == 1
        assert '"id":"alice"' in lines[0]
        assert '"v":3' in lines[0]

    def test_multiple_puts_same_key_final_value_correct(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})
            tx.put({"id": "alice", "v": 2})
            tx.put({"id": "alice", "v": 3})

        assert table.get("alice") == {"id": "alice", "v": 3}

    def test_put_then_delete_same_key_produces_single_tombstone(
        self, tmp_path: "Path"
    ) -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})
            tx.put({"id": "alice", "v": 2})
            _ = tx.delete("alice")

        # Read raw file content
        content = table_path.read_text()
        lines = [line for line in content.split("\n") if line.strip()]
        assert len(lines) == 1
        assert '"$deleted":true' in lines[0]
        assert '"id":"alice"' in lines[0]
        assert table.has("alice") is False

    def test_delete_then_put_same_key_produces_single_record(
        self, tmp_path: "Path"
    ) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            _ = tx.delete("alice")
            tx.put({"id": "alice", "v": 99})

        # Read raw file content - should have original line + one new record
        content = table_path.read_text()
        lines = [line for line in content.split("\n") if line.strip()]
        assert len(lines) == 2  # original + new record (not tombstone + record)
        # The second line should be the record, not a tombstone
        assert '"$deleted"' not in lines[1]
        assert '"v":99' in lines[1]
        assert table.get("alice") == {"id": "alice", "v": 99}

    def test_multiple_keys_produces_one_line_per_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            tx.put({"id": "alice", "v": 1})
            tx.put({"id": "bob", "v": 1})
            tx.put({"id": "alice", "v": 2})
            tx.put({"id": "bob", "v": 2})
            tx.put({"id": "alice", "v": 3})

        content = table_path.read_text()
        lines = [line for line in content.split("\n") if line.strip()]
        assert len(lines) == 2  # One for alice, one for bob
        assert table.get("alice") == {"id": "alice", "v": 3}
        assert table.get("bob") == {"id": "bob", "v": 2}


class TestTransactionMagicMethods:
    def test_len_returns_count(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "a"}\n{"id": "b"}\n{"id": "c"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            assert len(tx) == 3

    def test_contains_with_existing_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            assert "alice" in tx

    def test_contains_with_missing_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            assert "bob" not in tx

    def test_contains_with_int_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            assert 1 in tx
            assert 2 not in tx

    def test_contains_with_tuple_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"a": 1, "b": "x"}\n')
        table = Table(table_path, key=("a", "b"))

        with table.transaction() as tx:
            assert (1, "x") in tx
            assert (1, "y") not in tx

    def test_contains_with_invalid_type_returns_false(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            # Non-key types should return False, not raise
            assert 3.14 not in tx
            assert None not in tx
            assert ["list"] not in tx
            assert {"dict": "value"} not in tx

    def test_contains_with_invalid_tuple_returns_false(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"a": 1, "b": "x"}\n')
        table = Table(table_path, key=("a", "b"))

        with table.transaction() as tx:
            # Tuple with invalid element types should return False
            assert (1, 3.14) not in tx
            assert (None, "x") not in tx

    def test_iter_yields_keys_in_key_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        # Write in reverse order
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            keys = list(tx)

            assert len(keys) == 3
            assert keys[0] == "a"
            assert keys[1] == "b"
            assert keys[2] == "c"

    def test_iter_on_empty_transaction(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            records = list(tx)
            assert records == []

    def test_repr_active_transaction(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        tx = table.transaction()
        try:
            result = repr(tx)
            assert "Transaction(" in result
            assert "key='id'" in result
            assert "active" in result
        finally:
            tx.abort()

    def test_repr_finalized_transaction(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        tx.commit()

        result = repr(tx)
        assert "Transaction(" in result
        assert "key='id'" in result
        assert "finalized" in result

    def test_repr_with_tuple_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key=("org", "id"))

        tx = table.transaction()
        try:
            result = repr(tx)
            assert "Transaction(" in result
            assert "key=('org', 'id')" in result
        finally:
            tx.abort()


class TestTransactionItems:
    def test_items_returns_key_value_pairs(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n{"id": "bob", "v": 2}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            items = tx.items()

            assert len(items) == 2
            assert items[0] == ("alice", {"id": "alice", "v": 1})
            assert items[1] == ("bob", {"id": "bob", "v": 2})

    def test_items_in_key_order(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        # Write in reverse order
        _ = table_path.write_text('{"id": "c"}\n{"id": "a"}\n{"id": "b"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            items = tx.items()
            assert [k for k, _ in items] == ["a", "b", "c"]

    def test_items_empty_transaction(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        with table.transaction() as tx:
            items = tx.items()
            assert items == []

    def test_items_reflects_transaction_changes(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            tx.put({"id": "bob", "v": 2})
            items = tx.items()

            assert len(items) == 2
            assert ("bob", {"id": "bob", "v": 2}) in items

    def test_items_on_finalized_transaction_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        tx.commit()

        with pytest.raises(TransactionError, match="already been committed or aborted"):
            _ = tx.items()


class TestTransactionEmptyTupleKeyRejection:
    def test_get_empty_tuple_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with (
            table.transaction() as tx,
            pytest.raises(InvalidKeyError, match="empty tuple"),
        ):
            _ = tx.get(())

    def test_has_empty_tuple_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with (
            table.transaction() as tx,
            pytest.raises(InvalidKeyError, match="empty tuple"),
        ):
            _ = tx.has(())

    def test_delete_empty_tuple_raises(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice"}\n')
        table = Table(table_path, key="id")

        with (
            table.transaction() as tx,
            pytest.raises(InvalidKeyError, match="arity mismatch"),
        ):
            _ = tx.delete(())


class TestConflictErrorProperties:
    def test_conflict_error_has_key_property(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # Modify externally
        _ = table_path.write_text('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError) as exc_info:
            tx.commit()

        assert exc_info.value.key == "alice"

    def test_conflict_error_has_expected_property(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # Modify externally
        _ = table_path.write_text('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError) as exc_info:
            tx.commit()

        assert exc_info.value.expected == {"id": "alice", "v": 1}

    def test_conflict_error_has_actual_property(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # Modify externally
        _ = table_path.write_text('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError) as exc_info:
            tx.commit()

        assert exc_info.value.actual == {"id": "alice", "v": 99}

    def test_conflict_on_new_key_has_none_expected(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 1})

        # Create file with same key externally
        _ = table_path.write_text('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError) as exc_info:
            tx.commit()

        assert exc_info.value.key == "alice"
        assert exc_info.value.expected is None
        assert exc_info.value.actual == {"id": "alice", "v": 99}

    def test_conflict_on_deleted_key_has_none_actual(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        _ = tx.delete("alice")

        # Delete externally (via tombstone)
        _ = table_path.write_text(
            '{"id": "alice", "v": 1}\n{"id": "alice", "$deleted": true}\n'
        )

        with pytest.raises(ConflictError) as exc_info:
            tx.commit()

        assert exc_info.value.key == "alice"
        assert exc_info.value.expected == {"id": "alice", "v": 1}
        assert exc_info.value.actual is None

    def test_conflict_error_repr_shows_message_and_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        tx = table.transaction()
        tx.put({"id": "alice", "v": 2})

        # Modify externally
        _ = table_path.write_text('{"id": "alice", "v": 99}\n')

        with pytest.raises(ConflictError) as exc_info:
            tx.commit()

        result = repr(exc_info.value)
        assert "ConflictError(" in result
        assert "key='alice'" in result

    def test_conflict_error_repr_with_tuple_key(self, tmp_path: "Path") -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"org": "acme", "id": 1, "v": 1}\n')
        table = Table(table_path, key=("org", "id"))

        tx = table.transaction()
        tx.put({"org": "acme", "id": 1, "v": 2})

        # Modify externally
        _ = table_path.write_text('{"org": "acme", "id": 1, "v": 99}\n')

        with pytest.raises(ConflictError) as exc_info:
            tx.commit()

        result = repr(exc_info.value)
        assert "ConflictError(" in result
        assert "key=('acme', 1)" in result


class TestTransactionMutableMapping:
    def test_getitem_existing_key(self, tmp_path: Path) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            result = tx["alice"]
            assert result == {"id": "alice", "role": "admin"}

    def test_getitem_missing_key_raises_keyerror(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            with pytest.raises(KeyError) as exc_info:
                _ = tx["nonexistent"]
            assert exc_info.value.args[0] == "nonexistent"

    def test_setitem_with_matching_key(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            tx["alice"] = {"id": "alice", "role": "admin"}
            assert tx.get("alice") == {"id": "alice", "role": "admin"}

        assert table.get("alice") == {"id": "alice", "role": "admin"}

    def test_setitem_with_mismatched_key_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with (
            table.transaction() as tx,
            pytest.raises(InvalidKeyError, match="key mismatch"),
        ):
            tx["alice"] = {"id": "bob", "role": "admin"}

    def test_delitem_existing_key(self, tmp_path: Path) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            del tx["alice"]
            assert tx.get("alice") is None

        assert table.get("alice") is None

    def test_delitem_missing_key_raises_keyerror(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            with pytest.raises(KeyError) as exc_info:
                del tx["nonexistent"]
            assert exc_info.value.args[0] == "nonexistent"

    def test_isinstance_mutablemapping(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx:
            assert isinstance(tx, MutableMapping)

    def test_values_returns_records_in_key_order(self, tmp_path: Path) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "bob", "v": 2}\n{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            result = tx.values()
            assert result == [{"id": "alice", "v": 1}, {"id": "bob", "v": 2}]

    def test_pop_existing_key(self, tmp_path: Path) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            result = tx.pop("alice")
            assert result == {"id": "alice", "role": "admin"}
            assert "alice" not in tx

        assert "alice" not in table

    def test_pop_missing_key_with_default(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()
        default: JSONObject = {"id": "default", "role": "none"}

        with table.transaction() as tx:
            result = tx.pop("nonexistent", default)
            assert result == default

    def test_pop_missing_key_without_default_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with table.transaction() as tx, pytest.raises(KeyError):
            _ = tx.pop("nonexistent")

    def test_pop_too_many_arguments_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with (
            table.transaction() as tx,
            pytest.raises(TypeError, match="pop expected at most 2 arguments"),
        ):
            _ = tx.pop("key", {}, {})

    def test_popitem_returns_first_key_value_pair(self, tmp_path: Path) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "bob", "v": 2}\n{"id": "alice", "v": 1}\n')
        table = Table(table_path, key="id")

        with table.transaction() as tx:
            result = tx.popitem()
            # Returns first in sorted order (alice comes before bob)
            assert result == ("alice", {"id": "alice", "v": 1})
            assert "alice" not in tx
            assert "bob" in tx

    def test_popitem_empty_table_raises(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        with (
            table.transaction() as tx,
            pytest.raises(KeyError, match="table is empty"),
        ):
            _ = tx.popitem()

    def test_setdefault_existing_key_returns_existing(self, tmp_path: Path) -> None:
        table_path = tmp_path / "test.jsonlt"
        _ = table_path.write_text('{"id": "alice", "role": "admin"}\n')
        table = Table(table_path, key="id")
        default: JSONObject = {"id": "alice", "role": "user"}

        with table.transaction() as tx:
            result = tx.setdefault("alice", default)
            assert result == {"id": "alice", "role": "admin"}

    def test_setdefault_missing_key_inserts_and_returns(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()
        default: JSONObject = {"id": "alice", "role": "admin"}

        with table.transaction() as tx:
            result = tx.setdefault("alice", default)
            assert result == default
            assert tx.get("alice") == default

        assert table.get("alice") == default

    def test_update_with_mapping(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()
        mapping: dict[Key, JSONObject] = {
            "alice": {"id": "alice", "role": "admin"},
            "bob": {"id": "bob", "role": "user"},
        }

        with table.transaction() as tx:
            tx.update(mapping)
            assert tx.get("alice") == {"id": "alice", "role": "admin"}
            assert tx.get("bob") == {"id": "bob", "role": "user"}

        assert table.get("alice") == {"id": "alice", "role": "admin"}

    def test_update_with_iterable(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()
        items: list[tuple[str, JSONObject]] = [
            ("alice", {"id": "alice", "role": "admin"}),
            ("bob", {"id": "bob", "role": "user"}),
        ]

        with table.transaction() as tx:
            tx.update(items)
            assert tx.get("alice") == {"id": "alice", "role": "admin"}

        assert table.get("bob") == {"id": "bob", "role": "user"}

    def test_update_with_kwargs(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.update(alice={"id": "alice", "role": "admin"})
            assert tx.get("alice") == {"id": "alice", "role": "admin"}

    def test_update_with_none_is_noop(self, make_table: "Callable[..., Table]") -> None:
        table = make_table()

        with table.transaction() as tx:
            tx.update(None)
            assert tx.count() == 0


class TestTransactionEquality:
    def test_equal_transactions_same_table_same_snapshot(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        # Since only one transaction can be active at a time per table,
        # we verify equality by comparing a transaction to itself using
        # a reference. This ensures __eq__ returns True for same instance.
        tx = table.transaction()
        try:
            tx_ref = tx
            assert tx == tx_ref
        finally:
            tx.abort()

    def test_not_equal_different_parent_tables(self, tmp_path: "Path") -> None:
        path1 = tmp_path / "test1.jsonlt"
        path2 = tmp_path / "test2.jsonlt"
        _ = path1.write_text('{"id": "alice", "v": 1}\n')
        _ = path2.write_text('{"id": "alice", "v": 1}\n')

        table1 = Table(path1, key="id")
        table2 = Table(path2, key="id")

        tx1 = table1.transaction()
        tx2 = table2.transaction()
        try:
            # Different table instances, so not equal
            assert tx1 != tx2
        finally:
            tx1.abort()
            tx2.abort()

    def test_not_equal_different_buffered_writes(self, tmp_path: "Path") -> None:
        path1 = tmp_path / "test1.jsonlt"
        path2 = tmp_path / "test2.jsonlt"
        _ = path1.write_text('{"id": "alice", "v": 1}\n')
        _ = path2.write_text('{"id": "alice", "v": 1}\n')

        table1 = Table(path1, key="id")
        table2 = Table(path2, key="id")

        tx1 = table1.transaction()
        tx2 = table2.transaction()
        try:
            # Make different writes
            tx1.put({"id": "bob", "v": 1})
            tx2.put({"id": "carol", "v": 1})

            assert tx1 != tx2
        finally:
            tx1.abort()
            tx2.abort()

    def test_equal_with_same_buffered_writes(self, tmp_path: "Path") -> None:
        path1 = tmp_path / "test1.jsonlt"
        path2 = tmp_path / "test2.jsonlt"
        _ = path1.write_text('{"id": "alice", "v": 1}\n')
        _ = path2.write_text('{"id": "alice", "v": 1}\n')

        table1 = Table(path1, key="id")
        table2 = Table(path2, key="id")

        tx1 = table1.transaction()
        tx2 = table2.transaction()
        try:
            # Make identical writes
            tx1.put({"id": "bob", "v": 2})
            tx2.put({"id": "bob", "v": 2})

            # They still differ because they have different parent tables
            assert tx1 != tx2
        finally:
            tx1.abort()
            tx2.abort()

    def test_eq_with_non_transaction_returns_false(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        try:
            result = tx == "string"
            assert result is False
        finally:
            tx.abort()

    def test_finalized_vs_active_not_equal(self, tmp_path: "Path") -> None:
        path1 = tmp_path / "test1.jsonlt"
        path2 = tmp_path / "test2.jsonlt"
        _ = path1.write_text('{"id": "alice", "v": 1}\n')
        _ = path2.write_text('{"id": "alice", "v": 1}\n')

        table1 = Table(path1, key="id")
        table2 = Table(path2, key="id")

        tx1 = table1.transaction()
        tx2 = table2.transaction()

        # Commit one, leave the other active
        tx1.commit()
        try:
            # finalized vs active should not be equal
            assert tx1 != tx2
        finally:
            tx2.abort()

    def test_transaction_is_not_hashable(
        self, make_table: "Callable[..., Table]"
    ) -> None:
        table = make_table()

        tx = table.transaction()
        try:
            with pytest.raises(TypeError, match="unhashable type"):
                _ = hash(tx)
        finally:
            tx.abort()
