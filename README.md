# jsonlt

<!-- vale off -->
[![PyPI](https://img.shields.io/pypi/v/jsonlt-python)](https://pypi.org/project/jsonlt-python/)
[![CI](https://github.com/jsonlt/jsonlt-python/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jsonlt/jsonlt-python/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/jsonlt/jsonlt-python/branch/main/graph/badge.svg)](https://codecov.io/gh/jsonlt/jsonlt-python)
[![CodSpeed Badge](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/jsonlt/jsonlt-python)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
<!-- vale on -->

The Python reference implementation of [JSONLT (JSON Lines Table)](https://jsonlt.org), a data format for storing keyed records in append-only files. JSONLT builds on [JSON Lines](https://jsonlines.org/) and optimizes for version control. Modifications append new lines rather than rewriting existing content, producing clean and meaningful diffs.

> [!NOTE]
> This package is under active development. The API may change before the 1.0 release.

## Resources

- [Specification](https://spec.jsonlt.org): formal definition of the JSONLT format
- [Documentation](https://docs.jsonlt.org): guides, tutorials, and API reference
- [Conformance tests](https://github.com/jsonlt/jsonlt/tree/main/conformance): language-agnostic test suite

## Installation

```bash
pip install jsonlt-python

# Or

uv add jsonlt-python
```

Requires Python 3.10 or later.

## Quick start

```python
from jsonlt import Table

# Create a table with initial records
table = Table.from_records(
    "users.jsonlt",
    [
        {"id": "alice", "role": "admin", "email": "alice@example.com"},
        {"id": "bob", "role": "user", "email": "bob@example.com"},
    ],
    key="id",
)

# Read records
user = table.get("alice")  # Returns the record or None
exists = table.has("bob")  # Returns True

# Update a record
table.put({"id": "alice", "role": "admin", "email": "alice@newdomain.com"})

# Delete records (appends a tombstone)
table.delete("bob")

# Later, load the existing table
table = Table.from_file("users.jsonlt")

# Iterate over all records
for record in table.all():
    print(record)
```

The underlying file after these operations:

```jsonl
{"$jsonlt":{"key":"id","version":1}}
{"id":"alice","email":"alice@example.com","role":"admin"}
{"id":"bob","email":"bob@example.com","role":"user"}
{"id":"alice","email":"alice@newdomain.com","role":"admin"}
{"id":"bob","$deleted":true}
```

## When to use JSONLT

JSONLT works well for configuration, metadata, and small-to-medium datasets where you want human-readable files that play nicely with Git. It's a good fit when you need keyed record storage but don't want the overhead of a database, and when you want to see exactly what changed in a pull request. The append-only design supports safe concurrent access from multiple processes.

JSONLT is not a database. For large datasets, high write throughput, or complex queries, consider SQLite or a proper database.

## Compound keys

JSONLT supports multi-field compound keys for composite identifiers:

```python
orders = Table.from_records(
    "orders.jsonlt",
    [
        {"customer_id": "alice", "order_id": 1, "total": 99.99},
        {"customer_id": "alice", "order_id": 2, "total": 149.99},
    ],
    key=("customer_id", "order_id"),
)

order = orders.get(("alice", 1))
```

## Transactions

Transactions provide snapshot isolation and atomic writes with conflict detection:

```python
from jsonlt import Table, ConflictError

table = Table("counters.jsonlt", key="name")

with table.transaction() as tx:
    counter = tx.get("visits")
    new_count = (counter["count"] + 1) if counter else 1
    tx.put({"name": "visits", "count": new_count})
# Commits automatically; rolls back on exception

# Handle concurrent modification conflicts
try:
    with table.transaction() as tx:
        tx.put({"name": "counter", "value": 42})
except ConflictError as e:
    print(f"Conflict on key: {e.key}")
```

## Dictionary-like access

Tables can be used like dictionaries:

```python
# Get a record (raises KeyError if not found)
user = table["alice"]

# Set a record (key in record must match)
table["alice"] = {"id": "alice", "role": "admin"}

# Delete a record
del table["bob"]

# Check membership
if "alice" in table:
    print("Found alice")

# Iterate over keys
for key in table:
    print(key)

# Get record count
print(len(table))
```

Methods like `pop()`, `setdefault()`, and `update()` also work. The `keys()`, `values()`, and `items()` methods return sorted lists rather than views to maintain JSONLT's deterministic key ordering.

## Equality

Tables support value-based equality comparison:

```python
table1 = Table("users.jsonlt", key="id")
table2 = Table("users.jsonlt", key="id")

# Equal if same path, key specifier, and records
if table1 == table2:
    print("Tables have identical content")
```

Two tables are equal when they have the same resolved path, key specifier, and record state. Transactions are equal when they reference the same parent table instance and have identical snapshot state.

Tables and transactions are mutable and therefore not hashable (cannot be used as dictionary keys or in sets).

## Finding records

```python
# Find all records matching a predicate
expensive = table.find(lambda r: r.get("price", 0) > 100)

# Find with limit
top_3 = table.find(lambda r: r.get("in_stock"), limit=3)

# Find the first match
first = table.find_one(lambda r: r.get("category") == "electronics")
```

## Maintenance

```python
# Compact the file (removes tombstones and superseded records)
table.compact()

# Clear all records
table.clear()

# Force reload from disk
table.reload()
```

## API summary

### Table

| Method                                   | Description                    |
|------------------------------------------|--------------------------------|
| `Table(path, key)`                       | Open or create a table         |
| `Table.from_records(path, records, key)` | Create table with records      |
| `Table.from_file(path)`                  | Load existing table            |
| `get(key)`                               | Get a record by key, or `None` |
| `has(key)`                               | Check if a key exists          |
| `put(record)`                            | Insert or update a record      |
| `delete(key)`                            | Delete a record                |
| `all()`                                  | Iterate all records            |
| `keys()`                                 | Iterate all keys               |
| `items()`                                | Iterate (key, record) pairs    |
| `count()`                                | Number of records              |
| `find(predicate, limit=None)`            | Find matching records          |
| `find_one(predicate)`                    | Find first match               |
| `transaction()`                          | Start a transaction            |
| `compact()`                              | Remove historical entries      |
| `clear()`                                | Remove all records             |
| `reload()`                               | Reload from disk               |

Tables support `table[key]`, `table[key] = record`, `del table[key]`, `len(table)`, `key in table`, and `for key in table`.

### Transaction

| Method        | Description       |
|---------------|-------------------|
| `get(key)`    | Get from snapshot |
| `has(key)`    | Check in snapshot |
| `put(record)` | Buffer a write    |
| `delete(key)` | Buffer a deletion |
| `commit()`    | Write to disk     |
| `abort()`     | Discard changes   |

Transactions support the same dictionary-like access as tables.

### Exceptions

All exceptions inherit from `JSONLTError`:

| Exception          | Description               |
|--------------------|---------------------------|
| `ParseError`       | Invalid file format       |
| `InvalidKeyError`  | Invalid or missing key    |
| `FileError`        | I/O error                 |
| `LockError`        | Cannot get lock           |
| `LimitError`       | Size limit exceeded       |
| `TransactionError` | Invalid transaction state |
| `ConflictError`    | Write-write conflict      |

## Conformance

This library implements the [JSONLT 1.0 Specification](https://jsonlt.org/spec). It provides both Lenient and Strict Parser conformance profiles, and generates Strict-conformant output by default.

As the reference implementation, jsonlt-python passes the [JSONLT conformance test suite](https://github.com/jsonlt/jsonlt/tree/main/tests). Implementers of other JSONLT libraries can use these tests to verify compatibility.

## Acknowledgements

The JSONLT format draws from related work including [BEADS](https://github.com/steveyegge/beads), which uses JSONL for git-backed structured storage.

### AI disclosure

The development of this library involved AI language models, specifically Claude (Anthropic). AI tools contributed to drafting code, tests, and documentation. Human authors made all design decisions and final implementations, and they reviewed, edited, and validated AI-generated content. The authors take full responsibility for the correctness of this software.

## License

MIT License. See [LICENSE](LICENSE) for details.
