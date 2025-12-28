# JSONLT Python package

<!-- vale off -->
[![CI](https://github.com/jsonlt/jsonlt-python/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jsonlt/jsonlt-python/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/jsonlt/jsonlt-python/branch/main/graph/badge.svg)](https://codecov.io/gh/jsonlt/jsonlt-python)
[![CodSpeed Badge](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/jsonlt/jsonlt-python?utm_source=badge)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
<!-- vale on -->

**jsonlt** is the Python reference implementation of the [JSON Lines Table (JSONLT) specification][jsonlt].

JSONLT is a data format for storing keyed records in append-only files using [JSON Lines](https://jsonlines.org/). The format optimizes for version control diffs and human readability.

> [!NOTE]
> This package is in development and not yet ready for production use.

## Installation

```bash
pip install jsonlt

# Or

uv add jsonlt
```

## Quick start

### Basic operations

```python
from jsonlt import Table

# Open or create a table with a simple key
table = Table("users.jsonlt", key="id")

# Insert or update records
table.put({"id": "alice", "role": "admin", "email": "alice@example.com"})
table.put({"id": "bob", "role": "user", "email": "bob@example.com"})

# Read a record by key
user = table.get("alice")
print(user)  # {"id": "alice", "role": "admin", "email": "alice@example.com"}

# Check if a key exists
if table.has("bob"):
    print("Bob exists")

# Delete a record
table.delete("bob")

# Get all records
for record in table.all():
    print(record)
```

### Compound keys

JSONLT supports multi-field compound keys:

```python
# Using a tuple of field names for compound keys
orders = Table("orders.jsonlt", key=("customer_id", "order_id"))

orders.put({"customer_id": "alice", "order_id": 1, "total": 99.99})
orders.put({"customer_id": "alice", "order_id": 2, "total": 149.99})

# Access with compound key
order = orders.get(("alice", 1))
```

### Transactions

Use transactions for atomic updates with conflict detection:

```python
from jsonlt import Table, ConflictError

table = Table("counters.jsonlt", key="name")

# Context manager commits on success, aborts on exception
with table.transaction() as tx:
    counter = tx.get("visits")
    if counter:
        tx.put({"name": "visits", "count": counter["count"] + 1})
    else:
        tx.put({"name": "visits", "count": 1})

# Handle conflicts from concurrent modifications
try:
    with table.transaction() as tx:
        tx.put({"name": "counter", "value": 42})
except ConflictError as e:
    print(f"Conflict on key: {e.key}")
```

### Finding records

```python
from jsonlt import Table

table = Table("products.jsonlt", key="sku")

# Find all records matching a predicate
expensive = table.find(lambda r: r.get("price", 0) > 100)

# Find with limit
top_3 = table.find(lambda r: r.get("in_stock", False), limit=3)

# Find the first matching record
first_match = table.find_one(lambda r: r.get("category") == "electronics")
```

### Table maintenance

```python
from jsonlt import Table

table = Table("data.jsonlt", key="id")

# Compact the table (removes tombstones and superseded records)
table.compact()

# Clear all records (keeps header if present)
table.clear()
```

## API overview

### Table class

The `Table` class is the primary interface for working with JSONLT files.

| Method | Description |
| ------ | ----------- |
| `Table(path, key)` | Open or create a table at the given path |
| `get(key)` | Get a record by key, returns `None` if not found |
| `has(key)` | Check if a key exists |
| `put(record)` | Insert or update a record |
| `delete(key)` | Delete a record, returns whether it existed |
| `all()` | Get all records in key order |
| `keys()` | Get all keys in key order |
| `items()` | Get all (key, record) pairs in key order |
| `count()` | Get the number of records |
| `find(predicate, limit=None)` | Find records matching a predicate |
| `find_one(predicate)` | Find the first matching record |
| `transaction()` | Start a new transaction |
| `compact()` | Compact the table file |
| `clear()` | Remove all records |
| `reload()` | Force reload from disk |

The `Table` class also supports idiomatic Python operations:

- `len(table)` - number of records
- `key in table` - check if key exists
- `for record in table` - iterate over records

### Transaction class

The `Transaction` class provides snapshot isolation and buffered writes.

| Method | Description |
| ------ | ----------- |
| `get(key)` | Get a record from the transaction snapshot |
| `has(key)` | Check if a key exists in the snapshot |
| `put(record)` | Buffer a record for commit |
| `delete(key)` | Buffer a deletion for commit |
| `commit()` | Write buffered changes to disk |
| `abort()` | Discard buffered changes |

### Exception hierarchy

All exceptions inherit from `JSONLTError`:

| Exception | Description |
| --------- | ----------- |
| `ParseError` | Invalid file format or content |
| `InvalidKeyError` | Invalid or missing key |
| `FileError` | File I/O error |
| `LockError` | Cannot obtain file lock |
| `LimitError` | Size limit exceeded |
| `TransactionError` | Transaction state error |
| `ConflictError` | Write-write conflict detected |

### Type exports

For type annotations, the package exports:

- `Key` - A key value (`str`, `int`, or tuple of these)
- `KeySpecifier` - Key field names (single or multiple)
- `JSONObject` - A JSON object (`dict[str, Any]`)
- `Header` - Table header data class

## Documentation

For detailed documentation, tutorials, and the full specification, visit [jsonlt.org/docs](https://jsonlt.org/docs).

## License

MIT License. See [LICENSE](LICENSE) for details.

[jsonlt]: https://jsonlt.org/
