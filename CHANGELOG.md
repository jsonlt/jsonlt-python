# Changelog

This file documents all notable changes to this project.

This format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [PEP 440](https://peps.python.org/pep-0440/).

## [Unreleased]

### Added

- `Table.from_records()` and `Table.from_file()` factory methods for convenient table initialization
- Dictionary-like access for `Table` and `Transaction` (`table[key]`, `table[key] = record`, `del table[key]`, `pop`, `popitem`, `setdefault`, `update`)
- Value-based equality for `Table` and `Transaction` (`==` compares path, key specifier, and records)

## [0.1.0] - 2025-12-31

### Added

- `Table` class for working with JSONLT files supporting CRUD operations (`get`, `put`, `delete`, `has`) and Python protocols (`Sized`, `Container`, `Iterable`)
- `Transaction` class with snapshot isolation, optimistic concurrency control, context manager support, and Python protocols (`Sized`, `Container`, `Iterable`)
- Compound key support for multi-field keys (for example, `("customer_id", "order_id")`)
- Cross-platform file locking for concurrent access (fcntl on Unix, LockFileEx on Windows)
- Query operations: `find()` and `find_one()` with predicate functions
- Table maintenance: `compact()` to remove superseded records and tombstones
- Exception hierarchy with `ConflictError` for write-write conflict detection
- Full conformance with the JSONLT specification
- Property-based tests using Hypothesis for JSON serialization, records, and state
- Benchmark suite for performance testing
