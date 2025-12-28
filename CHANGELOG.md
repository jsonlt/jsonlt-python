# Changelog

This file documents all notable changes to this project.

This format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [PEP 440](https://peps.python.org/pep-0440/).

## [Unreleased]

### Added

- `Table` class for working with JSONLT files supporting CRUD operations (`get`, `put`, `delete`, `has`)
- `Transaction` class with snapshot isolation and optimistic concurrency control
- Compound key support for multi-field keys (for example, `("customer_id", "order_id")`)
- Cross-platform file locking for concurrent access (fcntl on Unix, LockFileEx on Windows)
- Query operations: `find()` and `find_one()` with predicate functions
- Table maintenance: `compact()` to remove superseded records and tombstones
- Exception hierarchy with `ConflictError` for write-write conflict detection
- Full conformance with the JSONLT specification
