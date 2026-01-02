# Benchmarks

This directory contains performance and memory benchmarks for JSONLT `Table` operations. The benchmarks measure CPU time and memory usage across different table sizes, record complexities, and key types.

## Overview

This directory organizes benchmarks into three categories:

- **Performance benchmarks** (`test_bench_table.py`) - Measure CPU time for all core Table operations
- **Memory benchmarks** (`test_bench_memory.py`) - Enforce memory limits using pytest-memray
- **Import time benchmark** (`test_bench_imports.py`) - Measures module import time

## Benchmark categories

### Performance benchmarks (`test_bench_table.py`)

Tests all core Table operations across different dimensions:

**Operations tested:**

- `load` - Initial table loading from file
- `reload` - Reloading table data from disk
- `get` - Single record retrieval (existing and nonexistent keys)
- `all` - Retrieving all records
- `find` - Predicate-based filtering with different selectivities
- `find_one` - Single record matching (early, late, and no match cases)
- `put` - Writing new and updating existing records
- `batch write` - Writing records in batches (10 and 100 record batches)
- `compact` - Compacting tables with history or tombstones
- `keys` - Retrieving all keys
- `items` - Retrieving all key-value pairs
- `count` - Counting records
- `has` - Checking key existence
- `delete` - Removing records

**Parameters:**

| Dimension    | Values                                                                      |
| ------------ | --------------------------------------------------------------------------- |
| Record sizes | small (~5 fields), medium (~20 fields), large (~100 fields with 1KB+ text)  |
| Key types    | string, integer, tuple (composite)                                          |
| Scales       | 100, 1000, 10000, 100000 records                                            |

### Memory benchmarks (`test_bench_memory.py`)

Uses pytest-memray with `@pytest.mark.limit_memory()` to enforce memory limits.

**Tests include:**

- Loading tables of different sizes (1k, 10k, 100k records)
- Read operations (`all`, `find`, `keys`)
- Write operations (`put`, `delete`, `compact`)

**Note:** memory benchmarks run only on Linux and macOS. They skip automatically on Windows where pytest-memray is not available.

### Import time benchmark (`test_bench_imports.py`)

Measures the time to import the `jsonlt` module, useful for detecting import-time regressions.

## Data generation

The `_generators.py` module provides deterministic test data generation:

- Uses seeded random instances for reproducibility across runs
- Generates records of varying sizes (small, medium, large)
- Supports all key types (string, integer, tuple)
- Provides helper functions for creating tables with history and tombstones

## Running benchmarks

### Running locally

```bash
# Run all benchmarks (excluding slow tests)
just benchmark

# Run specific test classes
just benchmark -k "TestBenchLoad"

# Include slow benchmarks (10k+ records)
just benchmark -m "benchmark and slow"
```

### Markers

| Marker         | Description                                                       |
| -------------- | ----------------------------------------------------------------- |
| `benchmark`    | Auto-applied to all tests in this directory                       |
| `slow`         | Applied to larger scale tests (10k+ records), excluded by default |
| `limit_memory` | Applied to memory tests (pytest-memray marker)                    |

Default test runs exclude benchmarks. The default marker expression in `pyproject.toml` is:

```text
-m "not benchmark and not slow"
```

This means running `pytest` alone only executes unit tests.

## CI integration

Benchmarks run on GitHub Actions via Codspeed:

- **Mode:** simulation (CPU instruction counting, not wall-clock time)
- **Sharding:** the workflow distributes tests across 7 parallel jobs for faster feedback
- **Slow tests:** excluded in CI to keep benchmark runs under 5 minutes
- **Triggers:** runs on pushes and pull requests to `main` when relevant files change

### CI shards

| Shard           | Test Classes                                              |
| --------------- | --------------------------------------------------------- |
| load-reload     | `TestBenchLoad`, `TestBenchReload`                        |
| get             | `TestBenchGet`                                            |
| find            | `TestBenchFind`                                           |
| find-one-delete | `TestBenchFindOne`, `TestBenchDelete`                     |
| write-compact   | `TestBenchPut`, `TestBenchBatchWrite`, `TestBenchCompact` |
| all-keys-items  | `TestBenchAll`, `TestBenchKeys`, `TestBenchItems`         |
| count-has       | `TestBenchCount`, `TestBenchHas`                          |

## Benchmark results

Codspeed tracks the results:

<https://codspeed.io/jsonlt/jsonlt-python/benchmarks>
