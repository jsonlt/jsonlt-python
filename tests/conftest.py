"""Pytest configuration and shared fixtures for the test suite."""

from pathlib import Path

import pytest

# Directory-to-marker mapping
_DIRECTORY_MARKERS: dict[str, str] = {
    "unit": "unit",
    "integration": "integration",
    "properties": "property",
    "benchmarks": "benchmark",
    "examples": "example",
    "fuzz": "fuzz",
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Automatically apply markers based on test directory."""
    tests_dir = Path(__file__).parent

    for item in items:
        item_path = Path(item.fspath)

        try:
            relative = item_path.relative_to(tests_dir)
            if relative.parts:
                subdir = relative.parts[0]
                if marker_name := _DIRECTORY_MARKERS.get(subdir):
                    # Dynamic marker access returns Any
                    marker = getattr(pytest.mark, marker_name)  # pyright: ignore[reportAny]
                    item.add_marker(marker)  # pyright: ignore[reportAny]
        except ValueError:
            pass
