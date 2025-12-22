from pathlib import Path

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    test_dir = Path(__file__).parent
    for item in items:
        if Path(item.fspath).is_relative_to(test_dir):
            item.add_marker(pytest.mark.benchmark)
