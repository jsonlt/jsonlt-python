import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_codspeed.plugin import BenchmarkFixture
    from pytest_mock import MockerFixture


def test_import_time(benchmark: "BenchmarkFixture", mocker: "MockerFixture") -> None:
    def import_jsonlt():
        _ = mocker.patch("sys.modules", {})
        _ = importlib.import_module("jsonlt", "test_bench_imports")

    benchmark(import_jsonlt)
