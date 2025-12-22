"""A library for using a JSON Lines (JSONL) file as a lightweight database."""

from importlib.metadata import version

from ._table import Table

__version__ = version("jsonlt")

__all__ = ["Table", "__version__"]
