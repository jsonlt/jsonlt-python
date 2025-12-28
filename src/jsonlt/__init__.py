"""A library for using a JSON Lines (JSONL) file as a lightweight database."""

from importlib.metadata import version

from ._exceptions import (
    ConflictError,
    FileError,
    InvalidKeyError,
    JSONLTError,
    LimitError,
    LockError,
    ParseError,
    TransactionError,
)
from ._header import Header
from ._json import JSONArray, JSONObject, JSONPrimitive, JSONValue
from ._keys import Key, KeySpecifier
from ._table import Table
from ._transaction import Transaction

__version__ = version("jsonlt")

__all__ = [
    "ConflictError",
    "FileError",
    "Header",
    "InvalidKeyError",
    "JSONArray",
    "JSONLTError",
    "JSONObject",
    "JSONPrimitive",
    "JSONValue",
    "Key",
    "KeySpecifier",
    "LimitError",
    "LockError",
    "ParseError",
    "Table",
    "Transaction",
    "TransactionError",
    "__version__",
]
