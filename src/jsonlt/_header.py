"""Header parsing and representation for JSONLT files.

This module defines the Header dataclass and functions for parsing and
validating JSONLT file headers per the specification section "Header".
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from ._constants import JSONLT_VERSION, MAX_TUPLE_ELEMENTS
from ._exceptions import ParseError
from ._json import serialize_json
from ._keys import KeySpecifier, is_valid_key_specifier

if TYPE_CHECKING:
    from ._json import JSONObject, JSONValue


@dataclass(frozen=True, slots=True)
class Header:
    """Represents a JSONLT file header.

    A header is an optional first line in a JSONLT file that provides
    metadata about the file. It contains a `$jsonlt` field with metadata.

    Attributes:
        version: The JSONLT specification version (must be 1).
        key: The key specifier for the table, if present.
        schema_url: A URL reference to a JSON Schema that validates records.
        schema: An inline JSON Schema object that validates records.
        meta: User-defined metadata.
    """

    version: int
    key: "KeySpecifier | None" = None
    schema_url: "str | None" = None
    schema: "JSONObject | None" = None
    meta: "JSONObject | None" = None


def is_header_line(obj: "JSONObject") -> bool:
    """Check if a parsed JSON object is a header line.

    A header line is a JSON object containing a single field `$jsonlt`.

    Args:
        obj: A parsed JSON object.

    Returns:
        True if the object contains the `$jsonlt` field, False otherwise.
    """
    return "$jsonlt" in obj


def _parse_key_specifier(value: object) -> KeySpecifier:
    """Parse a key specifier from a JSON value.

    Args:
        value: The value of the `key` field from the header.

    Returns:
        A valid key specifier (string or tuple of strings).

    Raises:
        ParseError: If the value is not a valid key specifier.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        if not value:
            msg = "key specifier cannot be an empty array"
            raise ParseError(msg)
        # Check all items are strings and build typed list
        # Cast to list[object] for type-safe iteration
        items = cast("list[object]", value)
        str_list: list[str] = []
        for item in items:
            if not isinstance(item, str):
                msg = "key specifier array must contain only strings"
                raise ParseError(msg)
            str_list.append(item)
        result: tuple[str, ...] = tuple(str_list)
        if len(result) > MAX_TUPLE_ELEMENTS:
            msg = f"key specifier exceeds maximum of {MAX_TUPLE_ELEMENTS} elements"
            raise ParseError(msg)
        if not is_valid_key_specifier(result):
            msg = "key specifier contains duplicate field names"
            raise ParseError(msg)
        return result
    msg = (
        f"key specifier must be a string or array of strings, "
        f"got {type(value).__name__}"
    )
    raise ParseError(msg)


def parse_header(obj: "JSONObject") -> Header:
    """Parse a header from a JSON object.

    The object must contain a `$jsonlt` field whose value is an object
    with the required `version` field and optional `key`, `$schema`,
    `schema`, and `meta` fields.

    Args:
        obj: A parsed JSON object containing the `$jsonlt` field.

    Returns:
        A Header instance with the parsed metadata.

    Raises:
        ParseError: If the header structure is invalid:
            - `$jsonlt` value is not an object
            - `version` field is missing or not an integer
            - `version` is not 1
            - Both `$schema` and `schema` are present
            - `key` is not a valid key specifier
            - `$schema` is not a string
            - `schema` is not an object
            - `meta` is not an object
    """
    jsonlt_value = obj.get("$jsonlt")

    if not isinstance(jsonlt_value, dict):
        msg = "$jsonlt value must be an object"
        raise ParseError(msg)

    # Validate version (required)
    if "version" not in jsonlt_value:
        msg = "header missing required 'version' field"
        raise ParseError(msg)

    version = jsonlt_value["version"]
    if not isinstance(version, int) or isinstance(version, bool):
        msg = f"version must be an integer, got {type(version).__name__}"
        raise ParseError(msg)

    if version != JSONLT_VERSION:
        msg = f"unsupported version {version}, expected {JSONLT_VERSION}"
        raise ParseError(msg)

    # Check for mutually exclusive schema fields
    has_schema_url = "$schema" in jsonlt_value
    has_inline_schema = "schema" in jsonlt_value

    if has_schema_url and has_inline_schema:
        msg = "$schema and schema are mutually exclusive"
        raise ParseError(msg)

    # Parse optional key specifier
    key: KeySpecifier | None = None
    if "key" in jsonlt_value:
        key = _parse_key_specifier(jsonlt_value["key"])

    # Parse optional schema URL
    schema_url: str | None = None
    if has_schema_url:
        schema_url_value = jsonlt_value["$schema"]
        if not isinstance(schema_url_value, str):
            msg = f"$schema must be a string, got {type(schema_url_value).__name__}"
            raise ParseError(msg)
        schema_url = schema_url_value

    # Parse optional inline schema
    schema: JSONObject | None = None
    if has_inline_schema:
        schema_value = jsonlt_value["schema"]
        if not isinstance(schema_value, dict):
            msg = f"schema must be an object, got {type(schema_value).__name__}"
            raise ParseError(msg)
        schema = schema_value

    # Parse optional meta
    meta: JSONObject | None = None
    if "meta" in jsonlt_value:
        meta_value = jsonlt_value["meta"]
        if not isinstance(meta_value, dict):
            msg = f"meta must be an object, got {type(meta_value).__name__}"
            raise ParseError(msg)
        meta = meta_value

    return Header(
        version=version,
        key=key,
        schema_url=schema_url,
        schema=schema,
        meta=meta,
    )


def serialize_header(header: Header) -> str:
    """Serialize a Header to a JSON line.

    Produces deterministic JSON output with sorted keys.

    Args:
        header: The header to serialize.

    Returns:
        The JSON line string (without trailing newline).
    """
    # Build the $jsonlt metadata object
    jsonlt_obj: dict[str, JSONValue] = {"version": header.version}

    if header.key is not None:
        # Convert tuple key specifier to list for JSON
        if isinstance(header.key, tuple):
            jsonlt_obj["key"] = list(header.key)
        else:
            jsonlt_obj["key"] = header.key

    if header.schema_url is not None:
        jsonlt_obj["$schema"] = header.schema_url

    if header.schema is not None:
        jsonlt_obj["schema"] = header.schema

    if header.meta is not None:
        jsonlt_obj["meta"] = header.meta

    # Build the full header object
    header_obj: dict[str, JSONValue] = {"$jsonlt": jsonlt_obj}

    return serialize_json(header_obj)
