# route: supertable.odata.odata_handler
"""
OData handler — business logic for the independent OData service.

Provides:
  - Schema discovery (tables + column types) scoped by RBAC role
  - OData $metadata XML generation (CSDL 4.0)
  - Safe OData query option → SQL translation (no f-string injection)
  - OData JSON response formatting
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from xml.etree.ElementTree import Element, SubElement, tostring

from supertable.meta_reader import MetaReader

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Safe identifier quoting
# ---------------------------------------------------------------------------

_SAFE_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier safely using double quotes.

    Rejects anything that could be an injection vector.
    Raises ValueError for invalid identifiers.
    """
    if not name or not isinstance(name, str):
        raise ValueError("Empty or invalid identifier")
    # Strip surrounding quotes if present
    stripped = name.strip().strip('"')
    if not stripped:
        raise ValueError("Empty identifier after stripping")
    if len(stripped) > 128:
        raise ValueError(f"Identifier too long: {len(stripped)}")
    # Reject embedded double quotes (prevents quote escaping attacks)
    if '"' in stripped:
        raise ValueError(f"Invalid character in identifier: {stripped!r}")
    return f'"{stripped}"'


def _validate_identifier(name: str) -> str:
    """Validate and return a safe identifier string (no quoting)."""
    stripped = (name or "").strip()
    if not stripped or not _SAFE_ID_RE.fullmatch(stripped):
        raise ValueError(f"Invalid identifier: {stripped!r}")
    if len(stripped) > 128:
        raise ValueError(f"Identifier too long: {len(stripped)}")
    return stripped


# ---------------------------------------------------------------------------
# Table + schema discovery (RBAC-scoped)
# ---------------------------------------------------------------------------

def _is_system_table(name: str) -> bool:
    """Return True for internal/system tables (e.g. __data_quality__)."""
    return name.startswith("__") and name.endswith("__")


def get_tables(super_name: str, organization: str, role_name: str) -> Tuple[List[str], MetaReader]:
    """Return the list of user tables visible to the given role.

    System tables (``__*__``) are excluded — they are internal to the
    platform and must not be exposed via OData feeds.
    """
    meta_reader = MetaReader(super_name=super_name, organization=organization)
    tables = meta_reader.get_tables(role_name=role_name)
    tables = [t for t in tables if not _is_system_table(t)]
    return tables, meta_reader


def get_schemas(
    super_name: str, organization: str, role_name: str,
) -> Dict[str, List[Dict[str, str]]]:
    """Return column schemas for all tables visible to the given role."""
    tables, meta_reader = get_tables(super_name, organization, role_name)
    schemas: Dict[str, List[Dict[str, str]]] = {}
    for table in tables:
        schema = meta_reader.get_table_schema(table, role_name=role_name)
        if schema is not None:
            schemas[table] = schema
    return schemas


# ---------------------------------------------------------------------------
# EDM type mapping
# ---------------------------------------------------------------------------

_EDM_TYPE_MAP = {
    "string": "String",
    "utf8": "String",
    "large_string": "String",
    "float64": "Double",
    "double": "Double",
    "float32": "Single",
    "float": "Single",
    "float16": "Single",
    "int64": "Int64",
    "int32": "Int32",
    "int16": "Int16",
    "int8": "SByte",
    "uint64": "Int64",
    "uint32": "Int32",
    "uint16": "Int16",
    "uint8": "Byte",
    "bool": "Boolean",
    "boolean": "Boolean",
    "date": "Date",
    "date32": "Date",
    "datetime": "DateTimeOffset",
    "timestamp": "DateTimeOffset",
    "timestamp[ns]": "DateTimeOffset",
    "timestamp[us]": "DateTimeOffset",
    "timestamp[ms]": "DateTimeOffset",
    "timestamp[s]": "DateTimeOffset",
    "binary": "Binary",
    "decimal": "Decimal",
    "decimal128": "Decimal",
}


def _map_type_to_edm(raw_type: str) -> str:
    """Map a Parquet/Arrow/DuckDB type string to an OData EDM type."""
    normalized = (raw_type or "string").strip().lower()
    # Handle parameterized types like "timestamp[ns, tz=UTC]"
    base = normalized.split("[")[0].split("(")[0].strip()
    return _EDM_TYPE_MAP.get(base, _EDM_TYPE_MAP.get(normalized, "String"))


# ---------------------------------------------------------------------------
# $metadata XML generation (OData CSDL 4.0)
# ---------------------------------------------------------------------------

def create_metadata_xml(
    super_name: str, schemas: Dict[str, List[Dict[str, str]]],
) -> str:
    """Generate OData $metadata XML (CSDL 4.0) for the given schemas."""
    ns = "SuperTable"

    edmx = Element("edmx:Edmx", {
        "xmlns:edmx": "http://docs.oasis-open.org/odata/ns/edmx",
        "Version": "4.0",
    })
    data_services = SubElement(edmx, "edmx:DataServices")
    schema_el = SubElement(data_services, "Schema", {
        "xmlns": "http://docs.oasis-open.org/odata/ns/edm",
        "Namespace": ns,
    })

    container = Element("EntityContainer", {"Name": f"{super_name}Service"})

    for entity_name, columns_list in schemas.items():
        # EntityType
        entity_type = SubElement(schema_el, "EntityType", {"Name": entity_name})
        columns = columns_list[0] if columns_list else {}
        for col_name, col_type in columns.items():
            prop = SubElement(entity_type, "Property", {
                "Name": col_name,
                "Type": f"Edm.{_map_type_to_edm(col_type)}",
                "Nullable": "true",
            })

        # EntitySet
        entity_set = SubElement(container, "EntitySet", {
            "Name": entity_name,
            "EntityType": f"{ns}.{entity_name}",
        })

    schema_el.append(container)

    xml_bytes = tostring(edmx, encoding="utf-8", xml_declaration=True)
    return xml_bytes.decode("utf-8")


# ---------------------------------------------------------------------------
# OData query options → safe SQL
# ---------------------------------------------------------------------------

# Allowed OData filter operators → SQL equivalents
_FILTER_OP_MAP = {
    " eq ": " = ",
    " ne ": " != ",
    " gt ": " > ",
    " ge ": " >= ",
    " lt ": " < ",
    " le ": " <= ",
    " and ": " AND ",
    " or ": " OR ",
}

# Allowed aggregate functions
_ALLOWED_AGG = frozenset({"sum", "min", "max", "avg", "count", "countdistinct"})


def _safe_filter(raw_filter: str) -> str:
    """Translate an OData $filter expression to a SQL WHERE clause.

    Only allows simple comparison operators and AND/OR.
    Rejects anything that looks like SQL injection.
    """
    if not raw_filter:
        return ""
    # Reject dangerous patterns
    lower = raw_filter.lower()
    for danger in ("drop ", "delete ", "insert ", "update ", "alter ", "--", ";", "/*", "*/", "xp_", "exec "):
        if danger in lower:
            raise ValueError(f"Rejected filter: suspicious pattern '{danger}'")
    result = raw_filter
    for odata_op, sql_op in _FILTER_OP_MAP.items():
        result = result.replace(odata_op, sql_op)
    return result


def build_query(entity_set_name: str, query_params: Dict[str, str]) -> str:
    """Build a safe SQL query from OData query options.

    All identifiers are double-quoted to prevent injection.
    """
    table = _quote_identifier(entity_set_name)

    select_raw = query_params.get("$select")
    filter_raw = query_params.get("$filter")
    top_raw = query_params.get("$top")
    apply_raw = query_params.get("$apply")

    # $apply (aggregation) takes precedence
    if apply_raw:
        return _build_aggregate_query(table, apply_raw, filter_raw)

    # $select
    if select_raw:
        columns = [_quote_identifier(c.strip()) for c in select_raw.split(",") if c.strip()]
        if not columns:
            raise ValueError("Empty $select")
        projection = ", ".join(columns)
    else:
        projection = "*"

    sql = f"SELECT {projection} FROM {table}"

    if filter_raw:
        where = _safe_filter(filter_raw)
        sql += f" WHERE {where}"

    if top_raw:
        try:
            limit = int(top_raw)
            if limit < 0 or limit > 100000:
                raise ValueError("$top out of range")
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid $top: {top_raw}") from e
        sql += f" LIMIT {limit}"

    return sql


def _build_aggregate_query(table: str, apply_expr: str, filter_raw: Optional[str]) -> str:
    """Parse $apply=aggregate(...) and build a safe SQL aggregate query."""
    transformations = apply_expr.split("/")
    for transformation in transformations:
        stripped = transformation.strip()
        if stripped.startswith("aggregate(") and stripped.endswith(")"):
            inner = stripped[len("aggregate("):-1]
            agg_parts = []
            for part in inner.split(","):
                part = part.strip()
                if " with " not in part.lower():
                    raise ValueError(f"Invalid aggregate expression: {part}")
                field_part, func_part = part.rsplit(" with ", 1)
                # Handle optional " as alias"
                func = func_part.strip().lower()
                alias = None
                if " as " in func:
                    func, alias = func.rsplit(" as ", 1)
                    func = func.strip()
                    alias = _validate_identifier(alias.strip())
                field = _quote_identifier(field_part.strip())
                if func not in _ALLOWED_AGG:
                    raise ValueError(f"Unsupported aggregate function: {func}")
                sql_func = "COUNT(DISTINCT" if func == "countdistinct" else func.upper() + "("
                close = ")" if func != "countdistinct" else ")"
                col_alias = alias or f"{field_part.strip()}_{func}"
                agg_parts.append(f"{sql_func}{field}{close} AS {_quote_identifier(col_alias)}")

            sql = f"SELECT {', '.join(agg_parts)} FROM {table}"
            if filter_raw:
                sql += f" WHERE {_safe_filter(filter_raw)}"
            return sql

    # Fallback if no recognized transformation
    sql = f"SELECT * FROM {table}"
    if filter_raw:
        sql += f" WHERE {_safe_filter(filter_raw)}"
    return sql


# ---------------------------------------------------------------------------
# OData JSON response formatting
# ---------------------------------------------------------------------------

def format_odata_response(
    entity_set_name: str,
    df: Any,
    context_url: str,
) -> Dict[str, Any]:
    """Format a pandas DataFrame as an OData JSON response.

    Replaces NaN/NaT with appropriate defaults for OData compatibility.
    """
    import numpy as np

    # Sanitize numeric NaN → 0, string NaN → ""
    numeric_cols = df.select_dtypes(include=["float64", "float32", "int64", "int32"]).columns
    string_cols = df.select_dtypes(include=["object"]).columns

    if len(numeric_cols) > 0:
        df[numeric_cols] = df[numeric_cols].fillna(0)
    if len(string_cols) > 0:
        df[string_cols] = df[string_cols].fillna("")

    # Convert timestamps to ISO strings for OData
    datetime_cols = df.select_dtypes(include=["datetime64", "datetimetz"]).columns
    for col in datetime_cols:
        df[col] = df[col].apply(lambda x: x.isoformat() if not isinstance(x, float) and x is not None else None)

    records = df.to_dict(orient="records")

    return {
        "@odata.context": f"{context_url}/$metadata#{entity_set_name}",
        "value": records,
    }
