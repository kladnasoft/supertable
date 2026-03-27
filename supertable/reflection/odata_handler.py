import numpy as np
import pandas as pd

from typing import Dict, List, Any
from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree
from supertable.meta_reader import MetaReader

from application import SERVER_IP, SERVER_PORT


def get_tables(super_name, organization=None):
    meta_reader = MetaReader(super_name, organization=organization) if organization else MetaReader(super_name)
    super_table = meta_reader.super_table.get_super_table()
    super_snapshot = super_table.get("snapshots")

    tables = [snapshot["hyper_name"] for snapshot in super_snapshot]
    tables.append(super_name)

    return tables, meta_reader


def get_schemas(super_name, organization=None):
    tables, meta_reader = get_tables(super_name, organization=organization)

    schemas = {}
    for table in tables:
        schemas[table] = meta_reader.get_table_schema(table)

    return schemas


# Function to map provided types to Edm types
def map_type_to_edm(provided_type: str) -> str:
    type_mapping = {
        "String": "String",
        "Float64": "Double",
        "Int64": "Int64",
        "Int32": "Int32",
        "Float32": "Float",
        "Date": "DateTime",
        "Datetime": "DateTime",
    }
    return type_mapping.get(provided_type, provided_type)


def create_property_xml(name: str, type: str) -> Element:
    """
    Create an XML element for an OData property with Nullable set to true.
    """
    edm_type = map_type_to_edm(type)  # Map the provided type to Edm type
    property_element = Element("Property")
    property_element.set("Name", name)
    property_element.set("Type", f"Edm.{edm_type}")
    property_element.set("Nullable", "true")
    return property_element


def create_entity_type_xml(entity_name: str, properties: Dict[str, str]) -> Element:
    """
    Create an XML element for an OData entity type.
    """
    entity_type_element = Element("EntityType")
    entity_type_element.set("Name", entity_name)

    for prop_name, prop_type in properties.items():
        property_element = create_property_xml(prop_name, prop_type)
        entity_type_element.append(property_element)

    return entity_type_element


def create_entity_set_xml(entity_name: str) -> Element:
    """
    Create an XML element for an OData entity set.
    """
    entity_set_element = Element("EntitySet")
    entity_set_element.set("Name", entity_name)
    entity_set_element.set("EntityType", f"Namespace.{entity_name}")
    return entity_set_element


def create_schema_xml(super_name: str, schemas: Dict[str, List[Dict[str, str]]]) -> str:
    """
    Generate the OData $metadata XML string based on the provided schemas.
    """
    edmx = Element(
        "edmx:Edmx",
        {"xmlns:edmx": "http://docs.oasis-open.org/odata/ns/edmx", "Version": "4.0"},
    )

    data_services = SubElement(edmx, "edmx:DataServices")
    schema = SubElement(
        data_services,
        "Schema",
        {"xmlns": "http://docs.oasis-open.org/odata/ns/edm", "Namespace": "Namespace"},
    )

    # Create EntityTypes and EntitySets for each schema
    entity_container = Element("EntityContainer", {"Name": f"{super_name}Container"})

    for entity_name, entity_props_list in schemas.items():
        entity_props = entity_props_list[0] if entity_props_list else {}
        entity_type = create_entity_type_xml(entity_name, entity_props)
        schema.append(entity_type)

        entity_set = create_entity_set_xml(entity_name)
        entity_container.append(entity_set)

    schema.append(entity_container)

    # Convert to string
    xml_str = tostring(edmx, encoding="utf-8", method="xml")
    return xml_str.decode("utf-8")


def apply_odata_query_options(entity_set_name, query_params):
    select = query_params.get("$select")
    filter_ = query_params.get("$filter")
    top = query_params.get("$top")
    apply = query_params.get("$apply")

    query = f"SELECT * FROM {entity_set_name}"

    if apply:
        transformations = apply.split("/")
        for transformation in transformations:
            if transformation.startswith("aggregate"):
                agg_params = transformation[len("aggregate(") : -1]
                agg_fields = []
                for agg_field in agg_params.split(","):
                    field, agg_func = agg_field.split(" with ")
                    field, agg_func = field.strip(), agg_func.strip()
                    agg_fields.append(
                        f"{agg_func}({field}) AS {field}_{agg_func.lower()}"
                    )
                query = f"SELECT {', '.join(agg_fields)} FROM {entity_set_name}"
                if filter_:
                    query += f" WHERE {filter_}"
                return query

    if select:
        columns = select.split(",")
        query = f"SELECT {', '.join(columns)} FROM {entity_set_name}"

    if filter_:
        query += f" WHERE {filter_}"

    if top:
        query += f" LIMIT {top}"

    return query


def create_odata_response(entity_set_name: str, df: pd.DataFrame) -> Dict[str, Any]:
    numeric_columns = df.select_dtypes(include=["float64", "int64"]).columns
    string_columns = df.select_dtypes(include=["object"]).columns
    # Replace NaN and similar values with 0.0 only in numeric columns
    df[numeric_columns] = df[numeric_columns].replace([np.nan, "nan", "NaN"], 0)
    df[string_columns] = df[string_columns].replace([np.nan, "nan", "NaN"], "")

    records = df.to_dict(orient="records")

    # Construct the OData response
    response = {
        "@odata.context": f"https://{SERVER_IP}:{SERVER_PORT}/odata/$metadata#{entity_set_name}",
        "value": records,
    }

    # Serialize to JSON using the custom encoder
    return response
