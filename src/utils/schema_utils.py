import json
from pathlib import Path
from typing import Any

import pyarrow as pa


def load_schema_from_json(file_path: str | Path = "schema.json") -> pa.Schema:
    """
    Load a PyArrow schema from a JSON file.

    Args:
        file_path: Path to the JSON file containing the schema. Defaults to 'schema.json'.

    Returns:
        pa.Schema: A PyArrow schema object.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the JSON structure is invalid or contains unsupported types.
        json.JSONDecodeError: If the JSON is malformed.
    """
    schema_path = Path(file_path)
    # Open and read the JSON schema
    try:
        schema_json = json.loads(schema_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file '{schema_path}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in schema file: {e}")

    # Ensure the JSON has a 'fields' key, basic sanity check
    if not isinstance(schema_json, dict) or "fields" not in schema_json:
        raise ValueError("JSON schema must be a dictionary containing a 'fields' key.")

    # Map JSON type strings to PyArrow types
    def _get_pa_type(type_str: str) -> pa.DataType:
        type_mapping: dict[str, pa.DataType] = {
            # Standard types
            "string": pa.string(),
            "int8": pa.int8(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "uint8": pa.uint8(),
            "uint16": pa.uint16(),
            "uint32": pa.uint32(),
            "uint64": pa.uint64(),
            "float16": pa.float16(),
            "float32": pa.float32(),
            "float": pa.float32(),  # Alias
            "float64": pa.float64(),
            "double": pa.float64(),  # Added 'double' as alias for float64
            "bool": pa.bool_(),
            "boolean": pa.bool_(),  # Alias
            # Date and time types
            "date32[day]": pa.date32(),
            "date64[ms]": pa.date64(),
            "timestamp[s]": pa.timestamp("s"),
            "timestamp[ms]": pa.timestamp("ms"),
            "timestamp[us]": pa.timestamp("us"),
            "timestamp[ns]": pa.timestamp("ns"),
            # Binary types
            "binary": pa.binary(),
            "large_binary": pa.large_binary(),
            "large_string": pa.large_string(),
        }
        if type_str not in type_mapping:
            raise ValueError(f"Unsupported type: '{type_str}'")
        return type_mapping[type_str]

    # Process fields with helper function
    def _process_field(field_def: dict[str, Any]) -> pa.Field:
        name = field_def.get("name")
        if not name:
            raise ValueError(f"Field definition missing 'name': {field_def}")

        nullable = field_def.get("nullable", True)
        metadata = field_def.get("metadata")
        field_type = field_def.get("type")

        if not field_type:
            raise ValueError(f"Field '{name}' is missing 'type' definition")

        match field_type:
            case str():
                # Simple type case
                pa_type = _get_pa_type(field_type)

            case dict() if "type" in field_type:
                match field_type["type"]:
                    case "dictionary":
                        value_type_str = field_type.get("valueType")
                        index_type_str = field_type.get("indexType", "int32")
                        ordered = field_type.get("ordered", False)

                        if not value_type_str:
                            raise ValueError(
                                f"Dictionary field '{name}' missing 'valueType'"
                            )

                        value_type = _get_pa_type(value_type_str)
                        index_type = _get_pa_type(index_type_str)
                        pa_type = pa.dictionary(index_type, value_type, ordered=ordered)  # type: ignore

                    case "list":
                        value_type_str = field_type.get("valueType")
                        if not value_type_str:
                            raise ValueError(f"List field '{name}' missing 'valueType'")

                        value_type = _get_pa_type(value_type_str)
                        pa_type = pa.list_(value_type)

                    case "struct":
                        subfields = field_type.get("fields", [])
                        if not subfields:
                            raise ValueError(
                                f"Struct field '{name}' has no subfields defined"
                            )

                        struct_fields = [
                            _process_field(subfield) for subfield in subfields
                        ]
                        pa_type = pa.struct(struct_fields)

                    case unknown_type:
                        raise ValueError(
                            f"Unsupported complex type '{unknown_type}' for field '{name}'"
                        )
            case _:
                raise ValueError(
                    f"Invalid type definition for field '{name}': {field_type}"
                )

        # Create field with metadata if provided
        meta_dict = {k: str(v) for k, v in metadata.items()} if metadata else None
        return pa.field(name, pa_type, nullable=nullable, metadata=meta_dict)

    # Map each field definition to a PyArrow field
    fields = [_process_field(field) for field in schema_json["fields"]]

    # Return the constructed PyArrow schema with any top-level metadata
    metadata = schema_json.get("metadata")
    meta_dict: dict[bytes | str, bytes | str] | None = (
        {k: str(v) for k, v in metadata.items()} if metadata else None
    )

    return pa.schema(fields, metadata=meta_dict)
