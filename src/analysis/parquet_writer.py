import argparse
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.dataset as ds
from typing import Any
import json
import zstandard as zstd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_true_false_from_data_dict(
    data_dict_path: Path,
) -> tuple[list[str], list[str]]:
    """
    Extract true and false values from a JSON data dictionary for CSV parsing.

    This function processes a JSON data dictionary to identify values representing
    True and False, based on common boolean patterns (e.g., 'T'/'F', 'Y'/'N').

    Parameters
    ----------
    data_dict_path : pathlib.Path
        Path to the JSON file containing the data dictionary. The dictionary should map
        field names to nested dictionaries of possible values (e.g., {"field": {"T": "True", "F": "False"}}).

    Returns
    -------
    tuple[list[str], list[str]]
        A tuple containing:
        - true_values : list[str]
            List of strings interpreted as True in boolean columns.
        - false_values : list[str]
            List of strings interpreted as False in boolean columns.

    Raises
    ------
    json.JSONDecodeError
        If the JSON file is malformed.
    FileNotFoundError
        If the file at `data_dict_path` does not exist.
    PermissionError
        If access to the file is denied.
    Exception
        For unexpected errors during file loading, with detailed logging.

    Examples
    --------
    >>> true_vals, false_vals = extract_boolean_values_from_dict(Path("data_dict.json"))
    >>> print(true_vals, false_vals)
    ['Y', 'T', 'YES', 'True'], ['N', 'F', 'NO', 'False']
    """
    # Load data dictionary:
    try:
        data_dict: dict[str, Any] = json.loads(
            data_dict_path.read_text(encoding="utf-8")
        )
        logger.debug("Loaded data dictionary from %s", data_dict_path)
    except (json.JSONDecodeError, FileNotFoundError, PermissionError):
        logger.exception("Failed to load data dictionary from %s", data_dict_path)
        return [], []
    except Exception:
        logger.critical(
            "Unexpected error loading data dictionary from %s",
            data_dict_path,
            exc_info=True,
            stack_info=True,
        )
        raise

    # Define boolean patterns as a dictionary mapping patterns to true/false keys
    BOOL_PATTERNS = {
        frozenset({"T", "F"}): ("T", "F"),
        frozenset({"Y", "N"}): ("Y", "N"),
    }

    # Extract true/false values efficiently
    true_values: set[str] = set()
    false_values: set[str] = set()

    for field_values in data_dict.values():
        if not isinstance(field_values, dict):
            continue
        keys = frozenset(field_values)
        if keys in BOOL_PATTERNS:
            true_values.update(
                str(field_values.get(key, "")).upper()
                for key in ("T", "Y")
                if key in field_values
            )
            false_values.update(
                str(field_values.get(key, "")).upper()
                for key in ("F", "N")
                if key in field_values
            )

    # Define base true/false values for CSV parsing
    BASE_TRUE_VALUES = frozenset({"y", "Y", "T", "t", "YES", "Yes", "True", "TRUE"})
    BASE_FALSE_VALUES = frozenset({"n", "N", "f", "F", "NO", "No", "False", "FALSE"})

    # Combine with extracted values, preserving case sensitivity
    combined_true_values = list(set(BASE_TRUE_VALUES) | true_values)
    combined_false_values = list(set(BASE_FALSE_VALUES) | false_values)

    logger.debug(
        "Found %d True values and %d False values from data dict",
        len(combined_true_values),
        len(combined_false_values),
    )
    return combined_true_values, combined_false_values


def load_schema_from_json(file_path: str | Path = "schema.json") -> pa.Schema:
    """
    Load a PyArrow schema from a JSON file.

    Args:
        file_path: Path to the JSON file containing the schema. Defaults to 'schema.json'.
                  Can be a string or Path object.

    Returns:
        A PyArrow schema object constructed from the JSON data.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If the JSON structure is invalid or contains unsupported types.
        json.JSONDecodeError: If the file contains invalid JSON.
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


def load_zstd_to_batches(
    zstd_path: Path,
    year: str,
    convert_opts: pa_csv.ConvertOptions,
    parse_opts: pa_csv.ParseOptions,
) -> tuple[list[pa.RecordBatch], pa.Schema]:
    """Stream zstd file and return a list of record batches with year column."""
    try:
        with (
            zstd_path.open("rb") as zstd_file,
            zstd.ZstdDecompressor().stream_reader(zstd_file) as reader,
        ):
            csv_reader = pa_csv.open_csv(
                reader,
                read_options=pa_csv.ReadOptions(block_size=64 << 20),  # 64MB batches
                parse_options=parse_opts,
                convert_options=convert_opts,
            )
            logger.debug("Schema: %s", csv_reader.schema)

            # Extend the schema to include the "year" field
            schema_with_year = csv_reader.schema.append(pa.field("year", pa.string()))

            batches = []
            for batch in csv_reader:
                year_array = pa.array([year] * batch.num_rows, type=pa.string())
                batch_with_year = pa.RecordBatch.from_arrays(
                    batch.columns + [year_array],
                    names=schema_with_year.names,
                )
                batches.append(batch_with_year)

            return batches, schema_with_year
    except Exception:
        logger.exception("Error processing %s", zstd_path)
        return [], csv_reader.schema if "csv_reader" in locals() else pa.schema([])


def create_partitioned_dataset(
    years_dirs: list[tuple[str, Path]],
    output_dir: Path,
    convert_opts: pa_csv.ConvertOptions,
    parse_opts: pa_csv.ParseOptions,
    overwrite: bool = True,
):
    """Create a partitioned dataset with one Parquet file per directory."""
    if output_dir.is_file() and not overwrite:
        logger.error(
            "Output file exists and overwrite=False; use overwrite=True or remove file."
        )
        return

    # Create output directory if using existing_data_behavior
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each directory (year)
    for year, input_dir in years_dirs:
        if not input_dir.is_dir():
            logger.warning("Directory %s does not exist, skipping %s", input_dir, year)
            continue

        # Accumulate all batches for the year
        all_batches = []
        schema = None
        zstd_files = sorted(input_dir.glob("*.zst"))
        if not zstd_files:
            logger.warning("No zstd files found in %s, skipping %s", input_dir, year)
            continue

        for file_path in zstd_files:
            batches, file_schema = load_zstd_to_batches(
                file_path, year, convert_opts, parse_opts
            )
            if batches:
                all_batches.extend(batches)
                if schema is None:
                    schema = file_schema

        if all_batches:
            # Write all batches for this year as one table
            ds.write_dataset(
                data=all_batches,
                base_dir=output_dir,
                format="parquet",
                partitioning=ds.partitioning(
                    pa.schema([schema.field("year")]),
                    flavor="hive",  # type: ignore
                ),
                existing_data_behavior="overwrite_or_ignore",
            )
            logger.info("Wrote %s partition", year)
        else:
            logger.warning("No valid data processed for %s", year)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    BASE_DIR = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description="Convert zstd-compressed CSVs to a partitioned Parquet dataset"
    )
    parser.add_argument(
        "--output-dir",
        type=lambda p: BASE_DIR / p,
        default=(BASE_DIR / Path("../../data/processed/awards_dataset")).resolve(),
        help="Path to output Parquet dataset directory",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output directory if it exists",
    )
    parser.add_argument(
        "--years",
        type=str,
        nargs="+",
        required=True,
        help="List of years to process (e.g., '2022 2023 2024')",
    )
    parser.add_argument(
        "--dirs",
        type=lambda p: BASE_DIR / p,
        nargs="*",
        help="Optional list of directories corresponding to years; defaults to '<year>_awards' if not provided",
    )
    parser.add_argument(
        "--data-dict",
        type=lambda p: BASE_DIR / p,
        default=(BASE_DIR / Path("../data_dict/data_dicts/data_dict.json")).resolve(),
        help="Path to the JSON data dictionary.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    BASE_DIR = Path(__file__).parent.parent.parent

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Construct years_dirs list
    if args.dirs and len(args.dirs) != len(args.years):
        raise ValueError("Number of directories must match number of years if provided")

    years_dirs = []
    for i, year in enumerate(args.years):
        directory = (
            args.dirs[i]
            if args.dirs and i < len(args.dirs)
            else (BASE_DIR / "data" / "raw" / f"{year}_awards")
        )
        years_dirs.append((year, directory))

    # Load schema and conversion options
    schema_file = BASE_DIR / "data" / "schema" / "contract_schema.json"
    improved_schema = load_schema_from_json(schema_file)

    combined_true_values, combined_false_values = get_true_false_from_data_dict(
        args.data_dict
    )
    convert_opts = pa_csv.ConvertOptions(
        column_types=improved_schema,
        strings_can_be_null=True,
        true_values=combined_true_values,
        false_values=combined_false_values,
    )
    parse_opts = pa_csv.ParseOptions(
        invalid_row_handler=lambda x: "skip",
    )
    create_partitioned_dataset(
        years_dirs, args.output_dir, convert_opts, parse_opts, args.overwrite
    )


if __name__ == "__main__":
    main()
