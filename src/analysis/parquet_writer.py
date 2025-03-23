import argparse
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.dataset as ds

from config.settings import BASE_DIR, SCHEMA_FILES
from src.utils import (
    get_true_false_from_data_dict,
    load_schema_from_json,
    load_zstd_to_batches,
)

logger = logging.getLogger(__name__)


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

    # Parquet file options for dataset
    file_options = ds.ParquetFileFormat(
        read_options=None,  # type: ignore
        default_fragment_scan_options=None,  # type: ignore
    ).make_write_options(
        compression="zstd",  # type: ignore
        compression_level=9,  # type: ignore
    )

    # Process each directory (year)
    for year, input_dir in years_dirs:
        if not input_dir.is_dir():
            logger.warning("Directory %s does not exist, skipping %s", input_dir, year)
            continue

        zstd_files = sorted(input_dir.glob("*.zst"))
        if not zstd_files:
            logger.warning("No zstd files found in %s, skipping %s", input_dir, year)
            continue

        for file_path in zstd_files:
            file_size = file_path.stat().st_size
            logger.info("Processing file %s with size %d bytes", file_path, file_size)

            batch_iter, file_schema = load_zstd_to_batches(
                file_path, year, convert_opts, parse_opts
            )

            required_fields = {"year", "file"}
            if not required_fields.issubset(file_schema.names):
                logger.warning(
                    "Skipping file %s due to missing fields %s",
                    file_path,
                    required_fields - set(file_schema.names),
                )
                continue

            try:
                partitioning_schema = pa.schema(
                    [
                        file_schema.field("year"),
                        file_schema.field("file"),
                    ]
                )
                ds.write_dataset(
                    data=batch_iter,
                    base_dir=output_dir,
                    schema=file_schema,
                    format="parquet",
                    partitioning=ds.partitioning(
                        partitioning_schema,
                        flavor="hive",
                    ),
                    existing_data_behavior="overwrite_or_ignore",
                    file_options=file_options,
                )
                logger.info("Wrote partition for year=%s", year)
            except Exception:
                logger.exception("Failed to write dataset for file %s", file_path)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert zstd-compressed CSVs to a partitioned Parquet dataset"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BASE_DIR / "data" / "processed" / "awards_dataset",
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
        type=Path,
        nargs="*",
        help="Optional list of directories corresponding to years; defaults to '<year>_awards' if not provided",
    )
    parser.add_argument(
        "--data-dict",
        type=Path,
        default=SCHEMA_FILES["data_dict"],
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
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Construct years_dirs list
    if args.dirs and len(args.dirs) != len(args.years):
        raise ValueError("Number of directories must match number of years if provided")

    years_dirs = []
    for i, year in enumerate(args.years):
        directory = (
            args.dirs[i]
            if args.dirs and i < len(args.dirs)
            else BASE_DIR / "data" / "raw" / f"{year}_awards"
        )
        years_dirs.append((year, directory))

    # Load schema and conversion options
    schema_file = BASE_DIR / "data" / "schemas" / "contract_schema.json"
    improved_schema = load_schema_from_json(schema_file)

    combined_true_values, combined_false_values = get_true_false_from_data_dict(
        args.data_dict
    )
    logger.info(
        "Combined true values: %s\nCombined false values: %s",
        combined_true_values,
        combined_false_values,
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
