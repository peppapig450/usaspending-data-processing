import argparse
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.dataset as ds
import zstandard as zstd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_zstd_to_batches(zstd_path: Path, year: str) -> list[pa.RecordBatch]:
    """Stream zstd file and return a list of record batches with year column."""
    try:
        with (
            zstd_path.open("rb") as zstd_file,
            zstd.ZstdDecompressor().stream_reader(zstd_file) as reader,
        ):
            csv_reader = pa_csv.open_csv(
                reader,
                read_options=pa_csv.ReadOptions(block_size=64 << 20),  # 64MB batches
                parse_options=pa_csv.ParseOptions(invalid_row_handler=lambda x: "skip"),
                convert_options=pa_csv.ConvertOptions(
                    column_types={
                        "national_interest_action_code": pa.string(),
                    },
                    null_values=["", "NA", "NULL", "NONE"],
                ),
            )
            logger.debug(f"Schema: {csv_reader.schema}")

            batches = []
            total_rows = 0
            for batch in csv_reader:
                year_array = pa.array([year] * batch.num_rows, type=pa.string())
                batch_with_year = pa.RecordBatch.from_arrays(
                    batch.columns + [year_array], names=batch.schema.names + ["year"]
                )
                batches.append(batch_with_year)
                total_rows += batch_with_year.num_rows

            return batches
    except Exception:
        logger.exception("Error processing %s", zstd_path)
        return []


def create_partitioned_dataset(
    years_dirs: list[tuple[str, Path]], output_dir: Path, overwrite: bool = True
):
    """Create a partitioned dataset with one Parquet file per directory."""
    if output_dir.is_file() and not overwrite:
        logger.error(
            "Output file exists and overwrite=False; use overwrite=True or remove file."
        )
        return

    # Get schema from first file
    first_year, first_dir = years_dirs[0]
    first_zstd = next(first_dir.glob("*.zst"), None)
    if not first_zstd:
        logger.error("No zstd files found in %s", first_dir)
        return

    first_batches = load_zstd_to_batches(first_zstd, first_year)
    if not first_batches:
        logger.error("Failed to load first table for schema")
        return

    # Use schema directly from the first batch instead of creating a table
    schema = first_batches[0].schema

    # Create output directory if using existing_data_behavior
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each directory (year)
    for year, input_dir in years_dirs:
        if not input_dir.is_dir():
            logger.warning("Directory %s does not exist, skipping %s", input_dir, year)
            continue

        # Accumulate all batches for the year
        all_batches = []
        zstd_files = sorted(input_dir.glob("*.zst"))
        if not zstd_files:
            logger.warning("No zstd files found in %s, skipping %s", input_dir, year)
            continue

        for file_path in zstd_files:
            batches = load_zstd_to_batches(file_path, year)
            all_batches.extend(batches)

        if all_batches:
            # Write all batches for this year as one table
            ds.write_dataset(
                data=all_batches,
                base_dir=output_dir,
                format="parquet",
                partitioning=ds.partitioning(schema, field_names=["year"]),
                existing_data_behavior="overwrite_or_ignore",
            )
            logger.info("Wrote %s partition", year)
        else:
            logger.warning("No valid data processed for %s", year)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert zstd-compressed CSVs to a partitioned Parquet dataset"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("awards_dataset"),
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
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Construct years_dirs list
    if args.dirs and len(args.dirs) != len(args.years):
        raise ValueError("Number of directories must match number of years if provided")

    years_dirs = []
    for i, year in enumerate(args.years):
        if args.dirs and i < len(args.dirs):
            directory = args.dirs[i]
        else:
            directory = Path(f"{year}_awards")
        years_dirs.append((year, directory))

    create_partitioned_dataset(years_dirs, args.output_dir, args.overwrite)


if __name__ == "__main__":
    main()
