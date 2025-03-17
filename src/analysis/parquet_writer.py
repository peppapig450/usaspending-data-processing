import argparse
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import zstandard as zstd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_zstd_to_arrow_table(zstd_path: Path, year: str) -> pa.Table | None:
    """Decompress zstd file, load into Arrow Table, and add 'year' column."""
    try:
        with (
            zstd_path.open("rb") as zstd_file,
            zstd.ZstdDecompressor().stream_reader(zstd_file) as reader,
        ):
            read_options = pa_csv.ReadOptions(
                block_size=64 << 20,  # 64MB chunks for efficient memory usage
            )
            parse_options = pa_csv.ParseOptions(
                invalid_row_handler=lambda x: "skip"  # Skip malformed rows
            )

            # Read all columns from the CSV
            table = pa_csv.read_csv(
                reader, read_options=read_options, parse_options=parse_options
            )

        # Add 'year' column
        year_array = pa.array([year] * len(table), type=pa.string())
        table = table.append_column("year", year_array)

        logger.info(f"Processed {zstd_path.name} - {len(table):,} rows")
        return table
    except Exception:
        logger.exception("Error processing %s", load_zstd_to_arrow_table)
        return None


def process_year_data(
    input_dir: Path, year: str, writer: pq.ParquetWriter, max_workers: int = 2
):
    """Process zstd files in parallel and write to Parquet."""
    zstd_files = sorted(input_dir.glob("*.zst"))
    if not zstd_files:
        logger.error("No zstd files found in %s", input_dir)
        return []

    total_size_gb = sum(file.stat().st_size / (2 << 30) for file in zstd_files)
    logger.info(
        f"Processing {len(zstd_files)} zstd files for {year} ({total_size_gb:.2f} GB compressed)"
    )

    for file_path in zstd_files:
        table = load_zstd_to_arrow_table(file_path, year)
        if table is not None:
            writer.write_table(table)


def create_parquet_from_years(
    years_dirs: list[tuple[str, Path]], output_parquet: Path, overwrite: bool = True
):
    """Create a Parquet file from zstd-compressed CSVs with specified row group size."""
    if output_parquet.is_file() and not overwrite:
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

    first_table = load_zstd_to_arrow_table(first_zstd, first_year)
    if not first_table:
        logger.error("Failed to load first table for schema")
        return

    schema = first_table.schema # TODO: schema error between two files

    # Write to Parquet with ZSTD compression
    writer_options = {
        "compression": "zstd",
        "compression_level": 10,
        "write_statistics": True,
        "write_batch_size": 64 << 20,  # 64MB
    }

    logger.info(f"Writing to {output_parquet} with schema: {schema}")
    with pq.ParquetWriter(output_parquet, schema, **writer_options) as writer:
        for year, input_dir in years_dirs:
            if not input_dir.is_dir():
                logger.warning(
                    "Directory %s does not exist, skipping %s", input_dir, year
                )
                continue
            process_year_data(input_dir, year, writer, schema)

    file_size_mb = output_parquet.stat().st_size / (2 << 20)
    logger.info(f"Parquet file written: {file_size_mb:.2f} MB")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert zstd-compressed CSVs to a Parquet file"
    )
    parser.add_argument(
        "--output-parquet",
        type=Path,
        default=Path("awards_data.parquet"),
        help="Path to output Parquet file",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
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
    return parser.parse_args()

# TODO: takes a minute+ to process each file: 
# run on just 2-3 files, and run memory profile and cprofile to figure out why
# maybe use polars for lazy loading?
def main():
    args = parse_args()

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

    args.output_parquet.parent.mkdir(parents=True, exist_ok=True)
    create_parquet_from_years(years_dirs, args.output_parquet, args.overwrite)


if __name__ == "__main__":
    main()
