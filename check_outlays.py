import logging
from pathlib import Path
import argparse

import polars as pl
import pyarrow.csv as pa_csv
import zstandard as zstd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Define required columns
REQUIRED_COLUMNS = [
    "period_of_performance_current_end_date",
    "total_outlayed_amount_for_overall_award",
    "current_total_value_of_award",
    "potential_total_value_of_award",
]

# Define a mapping from original column names to simpler names
COLUMN_RENAME_MAP = {
    "period_of_performance_current_end_date": "end_date",
    "total_outlayed_amount_for_overall_award": "outlay_amount",
    "current_total_value_of_award": "current_award_value",
    "potential_total_value_of_award": "potential_award_value",
}



def load_zstd_with_pyarrow(
    zstd_path: Path, file_index: int
) -> tuple[pl.LazyFrame | None, int]:
    """Decompress zstd file and load required columns with PyArrow."""
    try:
        # Open zstd file and decompress in streaming mode
        with (
            zstd_path.open("rb") as zstd_file,
            zstd.ZstdDecompressor().stream_reader(zstd_file) as reader,
        ):
            read_options = pa_csv.ReadOptions(
                block_size=64 << 20,  # 64MB chunks
            )
            convert_options = pa_csv.ConvertOptions(include_columns=REQUIRED_COLUMNS)
            parse_options = pa_csv.ParseOptions(
                invalid_row_handler=lambda x: "skip"  # Skip malformed rows
            )

            # Read decompressed CSV with PyArrow
            table = pa_csv.read_csv(
                reader,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )

        # Convert to Polars LazyFrame
        row_count = len(table)
        df = pl.from_arrow(table).rename(COLUMN_RENAME_MAP) #type: ignore
        logger.info(
            f"Processed {zstd_path.name} (file {file_index}) - {len(table):,} rows"
        )
        return df.lazy(), row_count  # type: ignore
    except Exception:
        logger.exception("Error processing %s", zstd_path)
        return None, 0


def run_query_over_all_data(
    lazy_dfs: list[pl.LazyFrame], total_rows: int, sample_fraction: float | None = None
) -> pl.DataFrame:
    """Execute query over combined LazyFrames."""
    combined_lazy_df = pl.concat(lazy_dfs, how="vertical_relaxed")
    logger.info(f"Total pre-filtered dataset size: {total_rows:,} rows")

    query = combined_lazy_df.filter(
        pl.col("end_date")
        .cast(pl.Date, strict=False)
        .dt.year()
        .eq(2024)
        & pl.col("outlay_amount")
        .cast(pl.Float64, strict=False)
        .gt(1_000_000)
    )

    logger.info("Executing query with streaming...")
    filtered_df = query.collect(
        predicate_pushdown=True,
        projection_pushdown=True,
        slice_pushdown=True
        if sample_fraction
        else False,  # Set to true if we work with a subset of data
    )

    filtered_rows = len(filtered_df)
    logger.info(
        f"Filtered dataset size: {filtered_rows:,} rows "
        f"({filtered_rows / total_rows * 100:.1f}% of total)"
    )

    # Apply sampling if specified
    if sample_fraction is not None:
        if not 0 < sample_fraction <= 1:
            raise ValueError("Sample fraction must be between 0 and 1")
        result_df = filtered_df.sample(fraction=sample_fraction, shuffle=True, seed=69)
        sampled_rows = len(result_df)
        logger.info(
            f"Sampled {sampled_rows:,} rows ({sample_fraction * 100:.1f}% of filtered data)"
        )
    else:
        result_df = filtered_df
        logger.info("Using entire filtered dataset (no sampling)")

    # Analysis
    total_rows_result = len(result_df)
    less_than_potential = result_df.filter(
        pl.col("outlay_amount").fill_null(0)
        < pl.col("potential_award_value").fill_null(0)
    )
    exceeds_current = result_df.filter(
        pl.col("outlay_amount").fill_null(0)
        > pl.col("current_award_value").fill_null(0)
    )

    logger.info(f"Total contracts: {total_rows_result:,}")
    logger.info(f"Outlay < Potential: {len(less_than_potential):,}")
    logger.info(
        f"Outlay > Current: {len(exceeds_current):,} ({len(exceeds_current) / total_rows_result * 100:.1f}%)"
    )

    return result_df


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Process 2024 awards data")
    parser.add_argument(
        "--filter",
        type=float,
        default=None,
        help="Fraction of filtered data to sample (0 to 1), e.g., 0.1 for 10%. If not provided, use full dataset.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    extract_dir = Path("2024_awards")

    zstd_files = list(extract_dir.glob("*.zst"))
    if not zstd_files:
        logging.error("No zstd files found in directory")
        return

    total_size_gb = sum(file.stat().st_size / (2 << 30) for file in zstd_files)
    logger.info(
        f"Processing {len(zstd_files)} zstd files ({total_size_gb:.2f} GB compressed)"
    )

    # Load each zstd file and track row counts
    lazy_dfs = []
    total_pre_filtered_rows = 0
    for i, zstd_file in enumerate(zstd_files, 1):
        lazy_df, row_count = load_zstd_with_pyarrow(zstd_file, i)
        if lazy_df is not None:
            lazy_dfs.append(lazy_df)
            total_pre_filtered_rows += row_count

    if not lazy_dfs:
        logging.error("No valid data to process")
        return

    # Run query with optional sampling
    try:
        result_df = run_query_over_all_data(
            lazy_dfs, total_pre_filtered_rows, sample_fraction=args.filter
        )
        logger.info(f"Sample result:\n{result_df.head(5)}")
    except ValueError:
        logger.exception("Invalid filter value")
        return


if __name__ == "__main__":
    main()
