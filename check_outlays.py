import cProfile
import logging
import pstats
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv
import zstandard as zstd
from memory_profiler import profile

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

FIRST_ROW_SIZE: int = 8682


@profile
def load_zstd_with_pyarrow(zstd_path: Path, file_index: int) -> pl.LazyFrame | None:
    """Decompress zstd file and load required columns with PyArrow."""
    try:
        # Open zstd file and decompress in streaming mode
        with (
            zstd_path.open("rb") as zstd_file,
            zstd.ZstdDecompressor().stream_reader(zstd_file) as reader,
        ):
            preview_size = (
                FIRST_ROW_SIZE  # 8.662KB for previewing headers and some rows
            )

            # Read a small chunk for header extraction
            if not (preview_buffer := reader.read(preview_size)):
                logger.error("Empty preview buffer for %s", zstd_path.name)
                return None

            # Use PyArrow to parse headers and detect delimiter
            table_preview = pa_csv.read_csv(
                pa.BufferReader(preview_buffer),
                read_options=pa_csv.ReadOptions(),  # Small block for preview
                parse_options=pa_csv.ParseOptions(),
            )
            headers = table_preview.column_names
            logger.debug(f"Headers in {zstd_path.name}: {headers}")

            # Check for missing required columns
            missing_cols = [col for col in REQUIRED_COLUMNS if col not in headers]
            if missing_cols:
                logger.error(
                    f"Skipping {zstd_path.name} (file {file_index}): "
                    f"Missing required columns: {missing_cols}"
                )
                return None

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
        df = pl.from_arrow(table)
        logger.info(
            f"Processed {zstd_path.name} (file {file_index}) - {len(table):,} rows"
        )
        return df.lazy()  # type: ignore
    except Exception:
        logger.exception("Error processing %s", zstd_path)
        return None


@profile
def run_query_over_all_data(lazy_dfs: list[pl.LazyFrame]) -> pl.DataFrame:
    """Execute query over combined LazyFrames."""
    combined_lazy_df = pl.concat(lazy_dfs, how="vertical_relaxed")

    query = combined_lazy_df.filter(
        pl.col("period_of_performance_current_end_date")
        .cast(pl.Date, strict=False)
        .dt.year()
        .eq(2024)
        & pl.col("total_outlayed_amount_for_overall_award")
        .cast(pl.Float64, strict=False)
        .gt(1_000_000)
    )

    logger.info("Executing query with streaming...")
    filtered_df = query.collect(
        predicate_pushdown=True,
        projection_pushdown=True,
        slice_pushdown=True,  # Set to true if we work with a subset of data
    )

    filtered_rows = len(filtered_df)
    logger.info(f"Filtered dataset size: {filtered_rows:,} rows")

    # Sample a fraction (e.g., 10%) of the filtered data
    fraction_to_sample = 0.25  # Adjust this value as needed (e.g., 0.05 for 5%)
    result_df = filtered_df.sample(fraction=fraction_to_sample, shuffle=True, seed=69)
    sampled_rows = len(result_df)
    logger.info(
        f"Sampled {sampled_rows:,} rows ({fraction_to_sample * 100:.1f}% of filtered data)"
    )

    # Analysis
    total_rows = len(result_df)
    less_than_potential = result_df.filter(
        pl.col("total_outlayed_amount_for_overall_award").fill_null(0)
        < pl.col("potential_total_value_of_award").fill_null(0)
    )
    exceeds_current = result_df.filter(
        pl.col("total_outlayed_amount_for_overall_award").fill_null(0)
        > pl.col("current_total_value_of_award").fill_null(0)
    )

    logger.info(f"Total contracts: {total_rows:,}")
    logger.info(f"Outlay < Potential: {len(less_than_potential):,}")
    logger.info(
        f"Outlay > Current: {len(exceeds_current):,} ({len(exceeds_current) / total_rows * 100:.1f}%)"
    )

    return result_df


def profile_main():
    extract_dir = Path("2024_awards")
    zstd_files = sorted(extract_dir.glob("*.zst"), key=lambda f: f.stat().st_size)

    if not zstd_files:
        logger.error("No zstd files found in directory")
        return

    # Get the first two files
    first_two_files = zstd_files[:2]

    for file in first_two_files:
        logging.info(
            f"Profiling file: {file.name} ({file.stat().st_size / (1 << 20):.2f} MB compressed)"
        )

        # Profile memory and time for loading
        lazy_df = load_zstd_with_pyarrow(file, 1)
        if lazy_df is None:
            logging.error(f"Failed to load file: {file.name}")
            continue  # Continue to the next file.

        # Profile memory and time for query execution
        profiler = cProfile.Profile()
        result_df = profiler.runcall(run_query_over_all_data, [lazy_df])
        profiler.dump_stats(f"query_profile_{file.name}.prof")

        # Analyze cProfile results
        stats = pstats.Stats(f"query_profile_{file.name}.prof")
        stats.sort_stats(pstats.SortKey.CUMULATIVE).print_stats(20)


def main():
    extract_dir = Path("2024_awards")

    zstd_files = list(extract_dir.glob("*.zst"))
    if not zstd_files:
        logging.error("No zstd files found in directory")
        return

    total_size_gb = sum(file.stat().st_size / (2 << 30) for file in zstd_files)
    logger.info(
        f"Processing {len(zstd_files)} zstd files ({total_size_gb:.2f} GB compressed)"
    )

    # Load each zstd file with PyArrow
    lazy_dfs = [
        lazy_df
        for i, zstd_file in enumerate(zstd_files, 1)
        if (lazy_df := load_zstd_with_pyarrow(zstd_file, i)) is not None
    ]

    if not lazy_dfs:
        logging.error("No valid data to process")
        return

    # Run query
    result_df = run_query_over_all_data(lazy_dfs)
    logger.info(f"Sample result:\n{result_df.head(5)}")


if __name__ == "__main__":
    profile_main()
