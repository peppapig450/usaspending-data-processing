import argparse
import logging
import time
from pathlib import Path

import polars as pl
import pyarrow.csv as pa_csv
import zstandard as zstd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

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
    "total_dollars_obligated",
]

# Define a mapping from original column names to simpler names
COLUMN_RENAME_MAP = {
    "period_of_performance_current_end_date": "end_date",
    "total_outlayed_amount_for_overall_award": "outlay_amount",
    "current_total_value_of_award": "current_award_value",
    "potential_total_value_of_award": "potential_award_value",
    "total_dollars_obligated": "obligated_amount",
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
        df = pl.from_arrow(table).rename(COLUMN_RENAME_MAP)  # type: ignore
        logger.info(
            f"Processed {zstd_path.name} (file {file_index}) - {len(table):,} rows"
        )
        return df.lazy(), row_count  # type: ignore
    except Exception:
        logger.exception("Error processing %s", zstd_path)
        return None, 0


def run_query_over_all_data(
    lazy_dfs: list[pl.LazyFrame], total_rows: int, sample_fraction: float | None = None
) -> tuple[pl.DataFrame, dict[str, object]]:
    """Execute query over combined LazyFrames and collect analysis results."""
    combined_lazy_df = pl.concat(lazy_dfs, how="vertical_relaxed")
    logger.info(f"Total pre-filtered dataset size: {total_rows:,} rows")

    query = combined_lazy_df.filter(
        pl.col("end_date").cast(pl.Date, strict=False).dt.year().eq(2024)
        & pl.col("outlay_amount").cast(pl.Float64, strict=False).gt(1_000_000)
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

    # Collect analysis results in a dictionary
    analysis_results: dict[str, object] = {
        "total_rows": total_rows,
        "filtered_rows": len(filtered_df),
        "result_rows": len(result_df),
    }

    # Calculate total dollar amounts
    analysis_results["total_outlay"] = float(
        result_df["outlay_amount"].fill_null(0).sum() or 0.0
    )
    analysis_results["total_current_value"] = float(
        result_df["current_award_value"].fill_null(0).sum() or 0.0
    )
    analysis_results["total_potential_value"] = float(
        result_df["potential_award_value"].fill_null(0).sum() or 0.0
    )
    analysis_results["total_obligated"] = float(
        result_df["obligated_amount"].fill_null(0).sum() or 0.0
    )

    # Comparison analysis
    less_than_potential = result_df.filter(
        pl.col("outlay_amount").fill_null(0)
        < pl.col("potential_award_value").fill_null(0)
    )
    exceeds_current = result_df.filter(
        pl.col("outlay_amount").fill_null(0)
        > pl.col("current_award_value").fill_null(0)
    )
    analysis_results["less_than_potential_count"] = len(less_than_potential)
    analysis_results["exceeds_current_count"] = len(exceeds_current)

    # Categorize obligations vs potential value
    comparison_df_outlays = result_df.select(
        [
            pl.col("outlay_amount").fill_null(0).alias("obligations"),
            pl.col("potential_award_value").fill_null(0).alias("potential"),
        ]
    ).with_columns(
        [
            # Categorize based on comparison
            pl.when(pl.col("obligations") < pl.col("potential"))
            .then(pl.lit("less_than"))
            .when(pl.col("obligations") == pl.col("potential"))
            .then(pl.lit("equal"))
            .otherwise(pl.lit("greater_than"))
            .alias("comparison")
        ]
    )

    # Count occurances in each category
    category_counts_outlays = (
        comparison_df_outlays.group_by("comparison")
        .agg(pl.len().alias("len"))
        .with_columns([(pl.col("len") / len(result_df) * 100).alias("percentage")])
    )

    # Obligated vs Potential comparison
    comparison_df_obligated = result_df.select(
        [
            pl.col("obligated_amount").fill_null(0).alias("obligated"),
            pl.col("potential_award_value").fill_null(0).alias("potential"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("obligated") < pl.col("potential"))
            .then(pl.lit("less_than"))
            .when(pl.col("obligated") == pl.col("potential"))
            .then(pl.lit("equal"))
            .otherwise(pl.lit("greater_than"))
            .alias("comparison")
        ]
    )

    category_counts_obligated = (
        comparison_df_obligated.group_by("comparison")
        .agg(pl.len().alias("len"))
        .with_columns([(pl.col("len") / len(result_df) * 100).alias("percentage")])
    )

    analysis_results["category_counts_outlays"] = category_counts_outlays
    analysis_results["category_counts_obligated"] = category_counts_obligated

    return result_df, analysis_results


def print_analysis_results(analysis_results: dict, time_taken: float):
    """Print all analysis results with formatted output using Rich."""
    console = Console()

    # Overall summary panel
    summary_text = (
        f"[bold]Total pre-filtered dataset size:[/bold] {analysis_results['total_rows']:,} rows\n"
        f"[bold]Filtered dataset size:[/bold] {analysis_results['filtered_rows']:,} rows "
        f"({analysis_results['filtered_rows'] / analysis_results['total_rows'] * 100:.1f}% of total)\n"
        f"[bold]Final result size:[/bold] {analysis_results['result_rows']:,} rows"
    )
    console.print(
        Panel(summary_text, title="[bold green]Final Analysis Results[/bold green]")
    )

    # Dollar Amounts Table
    dollar_table = Table(
        title="Total Dollar Amounts", show_edge=True, header_style="bold cyan"
    )
    dollar_table.add_column("Metric", style="dim", width=30)
    dollar_table.add_column("Amount", justify="right")
    dollar_table.add_row(
        "Total Outlay Amount", f"${analysis_results['total_outlay']:,.2f}"
    )
    dollar_table.add_row(
        "Total Current Value", f"${analysis_results['total_current_value']:,.2f}"
    )
    dollar_table.add_row(
        "Total Potential Value", f"${analysis_results['total_potential_value']:,.2f}"
    )
    dollar_table.add_row(
        "Total Obligated Amount", f"${analysis_results['total_obligated']:,.2f}"
    )
    console.print(dollar_table)

    # Contracts Summary Table
    contract_table = Table(
        title="Contracts Summary", show_edge=True, header_style="bold cyan"
    )
    contract_table.add_column("Metric", style="dim", width=30)
    contract_table.add_column("Count", justify="right")
    contract_table.add_row("Total Contracts", f"{analysis_results['result_rows']:,}")
    contract_table.add_row(
        "Outlay < Potential", f"{analysis_results['less_than_potential_count']:,}"
    )
    contract_table.add_row(
        "Outlay > Current",
        f"{analysis_results['exceeds_current_count']:,} ({analysis_results['exceeds_current_count'] / analysis_results['result_rows'] * 100:.1f}%)",
    )
    console.print(contract_table)

    # Outlays vs Potential Breakdown Table
    outlays_breakdown_table = Table(
        title="Outlays Vs Potential Value Breakdown",
        show_edge=True,
        header_style="bold cyan",
    )
    outlays_breakdown_table.add_column("Comparison", style="dim", width=30)
    outlays_breakdown_table.add_column("Percentage", justify="right")
    for row in analysis_results["category_counts_outlays"].iter_rows(named=True):
        if row["comparison"] == "less_than":
            label = "Outlays < Potential"
        elif row["comparison"] == "equal":
            label = "Outlays = Potential"
        elif row["comparison"] == "greater_than":
            label = "Outlays > Potential"
        outlays_breakdown_table.add_row(label, f"{row['percentage']:.1f}%")
    console.print(outlays_breakdown_table)

    # Obligated vs Potential Breakdown Table
    obligated_breakdown_table = Table(
        title="Obligated Vs Potential Value Breakdown",
        show_edge=True,
        header_style="bold cyan",
    )
    obligated_breakdown_table.add_column("Comparison", style="dim", width=30)
    obligated_breakdown_table.add_column("Percentage", justify="right")
    for row in analysis_results["category_counts_obligated"].iter_rows(named=True):
        if row["comparison"] == "less_than":
            label = "Obligated < Potential"
        elif row["comparison"] == "equal":
            label = "Obligated = Potential"
        elif row["comparison"] == "greater_than":
            label = "Obligated > Potential"
        obligated_breakdown_table.add_row(label, f"{row['percentage']:.1f}%")
    console.print(obligated_breakdown_table)

    # Processing Metrics Panel
    throughput = analysis_results["total_rows"] / time_taken if time_taken > 0 else 0
    processing_text = (
        f"[bold]Total Lines Processed:[/bold] {analysis_results['total_rows']:,} rows\n"
        f"[bold]Time Taken:[/bold] {time_taken:.2f} seconds\n"
        f"[bold]Throughput:[/bold] {throughput:,.0f} rows/second"
    )
    console.print(
        Panel(processing_text, title="[bold blue]Processing Metrics[/bold blue]")
    )


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
    extract_dir = (Path(__file__).parent / Path("../../data/raw/2024_awards")).resolve()
    print(extract_dir)

    zstd_files = list(extract_dir.glob("*.zst"))
    if not zstd_files:
        logging.error("No zstd files found in directory")
        return

    total_size_gb = sum(file.stat().st_size / (2 << 30) for file in zstd_files)
    logger.info(
        f"Processing {len(zstd_files)} zstd files ({total_size_gb:.2f} GB compressed)"
    )

    # Start timing
    start_time = time.time()

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
        result_df, analysis_results = run_query_over_all_data(
            lazy_dfs, total_pre_filtered_rows, sample_fraction=args.filter
        )

        # End timing
        end_time = time.time()
        time_taken = end_time - start_time

        # Print all analysis results at once
        print_analysis_results(analysis_results, time_taken)
    except ValueError:
        logger.exception("Invalid filter value")
        return


if __name__ == "__main__":
    main()
