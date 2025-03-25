from pathlib import Path

import polars as pl


def load_dataset(parquet_path: str | Path, columns: list[str]) -> pl.DataFrame:
    """
    Load a Hive-partitioned Parquet dataset and return it as a collected DataFrame.

    Args:
        parquet_path: Path to the dataset (e.g., "data/*/*/*.parquet")
        columns: List of columns to select

    Returns:
        pl.DataFrame: Collected DataFrame
    """
    return (
        pl.scan_parquet(parquet_path, hive_partitioning=True).select(columns).collect()
    )


def get_unique_values(df: pl.DataFrame, column: str) -> list:
    """Extract unique sorted values from a DataFrame column."""
    return df[column].unique().sort().to_list()
