"""
Subset Builder Module

This module processes a CSV file containing contract award data to generate a
subset of the dataset for testing purposes. The subset is created by:
- Selecting a fixed list of initial columns.
- Converting certain string-encoded booleans to actual boolean types.
- Filling missing values in the "award_type" column with "unknown".
- Sampling a stratified test subset by "award_type".
- Adding edge case records based on quantile thresholds for "current_total_value_of_award".
- Merging additional columns (if present) that are not part of the initial set.

The processed subset is written to a specified output CSV file. The module leverages
Polars for efficient lazy evaluation, making it suitable for processing large datasets.
"""

import argparse
from pathlib import Path

import polars as pl
from src.utils import to_bool

# Define schema with explicit data types
SCHEMA_DICT: dict[str, pl.DataType] = {
    "contract_award_unique_key": pl.String(),
    "current_total_value_of_award": pl.Float64(),
    "award_type": pl.Categorical(),
    "recipient_name": pl.String(),
    "naics_code": pl.UInt32(),
    "awarding_agency_code": pl.UInt16(),
    "national_interest_action_code": pl.String(),
    "minority_owned_business": pl.Boolean(),
    "woman_owned_business": pl.Boolean(),
}

# Derive INITIAL_COLUMNS dynamically
INITIAL_COLUMNS = list(SCHEMA_DICT.keys())

SCHEMA = pl.Schema(SCHEMA_DICT)



def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the input and output CSV file paths.

    The function utilizes argparse to define the following required arguments:
      - --input-file (-i): Path to the input CSV file.
      - --output-file (-o): Path to the output CSV file.

    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Process contract data and write a subset to a CSV file."
    )
    parser.add_argument(
        "-i",
        "--input-file",
        type=str,
        required=True,
        help="Path to the input CSV file.",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        type=str,
        required=True,
        help="Path to the output CSV file.",
    )
    return parser.parse_args()


def process_data(file_path: Path) -> pl.LazyFrame:
    """
    Process the input CSV file and generate a subset of contract award data.

    This function performs the following steps:
      1. Loads the CSV file lazily and selects the predefined INITIAL_COLUMNS.
      2. Converts string representations of booleans in 'woman_owned_business' and
         'minority_owned_business' columns using the to_bool function.
      3. Fills null values in the 'award_type' column with the string "unknown".
      4. Computes 95th and 5th quantiles for the 'current_total_value_of_award' column
         to establish thresholds for edge cases.
      5. Creates a stratified sample by grouping data by 'award_type' and sampling
         20% from each group.
      6. Identifies edge cases by filtering records that exceed the high threshold or
         fall below the low threshold.
      7. Concatenates the stratified sample with the edge cases, ensuring no duplicates.
      8. Loads additional columns (if any) that were not included in INITIAL_COLUMNS
         and joins them with the final subset based on 'contract_award_unique_key'.

    Parameters:
        file_path (Path): The path to the input CSV file.

    Returns:
        pl.LazyFrame: A lazy frame representing the final processed subset of data.
    """
    # Optimized lazy loading
    lazy_df = (
        pl.scan_csv(file_path)
        .select(INITIAL_COLUMNS)
        .with_columns(
            [
                pl.col("woman_owned_business").map_elements(
                    to_bool, return_dtype=pl.Boolean
                ),
                pl.col("minority_owned_business").map_elements(
                    to_bool, return_dtype=pl.Boolean
                ),
                pl.col("award_type").fill_null("unknown"),
            ]
        )
    )

    # Compute thresholds in a single pass
    thresholds = lazy_df.select(
        [
            pl.col("current_total_value_of_award").quantile(0.95).alias("high"),
            pl.col("current_total_value_of_award").quantile(0.05).alias("low"),
        ]
    ).collect()
    high_award_threshold, low_award_threshold = (
        thresholds["high"][0],
        thresholds["low"][0],
    )

    # Create test subset with stratification and edge cases
    test_subset_lazy = (
        lazy_df.group_by("award_type")
        .agg(pl.all().sample(fraction=0.2, seed=69))
        .explode(pl.exclude("award_type"))
        .select(INITIAL_COLUMNS)
    )

    edge_cases = (
        lazy_df.filter(
            (pl.col("current_total_value_of_award") > high_award_threshold)
            | (pl.col("current_total_value_of_award") < low_award_threshold)
        )
        .unique()
        .select(INITIAL_COLUMNS)
    )

    # Concatenate lazily
    final_subset = (
        pl.concat([test_subset_lazy, edge_cases], how="vertical", rechunk=True)
        .unique()
        .with_columns(pl.col("naics_code").cast(pl.UInt32))
    )

    # Load missing columns only for final subset
    missing_columns = [
        col
        for col in pl.scan_csv(
            file_path,
            schema_overrides={
                "dod_acquisition_program_code": pl.String,
                "parent_award_agency_id": pl.String,
                "recipient_phone_number": pl.String,
                "recipient_fax_number": pl.String,
                "recipient_zip_4_code": pl.String,
            },
        )
        .collect_schema()
        .keys()
        if col not in INITIAL_COLUMNS
    ]
    missing_columns_df = (
        pl.scan_csv(
            file_path,
            low_memory=True,
            schema_overrides={
                "dod_acquisition_program_code": pl.String,
                "parent_award_agency_id": pl.String,
                "recipient_phone_number": pl.String,
                "recipient_fax_number": pl.String,
                "recipient_zip_4_code": pl.String,
            },
        )
        .select(["contract_award_unique_key"] + missing_columns)
        .join(
            final_subset.select("contract_award_unique_key").lazy(),
            on="contract_award_unique_key",
            how="inner",
        )
        .lazy()
    )

    # Single join to avoid redundant operations
    final_subset = final_subset.join(
        missing_columns_df, on="contract_award_unique_key", how="left"
    )

    return final_subset


def main():
    """
    Main function to execute the subset builder process.

    The function performs the following:
      1. Parses command-line arguments for input and output file paths.
      2. Processes the CSV data using process_data.
      3. Collects the final lazy frame and writes it to the output CSV file.
      4. Handles exceptions by printing an error message and re-raising the exception.

    This function is the entry point of the script when executed from the command line.
    """
    args = parse_args()
    input_file = Path(args.input_file).resolve()
    output_file = Path(args.output_file).resolve()

    if not input_file.is_file():
        raise FileNotFoundError(f"Input file does not exist: {input_file}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Process the data
        final_subset = process_data(input_file)

        # Write to CSV
        final_subset.collect().write_csv(output_file)
        print(f"Results successfully written to {output_file}")
    except Exception as e:
        print(f"Error processing data or writing to file: {e}")
        raise


if __name__ == "__main__":
    main()
