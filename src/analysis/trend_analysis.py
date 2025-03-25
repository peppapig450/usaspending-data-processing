from functools import lru_cache

import polars as pl

# TODO: Compute sector growth rates or identify consistently top-performing sectors over time, though this would require more complex queries.

sector_mapping = {
    "11": "Agriculture, Forestry, Fishing and Hunting",
    "21": "Mining, Quarrying, and Oil and Gas Extraction",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing",
    "32": "Manufacturing",
    "33": "Manufacturing",
    "42": "Wholesale Trade",
    "44": "Retail Trade",
    "45": "Retail Trade",
    "48": "Transportation and Warehousing",
    "49": "Transportation and Warehousing",
    "51": "Information",
    "52": "Finance and Insurance",
    "53": "Real Estate and Rental and Leasing",
    "54": "Professional, Scientific, and Technical Services",
    "55": "Management of Companies and Enterprises",
    "56": "Administrative and Support and Waste Management",
    "61": "Educational Services",
    "62": "Health Care and Social Assistance",
    "71": "Arts, Entertainment, and Recreation",
    "72": "Accommodation and Food Services",
    "81": "Other Services (except Public Administration)",
    "92": "Public Administration",
}


@lru_cache(maxsize=len(sector_mapping))
def map_sector(sector_code: str) -> str:
    return sector_mapping.get(str(sector_code), "Unknown Sector")


def analyze_funding_trends(lazy_df: pl.LazyFrame) -> pl.DataFrame:
    """Analyze funding trends over time."""
    return (
        lazy_df.group_by("action_date_fiscal_year")
        .agg(
            [
                pl.col("total_dollars_obligated").sum().alias("total_obligated"),
                pl.col("total_outlayed_amount_for_overall_award")
                .sum()
                .alias("total_outlayed"),
                pl.col("federal_action_obligation").sum().alias("federal_obligation"),
                pl.len().alias("number_of_awards"),
                (pl.col("total_dollars_obligated").sum() / pl.len()).alias(
                    "average_obligated_per_award"
                ),
            ]
        )
        .sort("action_date_fiscal_year")
        .with_columns(
            pl.col("total_obligated").shift(1).alias("previous_year_obligated")
        )
        .with_columns(
            (
                (pl.col("total_obligated") - pl.col("previous_year_obligated"))
                / pl.col("previous_year_obligated")
                * 100
            ).alias("yoy_change_percent")
        )
        .collect()
    )


def analyze_sector_funding(lazy_df: pl.LazyFrame, top_n: int = 5) -> pl.DataFrame:
    """Analyze funding trends by sector (NAICS)."""
    sector_trends_lazy = (
        lazy_df.with_columns(
            pl.col("naics_code").cast(str).str.slice(0, 2).alias("sector")
        )
        .with_columns(
            pl.col("sector")
            .map_elements(map_sector, return_dtype=pl.String)
            .alias("sector_name")
        )
        .group_by(
            [
                "action_date_fiscal_year",
                "sector_name",
                "naics_code",
                "naics_description",
            ]
        )
        .agg(
            pl.col("total_dollars_obligated").sum().alias("total_funding"),
            pl.len().alias("number_of_awards"),
            (pl.col("total_dollars_obligated").sum() / pl.len()).alias(
                "average_funding_per_award"
            ),
        )
        .with_columns(
            (
                pl.col("total_funding")
                / pl.col("total_funding").sum().over("action_date_fiscal_year")
            ).alias("funding_share")
        )
        .sort(["action_date_fiscal_year", "total_funding"], descending=[False, True])
    )
    return sector_trends_lazy.group_by("action_date_fiscal_year").head(top_n).collect()


def analyze_award_types(lazy_df: pl.LazyFrame) -> pl.DataFrame:
    """Analyze evolution of award types over time."""
    result = (
        lazy_df.group_by(["action_date_fiscal_year", "award_type"])
        .agg(
            pl.len().alias("award_count"),
            pl.col("total_dollars_obligated").sum().alias("total_funding"),
        )
        .with_columns(
            (
                pl.col("award_count")
                / pl.col("award_count").sum().over("action_date_fiscal_year")
            ).alias("count_share"),
            (
                pl.col("total_funding")
                / pl.col("total_funding").sum().over("action_date_fiscal_year")
            ).alias("funding_share"),
        )
        .sort("action_date_fiscal_year")
    )
    with pl.StringCache():
        return result.collect()
