from functools import lru_cache
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

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


def load_dataset(
    parquet_path: str | Path, columns: list[str], filters: list | None = None
) -> pl.LazyFrame:
    """
    Lazily load a Hive-partitioned Parquet dataset with specific columns.

    Args:
        parquet_path: Path to the dataset (e.g., "data/*/*/*.parquet" for Hive structure)
        columns: List of columns to select
    """
    return pl.scan_parquet(parquet_path, hive_partitioning=True).select(columns)


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
        .group_by(["action_date_fiscal_year", "naics_code", "naics_description"])
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
    return (
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
        .collect()
    )


def plot_funding_totals(df: pl.DataFrame) -> go.Figure:
    """
    Create a line plot of total funding metrics over time.

    Args:
        df (pl.DataFrame): DataFrame from analyze_funding_trends

    Returns:
        go.Figure: Plotly figure object
    """
    fig = px.line(
        df,
        x="action_date_fiscal_year",
        y=["total_obligated", "total_outlayed", "federal_obligation"],
        title="Total Funding Over Time",
        labels={
            "value": "Amount ($)",
            "variable": "Funding Type",
            "action_date_fiscal_year": "Fiscal Year",
        },
    )
    return fig


def plot_number_of_awards(df: pl.DataFrame) -> go.Figure:
    """
    Create a line plot of the number of awards over time.

    Args:
        df (pl.DataFrame): DataFrame from analyze_funding_trends

    Returns:
        go.Figure: Plotly figure object
    """
    fig = px.line(
        df,
        x="action_date_fiscal_year",
        y="number_of_awards",
        title="Number of Awards Over Time",
        labels={
            "number_of_awards": "Number of Awards",
            "action_date_fiscal_year": "Fiscal Year",
        },
    )
    return fig


def plot_average_obligated(df: pl.DataFrame) -> go.Figure:
    """
    Create a bar plot of average obligated amount per award over time.

    Args:
        df (pl.DataFrame): DataFrame from analyze_funding_trends

    Returns:
        go.Figure: Plotly figure object
    """
    fig = px.bar(
        df,
        x="action_date_fiscal_year",
        y="average_obligated_per_award",
        title="Average Obligated per Award Over Time",
        labels={
            "average_obligated_per_award": "Average Amount ($)",
            "action_date_fiscal_year": "Fiscal Year",
        },
    )
    return fig


def plot_yoy_change(df: pl.DataFrame) -> go.Figure:
    """
    Create a line plot of year-over-year change in total obligated funding.

    Args:
        df (pl.DataFrame): DataFrame from analyze_funding_trends

    Returns:
        go.Figure: Plotly figure object
    """
    fig = px.line(
        df,
        x="action_date_fiscal_year",
        y="yoy_change_percent",
        title="Year-over-Year Change in Total Obligated (%)",
        labels={
            "yoy_change_percent": "Change (%)",
            "action_date_fiscal_year": "Fiscal Year",
        },
    )
    return fig


def plot_sector_funding(df: pl.DataFrame) -> go.Figure:
    """
    Create a stacked bar chart of top sectors' funding over time.

    Args:
        df (pl.DataFrame): DataFrame from analyze_sector_funding

    Returns:
        go.Figure: Plotly figure object
    """
    fig = px.bar(
        df,
        x="action_date_fiscal_year",
        y="total_funding",
        color="naics_description",
        title="Top Sectors Funding Over Time",
        labels={
            "total_funding": "Total Funding ($)",
            "naics_description": "Sector",
            "action_date_fiscal_year": "Fiscal Year",
        },
        barmode="stack",
    )
    return fig


def plot_award_types_count(df: pl.DataFrame) -> go.Figure:
    """
    Create a stacked bar chart of award counts by type over time.

    Args:
        df (pl.DataFrame): DataFrame from analyze_award_types

    Returns:
        go.Figure: Plotly figure object
    """
    fig = px.bar(
        df,
        x="action_date_fiscal_year",
        y="award_count",
        color="award_type",
        title="Award Counts by Type Over Time",
        labels={
            "award_count": "Number of Awards",
            "award_type": "Award Type",
            "action_date_fiscal_year": "Fiscal Year",
        },
        barmode="stack",
    )
    return fig


def plot_award_types_funding(df: pl.DataFrame) -> go.Figure:
    """
    Create a stacked bar chart of award funding by type over time.

    Args:
        df (pl.DataFrame): DataFrame from analyze_award_types

    Returns:
        go.Figure: Plotly figure object
    """
    fig = px.bar(
        df,
        x="action_date_fiscal_year",
        y="total_funding",
        color="award_type",
        title="Award Funding by Type Over Time",
        labels={
            "total_funding": "Total Funding ($)",
            "award_type": "Award Type",
            "action_date_fiscal_year": "Fiscal Year",
        },
        barmode="stack",
    )
    return fig


def plot_funding_trends_subplots(df: pl.DataFrame) -> go.Figure:
    """
    Create a subplot figure combining funding totals, number of awards,
    average obligated per award, and year-over-year change.

    Args:
        df (pl.DataFrame): DataFrame from analyze_funding_trends

    Returns:
        go.Figure: Plotly figure object with subplots
    """
    # Define subplot layout: 2 rows, 2 columns
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Total Funding Over Time",
            "Number of Awards Over Time",
            "Average Obligated per Award",
            "Year-over-Year Change (%)",
        ),
        vertical_spacing=0.15,
        horizontal_spacing=0.1,
    )

    # Plot 1: Total Funding (Line Plot)
    for col in ["total_obligated", "total_outlayed", "federal_obligation"]:
        fig.add_trace(
            go.Scatter(
                x=df["action_date_fiscal_year"],
                y=df[col],
                name=col.replace("_", " ").title(),
                mode="lines+markers",
            ),
            row=1,
            col=1,
        )
    fig.update_xaxes(title_text="Fiscal Year", row=1, col=1)
    fig.update_yaxes(title_text="Amount ($)", row=1, col=1)

    # Plot 2: Number of Awards (Line Plot)
    fig.add_trace(
        go.Scatter(
            x=df["action_date_fiscal_year"],
            y=df["number_of_awards"],
            name="Awards",
            mode="lines+markers",
            line=dict(color="royalblue"),
        ),
        row=1,
        col=2,
    )
    fig.update_xaxes(title_text="Fiscal Year", row=1, col=2)
    fig.update_yaxes(title_text="Number of Awards", row=1, col=2)

    # Plot 3: Average Obligated per Award (Bar Plot)
    fig.add_trace(
        go.Bar(
            x=df["action_date_fiscal_year"],
            y=df["average_obligated_per_award"],
            name="Avg Obligated",
            marker_color="green",
        ),
        row=2,
        col=1,
    )
    fig.update_xaxes(title_text="Fiscal Year", row=2, col=1)
    fig.update_yaxes(title_text="Average Amount ($)", row=2, col=1)

    # Plot 4: Year-over-Year Change (Line Plot)
    fig.add_trace(
        go.Scatter(
            x=df["action_date_fiscal_year"],
            y=df["yoy_change_percent"],
            name="YoY Change",
            mode="lines+markers",
            line=dict(color="red"),
        ),
        row=2,
        col=2,
    )
    fig.update_xaxes(title_text="Fiscal Year", row=2, col=2)
    fig.update_yaxes(title_text="Change (%)", row=2, col=2)

    # Update layout
    fig.update_layout(
        height=800, width=1000, title_text="Funding Trends Analysis", showlegend=True
    )

    return fig


def plot_sector_funding_subplots(df: pl.DataFrame) -> go.Figure:
    """
    Create a subplot figure combining total funding, number of awards, and
    average funding per award for top sectors.

    Args:
        df (pl.DataFrame): DataFrame from analyze_sector_funding

    Returns:
        go.Figure: Plotly figure object with subplots
    """
    # Create subplots: 2 rows, 2 columns (using only 3 slots)
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Total Funding by Sector",
            "Number of Awards by Sector",
            "Average Funding per Award",
        ),
        specs=[[{}, {}], [{"colspan": 2}, None]],
        vertical_spacing=0.15,
    )
    
    # Get unique sectors
    unique_sectors = df["sector_name"].unique().to_list()
    
    for sector in unique_sectors:
        sector_df = df.filter(pl.col("sector_name") == sector)
        
        # Total funding (Stacked Bar, Row 1, Col 1)
        fig.add_trace(
            go.Bar(
                x=sector_df["action_date_fiscal_year"],
                y=sector_df["total_funding"],
                name=sector,
                legendgroup="funding",
            ),
            row=1, col=1
        )
        
        # Number of Awards (Stacked Bar, Row 1, Col 1)
        fig.add_trace(
            go.Bar(
                x=sector_df["action_date_fiscal_year"],
                y=sector_df["number_of_awards"],
                name=sector,
                legendgroup="awards",
                showlegend=False
            ),
            row=1, col=2
        )
        
        # Average Funding per Award (Bar, Row 2, Col 1)
        fig.add_trace(
            go.Bar(
                x=sector_df["action_date_fiscal_year"],
                y=sector_df["average_funding_per_award"],
                name=sector,
                legendgroup="avg_funding",
                showlegend=False
            ),
            row=2, col=1
        )
        
    # Update layout and axes
    fig.update_layout(
        height=800,
        width=1000,
        title_text="Sector Funding Analysis (Top Sectors)",
        showlegend=True,
        barmode="stack"  # Stacked bars for all subplots
    )
     
    fig.update_xaxes(title_text="Fiscal Year", row=1, col=1)
    fig.update_xaxes(title_text="Total Funding ($)", row=1, col=1)
    fig.update_xaxes(title_text="Fiscal Year", row=1, col=2)
    fig.update_yaxes(title_text="Number of Awards", row=1, col=2)
    fig.update_xaxes(title_text="Fiscal Year", row=2, col=1)
    fig.update_yaxes(title_text="Average Funding ($)", row=2, col=1)

    return fig


def main():
    BASE_DIR = Path(__file__).parent.parent.parent
    parquet_path = BASE_DIR / "data" / "processed" / "awards_dataset"
    columns = [
        "action_date_fiscal_year",
        "total_dollars_obligated",
        "total_outlayed_amount_for_overall_award",
        "federal_action_obligation",
        "naics_code",
        "naics_description",
        "award_type",
    ]
    lazy_df = load_dataset(parquet_path, columns)

    #funding_trends = analyze_funding_trends(lazy_df)
    sector_funding = analyze_sector_funding(lazy_df, top_n=10)
    # award_types = analyze_award_types(lazy_df)

    #fund_fig = plot_funding_trends_subplots(funding_trends)
    sector_fig = plot_sector_funding(sector_funding)
    # plot_award_types_count(award_types).show()
    sector_fig.show()


if __name__ == "__main__":
    main()
