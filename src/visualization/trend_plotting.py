import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl


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
            row=1,
            col=1,
        )

        # Number of Awards (Stacked Bar, Row 1, Col 1)
        fig.add_trace(
            go.Bar(
                x=sector_df["action_date_fiscal_year"],
                y=sector_df["number_of_awards"],
                name=sector,
                legendgroup="awards",
                showlegend=False,
            ),
            row=1,
            col=2,
        )

        # Average Funding per Award (Bar, Row 2, Col 1)
        fig.add_trace(
            go.Bar(
                x=sector_df["action_date_fiscal_year"],
                y=sector_df["average_funding_per_award"],
                name=sector,
                legendgroup="avg_funding",
                showlegend=False,
            ),
            row=2,
            col=1,
        )

    # Update layout and axes
    fig.update_layout(
        height=800,
        width=1000,
        title_text="Sector Funding Analysis (Top Sectors)",
        showlegend=True,
        barmode="stack",  # Stacked bars for all subplots
    )

    fig.update_xaxes(title_text="Fiscal Year", row=1, col=1)
    fig.update_xaxes(title_text="Total Funding ($)", row=1, col=1)
    fig.update_xaxes(title_text="Fiscal Year", row=1, col=2)
    fig.update_yaxes(title_text="Number of Awards", row=1, col=2)
    fig.update_xaxes(title_text="Fiscal Year", row=2, col=1)
    fig.update_yaxes(title_text="Average Funding ($)", row=2, col=1)

    return fig


def plot_award_types_subplots(df: pl.DataFrame) -> go.Figure:
    """
    Create a subplot figure combining award counts and funding by award type.

    Args:
        df (pl.DataFrame): DataFrame from analyze_award_types

    Returns:
        go.Figure: Plotly figure object with subplots
    """
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Award Counts by Type", "Award Funding by Type"),
        horizontal_spacing=0.1,
    )

    award_types = df["award_type"].unique().drop_nulls()

    for award_type in award_types:
        type_df = df.filter(pl.col("award_type") == award_type)

        # Award Counts trace
        fig.add_trace(
            go.Bar(
                x=type_df["action_date_fiscal_year"],
                y=type_df["award_count"],
                name=award_type,
                legendgroup="counts",
            ),
            row=1,
            col=1,
        )

        # Award Funding trace
        fig.add_trace(
            go.Bar(
                x=type_df["action_date_fiscal_year"],
                y=type_df["total_funding"],
                name=award_type,
                legendgroup="funding",
                showlegend=False,
            ),
            row=1,
            col=2,
        )

    # Update axes
    fig.update_xaxes(title_text="Fiscal Year", row=1, col=1)
    fig.update_yaxes(title_text="Number of Awards", row=1, col=1)
    fig.update_xaxes(title_text="Fiscal Year", row=1, col=2)
    fig.update_yaxes(title_text="Total Funding ($)", row=1, col=2)

    # Update layout
    fig.update_layout(
        height=400,
        width=1000,
        title_text="Award Types Analysis",
        showlegend=True,
        barmode="stack",
    )

    return fig
