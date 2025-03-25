from __future__ import annotations

import dash
from dash import dcc, html, Input, Output
from pathlib import Path
import polars as pl
from ..utils import load_dataset, get_unique_values
from ..analysis.trend_analysis import (
    analyze_award_types,
    analyze_funding_trends,
    analyze_sector_funding,
    map_sector,
)
from ..visualization import (
    plot_award_types_subplots,
    plot_sector_funding_subplots,
    plot_funding_trends_subplots,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Sequence

# Styling
COLOR_SCHEME = {"background": "#f9f9f9", "text": "#333333", "accent": "#1f77b4"}
APP_STYLE = {
    "backgroundColor": COLOR_SCHEME["background"],
    "color": COLOR_SCHEME["text"],
    "fontFamily": "Arial, sans-serif",
    "padding": "20px",
}


class FundingDashboard:
    def __init__(self, data_path: str | Path, columns: list[str]) -> None:
        self.app = dash.Dash(__name__)
        self.df = load_dataset(data_path, columns)
        self.years = get_unique_values(self.df, "action_date_fiscal_year")
        self.sector_codes = get_unique_values(self.df, "naics_code")
        self.sectors = [map_sector(str(code)[:2]) for code in self.sector_codes]

        self._setup_layout()
        self._register_callbacks()

    def _setup_layout(self) -> None:
        self.app.layout = html.Div(
            style=APP_STYLE,
            children=[  # type: ignore
                html.H1(
                    "USASpending Analysis Dashboard",
                    style={"textAlign": "center", "color": COLOR_SCHEME["accent"]},
                ),
                dcc.Tabs(
                    id="tabs",
                    value="funding-trends",
                    children=[
                        dcc.Tab(
                            label="Funding Trends",
                            value="funding-trends",
                            children=[
                                html.Div(
                                    [
                                        html.H3("Explore Funding Trends Over Time"),
                                        dcc.Dropdown(
                                            id="year-range-dropdown",
                                            options=[
                                                {"label": str(year), "value": year}
                                                for year in self.years
                                            ],
                                            value=self.years,
                                            multi=True,
                                            style={
                                                "width": "50%",
                                                "marginBottom": "20px",
                                            },
                                        ),
                                        dcc.Graph(id="funding-trends-plot"),
                                    ],
                                    style={"padding": "20px"},
                                )
                            ],
                        ),
                        dcc.Tab(
                            label="Sector Funding",
                            value="sector-funding",
                            children=[
                                html.Div(
                                    [
                                        html.H3("Sector Funding Analysis"),
                                        dcc.Dropdown(
                                            id="sector-dropdown",
                                            options=[
                                                {"label": sector, "value": sector}
                                                for sector in sorted(set(self.sectors))
                                            ],
                                            value=self.sectors[:5],
                                            multi=True,
                                            style={
                                                "width": "50%",
                                                "marginBottom": "10px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="sector-year-dropdown",
                                            options=[
                                                {"label": str(year), "value": year}
                                                for year in self.years
                                            ],
                                            value=self.years,
                                            multi=True,
                                            style={
                                                "width": "50%",
                                                "marginBottom": "20px",
                                            },
                                        ),
                                        dcc.Graph(id="sector-funding-plot"),
                                    ],
                                    style={"padding": "20px"},
                                )
                            ],
                        ),
                        dcc.Tab(
                            label="Award Types",
                            value="award-type",
                            children=[
                                html.Div(
                                    [
                                        html.H3("Award Types Over Time"),
                                        dcc.Dropdown(
                                            id="award-year-dropdown",
                                            options=[
                                                {"label": str(year), "value": year}
                                                for year in self.years
                                            ],
                                            value=self.years,
                                            multi=True,
                                            style={
                                                "width": "50%",
                                                "marginBottom": "20px",
                                            },
                                        ),
                                        dcc.Graph(id="award-types-plot"),
                                    ],
                                    style={"padding": "20px"},
                                )
                            ],
                        ),
                    ],
                ),
            ],
        )

    def _register_callbacks(self):
        @self.app.callback(
            Output("funding-trends-plot", "figure"),
            [Input("year-range-dropdown", "value")],
        )
        def update_trending_funds(selected_years: Sequence[str]):
            filtered_df = self.df.filter(
                pl.col("action_date_fiscal_year").is_in(selected_years)
            ).lazy()
            funding_trends_df = analyze_funding_trends(filtered_df)
            fig = plot_funding_trends_subplots(funding_trends_df)
            fig.update_layout(
                template="plotly_white",
                height=800,
                width=1200,
                title_font_size=20,
                font=dict(family="Arial", size=12, color=COLOR_SCHEME["text"]),
            )
            return fig

        @self.app.callback(
            Output("sector-funding-plot", "figure"),
            [Input("sector-dropdown", "value"), Input("sector-year-dropdown", "value")],
        )
        def update_sector_funding(
            selected_sectors: Sequence[str], selected_years: Sequence[str]
        ):
            filtered_df = self.df.filter(
                (pl.col("action_date_fiscal_year").is_in(selected_years))
                & (
                    pl.col("naics_code")
                    .cast(str)
                    .str.slice(0, 2)
                    .map_elements(map_sector, return_dtype=pl.String)
                    .is_in(selected_sectors)
                )
            ).lazy()
            sector_funding_df = analyze_sector_funding(
                filtered_df, top_n=len(selected_sectors)
            )
            fig = plot_sector_funding_subplots(sector_funding_df)
            fig.update_layout(
                template="plotly_white",
                height=800,
                width=1200,
                title_font_size=20,
                font=dict(family="Arial", size=12, color=COLOR_SCHEME["text"]),
            )
            return fig

        @self.app.callback(
            Output("award-types-plot", "figure"),
            [Input("award-year-dropdown", "value")],
        )
        def update_award_types(selected_years: Sequence[str]):
            filtered_df = self.df.filter(
                pl.col("action_date_fiscal_year").is_in(selected_years)
            ).lazy()
            award_types_df = analyze_award_types(filtered_df)
            fig = plot_award_types_subplots(award_types_df)
            fig.update_layout(
                template="plotly_white",
                height=600,
                width=1200,
                title_font_size=20,
                font=dict(family="Arial", size=12, color=COLOR_SCHEME["text"]),
            )
            return fig

    def run(self, host: str = "127.0.0.1", port: int = 8050, *, debug: bool = True):
        """Run the Dash server."""
        self.app.run(debug=debug, host=host, port=port)
