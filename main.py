from pathlib import Path

from src import FundingDashboard
import polars as pl

pl.enable_string_cache()


def main():
    BASE_DIR = Path(__file__).parent
    DATA_PATH = BASE_DIR / "data" / "processed" / "awards_dataset"
    COLUMNS = [
        "action_date_fiscal_year",
        "total_dollars_obligated",
        "total_outlayed_amount_for_overall_award",
        "federal_action_obligation",
        "naics_code",
        "naics_description",
        "award_type",
    ]

    dashboard = FundingDashboard(DATA_PATH, COLUMNS)
    dashboard.run(debug=True)


if __name__ == "__main__":
    main()
