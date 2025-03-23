from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent
SCHEMA_FILES = {
    "contracts": BASE_DIR / "data" / "schemas" / "contract_schema.json",
    "data_dict": BASE_DIR / "data_dict" / "data_dicts" / "data_dict.json",
}