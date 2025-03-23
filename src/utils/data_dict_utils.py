import json
from pathlib import Path
from typing import Any
from .conversion_utils import generate_case_variations
import logging

logger = logging.getLogger(__name__)


def get_true_false_from_data_dict(data_dict_path: Path) -> tuple[list[str], list[str]]:
    """
    Extract true and false values from a JSON data dictionary for CSV parsing.

    Parameters
    ----------
    data_dict_path : pathlib.Path
        Path to the JSON file containing the data dictionary.

    Returns
    -------
    tuple[list[str], list[str]]
        A tuple of (true_values, false_values) lists for boolean parsing.

    Raises
    ------
    json.JSONDecodeError
        If the JSON file is malformed.
    FileNotFoundError
        If the file does not exist.
    PermissionError
        If access is denied.
    """
    try:
        data_dict: dict[str, Any] = json.loads(
            data_dict_path.read_text(encoding="utf-8")
        )
        logger.debug("Loaded data dictionary from %s", data_dict_path)
    except (json.JSONDecodeError, FileNotFoundError, PermissionError):
        logger.exception("Failed to load data dictionary from %s", data_dict_path)
        return [], []
    except Exception:
        logger.critical(
            "Unexpected error loading data dictionary from %s",
            data_dict_path,
            exc_info=True,
        )
        raise

    BASE_TRUE_VALUES = {"true", "yes", "t", "y", "on"}
    BASE_FALSE_VALUES = {"false", "no", "f", "n", "off"}
    true_values = set().union(
        *(generate_case_variations(val) for val in BASE_TRUE_VALUES)
    )
    false_values = set().union(
        *(generate_case_variations(val) for val in BASE_FALSE_VALUES)
    )
    BOOL_PATTERNS = {
        frozenset({"T", "F"}): ("T", "F"),
        frozenset({"Y", "N"}): ("Y", "N"),
    }

    for field_values in data_dict.values():
        if not isinstance(field_values, dict):
            continue
        keys_set = frozenset(field_values)
        for pattern, (true_key, false_key) in BOOL_PATTERNS.items():
            if keys_set.issuperset(pattern) and len(keys_set) == 2:
                if true_key in field_values:
                    val = str(field_values[true_key]).strip()
                    true_values.update(generate_case_variations(val))
                if false_key in field_values:
                    val = str(field_values[false_key]).strip()
                    false_values.update(generate_case_variations(val))

    combined_true_values = list(true_values)
    combined_false_values = list(false_values)
    logger.debug(
        "Found %d True values and %d False values from data dict",
        len(combined_true_values),
        len(combined_false_values),
    )
    return combined_true_values, combined_false_values
