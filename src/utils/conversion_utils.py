def generate_case_variations(base_value: str) -> set[str]:
    """
    Generate common capitalization variations for a given string.

    Args:
        base_value (str): The base string to vary (e.g., 'true').

    Returns:
        set[str]: A set of case variations (e.g., {'true', 'True', 'TRUE'}).
    """
    variations = {base_value.lower(), base_value.upper(), base_value.capitalize()}
    return variations


def to_bool(value):
    """
    Convert a string representation of a boolean to an actual boolean type.

    The conversion is performed using a match-case structure:
      - "t" (case-insensitive) returns True.
      - "f" (case-insensitive) returns False.
      - Any other value returns None.

    Parameters:
        value: The input value to be converted.

    Returns:
        bool or None: The converted boolean value or None if conversion is not possible.
    """
    match str(value).lower():
        case "t":
            return True
        case "f":
            return False
        case _:
            return None
