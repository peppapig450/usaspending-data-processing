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