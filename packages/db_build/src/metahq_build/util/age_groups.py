"""
Age group classification utilities.

Maps a continuous age in years to a discrete age group label.
"""

from metahq_build.config import AGE_GROUPS


def get_age_group(age: float) -> str | None:
    """
    Map an age in years to a discrete age group label.

    Arguments:
        age (float):
            Age in years. Negative values represent prenatal ages.

    Returns:
        (str | None): Age group name, or None if age falls outside all
            defined ranges.
    """
    for group in AGE_GROUPS:
        if group["min_age"] <= age <= group["max_age"]:
            return group["name"]
    return None
