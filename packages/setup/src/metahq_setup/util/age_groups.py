"""
Age group classification utilities.

Maps a continuous age in years to a discrete age group label.

Author: Parker Hicks
Date: 2025-10-14
"""

AGE_GROUPS = [
    {"name": "fetus", "min_age": -1, "max_age": 0},
    {"name": "infant", "min_age": 0, "max_age": 2},
    {"name": "child", "min_age": 2, "max_age": 10},
    {"name": "adolescent", "min_age": 10, "max_age": 20},
    {"name": "adult", "min_age": 20, "max_age": 50},
    {"name": "older_adult", "min_age": 50, "max_age": 80},
    {"name": "elderly_adult", "min_age": 80, "max_age": 150},
]


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
