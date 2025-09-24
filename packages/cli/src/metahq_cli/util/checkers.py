from metahq_cli.util.supported import REQUIRED_FILTERS


def check_filters(filters: dict[str, str]):
    unaccaptable = []
    for f in filters:
        if f not in REQUIRED_FILTERS:
            unaccaptable.append(f)
    return unaccaptable
