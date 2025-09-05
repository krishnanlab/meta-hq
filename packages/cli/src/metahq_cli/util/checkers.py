from metahq_cli.util.supported import FILTERS


def check_filters(filters: dict[str, str]):
    unaccaptable = []
    for f in filters:
        if f not in FILTERS:
            unaccaptable.append(f)
    return unaccaptable
