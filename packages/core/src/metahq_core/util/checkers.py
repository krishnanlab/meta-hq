from metahq_core.util.alltypes import DictKeys


def check_database(database: str, supported: DictKeys | list[str]):
    """Checks that input database is supported by MetaHQ."""
    if database not in supported:
        raise ValueError(f"Expected databse in {supported}, got {database}.")


def check_key(key: str, supported: DictKeys | list[str]):
    """Checks if a attribute key is supported by MetaHQ."""
    if key not in supported:
        raise ValueError(f"Expected key in {supported}, got {key}.")
