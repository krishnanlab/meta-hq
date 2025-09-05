class FilterParser:
    """Class to parse and return metahq retrieve <attribute> filters."""

    def __init__(self):
        self.filters = {}

    @classmethod
    def from_dict(cls, config):
        pass

    @classmethod
    def from_str(cls, filters: str):
        parser = cls()
        as_list: list[list[str]] = [f.split("=") for f in filters.split(",")]
        as_dict: dict[str, str] = {f[0]: f[1] for f in as_list}

        parser.filters = as_dict
        return parser
