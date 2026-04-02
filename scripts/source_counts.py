"""
Count the number of entries from each source in MetaHQ.

Author: Parker Hicks
Date: 2026-04-01

Last updated: 2026-04-01 by Parker Hicks
"""

from pprint import pprint

from metahq_core.util.io import load_bson
from metahq_core.util.supported import get_annotations


def main():
    """Count entries per source."""
    counts = {"sample": {}, "series": {}}
    for level in counts:
        data = load_bson(get_annotations(level))

        for entry in data.values():
            for attribute in ["tissue", "disease", "sex", "age"]:
                if attribute not in entry:
                    continue

                for source in entry[attribute]:
                    if source not in counts[level]:
                        counts[level].setdefault(source, 0)

                    counts[level][source] += 1
    pprint(counts)


if __name__ == "__main__":
    main()
