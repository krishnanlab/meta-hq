"""
Count the number of entries from each source in MetaHQ.

Author: Parker Hicks
Date: 2026-04-01

Last updated: 2026-04-01 by Parker Hicks
"""

from pprint import pprint

from metahq_core.sources import REFERENCE_MAP
from metahq_core.util.io import load_bson
from metahq_core.util.supported import get_annotations


def get_total_annotation_count():
    """
    Count the total number of annotations for each annotation source across attributes.
    Sources can provide more than one annotation type for a particular sample or study.
    """
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
    return counts


def get_total_entry_count():
    """
    Only count the number of samples or studies a source has annotations for,
    not the number of total annotations (a sample can have multiple annotations
    from a particular source).
    """
    counts = {"sample": {}, "series": {}}
    for level in counts:
        data = load_bson(get_annotations(level))

        for entry in data.values():
            for source in REFERENCE_MAP:
                for attribute in ["tissue", "disease", "sex", "age"]:
                    if attribute not in entry:
                        continue

                    if source not in entry[attribute]:
                        continue

                    if source not in counts[level]:
                        counts[level].setdefault(source, 0)

                    counts[level][source] += 1
                    break
    return counts


def main():
    """Count entries per source."""

    all_counts = get_total_annotation_count()
    entry_counts = get_total_entry_count()

    print("Total annotation counts:")
    pprint(all_counts)
    print("\n")

    print("Total entry counts:")
    pprint(entry_counts)


if __name__ == "__main__":
    main()
