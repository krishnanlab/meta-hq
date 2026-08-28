"""
Count the number of entries from each source in MetaHQ.

Author: Parker Hicks
Date: 2026-04-01

Last updated: 2026-04-01 by Parker Hicks
"""

from argparse import ArgumentParser
from pathlib import Path
from pprint import pprint

from metahq_core.sources import REFERENCE_MAP
from metahq_core.util.io import load_bson


def total(counts: dict[str, dict[str, int]]) -> dict[str, int]:
    """Sum all values in the counts dict"""
    sums = {"sample": 0, "series": 0}
    for level in sums:
        sources = counts[level].keys()
        for source in sources:
            sums[level] += counts[level][source]
    return sums


def get_total_annotation_count(dbs):
    """
    Count the total number of annotations for each annotation source across attributes.
    Sources can provide more than one annotation type for a particular sample or study.
    """
    counts = {"sample": {}, "series": {}}
    for level in counts:
        data = load_bson(dbs[level])

        for entry in data.values():
            for attribute in ["tissue", "disease", "sex", "age"]:
                if attribute not in entry:
                    continue

                for source in entry[attribute]:
                    if source not in counts[level]:
                        counts[level].setdefault(source, 0)

                    counts[level][source] += 1
    return counts


def get_total_entry_count(dbs):
    """
    Only count the number of samples or studies a source has annotations for,
    not the number of total annotations (a sample can have multiple annotations
    from a particular source).
    """
    counts = {"sample": {}, "series": {}}
    for level in counts:
        data = load_bson(dbs[level])

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
    parser = ArgumentParser()
    parser.add_argument(
        "--sample-db",
        help="Path to MetaHQ sample-level BSON database.",
        type=Path,
        default="data/processed/combined__level-sample.bson",
    )
    parser.add_argument(
        "--series-db",
        help="Path to MetaHQ series-level BSON database.",
        type=Path,
        default="data/processed/combined__level-series.bson",
    )
    args = parser.parse_args()

    dbs = {"sample": args.sample_db, "series": args.series_db}

    all_counts = get_total_annotation_count(dbs)
    entry_counts = get_total_entry_count(dbs)

    print("Total annotation counts:")
    pprint(all_counts)
    print(f"Total: {total(all_counts)}")
    print("\n")

    print("Total entry counts:")
    pprint(entry_counts)


if __name__ == "__main__":
    main()
