from argparse import ArgumentParser
from pathlib import Path

import numpy as np
import polars as pl
from metahq_core.util.io import checkdir, load_bson
from metahq_core.util.supported import ROOT
from numpy.typing import NDArray

DEFAULT_OUTDIR = ROOT / "results"

ATTRIBUTES = ["tissue", "disease", "sex", "age"]


def gather_source_contributions(
    db: dict, attributes: list[str]
) -> dict[str, dict[str, set[str]]]:
    """For each attribute, create a source to entries map storing
    which samples or studies a particular source contributed to the
    database.


    {attribute: {source: set(samples or studies)}}
    """
    attribute_source_entries: dict[str, dict[str, set[str]]] = {
        attribute: {} for attribute in attributes
    }
    for entry, anno in db.items():
        for attribute, source_map in attribute_source_entries.items():
            if attribute not in anno:
                continue

            # add each source and it's sample to the counts map
            for source in set(anno[attribute].keys()):
                source_map.setdefault(source, set())
                source_map[source].add(entry)

    return attribute_source_entries


def get_source_contribution_overlap(
    contributions: dict[str, set[str]]
) -> dict[str, NDArray]:
    """Given a map of sources to sample or study IDs, compute the absolute
    and percent overlap between them.

    Arguments:
        contributions (dict[str, set[str]]):
            Mapping of sources to sample or study IDs.

    Returns:
        (dict[str, NDArray]): Dictionary storing arrays of column/row names (sources),
            the absolute counts overlap and the percent overlap.
    """
    source_names = np.array(list(contributions.keys()))  # column/row names
    source_overlap_counts = np.zeros(
        (len(contributions), len(contributions)), dtype=np.int64
    )  # discrete overlap counts
    source_overlap_percent = np.zeros(
        (len(contributions), len(contributions)), dtype=np.float64
    )  # counts normalized by total number of entries between sources

    # compute overlap
    for i_source, i_entries in contributions.items():
        for j_source, j_entries in contributions.items():
            total = len(
                i_entries | j_entries
            )  # total number of samples between the two sources
            intersection = len(i_entries & j_entries)
            intersection_percent = intersection / total

            i_idx = np.where(source_names == i_source)[0]
            j_idx = np.where(source_names == j_source)[0]

            source_overlap_counts[i_idx, j_idx] = intersection
            source_overlap_percent[i_idx, j_idx] = intersection_percent

    return {
        "sources": source_names,
        "overlap_count": source_overlap_counts,
        "overlap_percent": source_overlap_percent,
    }


def main():
    """Main entry point."""
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--database",
        help="Path to MetaHQ BSON database.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "-l",
        "--level",
        help="Annotation level",
        choices=["sample", "series"],
        required=True,
        type=str,
    )
    parser.add_argument(
        "-o",
        "--outdir",
        help="Path to outdir to save attribtue overlap files to.",
        default=DEFAULT_OUTDIR,
        type=Path,
    )
    args = parser.parse_args()
    outdir = checkdir(args.outdir)

    db = load_bson(args.database)

    attribute_source_entries = gather_source_contributions(db, ATTRIBUTES)

    attribute_source_overlap: dict[str, dict[str, NDArray]] = {}
    for attribute, source_map in attribute_source_entries.items():
        source_overlap = get_source_contribution_overlap(source_map)

        attribute_source_overlap.setdefault(attribute, {})
        attribute_source_overlap[attribute] = source_overlap

    # save individual files for each attribute and each overlap type
    for attribute, results in attribute_source_overlap.items():
        schema = {source: pl.Float64 for source in results["sources"]}
        for overlap_type in ["overlap_count", "overlap_percent"]:
            outfile = (
                outdir
                / f"{overlap_type}__level-{args.level}__attribute-{attribute}.tsv"
            )
            pl.DataFrame(results[overlap_type], schema=schema).write_csv(
                outfile, separator="\t"
            )


if __name__ == "__main__":
    main()
