"""
Identifies the differences of annotation specificity across sources.

Author: Parker Hicks
Date: 2026-08-28
"""

from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import polars.selectors as cs
from metahq_build.config import MONDO_RELATIONS, UBERON_RELATIONS
from metahq_core.curations.annotations import Annotations
from metahq_core.util.alltypes import Attribute
from metahq_core.util.io import checkdir, load_bson
from metahq_core.util.supported import ROOT
from scipy import sparse
from util import dict_db_to_df

# annotation dataframe columns
ENTRY_COL: str = "entry"
SOURCE_COL: str = "source"
TERM_COL: str = "term"
DTYPE_ANNO = np.int32

DEFAULT_OUTDIR = ROOT / "results/source_contribution_differences"


def get_ontology_file(attribute: Attribute) -> Path:
    """Load the proper ontology relations parquet file
    columns are all term IDs in the ontology.
    """
    match attribute:
        case Attribute.TISSUE:
            return UBERON_RELATIONS
        case Attribute.DISEASE:
            return MONDO_RELATIONS
        case _:
            raise ValueError(f"Unsupported attribute {attribute.value}")


def get_ontology_name(attribute: Attribute) -> str:
    """Return the ontology name expected by `Annotations.propagate`."""
    match attribute:
        case Attribute.TISSUE:
            return "uberon"
        case Attribute.DISEASE:
            return "mondo"
        case _:
            raise ValueError(f"Unsupported attribute {attribute.value}")


def get_ontology_terms(attribute: Attribute) -> list[str]:
    """Load all ontology term IDs for a particular attribute."""
    file = get_ontology_file(attribute)
    return pl.scan_parquet(file).collect_schema().names()


def get_unique_annotation_sources(
    db: dict[str, dict[str, Any]], attribute: Attribute
) -> set[str]:
    """Identifies all unique annotation sources in a MetaHQ database."""
    sources = set()
    for records in db.values():
        if attribute.value in records:
            _sources = set(records[attribute.value].keys())
            sources.update(_sources)

    return sources


def build_source_matrices(
    df: pl.DataFrame, db: dict[str, dict[str, Any]], attribute: Attribute
) -> tuple[list[sparse.csr_matrix], list[str], np.ndarray, list[str]]:
    """Build sparse (entry x term) matrices of propagated annotation
    presence for every source, aligned to a shared entry/term index space.

    Returns:
        source_matrices: List of sparse CSR matrices, one per source, each of
            shape (n_entries, n_terms) where a nonzero value indicates the
            entry is annotated (directly or via propagation) to that term.
        sources: Source names, indexing the list of `source_matrices`.
        all_entries: Entry IDs, indexing the rows of each matrix.
        all_terms: Ontology term IDs, indexing the columns of each matrix.
    """
    all_terms: list[str] = get_ontology_terms(attribute)
    all_entries = np.array(list(db.keys()))
    entry_to_idx = {entry: idx for idx, entry in enumerate(all_entries)}

    sources = sorted(df[SOURCE_COL].unique())
    ontology_name = get_ontology_name(attribute)
    source_matrices = []

    for i, source in enumerate(sources):
        _df = (
            df.filter(pl.col(SOURCE_COL) == source)
            .with_columns(pl.lit(1).alias(SOURCE_COL))
            .pivot(on=TERM_COL, index=ENTRY_COL)
            .fill_null(0)
            .with_columns(pl.lit(source).alias(SOURCE_COL))
            .with_columns(
                pl.lit("placeholder_group").alias("group")
            )  # required for Annotations initialization
        )

        # propagate
        anno = Annotations.from_df(
            _df,
            index_col=ENTRY_COL,
            sources_col=SOURCE_COL,
            group_cols=["group"],
        )
        anno = anno.propagate(to_terms=all_terms, ontology=ontology_name, mode=0).pl()
        assert all_terms == anno.select(cs.numeric()).columns, "Mismatched term columns"

        # Build sparse matrix for this source
        source_entries = anno[ENTRY_COL].to_numpy().flatten()
        entry_indices = np.fromiter(
            (entry_to_idx[entry] for entry in source_entries),
            dtype=np.int64,
            count=len(source_entries),
        )

        # Create a sparse matrix: start with zeros, fill in the annotated entries
        source_data = anno.select(cs.numeric()).to_numpy()
        source_matrix = sparse.lil_matrix(
            (len(all_entries), len(all_terms)), dtype=DTYPE_ANNO
        )
        source_matrix[entry_indices] = source_data
        source_matrices.append(
            source_matrix.tocsr()
        )  # Convert to CSR for efficient operations

    return source_matrices, sources, all_entries, all_terms


def compute_term_counts(source_matrices: list[sparse.csr_matrix]) -> np.ndarray:
    """Count the number of terms each source annotates each entry to.

    Comparing a single entry's counts across sources shows which source
    annotated that entry with the most specificity.

    Returns:
        Array of shape (n_sources, n_entries).
    """
    term_counts = np.zeros(
        (len(source_matrices), source_matrices[0].shape[0]), dtype=np.int32
    )
    for i, matrix in enumerate(source_matrices):
        # For each entry (row), count nonzero terms (columns)
        term_counts[i] = np.array((matrix > 0).sum(axis=1)).flatten()
    return term_counts


def compute_unique_contribution(source_matrices: list[sparse.csr_matrix]) -> np.ndarray:
    """For each source, identify entry-term annotations that no other
    source provides, i.e. information only that source contributes.

    Returns:
        Array of shape (n_sources, n_entries) where each value is the count
        of unique entry-term annotations for that source.
    """
    # Stack all sparse matrices to compute total presence across sources
    n_sources = len(source_matrices)
    n_entries, n_terms = source_matrices[0].shape

    # Count how many sources annotate each (entry, term) pair
    # by summing presence across all sources
    n_sources_annotating = sparse.csr_matrix((n_entries, n_terms), dtype=np.int32)
    for matrix in source_matrices:
        n_sources_annotating += (matrix > 0).astype(np.int32)

    # For each source, find annotations where only that source has it
    unique_counts = np.zeros((n_sources, n_entries), dtype=np.int32)
    for i, matrix in enumerate(source_matrices):
        # Find where this source has annotation AND total count is 1
        presence = matrix > 0
        unique_to_source = presence.multiply(n_sources_annotating == 1)
        # Sum across terms (columns) to get count per entry (row)
        unique_counts[i] = np.array(unique_to_source.sum(axis=1)).flatten()

    return unique_counts


def matrix_to_df(
    matrix: np.ndarray, sources: list[str], all_entries: np.ndarray
) -> pl.DataFrame:
    """Convert a (n_sources, n_entries) matrix into an entry x source DataFrame."""
    data = {ENTRY_COL: all_entries}
    for i, source in enumerate(sources):
        data[source] = matrix[i]
    return pl.DataFrame(data)


def main():
    """Main entry point."""
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--database",
        help="Path to MetaHQ BSON databse.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "-a",
        "--attribute",
        help="Hierarchy-based attribute to analyze.",
        choices=["tissue", "disease"],
        type=str,
        default="tissue",
    )
    parser.add_argument(
        "-l",
        "--level",
        help="Annotation level.",
        choices=["sample", "series"],
        type=str,
        required=True,
    )
    parser.add_argument(
        "-o",
        "--outdir",
        help="Path to outdir to save contribution difference files to.",
        default=DEFAULT_OUTDIR,
        type=Path,
    )
    args = parser.parse_args()
    attribute = Attribute(args.attribute)
    outdir = checkdir(args.outdir)

    # load data
    db = load_bson(args.database)
    df = dict_db_to_df(db, attribute)

    source_matrices, sources, all_entries, _ = build_source_matrices(df, db, attribute)

    # for each entry, how many terms does each source annotate it to
    term_counts = compute_term_counts(source_matrices)
    matrix_to_df(term_counts, sources, all_entries).write_csv(
        outdir / f"term_counts__level-{args.level}__attribute-{attribute.value}.tsv",
        separator="\t",
    )

    # for each entry, how many terms does each source uniquely contribute
    unique_counts = compute_unique_contribution(source_matrices)
    matrix_to_df(unique_counts, sources, all_entries).write_csv(
        outdir
        / f"unique_contribution__level-{args.level}__attribute-{attribute.value}.tsv",
        separator="\t",
    )

    # total unique entry-term annotations contributed by each source overall
    pl.DataFrame(
        {
            SOURCE_COL: sources,
            "n_unique_annotations": unique_counts.sum(axis=1),
        }
    ).sort("n_unique_annotations", descending=True).write_csv(
        outdir
        / f"unique_contribution_totals__level-{args.level}__attribute-{attribute.value}.tsv",
        separator="\t",
    )


if __name__ == "__main__":
    main()
