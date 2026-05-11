"""
Identify all unique tissue and disease terms that can be obtained through propagation of annotations
of Terms in the MetaHQ database.

Stacks tissue and disease labels from MetaHQ retrieve across levels, species, and technologies.
Run run/get_all_propagated_tissue_disease_annotations.sh to aquire the data necessary to run this
script.
"""

from argparse import ArgumentParser
from pathlib import Path

import polars as pl

TERM_PREFIXES: list[str] = ["UBERON", "CL", "MONDO"]


def save_unique_terms(terms: set[str], outfile: Path):
    """Save unique terms to txt file."""
    if not outfile.parent.exists():
        outfile.parent.mkdir(exist_ok=True, parents=True)

    with open(outfile, "r", encoding="utf-8") as f:
        for term in terms:
            f.write(f"{term}\n")


def main():
    """Main entry point."""
    parser = ArgumentParser()
    parser.add_argument(
        "-i",
        "--indir",
        help="Path to a directory storing outputs from multiple MetaHQ runs."
        " Files should be parquet files.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="Path to outfile.txt storing all unique tissue and disease terms"
        " represented in MetaHQ.",
        type=Path,
        default="results/unique_propagated_tissue_disease_terms.txt",
    )
    args = parser.parse_args()

    all_terms = set()
    for file in args.indir.rglob("result*"):
        lf = pl.scan_parquet(file)

        columns = lf.collect_schema().names()
        term_columns = [col for col in columns if col.split(":")[0] in TERM_PREFIXES]

        lf = lf.select(term_columns)
        valid_terms = set(lf.select(pl.col("*").is_in([1]).any()).collect().columns)
        all_terms.update(valid_terms)

    save_unique_terms(all_terms, args.outfile)


if __name__ == "__main__":
    main()
