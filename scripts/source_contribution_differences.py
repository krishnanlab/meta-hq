"""
Identifies the differences of annotation specificity across sources.

Author: Parker Hicks
Date: 2026-08-28
"""

from argparse import ArgumentParser
from enum import Enum
from pathlib import Path
from typing import Any

import polars as pl
import polars.selectors as cs
from metahq_build.config import ID_KEY, MONDO_RELATIONS, UBERON_RELATIONS
from metahq_build.config.config import DELIMITER
from metahq_core.curations.annotations import Annotations
from metahq_core.util.io import load_bson

# annotation dataframe columns
ENTRY_COL: str = "entry"
SOURCE_COL: str = "source"
TERM_COL: str = "term"


class Attribute(Enum):
    "An attribute with supported annotations in MetaHQ."

    TISSUE = "tissue"
    DISEASE = "disease"
    SEX = "sex"
    AGE = "age"


def dict_db_to_df(db: dict[str, dict[str, Any]], attribute: Attribute) -> pl.DataFrame:
    """Transform the database from dictionary to polars.DataFrame
    where one row represents a single annotation for a particular
    attribute from a particular source.
    """
    _db = {"entry": [], "source": [], "term": []}
    for entry, records in db.items():
        if attribute.value not in records:
            continue

        for source, anno in records[attribute.value].items():
            _db["entry"].append(entry)
            _db["source"].append(source)
            _db["term"].append(anno[ID_KEY])

    return (
        pl.LazyFrame(_db)
        .with_columns(pl.col("term").str.split(DELIMITER))
        .explode("term", empty_as_null=False)
        .collect(engine="streaming")
    )


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


def main():
    """Main entry point."""
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--database",
        help="Path to MetaHQ BSON databse.",
        type=Path,
    )
    parser.add_argument(
        "-a",
        "--attribute",
        help="Hierarchy-based attribute to analyze.",
        choices=["tissue", "disease"],
        type=str,
        default="tissue",
    )
    args = parser.parse_args()
    attribute = Attribute(args.attribute)

    # load data
    db = load_bson(args.database)

    df = dict_db_to_df(db, attribute)

    all_terms: list[str] = get_ontology_terms(attribute)

    for source in df[SOURCE_COL]:
        _df = (
            df.filter(pl.col(SOURCE_COL) == source)
            .with_columns(pl.lit(1).alias(SOURCE_COL))
            .pivot(on=TERM_COL, index=ENTRY_COL)
            .fill_null(0)
            .with_columns(pl.lit("Gemma").alias(SOURCE_COL))
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
        anno.propagate(to_terms=all_terms, ontology="uberon", mode=0)


if __name__ == "__main__":
    main()
