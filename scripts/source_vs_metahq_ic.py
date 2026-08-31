from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from metahq_build.config import (
    COL_ACCESSION,
    COL_SOURCE,
    COL_TERM_ID,
    CONTROL_ID,
    MONDO_OBO,
    UBERON_OBO,
)
from metahq_build.config.config import COL_ATTRIBUTE, DELIMITER
from metahq_build.ontology import Graph
from metahq_build.util.logging import setup_logger
from metahq_core.util.alltypes import HIERARCHICAL_ATTRIBUTES, Attribute
from metahq_core.util.io import checkdir, load_bson
from util import dict_db_to_df


@dataclass
class Databases:
    specific: pl.DataFrame
    unspecific: pl.DataFrame
    all_terms: list[str]


def get_ontology_obo(attribute: Attribute) -> Path:
    """Load the proper ontology obo file for a particular attribute."""
    match attribute:
        case Attribute.TISSUE:
            return UBERON_OBO
        case Attribute.DISEASE:
            return MONDO_OBO
        case _:
            raise ValueError(f"Unsupported attribute {attribute.value}")


def load_dbs(specific: Path, unspecific: Path, attribute: Attribute) -> Databases:
    """Loads databases and identifies all unique terms represented for a particular attribute."""
    _specific = dict_db_to_df(load_bson(specific), attribute)
    _unspecific = dict_db_to_df(load_bson(unspecific), attribute)

    specific_terms = set(_specific[COL_TERM_ID].unique().to_list())
    unspecific_terms = set(_unspecific[COL_TERM_ID].unique().to_list())

    return Databases(_specific, _unspecific, list(specific_terms | unspecific_terms))


def get_information_content(attribute: Attribute, terms: list[str]) -> pl.DataFrame:
    """Get the information content for a particular list of terms for a particular attribute."""

    # the control ID (i.e., MONDO:0000000) is not a part of the ontology
    # need to handle separately
    if CONTROL_ID in terms:
        terms.remove(CONTROL_ID)

    obo_path = get_ontology_obo(attribute)
    graph = Graph.from_obo(obo_path)
    ic = graph.ic_from(terms).pl(index=COL_TERM_ID)

    # add IC value for control annotations
    # the IC value is the maximum IC since it technically has no descendants
    ctrl_ic = pl.DataFrame({COL_TERM_ID: CONTROL_ID, "ic": ic["ic"].max()})

    return ic.extend(ctrl_ic)


def main():
    """Main entry point."""
    parser = ArgumentParser()
    parser.add_argument(
        "--db-specific",
        help="Path to MetaHQ BSON database with selection of the most specific source"
        " annotation per entry.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--db-unspecific",
        help="Path to MetaHQ BSON database without selecting the most specific source"
        " annotation per entry.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="Path to parquet file storing results for all sources and all attributes.",
        type=Path,
        required=True,
    )
    args = parser.parse_args()
    logger = setup_logger(__name__)

    logger.info("Using MetaHQ database from: %s", args.db_specific)
    logger.info("Using unspecific database from: %s", args.db_unspecific)

    results = []
    for attribute in HIERARCHICAL_ATTRIBUTES:
        logger.info("Processing attribute: %s", attribute.value)

        logger.info("Loading databases...")
        db_info = load_dbs(args.db_specific, args.db_unspecific, attribute)

        logger.info("Computing information content values...")
        ic = get_information_content(attribute, db_info.all_terms)
        metahq_ic = (
            db_info.specific.join(ic, on=COL_TERM_ID, how="inner")
            .drop(
                COL_TERM_ID
            )  # doesn't matter which term it is as long as the ic is the same.
            # we care about specificity here not exact terms.
            .group_by([COL_ACCESSION, "ic"], maintain_order=True)
            .agg(pl.col(COL_SOURCE).str.join(DELIMITER))
            .select([COL_ACCESSION, COL_SOURCE, "ic"])
            .sort("ic", descending=True)
            .unique(COL_ACCESSION, keep="first")
            .rename({COL_SOURCE: "metahq_source", "ic": "metahq_ic"})
        )

        if attribute.value == "tissue":
            print(db_info.specific.filter(pl.col(COL_ACCESSION) == "GSM151989"))

            print(
                ic.filter(pl.col(COL_TERM_ID).is_in(["UBERON:0000178", "CL:0000738"]))
            )
            print(
                ic.filter(pl.col(COL_TERM_ID).is_in(["UBERON:0000178", "CL:0000738"]))
            )

            test = (
                db_info.specific.join(ic, on=COL_TERM_ID, how="inner")
                .filter(pl.col(COL_ACCESSION) == "GSM151989")
                .filter(pl.col(COL_TERM_ID).is_in(["UBERON:0000178", "CL:0000738"]))
                .drop(
                    COL_TERM_ID
                )  # doesn't matter which term it is as long as the ic is the same.
                # we care about specificity here not exact terms.
                .group_by([COL_ACCESSION, "ic"], maintain_order=True)
                .agg(pl.col(COL_SOURCE).str.join(DELIMITER))
                .select([COL_ACCESSION, COL_SOURCE, "ic"])
                .sort("ic", descending=True)
                .unique(COL_ACCESSION, keep="first")
                .rename({COL_SOURCE: "metahq_source", "ic": "metahq_ic"})
            )
            print(test)

        unique_sources = db_info.unspecific[COL_SOURCE].unique().to_list()

        logger.info("Collecting IC for original source annotations...")
        for source in unique_sources:

            # collect IC for the original source annotations
            source_ic = (
                db_info.unspecific.filter(pl.col(COL_SOURCE) == source)
                .join(ic, on=COL_TERM_ID, how="inner")
                .drop(COL_TERM_ID)
                .rename({COL_SOURCE: "original_source", "ic": "original_ic"})
            )
            source_vs_metahq = source_ic.join(
                metahq_ic, on=COL_ACCESSION, how="inner"
            ).with_columns(pl.lit(attribute.value).alias(COL_ATTRIBUTE))

            results.append(source_vs_metahq)

    logger.info("Saving to %s...", args.outfile)
    _ = checkdir(args.outfile, is_file=True)
    pl.concat(results, how="vertical").sort(
        [
            COL_ACCESSION,
            "original_source",
            "original_ic",
            "metahq_source",
            "metahq_ic",
            COL_ATTRIBUTE,
        ]
    ).write_parquet(args.outfile)


if __name__ == "__main__":
    main()
