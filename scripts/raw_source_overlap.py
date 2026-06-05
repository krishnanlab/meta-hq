"""
Characterize the overlap samples and studies across pre-harmonized sources.

Author: Parker Hicks
Date: 2026-06-04

Last updated: 2026-06-05 by Parker Hicks
"""

from argparse import ArgumentParser
from pathlib import Path
from typing import TypeAlias

import duckdb
import polars as pl
from metahq_build.analysis import OverlapResult, get_source_contribution_overlap
from metahq_build.config.config import (
    ALE_PROCESSED,
    BGEE_PROCESSED,
    CELLO_PROCESSED,
    COL_ACCESSION,
    COL_ATTRIBUTE,
    CREEDS_PROCESSED,
    DISIGN_ATLAS_PROCESSED,
    GEMMA_PROCESSED,
    GOLIGHTLY_PROCESSED,
    GU_PROCESSED,
    JOHNSON_2023_MICROARRAY_PROCESSED,
    JOHNSON_2023_RNASEQ_PROCESSED,
    KRISHNANLAB_PROCESSED,
    OMICIDX_DB,
    SIROTA_2011_PROCESSED,
    URSA_PROCESSED,
    URSAHD_PROCESSED,
)
from metahq_build.metadata.sample import sra2gsm_map
from metahq_build.util.logging import setup_logger
from metahq_core.util.io import checkdir
from metahq_core.util.supported import ROOT
from numpy.typing import NDArray

SourceMap: TypeAlias = dict[str, dict[str, str | Path]]
SourceAccessions: TypeAlias = dict[str, set[str]]
AttributeSourceAccessions: TypeAlias = dict[str, SourceAccessions]
AttributeSourceOverlap: TypeAlias = dict[str, dict[str, NDArray]]
LevelAttributeSourceAccessions: TypeAlias = dict[str, AttributeSourceAccessions]

COL_SOURCE: str = "source"
DEFAULT_OUTDIR: Path = ROOT / "results"

SOURCES: SourceMap = {
    "ALE": {"level": "sample", "file": ALE_PROCESSED},
    "BGee": {"level": "sample", "file": BGEE_PROCESSED},
    "CellO": {"level": "sample", "file": CELLO_PROCESSED},
    "CREEDS": {"level": "sample", "file": CREEDS_PROCESSED},
    "DiSignAtlas": {"level": "sample", "file": DISIGN_ATLAS_PROCESSED},
    "Gemma": {"level": "series", "file": GEMMA_PROCESSED},
    "Golightly_2028": {"level": "sample", "file": GOLIGHTLY_PROCESSED},
    "Gu_2023": {"level": "sample", "file": GU_PROCESSED},
    "Johnson_2023_rnaseq": {"level": "sample", "file": JOHNSON_2023_RNASEQ_PROCESSED},
    "Johnson_2023_microarray": {
        "level": "sample",
        "file": JOHNSON_2023_MICROARRAY_PROCESSED,
    },
    "KrishnanLab": {"level": "sample", "file": KRISHNANLAB_PROCESSED},
    "Sirota_2011": {"level": "sample", "file": SIROTA_2011_PROCESSED},
    "URSA": {"level": "sample", "file": URSA_PROCESSED},
    "URSA_HD": {"level": "sample", "file": URSAHD_PROCESSED},
}

logger = setup_logger(__name__)


def log_accession_counts(source: str, attribute_dict: dict[str, set[str]]):
    """Logs the number of accessions collected from a particular source across attributes"""
    counts = {attribute: len(ids) for attribute, ids in attribute_dict.items()}
    logger.info("%-30s attribute counts: %s", source, counts)


def collect_source_accessions(sources: SourceMap) -> AttributeSourceAccessions:
    """collect accession ids from each source."""
    collected: list[pl.LazyFrame] = []
    for source, metadata in sources.items():
        collected.append(
            pl.scan_parquet(metadata["file"])
            .select([COL_ATTRIBUTE, COL_ACCESSION])
            .with_columns(pl.lit(source).alias("source"))
        )
    df = (
        pl.concat(collected, how="vertical")
        .collect()
        .group_by(COL_ATTRIBUTE, COL_SOURCE)
        .agg(pl.col(COL_ACCESSION).unique())
        .sort(COL_ATTRIBUTE, COL_SOURCE)
    )

    results: AttributeSourceAccessions = {}
    for row in df.iter_rows(named=True):
        results.setdefault(row[COL_ATTRIBUTE], {})[row[COL_SOURCE]] = row[COL_ACCESSION]

    return results


def map_sra_to_geo(
    attribute_accessions: AttributeSourceAccessions,
) -> AttributeSourceAccessions:
    """Map SRA IDs to GSMs per source.

    Assumes there are no sources that contain both GEO and SRA IDs.
    """

    sra_accessions = set()
    for source_accessions in attribute_accessions.values():
        for accessions in source_accessions.values():
            sra_accessions.update(
                {id_ for id_ in accessions if not id_.startswith(("GSM"))}
            )

    xxr_ids = [id_ for id_ in sra_accessions if (len(id_) >= 3 and id_[2] == "R")]
    xxx_ids = [id_ for id_ in sra_accessions if (len(id_) >= 3 and id_[2] == "X")]
    logger.info(
        "Mapping %d SRA IDs (%d run, %d experiment) to GSM IDs with OmicIDX...",
        len(sra_accessions),
        len(xxr_ids),
        len(xxx_ids),
    )

    gsm_map = sra2gsm_map(xxr_ids, xxx_ids, db_path=OMICIDX_DB).rename(
        {"original_id": "sra"}
    )
    # gsm_map = dict(zip(gsm_map["original_id"], gsm_map["gsm"]))
    logger.info("Mapped to %d GSM IDs", len(gsm_map))

    mapped: AttributeSourceAccessions = {}
    for attribute, source_accessions in attribute_accessions.items():
        for source, accessions in source_accessions.items():
            if not next(iter(accessions)).startswith("GSM"):
                mapped.setdefault(attribute, {})[source] = set(
                    gsm_map.filter(pl.col("sra").is_in(accessions))["gsm"].to_list()
                )
            else:
                mapped.setdefault(attribute, {})[source] = set(accessions)

    return mapped


def retrieve_series_ids(
    attribute_accessions: AttributeSourceAccessions,
) -> pl.DataFrame:
    """Retrieve series IDs for a list of samples"""
    sample_ids: list[str] = []
    for source_accessions in attribute_accessions.values():
        for accessions in source_accessions.values():
            sample_ids.extend(list(accessions))

    logger.debug("Querying series-to-sample mapping from OmicIDX")
    with duckdb.connect(OMICIDX_DB, read_only=True) as conn:
        result = conn.execute(
            """SELECT accession AS series, unnest(sample_id) AS sample FROM src_geo_series"""
        ).pl()
    logger.info("Retrieved %d series-sample pairs from OmicIDX", len(result))
    return result


def source_samples_to_series(
    attribute_accessions: AttributeSourceAccessions,
) -> AttributeSourceAccessions:
    """Convert sample IDs to series IDs."""
    sample2series: pl.DataFrame = retrieve_series_ids(attribute_accessions)

    attribute_series: AttributeSourceAccessions = {}
    for attribute, source_accessions in attribute_accessions.items():
        for source, accessions in source_accessions.items():
            series = set(
                sample2series.filter(pl.col("sample").is_in(accessions))["series"]
            )
            attribute_series.setdefault(attribute, {})[source] = series
            logger.info(
                "%-30s %d samples -> %d series", source, len(accessions), len(series)
            )

    return attribute_series


def get_overlap(
    attribute_accessions: AttributeSourceAccessions,
) -> dict[str, OverlapResult]:
    """Compute source entry overlap across attributes."""

    results: dict[str, OverlapResult] = {}
    for attribute, source_accessions in attribute_accessions.items():
        results[attribute] = get_source_contribution_overlap(source_accessions)

    return results


def combine_johnson_samples(
    attribute_accessions: AttributeSourceAccessions,
) -> AttributeSourceAccessions:
    """Merge Johnson_2023 microarray and RNA-Seq samples."""
    result: AttributeSourceAccessions = {}
    for attribute, source_accessions in attribute_accessions.items():
        result.setdefault(attribute, {})
        for source, accessions in source_accessions.items():
            if source in ["Johnson_2023_microarray", "Johnson_2023_rnaseq"]:
                result[attribute].setdefault("Johnson_2023", set()).update(accessions)
            else:
                result[attribute][source] = accessions

    return result


def save(outdir: Path, level: str, results: dict[str, OverlapResult]) -> None:
    """Save one file per attribute and level combination."""
    for attribute, result in results.items():
        for field in result.fields:
            outfile = (
                outdir / f"overlap_{field}__level-{level}__attribute-{attribute}.tsv"
            )
            result.save_field(field, outfile)


def main():
    """Main entry point."""
    parser = ArgumentParser()
    parser.add_argument(
        "-o",
        "--outdir",
        help="Path to outdir to save attribtue overlap files to.",
        default=DEFAULT_OUTDIR,
        type=Path,
    )
    args = parser.parse_args()
    outdir = checkdir(args.outdir)

    source_accessions: LevelAttributeSourceAccessions = {}
    for level in ["sample", "series"]:
        logger.info("Collecting %s-level accessions...", level)
        sources = {source: v for source, v in SOURCES.items() if v["level"] == level}
        source_accessions[level] = collect_source_accessions(sources)

    logger.info("Mapping SRA accessions to GSM IDs...")
    source_accessions["sample"] = map_sra_to_geo(source_accessions["sample"])

    logger.info("Combining Johnson_2023...")
    source_accessions["sample"] = combine_johnson_samples(source_accessions["sample"])

    logger.info("Computing sample-level overlaps...")
    sample_overlaps: dict[str, OverlapResult] = get_overlap(source_accessions["sample"])

    logger.info("Mapping sample-level sources to series...")
    mapped = source_samples_to_series(source_accessions["sample"])
    source_accessions["series"] = source_accessions["series"] | mapped

    logger.info("Computing series-level overlaps...")
    series_overlaps: dict[str, OverlapResult] = get_overlap(source_accessions["series"])

    logger.info("Saving...")
    save(outdir, level="sample", results=sample_overlaps)
    save(outdir, level="series", results=series_overlaps)
    logger.info("Done!")


if __name__ == "__main__":
    main()
