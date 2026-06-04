"""
Characterize the overlap samples and studies across pre-harmonized sources.

Author: Parker Hicks
Date: 2026-06-04
"""

from argparse import ArgumentParser
from pathlib import Path
from typing import TypeAlias

import duckdb
import polars as pl
from metahq_build.analysis import get_source_contribution_overlap
from metahq_build.config.config import (
    ALE_PROCESSED,
    BGEE_PROCESSED,
    CELLO_PROCESSED,
    COL_ACCESSION,
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
from polars.series import series

logger = setup_logger(__name__)
SourceMap: TypeAlias = dict[str, dict[str, str | Path]]
DEFAULT_OUTDIR = ROOT / "results"

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


def collect_source_accessions(sources: SourceMap) -> dict[str, set[str]]:
    """Collect accession IDs from each source."""
    collected: dict[str, set[str]] = {}
    for source, metadata in sources.items():
        logger.debug("Collecting accessions from %s", source)
        accessions = set(
            pl.scan_parquet(metadata["file"])
            .select(COL_ACCESSION)
            .collect()[COL_ACCESSION]
            .to_list()
        )
        collected[source] = accessions
        logger.info("%-30s %d accessions", source, len(accessions))
    return collected


def map_sra_to_geo(source_accessions: dict[str, set[str]]) -> dict[str, set[str]]:
    """Map SRA IDs to GSMs per source.

    Assumes there are no sources that contain both GEO and SRA IDs.
    """
    mapped: dict[str, set[str]] = {}
    for source, accessions in source_accessions.items():
        sra_accessions = {id_ for id_ in accessions if not id_.startswith(("GSM"))}
        if len(sra_accessions) == 0:
            logger.debug("%s: all accessions are GSM IDs, skipping SRA mapping", source)
            mapped[source] = accessions
            continue

        xxr_ids = [id_ for id_ in sra_accessions if (len(id_) >= 3 and id_[2] == "R")]
        xxx_ids = [id_ for id_ in sra_accessions if (len(id_) >= 3 and id_[2] == "X")]
        logger.info(
            "%s: mapping %d SRA IDs (%d run, %d experiment) to GSM",
            source,
            len(sra_accessions),
            len(xxr_ids),
            len(xxx_ids),
        )

        gsm_ids = set(sra2gsm_map(xxr_ids, xxx_ids, db_path=OMICIDX_DB)["gsm"])
        logger.info("%s: mapped to %d GSM IDs", source, len(gsm_ids))
        mapped[source] = gsm_ids

    return mapped


def retrieve_series_ids(source_accessions: dict[str, set[str]]) -> pl.DataFrame:
    """Retrieve series IDs for a list of samples"""
    sample_ids: list[str] = []
    for accessions in source_accessions.values():
        sample_ids.extend(list(accessions))

    logger.debug("Querying series-to-sample mapping from OmicIDX")
    with duckdb.connect(OMICIDX_DB, read_only=True) as conn:
        result = conn.execute("""
                SELECT accession AS series, unnest(sample_id) AS sample FROM src_geo_series
                """).pl()
    logger.info("Retrieved %d series-sample pairs from OmicIDX", len(result))
    return result


def source_samples_to_series(
    source_accessions: dict[str, set[str]],
) -> dict[str, set[str]]:

    sample2series: pl.DataFrame = retrieve_series_ids(source_accessions)

    source_series: dict[str, set[str]] = {}
    for source, accessions in source_accessions.items():
        series = set(sample2series.filter(pl.col("sample").is_in(accessions))["series"])
        source_series[source] = series
        logger.info(
            "%-30s %d samples -> %d series", source, len(accessions), len(series)
        )

    return source_series


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

    source_accessions: dict[str, dict[str, set[str]]] = {}
    for level in ["sample", "series"]:
        logger.info("--- Collecting %s-level accessions ---", level)
        sources = {source: v for source, v in SOURCES.items() if v["level"] == level}
        source_accessions[level] = collect_source_accessions(sources)

    logger.info("--- Mapping SRA accessions to GSM IDs ---")
    source_accessions["sample"] = map_sra_to_geo(source_accessions["sample"])

    logger.info("--- Computing sample-level overlaps ---")
    sample_overlaps = get_source_contribution_overlap(source_accessions["sample"])

    logger.info("--- Mapping sample-level sources to series ---")
    mapped = source_samples_to_series(source_accessions["sample"])
    source_accessions["series"] = source_accessions["series"] | mapped

    logger.info("--- Computing series-level overlaps ---")
    series_overlaps = get_source_contribution_overlap(source_accessions["series"])

    print(sample_overlaps)
    print(series_overlaps)


if __name__ == "__main__":
    main()
