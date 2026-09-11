"""
Metadata queries for SRA runs using OmicIDX.
"""

from pathlib import Path

import duckdb
import polars as pl

from metahq_build.config import OMICIDX_DB


def srr_to_geo(sra_ids: list[str], db_path: Path | str = OMICIDX_DB) -> pl.DataFrame:
    """Query a full srr, srx, srp, gsm, gse map for a list of SRA run IDs.

    Arguments:
        sra_ids (list[str]):
            A list of SRA run IDs.
        db_path (Path | str):
            Path to OmicIDX duckdb database.

    Returns:
        (pl.DataFrame): Mappings from SRA runs to SRA experiments, SRA projects,
            GEO samples, and GEO series.
    """
    with duckdb.connect(db_path, read_only=True) as conn:
        srr_gsm_map = conn.execute(
            """
                WITH 
                    srr_srx_map AS (
                        SELECT accession as srr, experiment_accession as srx
                        FROM src_sra_runs
                        WHERE accession = Any($1)
                    ),
                    srx_srp_map AS (
                        SELECT accession as srx, study_accession as srp
                        FROM src_sra_experiments
                        WHERE accession IN (SELECT srx FROM srr_srx_map)
                    ),
                    gsm_srx_map AS (
                        SELECT accession as gsm, trim(sra_experiment, '"') as srx
                        FROM src_geo_samples
                        --WHERE sra_experiment IN (SELECT srx FROM srr_srx_map)
                    )
                SELECT srr, srr_srx_map.srx, srx_srp_map.srp, gsm_srx_map.gsm
                FROM srr_srx_map
                JOIN gsm_srx_map ON srr_srx_map.srx::VARCHAR = trim(gsm_srx_map.srx::VARCHAR, '"')
                JOIN srx_srp_map ON srr_srx_map.srx = srx_srp_map.srx
                """,
            [sra_ids],
        ).pl()

        gsm_ids = srr_gsm_map["gsm"].to_list()
        srr_gsm_map = srr_gsm_map.lazy()

        gsm_gse_map = (
            conn.execute(
                """
                WITH gse_gsm_map AS (
                    SELECT accession as gse, unnest(sample_id) as gsm
                    FROM src_geo_series
                )
                SELECT gsm, gse
                FROM gse_gsm_map
                WHERE gsm = Any($1)
                """,
                [gsm_ids],
            )
            .pl()
            .lazy()
        )

        return srr_gsm_map.join(gsm_gse_map, on="gsm", how="inner").collect(
            engine="streaming"
        )
