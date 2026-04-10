"""
SRA annotation combiner.

Combines processed annotations from SRA-based sources into a single BSON
file keyed by GEO sample IDs (GSM).

SRA sources use run-level (xxR: SRR, DRR, ERR) or experiment-level
(xxX: SRX, DRX, ERX) accession IDs. These are mapped to GSM IDs via the
OmicIDX DuckDB database before combining:

    xxR → xxX   via src_sra_runs (accession → experiment_accession)
    xxX → GSM   via src_geo_samples (sra_experiment JSON → accession)

Study-level IDs (xxP: SRP, DRP, ERP) are not handled here — they belong
in a study-level combiner that maps xxP → GSE via src_sra_studies.
"""

from pathlib import Path

import duckdb
import polars as pl

from metahq_setup.combiners.base import BaseAnnotationCombiner
from metahq_setup.config.config import (
    BGEE_PROCESSED,
    CELLO_PROCESSED,
    GU_PROCESSED,
    JOHNSON_2023_RNASEQ_PROCESSED,
    OMICIDX_DB,
    PROCESSED_DIR,
)

# Maps source name → default processed parquet path.
SRA_SOURCES: dict[str, Path] = {
    "bgee": BGEE_PROCESSED,
    "cello": CELLO_PROCESSED,
    "gu": GU_PROCESSED,
    "johnson_2023_rnaseq": JOHNSON_2023_RNASEQ_PROCESSED,
}

# Default output path for the combined SRA annotations.
SRA_COMBINED_BSON: Path = PROCESSED_DIR / "sra_combined.bson"


class SraCombiner(BaseAnnotationCombiner):
    """
    Combines annotations from SRA-based sources, mapping accession IDs to GSM.

    Run-level (xxR) IDs are first resolved to experiment-level (xxX) IDs via
    ``src_sra_runs``, then both xxR and xxX IDs are mapped to GEO sample IDs
    (GSM) via ``src_geo_samples`` in the OmicIDX DuckDB database.

    Example:
        >>> combiner = SraCombiner()
        >>> combiner.combine().clean().save(SRA_COMBINED_BSON)
    """

    def combine(
        self,
        db_path: Path = OMICIDX_DB,
        overrides: dict[str, Path] | None = None,
    ) -> "SraCombiner":
        """
        Load and combine all SRA source parquets, mapping IDs to GSM.

        Sources whose parquet file does not exist are skipped with a warning.
        Within each source, rows whose accession ID cannot be mapped to a GSM
        are dropped and counted.

        Arguments:
            db_path (Path):
                Path to the OmicIDX DuckDB database file.
                Defaults to the package-wide ``OMICIDX_DB`` constant.
            overrides (dict[str, Path] | None):
                Per-source path overrides. Keys are source names from
                ``SRA_SOURCES``; values replace the default path for that source.

        Returns:
            (SraCombiner): self, for chaining.
        """
        overrides = overrides or {}

        # Collect all unique sample IDs across all sources first so we can
        # build the mapping in a single DuckDB query.
        source_data: dict[str, pl.DataFrame] = {}
        all_sample_ids: set[str] = set()

        for source_name, default_path in SRA_SOURCES.items():
            path = overrides.get(source_name, default_path)

            if not path.exists():
                self.logger.warning(
                    "Skipping '%s': file not found at %s.", source_name, path
                )
                continue

            self.logger.info("Loading '%s' from %s...", source_name, path)
            data = pl.read_parquet(path)
            source_data[source_name] = data
            all_sample_ids.update(data["sample_id"].unique().to_list())

        if not all_sample_ids:
            self.logger.warning("No sample IDs found across all SRA sources.")
            return self

        # Partition IDs by level.
        xxr_ids = [i for i in all_sample_ids if len(i) >= 3 and i[2].upper() == "R"]
        xxx_ids = [i for i in all_sample_ids if len(i) >= 3 and i[2].upper() == "X"]
        xxp_ids = [i for i in all_sample_ids if len(i) >= 3 and i[2].upper() == "P"]

        if xxp_ids:
            self.logger.warning(
                "%d study-level (xxP) IDs found and skipped — use a study-level "
                "combiner with src_sra_studies → src_geo_series for these: %s...",
                len(xxp_ids),
                xxp_ids[:5],
            )

        self.logger.info(
            "Resolving %d xxR and %d xxX IDs to GSM via OmicIDX...",
            len(xxr_ids),
            len(xxx_ids),
        )
        mapping = self._build_gsm_mapping(xxr_ids, xxx_ids, db_path)
        self.logger.info("Resolved %d IDs to GSM.", len(mapping))

        # Apply mapping and add each source.
        for source_name, data in source_data.items():
            before = data.height
            data = data.with_columns(
                pl.col("sample_id").replace(mapping, default=None)
            ).filter(pl.col("sample_id").is_not_null())

            dropped = before - data.height
            if dropped:
                self.logger.warning(
                    "'%s': dropped %d rows with no GSM mapping (kept %d).",
                    source_name,
                    dropped,
                    data.height,
                )

            self.add_source(source_name, data)

        return self

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_gsm_mapping(
        xxr_ids: list[str],
        xxx_ids: list[str],
        db_path: Path,
    ) -> dict[str, str]:
        """
        Query OmicIDX to build a mapping from SRA accession IDs to GSM.

        xxR IDs are first joined to their experiment accession (xxX) via
        ``src_sra_runs``, then both original xxR and xxX IDs are joined to
        ``src_geo_samples`` on the ``sra_experiment`` JSON field.

        Arguments:
            xxr_ids (list[str]):
                Run-level accession IDs (SRR, DRR, ERR).
            xxx_ids (list[str]):
                Experiment-level accession IDs (SRX, DRX, ERX).
            db_path (Path):
                Path to the OmicIDX DuckDB file.

        Returns:
            (dict[str, str]): Mapping of original SRA ID → GSM accession.
        """
        if not xxr_ids and not xxx_ids:
            return {}

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            # Resolve xxR → xxX, then join to GSM.
            # Resolve xxX → GSM directly.
            # Union both result sets.
            query = """
                WITH
                -- Resolve xxR to xxX, carry original xxR as the key.
                run_to_exp AS (
                    SELECT
                        r.accession       AS original_id,
                        r.experiment_accession AS xxx_id
                    FROM src_sra_runs r
                    WHERE r.accession = ANY($1)
                ),
                -- xxX IDs passed in directly.
                direct_exp AS (
                    SELECT unnest($2) AS original_id, unnest($2) AS xxx_id
                ),
                -- All IDs at the xxX level with their original key.
                all_exp AS (
                    SELECT * FROM run_to_exp
                    UNION ALL
                    SELECT * FROM direct_exp
                )
                SELECT
                    a.original_id,
                    g.accession AS gsm
                FROM all_exp a
                JOIN src_geo_samples g
                    ON a.xxx_id = json_extract_string(g.sra_experiment, '$')
                WHERE g.accession IS NOT NULL
            """
            rows = con.execute(query, [xxr_ids, xxx_ids]).fetchall()
        finally:
            con.close()

        return {original_id: gsm for original_id, gsm in rows}
