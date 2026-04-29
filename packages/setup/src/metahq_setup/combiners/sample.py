"""
Sample-level annotation combiner.

Merges the GSM-keyed outputs of GeoCombiner and SraCombiner into a single
BSON file containing all annotations for every sample.

Both input BSONs must already be keyed by GSM (GeoCombiner outputs GSM
directly; SraCombiner maps SRA accessions to GSM before writing). The merge
is a deep union: for each GSM, annotation-type dicts (tissue, disease, sex,
age) are merged by source name, and accession_ids dicts are unioned.

After merging, accession_ids are enriched from OmicIDX with:
- ``series``   — pipe-joined GSE accession(s)
- ``platform`` — GPL accession
- ``srx``      — SRA experiment accession
- ``srs``      — SRA sample accession
- ``srp``      — SRA study accession
"""

from pathlib import Path
from typing import Any

import bson
import duckdb
import polars as pl

from metahq_setup.combiners.base import BaseAnnotationCombiner
from metahq_setup.config import (
    ACCESSIONS_KEY,
    ATTRIBUTE_KEYS,
    COL_TECHNOLOGY_MAP_GPL,
    DELTED_SAMPLES,
    GEO_COMBINED_BSON,
    OMICIDX_DB,
    PLATFORM_ACCESSION_KEY,
    SRA_COMBINED_BSON,
    TECHNOLOGY_MAP,
)


class SampleCombiner(BaseAnnotationCombiner):
    """
    Merges GEO and SRA combined annotation BSONs into a single sample-level DB.

    Example:
        >>> combiner = SampleCombiner()
        >>> combiner.combine().clean().save(SAMPLE_COMBINED_BSON)
    """

    def combine(
        self,
        geo_bson: Path = GEO_COMBINED_BSON,
        sra_bson: Path = SRA_COMBINED_BSON,
        db_path: Path = OMICIDX_DB,
    ) -> "SampleCombiner":
        """
        Load and merge the GEO and SRA combined BSONs, then enrich accession IDs.

        For each GSM present in either source, annotation-type dicts are
        merged by source name. ``accession_ids`` dicts are unioned, with
        the SRA entry taking precedence for conflicting keys. Accession IDs
        are then enriched from OmicIDX with ``series``, ``platform``, ``srx``,
        and ``srp`` fields.

        Arguments:
            geo_bson (Path):
                Path to the GEO combined BSON file.
                Defaults to ``PROCESSED_DIR/geo_combined.bson``.
            sra_bson (Path):
                Path to the SRA combined BSON file.
                Defaults to ``PROCESSED_DIR/sra_combined.bson``.
            db_path (Path):
                Path to the OmicIDX DuckDB database file.
                Defaults to the package-wide ``OMICIDX_DB`` constant.

        Returns:
            (SampleCombiner): self, for chaining.
        """
        geo = self._load_bson(geo_bson, "GEO")
        sra = self._load_bson(sra_bson, "SRA")

        all_gsm = geo.keys() | sra.keys()
        self.logger.info(
            "Merging %d GEO and %d SRA entries (%d unique GSMs).",
            len(geo),
            len(sra),
            len(all_gsm),
        )

        for gsm in all_gsm:
            geo_entry = geo.get(gsm)
            sra_entry = sra.get(gsm)

            if geo_entry is not None and sra_entry is not None:
                self.anno[gsm] = self._merge_entries(geo_entry, sra_entry)
            else:
                self.anno[gsm] = geo_entry if geo_entry is not None else sra_entry

        self.logger.info("Combined annotations for %d samples.", len(self.anno))

        # Enrich accession_ids with series, platform, srx, srp from OmicIDX
        self.logger.info("Enriching accession IDs from OmicIDX...")

        self.logger.info("[1/2] Fetching organism information...")
        organism_map = self._build_organism_map(list(all_gsm), db_path)

        self.logger.info("[2/2] Fetching accession IDs...")
        accession_map = self._build_accession_id_map(list(all_gsm), db_path)

        enriched = 0
        for gsm, ids in accession_map.items():
            if gsm in self.anno:
                self.anno[gsm]["accession_ids"].update(ids)
                self.anno[gsm]["organism"] = organism_map[gsm]
                enriched += 1
        self.logger.info("Enriched accession IDs for %d samples.", enriched)

        self.logger.info("Removing non-transcriptomcs samples")
        before = len(self.anno)
        ok_platforms = pl.read_parquet(TECHNOLOGY_MAP)[COL_TECHNOLOGY_MAP_GPL].to_list()
        self._remove_non_transcriptomics_samples(ok_platforms)
        self.logger.info(
            "Removed %d non-transcriptomcs samples", before - len(self.anno)
        )

        return self

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_bson(self, path: Path, label: str) -> dict[str, Any]:
        """Load a BSON file and return its decoded dict.

        Arguments:
            path (Path):
                Path to the BSON file.
            label (str):
                Human-readable label used in log messages.

        Returns:
            (dict[str, Any]): Decoded annotation dict, or empty dict if the
                file does not exist.
        """
        if not path.exists():
            self.logger.warning("%s BSON not found at %s — skipping.", label, path)
            return {}

        with open(path, "rb") as f:
            data = bson.decode(f.read())

        before = len(data)
        self.logger.info("Loaded %d %s entries from %s.", before, label, path)

        num_gses = len([k for k in data if k.startswith("GSE")])
        data = {k: v for k, v in data.items() if k.startswith("GSM")}

        after = len(data)
        diff = before - after

        if diff > 0:
            self.logger.warning(
                "Removed %d non-GSM entries. Using %d entries.", diff, after
            )

            if num_gses == diff:
                self.logger.warning(
                    "This is expected as there are %d GSE entries that were removed",
                    num_gses,
                )
            else:
                self.logger.warning(
                    "This is unexpected. More than just GSE entries are being removed."
                )

        data = self._remove_deleted_samples(data)

        return data

    def _build_accession_id_map(
        self,
        gsm_ids: list[str],
        db_path: Path,
    ) -> dict[str, dict[str, str]]:
        """Query OmicIDX to build a mapping of GSM → additional accession IDs.

        Pulls ``platform`` (GPL) and ``srx`` (SRA experiment) from
        ``src_geo_samples``, ``series`` (pipe-joined GSEs) from
        ``src_geo_series``, and ``srs`` (SRA sample) and ``srp`` (SRA study)
        from ``src_sra_experiments``.

        Arguments:
            gsm_ids (list[str]):
                GSM accession IDs to look up.
            db_path (Path):
                Path to the OmicIDX DuckDB file.

        Returns:
            (dict[str, dict[str, str]]): Mapping of GSM → dict with keys
                ``series``, ``platform``, ``srx``, ``srs``, ``srp`` (any may
                be absent if no data was found).
        """
        if not gsm_ids:
            return {}

        if not db_path.exists():
            self.logger.warning(
                "OmicIDX DB not found at %s — skipping accession ID enrichment.",
                db_path,
            )
            return {}

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = con.execute(
                """
                WITH
                -- platform and SRX come directly from src_geo_samples
                samples AS (
                    SELECT
                        accession                                    AS gsm,
                        platform_id                                  AS platform,
                        json_extract_string(sra_experiment, '$')     AS srx
                    FROM src_geo_samples
                    WHERE accession = ANY($1)
                ),
                -- GSE: unnest src_geo_series.sample_id and pipe-join per GSM
                series_unnested AS (
                    SELECT accession AS gse, unnest(sample_id) AS gsm
                    FROM src_geo_series
                ),
                series_agg AS (
                    SELECT gsm, string_agg(gse, '|' ORDER BY gse) AS series
                    FROM series_unnested
                    WHERE gsm = ANY($1)
                    GROUP BY gsm
                ),
                -- SRS and SRP: join SRX to src_sra_experiments
                sra_map AS (
                    SELECT accession AS srx, sample_accession AS srs, study_accession AS srp
                    FROM src_sra_experiments
                    WHERE accession IN (SELECT srx FROM samples WHERE srx IS NOT NULL)
                )
                SELECT
                    s.gsm,
                    se.series,
                    s.platform,
                    s.srx,
                    sra.srs,
                    sra.srp
                FROM samples s
                LEFT JOIN series_agg se  ON s.gsm = se.gsm
                LEFT JOIN sra_map    sra ON s.srx  = sra.srx
                """,
                [gsm_ids],
            ).fetchall()
        finally:
            con.close()

        result: dict[str, dict[str, str]] = {}
        for gsm, series, platform, srx, srs, srp in rows:
            ids: dict[str, str] = {}
            if series:
                ids["series"] = series
            if platform:
                ids["platform"] = platform
            if srx:
                ids["srx"] = srx
            if srs:
                ids["srs"] = srs
            if srp:
                ids["srp"] = srp
            if ids:
                result[gsm] = ids

        self.logger.info(
            "Retrieved accession IDs for %d of %d GSMs from OmicIDX.",
            len(result),
            len(gsm_ids),
        )
        return result

    def _remove_non_transcriptomics_samples(self, ok_platforms: list[str]):
        transcriptomics_samples: dict[str, Any] = {}
        for entry, values in self.anno.items():
            platform = values[ACCESSIONS_KEY][PLATFORM_ACCESSION_KEY]
            if platform in ok_platforms:
                transcriptomics_samples[entry] = values

        self.anno = transcriptomics_samples

    def _build_organism_map(self, gsm_ids: list[str], db_path: Path) -> dict[str, str]:
        if not gsm_ids:
            return {}

        if not db_path.exists():
            self.logger.warning(
                "OmicIDX DB not found at %s — skipping accession ID enrichment.",
                db_path,
            )
            return {}

        with duckdb.connect(db_path, read_only=True) as conn:
            result = conn.execute(
                """
                WITH samples AS (
                    SELECT s.accession, s.channels
                    FROM src_geo_samples s
                    WHERE s.accession = ANY($1)
                )
                SELECT accession, unnest(channels).organism
                FROM samples;
                """,
                [gsm_ids],
            ).fetchall()

        return {row[0]: row[1].lower() for row in result}

    def _remove_deleted_samples(self, data: dict) -> dict:
        """Remove samples deleted from GEO."""
        with open(DELTED_SAMPLES, "r", encoding="utf-8") as f:
            delted_samples = [line.strip() for line in f.readlines()]

        before = len(data)
        data = {k: v for k, v in data.items() if k not in delted_samples}

        after = len(data)
        diff = before - after

        if diff > 0:
            self.logger.info(
                "Removed %d samples deleted from GEO using %s", diff, DELTED_SAMPLES
            )

        return data

    @staticmethod
    def _merge_entries(
        geo_entry: dict[str, Any],
        sra_entry: dict[str, Any],
    ) -> dict[str, Any]:
        """Deep-merge one GEO and one SRA entry for the same GSM.

        Annotation-type dicts are merged by source name (no conflicts expected
        since GEO and SRA sources are disjoint). ``accession_ids`` are unioned,
        with SRA values overwriting GEO values for any shared keys.

        Arguments:
            geo_entry (dict[str, Any]):
                GEO annotation entry for a GSM.
            sra_entry (dict[str, Any]):
                SRA annotation entry for the same GSM.

        Returns:
            (dict[str, Any]): Merged annotation entry.
        """
        merged: dict[str, Any] = {}

        for key in ATTRIBUTE_KEYS:
            geo_sources = geo_entry.get(key, {})
            sra_sources = sra_entry.get(key, {})
            merged[key] = {**geo_sources, **sra_sources}

        merged["accession_ids"] = {
            **geo_entry.get("accession_ids", {}),
            **sra_entry.get("accession_ids", {}),
        }

        return merged
