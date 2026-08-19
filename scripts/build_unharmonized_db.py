"""Builds a MetaHQ database without harmonizing (resolving annotations across sources)"""

from pathlib import Path
from pprint import pprint

import polars as pl
from metahq_build.combiners.geo import GEO_SOURCES, GEO_STUDY_SOURCES
from metahq_build.combiners.sra import SRA_SOURCES
from metahq_build.config import (
    COL_ACCESSION,
    OMICIDX_DB,
    SRA_EXPERIMENT_PREFIXES,
    SRA_RUN_PREFIXES,
)
from metahq_build.config.config import (
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    DELIMITER,
    ECODE_KEY,
    ID_KEY,
)
from metahq_build.metadata.sra_experiments import srx_to_geo
from metahq_build.metadata.sra_runs import srr_to_geo
from metahq_build.util.logging import setup_logger
from tqdm import tqdm

COL_SOURCE: str = "source"


class UnharmonizedBuilder:
    def __init__(self, metadata_db_path: Path | str = OMICIDX_DB):
        self.metadata_db_path = metadata_db_path

        self.logger = setup_logger(__name__)

    def build(self):
        """Builds the unharmonized database."""
        geo_samples = self.load_parquet_tables(GEO_SOURCES)
        geo_series = self.load_parquet_tables(GEO_STUDY_SOURCES)
        sra_entries = self.load_parquet_tables(SRA_SOURCES).slice(
            0, 1000
        )  # TODO: Remove after testing

        # map SRA entries to GEO samples and combine with GEO sample annotations
        mapped_sra_entries = self._sra_entries_to_geo(
            sra_entries, column_order=geo_samples.columns
        )
        sample_df = pl.concat(
            [mapped_sra_entries, geo_samples], how="vertical"
        ).unique()

        sample_anno = self._sample_df_to_bson(sample_df)

        examples = list(sample_anno.keys())[0:20]
        for example in examples:
            print("")
            print(f"{example}: ")
            pprint(sample_anno[example])

    def load_parquet_tables(self, files: dict[str, Path]):
        """Loads parquet files that all have the same schema into a single table."""
        lfs: list[pl.LazyFrame] = []
        for source, file in files.items():
            lf = pl.scan_parquet(file).with_columns(pl.lit(source).alias(COL_SOURCE))
            lfs.append(lf)

        return pl.concat(lfs).collect(engine="streaming")

    # ==================================================
    # ========= MetaHQ BSON db build
    # ==================================================
    def _sample_df_to_bson(self, df: pl.DataFrame) -> dict:
        """
        Converts a polars DataFrame of annotations to the MetaHQ BSON schema, leaving
        annotations unresolved across sources.
        """
        anno = {}

        for row in tqdm(
            df.iter_rows(named=True),
            total=df.height,
            desc="Combining source annotations",
        ):
            sample_id = row[COL_ACCESSION]
            attribute = row[COL_ATTRIBUTE]
            term_id = row[COL_TERM_ID]
            ecode = row[COL_ECODE]
            source = row[COL_SOURCE]

            anno.setdefault(sample_id, {})
            anno[sample_id].setdefault(attribute, {})
            anno[sample_id][attribute].setdefault(source, {})

            if len(anno[sample_id][attribute][source]) > 0:
                existing_ids = set(
                    anno[sample_id][attribute][source][ID_KEY].split(DELIMITER)
                )
                existing_ids.add(term_id)
                anno[sample_id][attribute][source][ID_KEY] = DELIMITER.join(
                    sorted(existing_ids)
                )
            else:
                anno[sample_id][attribute][source] = {ID_KEY: term_id, ECODE_KEY: ecode}

        return anno

    # ==================================================
    # ========= SRA to GEO methods
    # ==================================================

    def _separate_sra_runs_and_experiments(
        self,
        sra_entries: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Splits SRA annotations into SRA runs and SRA experiments."""
        runs_regex = f"^(?:{'|'.join(SRA_RUN_PREFIXES)})"
        experiments_regex = f"^(?:{'|'.join(SRA_EXPERIMENT_PREFIXES)})"

        return sra_entries.filter(
            pl.col(COL_ACCESSION).str.contains(runs_regex)
        ), sra_entries.filter(pl.col(COL_ACCESSION).str.contains(experiments_regex))

    def _sra_entries_to_geo(
        self, sra_entries: pl.DataFrame, column_order: list[str]
    ) -> pl.DataFrame:
        sra_runs, sra_experiments = self._separate_sra_runs_and_experiments(sra_entries)

        # generate mappings
        run_to_gsm = self._srx_runs_to_geo(sra_runs[COL_ACCESSION].unique().to_list())
        experiment_to_gsm = self._srx_experiments_to_geo(
            sra_experiments[COL_ACCESSION].unique().to_list()
        )

        sra_runs = (
            sra_runs.join(run_to_gsm, on=COL_ACCESSION)
            .drop(COL_ACCESSION)
            .rename({"gsm": COL_ACCESSION})
            .select(column_order)
        )
        sra_experiments = (
            sra_experiments.join(experiment_to_gsm, on=COL_ACCESSION)
            .drop(COL_ACCESSION)
            .rename({"gsm": COL_ACCESSION})
            .select(column_order)
        )

        return pl.concat([sra_runs, sra_experiments], how="vertical").unique()

    def _srx_experiments_to_geo(self, ids: list[str]) -> pl.DataFrame:
        """Takes SRA experiment IDs as input and returns mappings to SRP and GSM IDs."""
        # map SRA experiments to GSM
        self.logger.info(
            "There are %d SRA experiments to be mapped to GEO samples", len(ids)
        )

        experiment_to_gsm = (
            srx_to_geo(ids, db_path=self.metadata_db_path)
            .drop(["srp", "gse"])
            .unique()
            .rename({"srx": COL_ACCESSION})
        )
        self.logger.info(
            "Successfully mapped %d SRA experiments", experiment_to_gsm.height
        )
        return experiment_to_gsm

    def _srx_runs_to_geo(self, ids: list[str]) -> pl.DataFrame:
        """Takes SRA run IDs as input and returns mappings to GSM IDs."""
        # map SRA experiments to GSM
        self.logger.info("There are %d SRA runs to be mapped to GEO samples", len(ids))

        experiment_to_gsm = (
            srr_to_geo(ids, db_path=self.metadata_db_path)
            .drop(["srx", "srp", "gse"])
            .unique()
            .rename({"srr": COL_ACCESSION})
        )
        self.logger.info("Successfully mapped %d SRA runs", experiment_to_gsm.height)
        return experiment_to_gsm


def main():
    builder = UnharmonizedBuilder()
    builder.build()


if __name__ == "__main__":
    main()
