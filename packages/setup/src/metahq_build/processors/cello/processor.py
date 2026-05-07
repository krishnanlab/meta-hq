"""
CellO cell type annotation processor.

Processes automated cell type annotations from CellO, which provides Cell
Ontology (CL) term predictions for bulk RNA-seq samples. The source file is a
JSON object mapping SRX accession IDs to lists of CL term IDs that have already
been formatted as ``"CL:XXXXXXX"`` strings.
"""

import json
from pathlib import Path

import polars as pl

from metahq_build.config.config import (
    CELLO_JSON,
    CL_OBO,
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    ECODE_EXPERT,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_build.ontology import Ontology, get_system_descendants
from metahq_build.processors.base import BaseProcessor
from metahq_build.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class CellOProcessor(BaseProcessor):
    """
    Processor for CellO automated cell type annotations.

    CellO provides automated cell type predictions using the Cell Ontology (CL)
    for bulk RNA-seq samples. The raw source is a JSON object keyed by SRX
    accession with a list of CL term IDs as the value for each sample.

    The processor explodes the per-sample term lists into one row per
    (sample, CL term), resolves each CL ID to a human-readable name via the
    CL OBO file, and drops any terms that cannot be resolved.
    """

    source_name = "Cello"
    version = "1.0.0"
    description = "CellO automated cell type annotations for bulk RNA-seq"

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """Process the CellO JSON file into standardized sample annotations.

        Reads the JSON source, builds one row per (sample, CL term), maps each
        CL ID to its human-readable label via the CL OBO, drops unmapped terms,
        and writes the result to a parquet file.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path | str) — override the default CellO JSON
                input path (defaults to ``CELLO_JSON`` from config).

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``COL_ACCESSION``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", CELLO_JSON))
        self.logger.info("Processing CellO JSON file: %s", input_path)

        with input_path.open("r") as fh:
            raw: dict[str, list[str]] = json.load(fh)

        self.logger.info("Loaded %s sample entries from CellO JSON.", len(raw))

        # Build a DataFrame with one row per sample, term_ids as a list column.
        sample_ids: list[str] = []
        term_id_lists: list[list[str]] = []
        for sample_id, term_ids in raw.items():
            sample_ids.append(sample_id)
            term_id_lists.append(term_ids if isinstance(term_ids, list) else [])

        df = (
            pl.DataFrame(
                {COL_ACCESSION: sample_ids, "term_ids": term_id_lists},
                schema={COL_ACCESSION: pl.String, "term_ids": pl.List(pl.String)},
            )
            .explode("term_ids")
            .rename({"term_ids": COL_TERM_ID})
        )

        self.logger.info(
            "Exploded to %s (sample, term) rows before ontology mapping.", df.height
        )

        # Map CL IDs to human-readable names.
        self.logger.info("Loading CL ontology for ID -> name mapping: %s", CL_OBO)
        cl_class_dict: dict[str, str] = Ontology.from_obo(CL_OBO).class_dict

        df = df.with_columns(
            pl.col(COL_TERM_ID)
            .replace(cl_class_dict, default="NA")
            .alias(COL_TERM_NAME)
        )

        unmapped = df.filter(pl.col(COL_TERM_NAME) == "NA").height
        if unmapped > 0:
            self.logger.warning(
                "%s (sample, term) rows could not be mapped to a CL name and will be dropped.",
                unmapped,
            )

        df = df.filter(pl.col(COL_TERM_NAME) != "NA")

        # Filter to descendants of UBERON/CL system-level terms.
        self.logger.info("Loading UBERON system descendants for filtering...")
        valid_terms = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)
        before = df.height
        df = df.filter(pl.col(COL_TERM_ID).is_in(valid_terms))
        self.logger.info(
            "Filtered annotations from %s to %s using UBERON/CL system descendants.",
            before,
            df.height,
        )

        result_df = df.select(
            pl.col(COL_ACCESSION),
            pl.lit("tissue").alias(COL_ATTRIBUTE),
            pl.col(COL_TERM_ID),
            pl.col(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        ).sort(COL_ACCESSION)

        self.logger.info(
            "Produced %s tissue annotations across %s unique samples.",
            result_df.height,
            result_df[COL_ACCESSION].n_unique(),
        )

        output_file = output_dir / "cello_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """Validate that processed CellO data meets minimum requirements.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        has_tissue = "tissue" in data[COL_ATTRIBUTE].unique().to_list()
        if not has_tissue:
            self.logger.warning("No tissue annotations found in CellO output.")

        return True
