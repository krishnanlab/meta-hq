"""
Base class for annotation combiners.

Defines the shared logic for reading standard-schema processor output
and building the nested annotation dict that forms the MetaHQ database.

Output structure
----------------
The combined dict is keyed by sample ID (GSM, SRR, etc.) and has the form::

    {
        "<sample_id>": {
            "tissue": {
                "<source_name>": {"id": "UBERON:...", "value": "brain", "ecode": "expert"}
            },
            "disease": { ... },
            "sex":     { ... },
            "age":     { ... },
            "accession_ids": {"sample": "<sample_id>", ...}
        }
    }

Multiple ontology terms for the same (sample, source, annotation_type) are
joined with ``||``.
"""

from pathlib import Path
from typing import Any

import bson
import polars as pl

from metahq_setup.util.logging import setup_logger

# Annotation type names as produced by the processors.
ANNOTATION_TYPES: frozenset[str] = frozenset({"tissue", "disease", "sex", "age"})

# Values treated as absent / undesired during the clean step.
UNDESIRED: frozenset = frozenset({"na", "", "NA", "none", "not annotated"})


class BaseAnnotationCombiner:
    """
    Base class for building a combined annotation dict from processor outputs.

    Subclasses implement ``combine()`` to load source data and call
    ``add_source()`` for each. The ``clean()`` → ``save()`` workflow is
    defined here and shared across all combiners.

    Attributes:
        anno (dict[str, Any]):
            The combined annotation dictionary, keyed by sample ID.
    """

    def __init__(self):
        self.anno: dict[str, Any] = {}
        self.logger = setup_logger(f"metahq_setup.combiners.{self.__class__.__name__}")

    def add_source(self, source_name: str, data: pl.DataFrame) -> None:
        """
        Add annotations from a standard-schema DataFrame.

        Rows are grouped by ``(sample_id, annotation_type)``. Multiple term
        IDs and labels for the same group are joined with ``||``. The ecode
        of the first row in the group is used (processors produce a single
        ecode per source).

        Arguments:
            source_name (str):
                Name of the data source, used as the key in the nested dict
                (e.g., ``"ale"``, ``"gemma"``).
            data (pl.DataFrame):
                Standard-schema DataFrame with columns ``sample_id``,
                ``annotation_type``, ``term_id``, ``term_label``, ``ecode``.
        """
        data = data.filter(pl.col("annotation_type").is_in(ANNOTATION_TYPES))

        if data.is_empty():
            self.logger.warning(
                "No supported annotations found for source '%s'.", source_name
            )
            return

        grouped = (
            data.sort(["sample_id", "annotation_type", "term_id"])
            .group_by(["sample_id", "annotation_type"])
            .agg(
                pl.col("term_id").drop_nulls().str.join("||").alias("term_id"),
                pl.col("term_label").drop_nulls().str.join("||").alias("term_label"),
                pl.col("ecode").first().alias("ecode"),
            )
        )

        for row in grouped.iter_rows(named=True):
            sample_id = row["sample_id"]
            annotation_type = row["annotation_type"]

            self._init_entry(sample_id)

            self.anno[sample_id][annotation_type][source_name] = {
                "id": row["term_id"],
                "value": row["term_label"],
                "ecode": row["ecode"],
            }

        self.logger.info(
            "Added %d annotations from '%s' across %d samples.",
            data.height,
            source_name,
            grouped["sample_id"].n_unique(),
        )

    def clean(self) -> "BaseAnnotationCombiner":
        """
        Remove empty and undesired annotation entries.

        Drops source entries where every value is in ``UNDESIRED`` or where
        the only key remaining after filtering is ``ecode``. Drops sample
        entries that have no substantive annotations after cleaning.

        Returns:
            (BaseAnnotationCombiner): self, for chaining.
        """
        cleaned: dict[str, Any] = {}

        for sample_id, annos in self.anno.items():
            cleaned_entry: dict[str, Any] = {}

            for attribute, value in annos.items():
                if attribute in ["accession_ids", "organism"]:
                    cleaned_entry[attribute] = value
                    continue

                if not isinstance(value, dict):
                    continue

                cleaned_attr = self._clean_attribute(value)
                if cleaned_attr:
                    cleaned_entry[attribute] = cleaned_attr

            # Only keep entries with at least one annotation beyond accession_ids.
            if any(k not in ["accession_ids", "organism"] for k in cleaned_entry):
                cleaned[sample_id] = cleaned_entry

        self.anno = cleaned
        self.logger.info("Retained %d samples after cleaning.", len(self.anno))
        return self

    def save(self, output_path: Path) -> None:
        """
        Save the combined annotation dict to a BSON file.

        Arguments:
            output_path (Path):
                Destination file path (parent directories are created if needed).
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "wb") as f:
            f.write(bson.encode(self.anno))

        self.logger.info(
            "Saved %d sample annotations to %s", len(self.anno), output_path
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_entry(self, sample_id: str) -> None:
        """Initialize the annotation entry for a sample if not yet present."""
        if sample_id not in self.anno:
            self.anno[sample_id] = {
                "tissue": {},
                "disease": {},
                "sex": {},
                "age": {},
                "accession_ids": {"sample": sample_id},
            }

    @staticmethod
    def _clean_attribute(attribute: dict[str, dict]) -> dict[str, dict]:
        """
        Remove source entries with only undesired values or only an ecode.

        Arguments:
            attribute (dict[str, dict]):
                Mapping of source name → annotation dict for one attribute.

        Returns:
            (dict[str, dict]): Cleaned attribute dict.
        """
        cleaned: dict[str, dict] = {}
        for source_name, source_anno in attribute.items():
            filtered = {
                k: v
                for k, v in source_anno.items()
                if v is not None and v not in UNDESIRED
            }
            # Require an 'id' key — annotations without one can't be queried.
            if "id" not in filtered:
                continue
            # Keep only if there's more than just the ecode field.
            if len(filtered) > 1:
                cleaned[source_name] = filtered
        return cleaned
