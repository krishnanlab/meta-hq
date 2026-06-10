"""
CREEDS perturbation annotation processor.

Processes annotations from CREEDS (CRowd Extracted Expression of Differential Signatures),
which provides crowd-sourced disease perturbation annotations.
"""

import json
from pathlib import Path
from typing import Any

import polars as pl

from metahq_build.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    CONTROL_ID,
    CONTROL_VALUE,
    CREEDS_JSON,
    CREEDS_TISSUE_NAME_TO_UBERON,
    ECODE_CROWD,
    MONDO_OBO,
    MONDO_SYSTEMS,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_build.ontology import Ontology, get_system_descendants
from metahq_build.processors.base import BaseProcessor
from metahq_build.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class CREEDSProcessor(BaseProcessor):
    """
    Processor for CREEDS crowd-sourced disease annotations.

    CREEDS provides crowd-sourced annotations for disease perturbations
    with both control and perturbation sample annotations.
    """

    source_name = "CREEDS"
    version = "1.0.0"
    description = "CREEDS crowd-sourced perturbation annotations"

    def process(self, output_dir: Path, **kwargs: Any) -> pl.DataFrame:
        """Process CREEDS annotations into standardized format.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path) - override CREEDS JSON input file

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", CREEDS_JSON))
        self.logger.info("Processing CREEDS annotations from %s", input_path)

        # Load CREEDS JSON data
        with open(input_path, "r") as f:
            creeds_data = json.load(f)

        self.logger.info("Loaded %s CREEDS signature entries", len(creeds_data))

        # Process disease annotations
        disease_records = self._process_disease_annotations(creeds_data)

        # Process tissue annotations
        tissue_records = self._process_tissue_annotations(creeds_data)

        # Combine all records
        all_records = disease_records + tissue_records

        result_df = pl.DataFrame(all_records).sort(
            [COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME]
        )

        self.logger.info(
            "Produced %s total annotations from CREEDS (%s disease + %s tissue)",
            len(result_df),
            len(disease_records),
            len(tissue_records),
        )

        # Save processed data
        output_file = output_dir / "creeds_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def _process_disease_annotations(self, creeds_data: list[dict]) -> list[dict]:
        """Process disease annotations from CREEDS data.

        Arguments:
            creeds_data (list[dict]):
                List of CREEDS signature entries.

        Returns:
            (list[dict]): List of disease annotation records.
        """
        # Load MONDO ontology for DOID mapping
        self.logger.info("Loading MONDO ontology for DOID mapping...")
        mondo = Ontology.from_obo(MONDO_OBO)

        # Get unique DOIDs from data (filter for valid DOIDs only)
        valid_doids = set()
        for entry in creeds_data:
            if self._is_valid_entry(entry):
                doid = entry["do_id"]
                if doid and isinstance(doid, str) and doid.startswith("DOID:"):
                    valid_doids.add(doid)

        self.logger.info("Found %s unique valid DOIDs to map", len(valid_doids))

        # Map DOID to MONDO
        xref_mappings = mondo.xref("DOID")
        # Add custom control mapping
        xref_mappings.add({"MONDO:0000000": ["DOID:0000000"]})
        reverse_map = xref_mappings.reverse()
        doid_to_mondo = {term: reverse_map.get(term, "NA") for term in valid_doids}

        # Load MONDO system descendants for filtering
        self.logger.info("Loading MONDO system descendants for filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)

        # Process entries and create annotation records
        records = []
        skipped_system_level = 0
        for entry in creeds_data:
            if not self._is_valid_entry(entry):
                continue

            doid = entry["do_id"]

            # Skip if DOID doesn't map to MONDO
            if doid not in doid_to_mondo:
                continue

            mondo_id = doid_to_mondo[doid]
            if mondo_id == "NA":
                continue

            # Skip if MONDO ID is at system level or higher (not in descendants)
            if mondo_id not in valid_mondo:
                skipped_system_level += 1
                continue

            disease_name = entry.get("disease_name", "unknown")

            # Process perturbation samples (disease samples)
            pert_ids = entry.get("pert_ids", [])
            for gsm_id in pert_ids:
                records.append(
                    {
                        COL_ACCESSION: gsm_id,
                        COL_ATTRIBUTE: "disease",
                        COL_TERM_ID: mondo_id,
                        COL_TERM_NAME: disease_name,
                        COL_ECODE: ECODE_CROWD,
                    }
                )

            # Process control samples
            ctrl_ids = entry.get("ctrl_ids", [])
            for gsm_id in ctrl_ids:
                records.append(
                    {
                        COL_ACCESSION: gsm_id,
                        COL_ATTRIBUTE: "disease",
                        COL_TERM_ID: CONTROL_ID,  # Control samples
                        COL_TERM_NAME: CONTROL_VALUE,
                        COL_ECODE: ECODE_CROWD,
                    }
                )

        if skipped_system_level > 0:
            self.logger.info(
                "Skipped %s entries with system-level or higher MONDO terms.",
                skipped_system_level,
            )

        self.logger.info(
            "Produced %s disease annotations from CREEDS (%s perturbation + %s control)",
            len(records),
            len([r for r in records if r[COL_TERM_ID] != CONTROL_ID]),
            len([r for r in records if r[COL_TERM_ID] == CONTROL_ID]),
        )

        return records

    def _process_tissue_annotations(self, creeds_data: list[dict]) -> list[dict]:
        """Process tissue annotations from CREEDS data.

        Maps free-text tissue names to UBERON/CL ontology terms using the
        manual CREEDS tissue mapping file.

        Arguments:
            creeds_data (list[dict]):
                List of CREEDS signature entries.

        Returns:
            (list[dict]): List of tissue annotation records.
        """
        # Collect unique tissue names from data
        tissue_names = set()
        for entry in creeds_data:
            if self._is_valid_entry(entry):
                cell_type = entry.get("cell_type", "")
                if cell_type and isinstance(cell_type, str):
                    tissue_names.add(cell_type.lower())

        self.logger.info(
            "Found %s unique tissue names in CREEDS data", len(tissue_names)
        )

        # Load manual tissue mapping
        self.logger.info(
            "Loading manual tissue mappings from %s", CREEDS_TISSUE_NAME_TO_UBERON
        )
        manual_mapping_df = pl.read_csv(CREEDS_TISSUE_NAME_TO_UBERON)
        tissue_name_to_terms = {
            row["name"].lower(): row["id"]
            for row in manual_mapping_df.iter_rows(named=True)
            if row["id"] and row["id"] != "na"
        }

        exact_matches = 0
        onto_ids_names = Ontology.from_obo(UBERON_OBO).id_map("polars")
        for tissue in tissue_names:
            if tissue.lower() in onto_ids_names["name"]:
                exact_matches += 1

        # Track mapping statistics
        mapped_tissues = set(tissue_names) & set(tissue_name_to_terms.keys())
        unmapped_tissues = set(tissue_names) - set(tissue_name_to_terms.keys())

        self.logger.info(
            "Mapped %s/%s tissue names using manual CREEDS mappings",
            len(mapped_tissues),
            len(tissue_names),
        )

        self.logger.info("%d tissue names mapped through exact matches", exact_matches)

        if unmapped_tissues:
            self.logger.info(
                "Unmapped tissue names (%s): %s",
                len(unmapped_tissues),
                sorted(unmapped_tissues)[:10]  # Show first 10
                + (["..."] if len(unmapped_tissues) > 10 else []),
            )

        # Load UBERON system descendants for filtering
        self.logger.info("Loading UBERON system descendants for filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        # Process entries and create annotation records
        records = []
        skipped_system_level = 0
        for entry in creeds_data:
            if not self._is_valid_entry(entry):
                continue

            cell_type = entry.get("cell_type", "")
            if not cell_type or not isinstance(cell_type, str):
                continue

            cell_type_lower = cell_type.lower()
            if cell_type_lower not in tissue_name_to_terms:
                continue

            term_id = tissue_name_to_terms[cell_type_lower]

            # Handle multiple term IDs separated by pipe
            term_ids = [t.strip() for t in term_id.split("|")]

            for tid in term_ids:
                # Skip if term is at system level or higher (not in descendants)
                if tid not in valid_uberon:
                    skipped_system_level += 1
                    continue

                # Process both perturbation and control samples for tissue
                pert_ids = entry.get("pert_ids", [])
                ctrl_ids = entry.get("ctrl_ids", [])

                for gsm_id in pert_ids + ctrl_ids:
                    records.append(
                        {
                            COL_ACCESSION: gsm_id,
                            COL_ATTRIBUTE: "tissue",
                            COL_TERM_ID: tid,
                            COL_TERM_NAME: cell_type.lower(),
                            COL_ECODE: ECODE_CROWD,
                        }
                    )

        if skipped_system_level > 0:
            self.logger.info(
                "Skipped %s tissue annotations with system-level or higher UBERON terms.",
                skipped_system_level,
            )

        self.logger.info("Produced %s tissue annotations from CREEDS", len(records))

        return records

    def _is_valid_entry(self, entry: dict) -> bool:
        """Check if CREEDS entry is valid for processing.

        Arguments:
            entry (dict):
                CREEDS signature entry.

        Returns:
            (bool): True if entry is human and has valid DOID.
        """
        # Must be human organism
        if entry.get("organism") != "human":
            return False

        # Must have a valid DOID
        do_id = entry.get("do_id")
        if not do_id or not isinstance(do_id, str):
            return False

        return True

    def validate(self, data: pl.DataFrame) -> bool:
        """Validate processed CREEDS data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        # Check that disease and tissue annotations are present
        annotation_types = data[COL_ATTRIBUTE].unique().to_list()
        if "disease" not in annotation_types:
            self.logger.warning("No disease annotations found in CREEDS output.")
        if "tissue" not in annotation_types:
            self.logger.warning("No tissue annotations found in CREEDS output.")

        # Verify all records have ecode='crowd'
        if not all(e == ECODE_CROWD for e in data[COL_ECODE].unique().to_list()):
            self.logger.warning("Found non-crowd ecode values in CREEDS data.")

        return True
