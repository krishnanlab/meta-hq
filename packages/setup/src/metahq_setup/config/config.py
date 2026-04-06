"""
Project-wide path and structural constants for metahq-setup.

This module is the single source of truth for repo-root-anchored paths and
other constants that are referenced across multiple processors or modules.
Derive all constants from ``REPO_ROOT`` so that the package remains correct
regardless of the working directory when it is invoked.
"""

from pathlib import Path

# Root of the meta-hq monorepo.
# Resolved from this file's location:
#   config.py → config/ → metahq_setup/ → src/ → setup/ → packages/ → meta-hq/
REPO_ROOT: Path = Path(__file__).parents[5]

# Top-level data directories.
DATA_DIR: Path = REPO_ROOT / "data"
UNPROCESSED_DIR: Path = DATA_DIR / "unprocessed"
PROCESSED_DIR: Path = DATA_DIR / "processed"
ONTOLOGY_DIR: Path = DATA_DIR / "ontology"
HELPERS_DIR: Path = DATA_DIR / "helpers"

# Ontology OBO files.
MONDO_OBO: Path = ONTOLOGY_DIR / "mondo" / "mondo.obo.gz"
UBERON_OBO: Path = ONTOLOGY_DIR / "uberon_ext" / "uberon_ext.obo.gz"
BTO_OBO: Path = ONTOLOGY_DIR / "bto" / "bto.obo.gz"
CL_OBO: Path = ONTOLOGY_DIR / "cl" / "cl.obo.gz"

# System-level term lists (used to filter annotations to biologically meaningful terms).
MONDO_SYSTEMS: Path = ONTOLOGY_DIR / "mondo" / "systems.txt"
UBERON_SYSTEMS: Path = ONTOLOGY_DIR / "uberon_ext" / "systems.txt"

# Known input file paths for static / manually-obtained datasets.
DISIGN_ATLAS_GMT: Path = UNPROCESSED_DIR / "disign_atlas.gmt"
CELLO_JSON: Path = UNPROCESSED_DIR / "cello.json"
CREEDS_JSON: Path = UNPROCESSED_DIR / "creeds.json"
KRISHNANLAB_TSV: Path = UNPROCESSED_DIR / "krishnanlab.tsv"
SIROTA_2011_CSV: Path = UNPROCESSED_DIR / "sirota_2011.csv"
SIROTA_UMLS_UBERON: Path = HELPERS_DIR / "sirota_2011_umls_uberon_manual_mappings.csv"
SIROTA_UMLS_MONDO: Path = HELPERS_DIR / "sirota_2011_umls_mondo_manual_mappings.csv"
URSA_CSV: Path = UNPROCESSED_DIR / "ursa.csv"
URSAHD_CSV: Path = UNPROCESSED_DIR / "ursahd.csv"
URSAHD_GSE_UBERON: Path = HELPERS_DIR / "ursahd_gse_uberon_manual_annotations.csv"
URSAHD_RAW_TISSUE: Path = HELPERS_DIR / "ursahd_name_to_uberon_manual_annotations.csv"
JOHNSON_MICROARRAY_TSV: Path = UNPROCESSED_DIR / "johnson_2023__microarray.tsv"
JOHNSON_RNASEQ_TSV: Path = UNPROCESSED_DIR / "johnson_2023__rnaseq.tsv"
JOHNSON_MICROARRAY_MESH_MONDO: Path = HELPERS_DIR / "johnson_microarray_mesh_mondo_map.tsv"
JOHNSON_MICROARRAY_MESH_UBERON: Path = HELPERS_DIR / "johnson_microarray_mesh_uberon_map.csv"
JOHNSON_RNASEQ_DOID_MONDO: Path = HELPERS_DIR / "johnson_rnaseq_doid_mondo_map.csv"
JOHNSON_RNASEQ_UBERON: Path = HELPERS_DIR / "johnson_rnaseq_uberon_map.csv"
