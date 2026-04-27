"""
Project-wide path and structural constants for metahq-setup.

This module is the single source of truth for repo-root-anchored paths and
other constants that are referenced across multiple processors or modules.
Derive all constants from ``REPO_ROOT`` so that the package remains correct
regardless of the working directory when it is invoked.
"""

from pathlib import Path

# ===============================================
# ====== Annotation schema
# ===============================================

# Annotation type keys that hold source-keyed dicts (not scalars).
ATTRIBUTE_KEYS: frozenset[str] = frozenset({"tissue", "disease", "sex", "age"})
ORGANISM_KEY: str = "organism"
ACCESSIONS_KEY: str = "accession_ids"
STUDY_ACCESSION_KEY: str = "series"
SAMPLE_ACCESSION_KEY: str = "sample"
PLATFORM_ACCESSION_KEY: str = "platform"

# Attribute
ID_KEY: str = "id"
VALUE_KEY: str = "value"
ECODE_KEY: str = "ecode"
DELIMITER: str = "|"
ATTRIBUTE_ANNOTATION_KEYS: list[str] = [ID_KEY, VALUE_KEY, ECODE_KEY]

ALL_METAHQ_KEYS: list[str] = list(ATTRIBUTE_KEYS) + [ORGANISM_KEY, ACCESSIONS_KEY]

# Control IDs
CONTROL_ID: str = "MONDO:0000000"
CONTROL_VALUE: str = "control"

# ===============================================
# ====== Processor output schema column names
# ===============================================

COL_ACCESSION: str = "accession"
COL_ATTRIBUTE: str = "attribute"
COL_TERM_ID: str = "term_id"
COL_TERM_NAME: str = "term_name"
COL_ECODE: str = "ecode"

# ===============================================
# ====== ACCESSION PROPERTIES
# ===============================================

SAMPLE_ID_PREFIX: str = "GSM"
STUDY_ID_PREFIX: str = "GSE"


# ===============================================
# ====== Hard-coded Paths
# ===============================================

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
ALE_TSV: Path = UNPROCESSED_DIR / "ale.tsv"
ALE_BTO_UBERON: Path = HELPERS_DIR / "ale_bto_to_uberon_map.csv"
DISIGN_ATLAS_GMT: Path = UNPROCESSED_DIR / "disign_atlas.gmt"
DISIGN_ATLAS_TISSUE_MAP: Path = HELPERS_DIR / "disign_atlas_tissue_name_uberon_map.csv"
DISIGN_ATLAS_CORRECTIONS: Path = HELPERS_DIR / "disign_atlas_corrected_annotations.csv"
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
JOHNSON_MICROARRAY_MESH_MONDO: Path = (
    HELPERS_DIR / "johnson_microarray_mesh_mondo_map.tsv"
)
JOHNSON_MICROARRAY_MESH_UBERON: Path = (
    HELPERS_DIR / "johnson_microarray_mesh_uberon_map.csv"
)
JOHNSON_RNASEQ_DOID_MONDO: Path = HELPERS_DIR / "johnson_rnaseq_doid_mondo_map.csv"
JOHNSON_RNASEQ_UBERON: Path = HELPERS_DIR / "johnson_rnaseq_uberon_map.csv"
GU_2023_CSV: Path = UNPROCESSED_DIR / "gu_2023.csv"
GOLIGHTLY_ZIP: Path = UNPROCESSED_DIR / "golightly_2018.zip"
GU_DISEASE_MONDO: Path = HELPERS_DIR / "gu_disease_name_to_mondo.csv"
GU_TISSUE_UBERON: Path = HELPERS_DIR / "gu_tissue_name_to_uberon.csv"

# OmicIDX DuckDB database.
OMICIDX_DB: Path = DATA_DIR / "omicidx.duckdb"

# Processed output parquets (one per processor).
ALE_PROCESSED: Path = PROCESSED_DIR / "ale_processed.parquet"
BGEE_PROCESSED: Path = PROCESSED_DIR / "bgee_processed.parquet"
CELLO_PROCESSED: Path = PROCESSED_DIR / "cello_processed.parquet"
CREEDS_PROCESSED: Path = PROCESSED_DIR / "creeds_processed.parquet"
DISIGN_ATLAS_PROCESSED: Path = PROCESSED_DIR / "disign_atlas_processed.parquet"
GEMMA_PROCESSED: Path = PROCESSED_DIR / "gemma_processed.parquet"
GOLIGHTLY_PROCESSED: Path = PROCESSED_DIR / "golightly_processed.parquet"
GU_PROCESSED: Path = PROCESSED_DIR / "gu_processed.parquet"
JOHNSON_2023_MICROARRAY_PROCESSED: Path = (
    PROCESSED_DIR / "johnson_2023__microarray.parquet"
)
JOHNSON_2023_RNASEQ_PROCESSED: Path = PROCESSED_DIR / "johnson_2023__rnaseq.parquet"
KRISHNANLAB_PROCESSED: Path = PROCESSED_DIR / "krishnanlab_processed.parquet"
SIROTA_2011_PROCESSED: Path = PROCESSED_DIR / "sirota_2011_processed.parquet"
URSA_PROCESSED: Path = PROCESSED_DIR / "ursa_processed.parquet"
URSAHD_PROCESSED: Path = PROCESSED_DIR / "ursahd_processed.parquet"

# Gemma raw download output
GEMMA_RAW: Path = UNPROCESSED_DIR / "gemma.json"
GEMMA_DEV_STAGE_TO_AGE_GROUP: Path = (
    HELPERS_DIR / "gemma_developmental_stage_id_to_age_group_map.csv"
)

# Bgee RNA-Seq library files for multiple species
BGEE_DIR: Path = UNPROCESSED_DIR / "bgee"
BGEE_MOUSE: Path = (
    BGEE_DIR / "Mus_musculus_Bgee_15_0" / "Mus_musculus_RNA-Seq_libraries.tsv"
)
BGEE_HUMAN: Path = (
    BGEE_DIR / "Homo_sapiens_Bgee_15_0" / "Homo_sapiens_RNA-Seq_libraries.tsv"
)
BGEE_RAT: Path = (
    BGEE_DIR / "Rattus_norvegicus_Bgee_15_0" / "Rattus_norvegicus_RNA-Seq_libraries.tsv"
)
BGEE_WORM: Path = (
    BGEE_DIR
    / "Caenorhabditis_elegans_Bgee_15_0"
    / "Caenorhabditis_elegans_RNA-Seq_libraries.tsv"
)
BGEE_FISH: Path = (
    BGEE_DIR / "Danio_rerio_Bgee_15_0" / "Danio_rerio_RNA-Seq_libraries.tsv"
)
BGEE_FLY: Path = (
    BGEE_DIR
    / "Drosophila_melanogaster_Bgee_15_0"
    / "Drosophila_melanogaster_RNA-Seq_libraries.tsv"
)

# Samples deleted from GEO to remove from MetaHQ
DELTED_SAMPLES: Path = HELPERS_DIR / "deleted_samples.txt"
DELTED_STUDIES: Path = HELPERS_DIR / "deleted_studies.txt"


# Study-forward annotations
PROCESSED_STUDY_ANNOTATIONS: dict[str, Path] = {"Gemma": GEMMA_PROCESSED}
