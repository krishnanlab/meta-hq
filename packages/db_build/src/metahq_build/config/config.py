"""
Project-wide path and structural constants for metahq-build.

This module is the single source of truth for repo-root-anchored paths and
other constants that are referenced across multiple processors or modules.
Derive all constants from ``REPO_ROOT`` so that the package remains correct
regardless of the working directory when it is invoked.
"""

from pathlib import Path

# ===============================================
# ====== Annotation schema
# ===============================================

# MetaHQ BSON database key names
SEX_KEY: str = "sex"
AGE_KEY: str = "age"
TISSUE_KEY: str = "tissue"
DISEASE_KEY: str = "disease"
ORGANISM_KEY: str = "organism"
ACCESSIONS_KEY: str = "accession_ids"
STUDY_ACCESSION_KEY: str = "series"
SAMPLE_ACCESSION_KEY: str = "sample"
PLATFORM_ACCESSION_KEY: str = "platform"
SRX_ACCESSION_KEY: str = "srx"
SRS_ACCESSION_KEY: str = "srs"
SRP_ACCESSION_KEY: str = "srp"

# all accession IDs stored in the ACCESSIONS_KEY in a MetaHQ database
ACCESSION_ID_KEYS: frozenset = frozenset(
    {
        STUDY_ACCESSION_KEY,
        SAMPLE_ACCESSION_KEY,
        PLATFORM_ACCESSION_KEY,
        SRX_ACCESSION_KEY,
        SRS_ACCESSION_KEY,
        SRP_ACCESSION_KEY,
    }
)

# MetaHQ BSON database key sets
ATTRIBUTE_KEYS: frozenset[str] = frozenset({TISSUE_KEY, DISEASE_KEY, SEX_KEY, AGE_KEY})
ONTOLOGY_BASED_KEYS: frozenset[str] = frozenset({TISSUE_KEY, DISEASE_KEY})
NON_ONTOLOGY_BASED_KEYS: frozenset[str] = frozenset(
    {SEX_KEY, AGE_KEY, ACCESSIONS_KEY, ORGANISM_KEY}
)

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

# Standardized annotation values
## Ontologies
VALID_ONTOLOGIES: frozenset[str] = frozenset({"UBERON", "CL", "MONDO"})

## Organisms
VALID_ORGANISMS: frozenset[str] = frozenset(
    {
        "homo sapiens",
        "mus musculus",
        "caenorhabditis elegans",
        "rattus norvegicus",
        "danio rerio",
        "drosophila melanogaster",
    }
)

## Ecodes
ECODE_EXPERT: str = "expert-curated"
ECODE_CROWD: str = "crowd-sourced"
VALID_ECODES: frozenset[str] = frozenset({ECODE_EXPERT, ECODE_CROWD})

## Sexes
SEX_MALE_ID: str = "M"
SEX_FEMALE_ID: str = "F"
VALID_SEXES: frozenset[str] = frozenset({SEX_MALE_ID, SEX_FEMALE_ID})

## Age groups
AGE_GROUPS = [
    {"name": "fetus", "min_age": -1, "max_age": 0},
    {"name": "infant", "min_age": 0, "max_age": 2},
    {"name": "child", "min_age": 2, "max_age": 10},
    {"name": "adolescent", "min_age": 10, "max_age": 20},
    {"name": "adult", "min_age": 20, "max_age": 50},
    {"name": "older_adult", "min_age": 50, "max_age": 80},
    {"name": "elderly_adult", "min_age": 80, "max_age": 150},
]
VALID_AGE_GROUPS: frozenset[str] = frozenset({v["name"] for v in AGE_GROUPS})

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
#   config.py → config/ → metahq_build/ → src/ → setup/ → packages/ → meta-hq/
REPO_ROOT: Path = Path(__file__).parents[5]

# Top-level data directories.
DATA_DIR: Path = REPO_ROOT / "data"
UNPROCESSED_DIR: Path = DATA_DIR / "unprocessed"
PROCESSED_DIR: Path = DATA_DIR / "processed"
ONTOLOGY_DIR: Path = DATA_DIR / "ontology"
HELPERS_DIR: Path = DATA_DIR / "helpers"
METADATA_DIR: Path = DATA_DIR / "metadata"

# Ontology
## Ontology OBO files.
MONDO_OBO: Path = ONTOLOGY_DIR / "mondo" / "mondo.obo.gz"
UBERON_OBO: Path = ONTOLOGY_DIR / "uberon_ext" / "uberon_ext.obo.gz"
BTO_OBO: Path = ONTOLOGY_DIR / "bto" / "bto.obo.gz"
CL_OBO: Path = ONTOLOGY_DIR / "cl" / "cl.obo.gz"

## Default relations paths
MONDO_RELATIONS: Path = ONTOLOGY_DIR / "mondo" / "relations.parquet"
UBERON_RELATIONS: Path = ONTOLOGY_DIR / "uberon_ext" / "relations.parquet"

## Default names and synonyms paths
MONDO_NAMES_SYNONYMS: Path = ONTOLOGY_DIR / "mondo" / "names_synonyms.json"
UBERON_CL_NAMES_SYNONYMS: Path = ONTOLOGY_DIR / "uberon_ext" / "names_synonyms.json"
ONTOLOGY_SEARCH_DB: Path = ONTOLOGY_DIR / "ontology_search.duckdb"

## System-level term lists (used to filter annotations to biologically meaningful terms).
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

# Miscellaneous samples to remove. See helpers README for explanations
MISC_SAMPLES_TO_REMOVE: Path = HELPERS_DIR / "misc_samples_to_remove.txt"

# Study-forward annotations
PROCESSED_STUDY_ANNOTATIONS: dict[str, Path] = {"Gemma": GEMMA_PROCESSED}

# Database file names
SAMPLE_COMBINED_BSON: Path = PROCESSED_DIR / "combined__level-sample.bson"
SERIES_COMBINED_BSON: Path = PROCESSED_DIR / "combined__level-series.bson"
GEO_COMBINED_BSON: Path = PROCESSED_DIR / "geo_combined.bson"
SRA_COMBINED_BSON: Path = PROCESSED_DIR / "sra_combined.bson"

# Transcriptomics technology map (GPL -> microarray|rnaseq)
TECHNOLOGY_MAP: Path = METADATA_DIR / "technologies.parquet"
COL_TECHNOLOGY_MAP_GPL: str = "id"

# OmicIDX properties
OMICIDX_DB: Path = DATA_DIR / "omicidx.duckdb"
OMICIDX_SAMPLE_TABLE: str = "src_geo_samples"
OMICIDX_SERIES_TABLE: str = "src_geo_series"
OMICIDX_COL_ACCESSION: str = "accession"
OMICIDX_COL_CHANNELS: str = "channels"

# MetaHQ database required metadata fields
SAMPLE_METADATA_FIELDS: list[str] = [
    "accession",
    "title",
    "description",
    "source_name",
    "characteristics",
]
SERIES_METADATA_FIELDS: list[str] = [
    "accession",
    "title",
    "summary",
    "overall_design",
    "sample_id",
]

# Metadata locations
SAMPLE_METADATA = METADATA_DIR / "metadata__level-sample.parquet"
SERIES_METADATA = METADATA_DIR / "metadata__level-series.parquet"

# Source count shield endpoint directory
SOURCE_COUNT_SHIELD_OUTDIR: Path = DATA_DIR / "shields"
