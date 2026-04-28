"""
Configuration management for metahq-setup.

Provides Pydantic schemas for configuration validation and utilities
for loading configuration from files, environment variables, and overrides.
"""

from metahq_setup.config.config import (
    ACCESSIONS_KEY,
    ATTRIBUTE_KEYS,
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    DELIMITER,
    ECODE_KEY,
    GEO_COMBINED_BSON,
    ID_KEY,
    OMICIDX_DB,
    OMICIDX_SAMPLE_TABLE,
    OMICIDX_SERIES_TABLE,
    ORGANISM_KEY,
    PROCESSED_DIR,
    SAMPLE_ACCESSION_KEY,
    SAMPLE_COMBINED_BSON,
    SAMPLE_ID_PREFIX,
    SERIES_COMBINED_BSON,
    SRA_COMBINED_BSON,
    STUDY_ACCESSION_KEY,
    STUDY_ID_PREFIX,
    VALUE_KEY,
)
from metahq_setup.config.loader import (
    get_default_config,
    load_config,
    load_env_overrides,
    load_yaml,
    merge_configs,
    save_config,
)
from metahq_setup.config.schema import (
    OntologyConfig,
    ParallelConfig,
    PipelineConfig,
    PipelineStageConfig,
    ProcessorConfig,
    ValidationConfig,
)

__all__ = [
    # Schema classes
    "PipelineConfig",
    "ProcessorConfig",
    "OntologyConfig",
    "PipelineStageConfig",
    "ParallelConfig",
    "ValidationConfig",
    # Loader functions
    "load_config",
    "save_config",
    "get_default_config",
    "load_yaml",
    "merge_configs",
    "load_env_overrides",
    # Constants
    "ACCESSIONS_KEY",
    "ATTRIBUTE_KEYS",
    "COL_ACCESSION",
    "COL_ATTRIBUTE",
    "COL_ECODE",
    "COL_TERM_ID",
    "COL_TERM_NAME",
    "DELIMITER",
    "ECODE_KEY",
    "GEO_COMBINED_BSON",
    "ID_KEY",
    "PROCESSED_DIR",
    "OMICIDX_DB",
    "OMICIDX_SAMPLE_TABLE",
    "OMICIDX_SERIES_TABLE",
    "ORGANISM_KEY",
    "SAMPLE_ACCESSION_KEY",
    "SAMPLE_COMBINED_BSON",
    "SAMPLE_ID_PREFIX",
    "SERIES_COMBINED_BSON",
    "SRA_COMBINED_BSON",
    "STUDY_ACCESSION_KEY",
    "STUDY_ID_PREFIX",
    "VALUE_KEY",
]
