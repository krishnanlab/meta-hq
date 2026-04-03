"""
Configuration management for metahq-setup.

Provides Pydantic schemas for configuration validation and utilities
for loading configuration from files, environment variables, and overrides.
"""

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
]
