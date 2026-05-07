"""
Configuration schemas for metahq-build pipeline.

Defines Pydantic models for validating pipeline configuration with
sensible defaults and clear documentation.
"""

from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class ProcessorConfig(BaseModel):
    """
    Configuration for a single data source processor.

    Attributes:
        enabled (bool):
            Whether this processor should run
        download (bool):
            Whether to download raw data
        max_records (int | None):
            Maximum number of records to process (None for all)
        custom_params (dict):
            Processor-specific parameters
    """

    enabled: bool = Field(default=True, description="Enable this processor")
    download: bool = Field(default=True, description="Download raw data")
    max_records: int | None = Field(
        default=None, description="Maximum records to process"
    )
    custom_params: dict = Field(
        default_factory=dict, description="Custom processor parameters"
    )


class OntologyConfig(BaseModel):
    """
    Configuration for ontology processing.

    Attributes:
        name (str):
            Name of the ontology (e.g., "mondo", "uberon")
        download (bool):
            Whether to download the ontology file
        url (str | None):
            Custom URL for ontology download
        extract_relations (bool):
            Whether to extract ancestor/descendant relations
    """

    name: str = Field(description="Ontology name")
    download: bool = Field(default=True, description="Download ontology file")
    url: str | None = Field(default=None, description="Custom download URL")
    extract_relations: bool = Field(
        default=True, description="Extract ontology relations"
    )


class PipelineStageConfig(BaseModel):
    """
    Configuration for a pipeline stage.

    Attributes:
        skip (bool):
            Whether to skip this stage
        use_checkpoint (bool):
            Whether to use checkpoint if available
        timeout (int | None):
            Timeout in seconds (None for no timeout)
    """

    skip: bool = Field(default=False, description="Skip this stage")
    use_checkpoint: bool = Field(
        default=True, description="Use checkpoint if available"
    )
    timeout: int | None = Field(default=None, description="Timeout in seconds")


class ParallelConfig(BaseModel):
    """
    Configuration for parallel processing.

    Attributes:
        num_workers (int):
            Number of parallel workers
        chunk_size (int):
            Number of items per chunk
        use_multiprocessing (bool):
            Use multiprocessing vs threading
    """

    num_workers: int = Field(
        default=4, ge=1, le=128, description="Number of parallel workers"
    )
    chunk_size: int = Field(
        default=1000, ge=1, description="Items per processing chunk"
    )
    use_multiprocessing: bool = Field(default=True, description="Use multiprocessing")


class ValidationConfig(BaseModel):
    """
    Configuration for data validation.

    Attributes:
        strict (bool):
            Strict validation mode (fail on any error)
        warn_only (bool):
            Only warn on validation failures
        check_ontology_coverage (bool):
            Validate ontology term coverage
    """

    strict: bool = Field(default=True, description="Strict validation mode")
    warn_only: bool = Field(default=False, description="Only warn on failures")
    check_ontology_coverage: bool = Field(
        default=True, description="Check ontology coverage"
    )


class PipelineConfig(BaseModel):
    """
    Main pipeline configuration.

    Attributes:
        data_dir (Path):
            Root directory for data files
        output_dir (Path):
            Output directory for built database
        temp_dir (Path):
            Temporary directory for intermediate files
        checkpoint_dir (Path):
            Directory for pipeline checkpoints
        log_dir (Path):
            Directory for log files
        processors (dict[str, ProcessorConfig]):
            Configuration for each data source processor
        ontologies (list[OntologyConfig]):
            Ontologies to process
        stages (dict[str, PipelineStageConfig]):
            Configuration for pipeline stages
        parallel (ParallelConfig):
            Parallel processing configuration
        validation (ValidationConfig):
            Validation configuration
        clean_temp (bool):
            Clean temporary files after completion
        verbose (bool):
            Enable verbose output
    """

    # Directories
    data_dir: Path = Field(description="Root data directory")
    output_dir: Path = Field(description="Output directory for database")
    temp_dir: Path = Field(
        default=Path("/tmp/metahq_build"), description="Temporary directory"
    )
    checkpoint_dir: Path = Field(
        default=Path(".checkpoints"), description="Checkpoint directory"
    )
    log_dir: Path = Field(default=Path(".log"), description="Log directory")

    # Processors
    processors: dict[str, ProcessorConfig] = Field(
        default_factory=dict, description="Data source processor configs"
    )

    # Ontologies
    ontologies: list[OntologyConfig] = Field(
        default_factory=lambda: [
            OntologyConfig(name="mondo"),
            OntologyConfig(name="uberon"),
            OntologyConfig(name="cl"),
            OntologyConfig(name="bto"),
        ],
        description="Ontologies to process",
    )

    # Pipeline stages
    stages: dict[str, PipelineStageConfig] = Field(
        default_factory=dict, description="Pipeline stage configurations"
    )

    # Parallel processing
    parallel: ParallelConfig = Field(
        default_factory=ParallelConfig, description="Parallel processing config"
    )

    # Validation
    validation: ValidationConfig = Field(
        default_factory=ValidationConfig, description="Validation config"
    )

    # General options
    clean_temp: bool = Field(
        default=True, description="Clean temporary files after completion"
    )
    verbose: bool = Field(default=False, description="Enable verbose output")


class FileEntry(BaseModel):
    """A source → destination file mapping in the data package structure."""

    source: Path
    destination: Path


class DataPackageConfig(BaseModel):
    """Full configuration for the MetaHQ setup pipeline, driven by metahq_build.yaml.

    Attributes:
        data_dir (Path):
            Root directory for all input data.
        output_dir (Path):
            Directory where data packages are written.
        package_name (str):
            Name of the data package.
        overwrite (bool):
            Overwrite an existing package with the same name.
        omicidx_path (Path):
            Path to the OmicIDX DuckDB database.
        temp_dir (Path):
            Temporary directory for intermediate files.
        checkpoint_dir (Path):
            Directory for pipeline checkpoint state.
        log_dir (Path):
            Directory for log files.
        validation (ValidationConfig):
            Validation settings.
        processors (dict[str, ProcessorConfig]):
            Per-source processor settings.
        stages (dict[str, PipelineStageConfig]):
            Per-stage settings.
        structure (list[FileEntry]):
            Data package file mapping.
        clean_temp (bool):
            Remove temp files after completion.
        verbose (bool):
            Enable verbose output.
    """

    data_dir: Path = Field(description="Root data directory")
    output_dir: Path = Field(description="Data package output directory")
    package_name: str = Field(description="Data package name")
    overwrite: bool = Field(default=False, description="Overwrite existing package")
    omicidx_path: Path = Field(description="Path to OmicIDX DuckDB database")
    temp_dir: Path = Field(
        default=Path("/tmp/metahq_build"), description="Temporary directory"
    )
    checkpoint_dir: Path = Field(
        default=Path(".checkpoints"), description="Checkpoint directory"
    )
    log_dir: Path = Field(default=Path(".log"), description="Log directory")
    specific: bool = Field(
        default=False, description="Filter for most specific annotations."
    )

    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    processors: dict[str, ProcessorConfig] = Field(default_factory=dict)
    stages: dict[str, PipelineStageConfig] = Field(default_factory=dict)
    structure: list[FileEntry] = Field(default_factory=list)

    clean_temp: bool = Field(
        default=True, description="Clean temp files after completion"
    )
    verbose: bool = Field(default=False, description="Enable verbose output")

    @field_validator(
        "data_dir",
        "output_dir",
        "omicidx_path",
        "temp_dir",
        "checkpoint_dir",
        "log_dir",
        mode="before",
    )
    @classmethod
    def expand_paths(cls, v: str | Path) -> Path:
        return Path(v).expanduser().resolve()

    @property
    def data_package_path(self) -> Path:
        """Return the full path to the data package directory."""
        return self.output_dir / self.package_name

    @classmethod
    def from_yaml(cls, file: Path) -> "DataPackageConfig":
        """Load and validate config from metahq_build.yaml.

        Flattens ``params`` keys into the top level and resolves
        ``{output_dir}/{package_name}`` placeholders in structure destinations.
        """
        import yaml

        with open(file, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        params = raw.pop("params", {})
        config_dict = {**params, **raw}

        for entry in config_dict.get("structure", []):
            entry["destination"] = entry["destination"].format(**params)

        return cls(**config_dict)

    def get_processor_config(self, name: str) -> ProcessorConfig:
        """Return config for a named processor, falling back to defaults."""
        return self.processors.get(name, ProcessorConfig())

    def get_stage_config(self, name: str) -> PipelineStageConfig:
        """Return config for a named pipeline stage, falling back to defaults."""
        return self.stages.get(name, PipelineStageConfig())

    def verify_source_files(self) -> None:
        """Ensure every source file listed in the structure exists.

        Logs all missing files before exiting so the user can fix them all at once.
        """
        import sys

        from metahq_build.util.logging import setup_logger

        logger = setup_logger("metahq_build.config.DataPackageConfig")
        error_raised = False
        for entry in self.structure:
            if not entry.source.exists():
                error_raised = True
                logger.error("Source file does not exist: %s", entry.source)
        if error_raised:
            logger.error("Missing source files. Exiting...")
            sys.exit(1)

    def create_directories(self) -> None:
        """Create all required pipeline directories."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
