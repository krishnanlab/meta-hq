"""
Configuration schemas for metahq-setup pipeline.

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
    use_multiprocessing: bool = Field(
        default=True, description="Use multiprocessing"
    )


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
        default=Path("/tmp/metahq_setup"), description="Temporary directory"
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

    @field_validator("data_dir", "output_dir", mode="before")
    @classmethod
    def expand_paths(cls, v: str | Path) -> Path:
        """
        Expand paths to absolute paths.

        Arguments:
            v (str | Path):
                Path to expand

        Returns:
            (Path): Expanded absolute path
        """
        path = Path(v)
        return path.expanduser().resolve()

    def get_processor_config(self, processor_name: str) -> ProcessorConfig:
        """
        Get configuration for a specific processor.

        Arguments:
            processor_name (str):
                Name of the processor

        Returns:
            (ProcessorConfig): Processor configuration
        """
        return self.processors.get(processor_name, ProcessorConfig())

    def get_stage_config(self, stage_name: str) -> PipelineStageConfig:
        """
        Get configuration for a specific stage.

        Arguments:
            stage_name (str):
                Name of the stage

        Returns:
            (PipelineStageConfig): Stage configuration
        """
        return self.stages.get(stage_name, PipelineStageConfig())

    def get_ontology_config(self, ontology_name: str) -> OntologyConfig | None:
        """
        Get configuration for a specific ontology.

        Arguments:
            ontology_name (str):
                Name of the ontology

        Returns:
            (OntologyConfig | None): Ontology config, or None if not found
        """
        for onto_config in self.ontologies:
            if onto_config.name == ontology_name:
                return onto_config
        return None

    def create_directories(self) -> None:
        """Create all required directories."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
