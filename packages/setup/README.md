# metahq-setup

Data pipeline package for building the MetaHQ database from raw biomedical annotations.

## Overview

`metahq-setup` is a production-ready data pipeline package that processes annotations from multiple biomedical data sources and builds the MetaHQ database. It provides:

- **Plugin architecture** for data source processors
- **Pipeline orchestration** with checkpointing and fault tolerance
- **Ontology processing** for MONDO, UBERON, CL, and BTO
- **ID mapping** between GEO and SRA databases
- **Annotation propagation** through ontology hierarchies
- **Database building** in BSON and Parquet formats

## Installation

```bash
# Install from source (within the monorepo)
cd packages/setup
pip install -e .

# With development dependencies
pip install -e ".[dev]"
```

## Quick Start

### Build the Database

```bash
# Initialize a configuration file
metahq-setup init-config my_pipeline.yaml

# Edit the configuration to customize settings
# Then build the database
metahq-setup build --config my_pipeline.yaml
```

### Process Individual Sources

```bash
# List available data sources
metahq-setup list-sources

# Process a specific source
metahq-setup process gemma --output-dir ./data/gemma
```

### Pipeline Management

```bash
# Check pipeline status
metahq-setup status

# Resume from a checkpoint
metahq-setup build --start-from propagate

# Clear checkpoints to restart
metahq-setup clear-checkpoints
```

## Configuration

Configuration can be provided via:

1. YAML configuration file
2. Environment variables (`METAHQ_SETUP_*`)
3. Command-line arguments

Example configuration:

```yaml
data_dir: ./data
output_dir: ./output

processors:
  gemma:
    enabled: true
    download: true
  ale:
    enabled: true

ontologies:
  - name: mondo
    download: true
  - name: uberon
    download: true

parallel:
  num_workers: 8

validation:
  strict: true
```

## Architecture

### Package Structure

```
metahq_setup/
├── cli/              # Command-line interface
├── config/           # Configuration management
├── processors/       # Data source processors (plugin system)
├── pipeline/         # Pipeline orchestration
├── combiners/        # Annotation combination
├── ontology/         # Ontology processing
├── propagation/      # Annotation propagation
├── mapping/          # ID mapping (GEO ↔ SRA)
├── builders/         # Database builders
├── fetchers/         # External data fetchers
└── util/             # Logging, progress, checkpointing
```

### Adding a New Data Source

Create a processor by inheriting from `BaseProcessor`:

```python
from metahq_setup.processors import BaseProcessor, ProcessorRegistry
import polars as pl
from pathlib import Path

@ProcessorRegistry.register
class MyProcessor(BaseProcessor):
    source_name = "my_source"
    version = "1.0.0"
    description = "My custom data source"

    def download(self, output_dir: Path, **kwargs) -> Path:
        # Download logic
        pass

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        # Return DataFrame with required columns:
        # sample_id, annotation_type, term_id, term_label, source
        pass

    def validate(self, data: pl.DataFrame) -> bool:
        self._validate_required_columns(data)
        return True
```

Then run:

```bash
metahq-setup process my_source --output-dir ./data/my_source
```

## Pipeline Stages

The database build pipeline consists of these stages:

1. **fetch_metadata** - Fetch GEO/SRA metadata
2. **download_ontologies** - Download ontology files
3. **extract_relations** - Extract ontology relationships
4. **process_sources** - Run all data source processors
5. **combine_geo** - Combine GEO annotations
6. **combine_sra** - Combine SRA annotations
7. **map_ids** - Map GEO ↔ SRA identifiers
8. **merge_annotations** - Merge GEO and SRA annotations
9. **propagate** - Propagate annotations through ontologies
10. **build_metadata** - Build metadata files
11. **build_database** - Build final BSON/Parquet files
12. **validate** - Validate final database

## Data Sources

Supported data source processors:

- **gemma** - Gemma database annotations
- **ale** - ALE (Giles et al.) annotations
- **cello** - CellO cell type annotations
- **creeds** - CREEDS perturbation annotations
- **golightly** - Golightly sex annotations
- **gu** - Gu et al. annotations
- **sampleclass_zoo** - Sample classification zoo
- **ursa** - URSA annotations
- **disign_atlas** - DiSign Atlas annotations

## Development

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run type checking
mypy src/metahq_setup

# Format code
black src/metahq_setup
isort src/metahq_setup
```

## Documentation

Documentation: [https://meta-hq.readthedocs.io/en/latest](https://meta-hq.readthedocs.io/en/latest/)

Source Code: [https://github.com/krishnanlab/meta-hq/tree/main/packages/setup](https://github.com/krishnanlab/meta-hq/tree/main/packages/setup)

## License

BSD 3-Clause License
