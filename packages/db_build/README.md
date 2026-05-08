# metahq-build

Data pipeline package for building the MetaHQ database from raw biomedical annotations.

## Overview

`metahq-build` processes annotations from multiple biomedical data sources and assembles them into the MetaHQ data package — a set of BSON annotation databases, ontology relation matrices, and sample/series metadata Parquets consumed by `metahq-core` and `metahq-cli`.

Key capabilities:

- **Plugin architecture** — each data source is an isolated processor class
- **Pipeline orchestration** with checkpoint-based fault tolerance and resume support
- **Ontology processing** for MONDO (disease) and UBERON/CL (tissue/cell type)
- **ID mapping** from SRA accession IDs (SRR/SRX) to GEO sample IDs (GSM) via OmicIDX
- **Specificity filtering** to retain only the most specific ontology terms per sample

## Installation

```bash
# Install from source (within the monorepo)
cd packages/db_build
pip install -e .

# With development dependencies
pip install -e ".[dev]"
```

## Prerequisites

The pipeline expects a specific directory layout under `data/`. Before running:

| Required file                                | Source                                                                 | Notes                                            |
| -------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------ |
| `data/omicidx.duckdb`                        | OmicIDX project                                                        | Provides SRA→GSM ID mapping and GEO/SRA metadata |
| `data/ontology/mondo/mondo.obo.gz`           | [MONDO releases](https://github.com/monarch-initiative/mondo/releases) | Used to extract disease relations                |
| `data/ontology/uberon_ext/uberon_ext.obo.gz` | [UBERON releases](https://github.com/obophenotype/uberon/releases)     | Used to extract tissue/cell-type relations       |
| `data/unprocessed/`                          | See `data/unprocessed/README.md`                                       | Raw input files for each processor               |

OBO files must be present before running `extract__mondo__relations` and `extract__uberon__relations` stages. The `data/unprocessed/README.md` lists download links for every raw source file.

## Quick Start

### 1. Copy and edit the configuration

```bash
cp packages/db_build/metahq_build.yaml my_config.yaml
# Edit params, processors, and stages to suit your environment
```

### 2. (Optional) Download Gemma annotations

Gemma data is fetched from its REST API and must be downloaded before processing:

```bash
metahq-build download gemma
# Or to a custom path:
metahq-build download gemma --output /data/gemma.json
```

### 3. Run the full pipeline

```bash
metahq-build package --config my_config.yaml
```

The pipeline runs all enabled stages in order, writing checkpoints after each one. If interrupted, re-run the same command and already-completed stages are skipped automatically.

### 4. Resume from a specific stage

```bash
# Skip everything before combine__geo
metahq-build package --config my_config.yaml --start-from combine__geo

# Run only up through combine__sample
metahq-build package --config my_config.yaml --end-at combine__sample
```

## Configuration

The pipeline is driven entirely by a `metahq_build.yaml` file. The command `metahq-build package --config <file>` is the only required argument.

### Full example (`metahq_build.yaml`)

```yaml
# -----------------------------------------------------------------
# Parameters
# -----------------------------------------------------------------
params:
  data_dir: ./data # root for all input data (unprocessed/, helpers/, ontology/, etc.)
  output_dir: ./data/data_packages # where finished packages are written
  package_name: metahq_data__v1.0.2 # directory name for the built package
  omicidx_path: ./data/omicidx.duckdb
  specific: true # filter for most-specific ontology annotations per sample
  overwrite: false # overwrite an existing package with the same name
  temp_dir: /tmp/metahq_build
  checkpoint_dir: .checkpoints
  log_dir: .log

# -----------------------------------------------------------------
# Validation
# -----------------------------------------------------------------
validation:
  strict: true # fail on any validation error
  warn_only: false # set true to log errors without stopping
  check_ontology_coverage: true

# -----------------------------------------------------------------
# Per-source processor settings
# The name must exactly match source_name defined on the processor class.
# Omitted processors default to enabled: true, download: true.
# -----------------------------------------------------------------
processors:
  ALE:
    enabled: true
    download: false
  BGee:
    enabled: true
    download: false
  Cello:
    enabled: true
    download: false
  CREEDS:
    enabled: true
    download: false
  DiSignAtlas:
    enabled: true
    download: false
  Gemma:
    enabled: true
    download: false
  Golightly_2018:
    enabled: true
    download: false
  Gu_2023:
    enabled: true
    download: false
  Johnson_2023:
    enabled: true
    download: false
  KrishnanLab:
    enabled: true
    download: false
  Sirota_2011:
    enabled: true
    download: false
  URSA:
    enabled: true
    download: false
  URSA_HD:
    enabled: true
    download: false

# -----------------------------------------------------------------
# Per-stage settings
# Set skip: true to bypass a stage.
# Set use_checkpoint: false to always re-run a stage even if completed.
# -----------------------------------------------------------------
stages:
  extract__mondo__relations:
    skip: false
    use_checkpoint: true
  extract__uberon__relations:
    skip: false
    use_checkpoint: true
  combine__geo:
    skip: false
    use_checkpoint: true
  combine__sra:
    skip: false
    use_checkpoint: true
  combine__sample:
    skip: false
    use_checkpoint: true

# -----------------------------------------------------------------
# Data package structure (source → destination copy map)
# {output_dir} and {package_name} are resolved from params above.
# -----------------------------------------------------------------
structure:
  - source: data/processed/combined__level-sample.bson
    destination: "{output_dir}/{package_name}/annotations/combined__level-sample.bson"

  - source: data/processed/combined__level-series.bson
    destination: "{output_dir}/{package_name}/annotations/combined__level-series.bson"

  - source: data/ontology/mondo/relations.parquet
    destination: "{output_dir}/{package_name}/ontology/mondo/relations.parquet"

  - source: data/ontology/uberon_ext/relations.parquet
    destination: "{output_dir}/{package_name}/ontology/uberon_ext/relations.parquet"

  - source: data/ontology/ontology_search.duckdb
    destination: "{output_dir}/{package_name}/ontology/ontology_search.duckdb"

  - source: data/metadata/metadata__level-sample.parquet
    destination: "{output_dir}/{package_name}/metadata/metadata__level-sample.parquet"

  - source: data/metadata/metadata__level-series.parquet
    destination: "{output_dir}/{package_name}/metadata/metadata__level-series.parquet"

clean_temp: true
verbose: false
```

### Key parameters

| Parameter      | Description                                                                                                 |
| -------------- | ----------------------------------------------------------------------------------------------------------- |
| `data_dir`     | Root directory containing `unprocessed/`, `processed/`, `ontology/`, `helpers/`, `metadata/` subdirectories |
| `output_dir`   | Where the assembled data package directory is written                                                       |
| `package_name` | Name of the output package directory                                                                        |
| `omicidx_path` | Path to the OmicIDX DuckDB file (required for SRA ID mapping and metadata)                                  |
| `specific`     | When `true`, the combiner keeps only the most-specific ontology term per sample per attribute               |
| `overwrite`    | When `false` (default), the pipeline exits if the output package already exists                             |

### Environment variable overrides

Any `params` key can be overridden at runtime using `METAHQ_SETUP_<KEY>` environment variables. Nested keys use double underscores.

```bash
export METAHQ_SETUP_DATA_DIR=/custom/data
export METAHQ_SETUP_OMICIDX_PATH=/data/omicidx.duckdb
```

## CLI Reference

### `metahq-build package`

Run the complete build pipeline.

```bash
metahq-build package --config metahq_build.yaml
metahq-build package --config metahq_build.yaml --start-from combine__geo
metahq-build package --config metahq_build.yaml --end-at combine__sample
metahq-build package --config metahq_build.yaml --data-dir /data --output-dir /out
```

### `metahq-build process`

Process a single data source and write its Parquet to `data/processed/`.

```bash
metahq-build process ALE
metahq-build process ALE --output-dir /custom/output
metahq-build process ALE --no-validate    # skip output validation
metahq-build list-sources                 # show all registered processors
```

### `metahq-build download`

Download raw data from external APIs.

```bash
metahq-build download gemma
metahq-build download gemma --output /data/gemma.json --max-studies 60000
```

### `metahq-build combine`

Combine processed Parquets into annotation BSON files. Run after all relevant processors.

```bash
metahq-build combine geo     # combine GEO-based sources → geo_combined.bson
metahq-build combine sra     # combine SRA-based sources → sra_combined.bson (requires OmicIDX)
metahq-build combine sample  # merge GEO + SRA BSONs → combined__level-sample.bson
metahq-build combine series  # aggregate samples to study level → combined__level-series.bson
```

Each sub-command accepts `--output` and source-specific path overrides; see `--help` for details.

### `metahq-build ontology`

```bash
metahq-build ontology relations --obo_file data/ontology/mondo/mondo.obo.gz \
    --outfile data/ontology/mondo/relations.parquet

metahq-build ontology search-db   # build the DuckDB ontology term search database
```

### `metahq-build metadata`

Query OmicIDX for sample or series metadata.

```bash
metahq-build metadata list-fields --level sample
metahq-build metadata list-fields --level series

metahq-build metadata sample --fields accession,title,characteristics
metahq-build metadata series --fields accession,title,summary,overall_design
```

### `metahq-build status` / `metahq-build clear-checkpoints`

```bash
metahq-build status                       # show completed pipeline stages
metahq-build clear-checkpoints            # clear all checkpoints (prompts for confirmation)
metahq-build clear-checkpoints --from-stage combine__geo  # clear from a specific stage
```

## Pipeline Stages

Stages run in this order. Each stage name is the key used in the `stages:` config block and with `--start-from` / `--end-at`.

| Stage name                   | Description                                                        |
| ---------------------------- | ------------------------------------------------------------------ |
| `extract__mondo__relations`  | Parse MONDO OBO and write `relations.parquet`                      |
| `extract__uberon__relations` | Parse UBERON OBO and write `relations.parquet`                     |
| `build__ontology_search`     | Build the DuckDB ontology term search database                     |
| `process__<source_name>`     | Run each enabled processor (e.g., `process__ALE`, `process__BGee`) |
| `combine__geo`               | Combine all GEO-sourced Parquets into `geo_combined.bson`          |
| `combine__sra`               | Map SRA IDs to GSM and combine into `sra_combined.bson`            |
| `combine__sample`            | Merge GEO + SRA BSONs into `combined__level-sample.bson`           |
| `combine__series`            | Aggregate samples to series level in `combined__level-series.bson` |
| `build__shield_endpoints`    | Generate shield.io JSON badge endpoints                            |
| `build__metadata`            | Query OmicIDX and write sample/series metadata Parquets            |
| `build__data_package`        | Copy all outputs to the final package directory structure          |

Completed stages are checkpointed. Re-running the pipeline skips them unless `use_checkpoint: false` is set for that stage or checkpoints are cleared.

## Package Structure

```
metahq_build/
├── cli/              # Click command definitions
├── config/           # Pydantic schemas (DataPackageConfig) and path constants
├── processors/       # Data source processors (plugin system)
├── combiners/        # GEO, SRA, sample, and study combiners
├── builders/         # DataPackageBuilder, MetadataBuilder, OntologySearchDbBuilder
├── ontology/         # OBO parsing, DAG construction, relations matrix
├── fetchers/         # GemmaFetcher (REST API download)
├── metadata/         # OmicIDX DuckDB query helpers
└── util/             # Logging, checkpointing, age group mapping
```

## Data Sources

### GEO sources

Annotations keyed by GSM or GSE IDs. Combined by `GeoCombiner`.

| Source name      | Processor class        | Annotations               |
| ---------------- | ---------------------- | ------------------------- |
| `ALE`            | `ALEProcessor`         | tissue, sex, age          |
| `CREEDS`         | `CREEDSProcessor`      | disease                   |
| `DiSignAtlas`    | `DiSignAtlasProcessor` | disease, tissue           |
| `Gemma`          | `GemmaProcessor`       | tissue, disease, sex, age |
| `Golightly_2018` | `GolightlyProcessor`   | sex                       |
| `Johnson_2023`   | `Johnson2023Processor` | disease, tissue, sex, age |
| `KrishnanLab`    | `KrishnanLabProcessor` | tissue, disease           |
| `Sirota_2011`    | `Sirota2011Processor`  | disease, tissue           |
| `URSA`           | `URSAProcessor`        | tissue                    |
| `URSA_HD`        | `URSAHDProcessor`      | disease, tissue, age, sex |

### SRA sources

Annotations keyed by SRR/SRX IDs, mapped to GSM before combining. Combined by `SraCombiner`.

| Source name    | Processor class        | Annotations               |
| -------------- | ---------------------- | ------------------------- |
| `BGee`         | `BgeeProcessor`        | tissue                    |
| `Cello`        | `CellOProcessor`       | tissue                    |
| `Gu_2023`      | `GuProcessor`          | disease, tissue           |
| `Johnson_2023` | `Johnson2023Processor` | disease, tissue, sex, age |

---

## Adding a New Data Source

Follow these steps to add a new processor. Use `processors/ale/processor.py` as a reference implementation.

### 1. Choose GEO or SRA

Determine whether the source uses **GEO accession IDs** (GSM/GSE) or **SRA accession IDs** (SRR/SRX). This controls which combiner picks up the output.

- GEO → output goes into `GeoCombiner` (`combiners/geo.py`, `GEO_SOURCES` dict)
- SRA → output goes into `SraCombiner` (`combiners/sra.py`, `SRA_SOURCES` dict)

### 2. Create the processor module

```
processors/
└── my_source/
    ├── __init__.py
    └── processor.py
```

**`processors/my_source/__init__.py`:**

```python
"""My Source annotation processor."""

from metahq_build.processors.my_source.processor import MySourceProcessor

__all__ = ["MySourceProcessor"]
```

### 3. Implement `BaseProcessor`

**`processors/my_source/processor.py`:**

```python
from pathlib import Path
import polars as pl
from metahq_build.config.config import (
    COL_ACCESSION, COL_ATTRIBUTE, COL_ECODE, COL_TERM_ID, COL_TERM_NAME,
    ECODE_EXPERT, PROCESSED_DIR,
)
from metahq_build.processors.base import BaseProcessor, ValidationError
from metahq_build.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class MySourceProcessor(BaseProcessor):
    source_name = "MySource"   # must be unique; this is the key used everywhere
    version = "1.0.0"
    description = "Short description of the data source"

    def process(self, output_dir: Path = PROCESSED_DIR, **kwargs) -> pl.DataFrame:
        input_path = Path(kwargs.get("input_path", MY_SOURCE_RAW))

        # ... load and transform raw data ...

        result = pl.DataFrame({
            COL_ACCESSION: [...],   # GSM or SRR/SRX IDs
            COL_ATTRIBUTE: [...],   # "tissue" | "disease" | "sex" | "age"
            COL_TERM_ID:   [...],   # e.g. "UBERON:0000948" or "M" / "F" for sex
            COL_TERM_NAME: [...],   # human-readable label
            COL_ECODE:     [...],   # "expert-curated" | "crowd-sourced"
        })

        output_file = output_dir / "my_source_processed.parquet"
        result.write_parquet(output_file)
        return result

    def validate(self, data: pl.DataFrame) -> bool:
        self._validate_required_columns(data)
        # Add any source-specific checks here
        return True
```

#### Output schema

Every processor must return a Polars DataFrame with exactly these five columns:

| Column      | Type  | Values                                                                                                               |
| ----------- | ----- | -------------------------------------------------------------------------------------------------------------------- |
| `accession` | `str` | GEO sample ID (`GSM…`) or SRA run/experiment ID (`SRR…`, `SRX…`)                                                     |
| `attribute` | `str` | `"tissue"` \| `"disease"` \| `"sex"` \| `"age"`                                                                      |
| `term_id`   | `str` | Ontology term ID (e.g., `UBERON:0000948`, `MONDO:0004994`), or `"M"` / `"F"` for sex, or an age group string for age |
| `term_name` | `str` | Human-readable term label                                                                                            |
| `ecode`     | `str` | `"expert-curated"` \| `"crowd-sourced"`                                                                              |

Control/normal samples in disease annotations use `term_id = "MONDO:0000000"` and `term_name = "control"`.

### 4. Register the processor in `processors/__init__.py`

The registry relies on the import to trigger `@ProcessorRegistry.register`. Add two lines to `processors/__init__.py`:

```python
# In the import block:
from metahq_build.processors.my_source import MySourceProcessor

# In __all__:
"MySourceProcessor",
```

### 5. Add path constants to `config/config.py`

Add the raw input path and processed output path:

```python
# Input
MY_SOURCE_RAW: Path = UNPROCESSED_DIR / "my_source.csv"

# Output
MY_SOURCE_PROCESSED: Path = PROCESSED_DIR / "my_source_processed.parquet"
```

### 6. Add the source to the appropriate combiner

**For a GEO source** — edit `combiners/geo.py`:

```python
from metahq_build.config.config import MY_SOURCE_PROCESSED

GEO_SOURCES: dict[str, Path] = {
    ...
    "MySource": MY_SOURCE_PROCESSED,
}
```

**For an SRA source** — edit `combiners/sra.py`:

```python
from metahq_build.config.config import MY_SOURCE_PROCESSED

SRA_SOURCES: dict[str, Path] = {
    ...
    "MySource": MY_SOURCE_PROCESSED,
}
```

The combiner key must match `source_name` exactly — it becomes the source label stored in the BSON annotation database.

### 7. Add the source to `metahq_build.yaml`

```yaml
processors:
  MySource:
    enabled: true
    download: false
```

The key must exactly match the `source_name` class attribute on the processor.

### 8. Test the processor in isolation

```bash
metahq-build process MySource
```

Or from Python:

```python
from metahq_build.processors import ProcessorRegistry

processor = ProcessorRegistry.get("MySource")
df = processor.run()
print(df.head())
```

---

## Development

```bash
pip install -e ".[dev]"
pytest
mypy src/metahq_build
```

## Documentation

Source Code: [https://github.com/krishnanlab/meta-hq/tree/main/packages/db_build](https://github.com/krishnanlab/meta-hq/tree/main/packages/db_build)

## License

BSD 3-Clause License
