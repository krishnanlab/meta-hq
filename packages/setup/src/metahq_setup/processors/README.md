# Processors

Each processor ingests a raw data source, standardizes it into a common annotation schema, and writes a `.parquet` file to the output directory.

Raw input files live in `data/unprocessed/`. See `data/unprocessed/README.md` for download links.

## Output schema

All processors produce a DataFrame with these columns:

| Column | Type | Description |
|---|---|---|
| `sample_id` | `str` | GEO/SRA sample ID (e.g., GSM, SRR) |
| `annotation_type` | `str` | One of: `tissue`, `disease`, `cell_type`, `sex`, `age` |
| `term_id` | `str` | Ontology term ID (e.g., `UBERON:0000948`), or `"na"` if unmapped |
| `term_label` | `str` | Human-readable term label |
| `ecode` | `str` | Evidence code: `expert`, `semi`, `crowd`, or `automated` |

---

## Running a processor

### CLI

```bash
# Process a single source (output defaults to data/processed/)
metahq-setup process <source_name>

# Override output directory
metahq-setup process <source_name> --output-dir /custom/output

# Skip output validation
metahq-setup process <source_name> --no-validate

# List all registered processors
metahq-setup list-sources
```

### Python

```python
from pathlib import Path
from metahq_setup.processors.registry import ProcessorRegistry

processor = ProcessorRegistry.get("source_name")

# Process and validate (output_dir defaults to data/processed/)
df = processor.run()

# Override output directory
df = processor.run(output_dir=Path("/custom/output"))

# Override input file path
df = processor.run(input_path=Path("./data/unprocessed/source_file.ext"))
```

---

## Processors

### DiSignAtlas (`disign_atlas`)

Disease signatures and tissue annotations from DiSignAtlas. Each dataset is split into control and case sample groups.

- **Input:** `data/unprocessed/disign_atlas.gmt`
- **Annotations:** `disease`, `tissue`
- **ecode:** `expert`
- **Output:** `disign_atlas_processed.parquet`
- **Note:** Control samples receive `term_label = "Control"` and `term_id = "MONDO:0000000"`. Disease IDs are mapped from UMLS to MONDO.

```bash
metahq-setup process disign_atlas
```

```python
processor = ProcessorRegistry.get("disign_atlas")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/disign_atlas.gmt"))
```

---

### CellO (`cello`)

Automated cell type annotations for bulk RNA-seq samples using the Cell Ontology (CL).

- **Input:** `data/unprocessed/cello.json`
- **Annotations:** `tissue`
- **ecode:** `expert`
- **Output:** `cello_processed.parquet`
- **Note:** Each sample maps to multiple CL terms. Terms not found in the CL OBO are dropped.

```bash
metahq-setup process cello
```

```python
processor = ProcessorRegistry.get("cello")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/cello.json"))
```

---

### URSA (`ursa`)

Expert-curated tissue annotations from URSA, providing UBERON/CL term IDs for GEO samples.

- **Input:** `data/unprocessed/ursa.csv`
- **Annotations:** `tissue`
- **ecode:** `expert`
- **Output:** `ursa_processed.parquet`

```bash
metahq-setup process ursa
```

```python
processor = ProcessorRegistry.get("ursa")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/ursa.csv"))
```

---

## Adding a new processor

1. Create `processors/<source_name>/` with `__init__.py` and `processor.py`.
2. Inherit from `BaseProcessor` and implement `process` and `validate`.
3. Decorate the class with `@ProcessorRegistry.register`.
4. Export the class from `__init__.py`.

See `processors/base.py` for the full interface and `processors/ale/processor.py` as a reference implementation.
