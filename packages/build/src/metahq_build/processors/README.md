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
metahq-build process <source_name>

# Override output directory
metahq-build process <source_name> --output-dir /custom/output

# Skip output validation
metahq-build process <source_name> --no-validate

# List all registered processors
metahq-build list-sources
```

### Python

```python
from pathlib import Path
from metahq_build.processors.registry import ProcessorRegistry

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
metahq-build process disign_atlas
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
metahq-build process cello
```

```python
processor = ProcessorRegistry.get("cello")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/cello.json"))
```

---

### CREEDS (`creeds`)

Crowd-sourced disease annotations from CREEDS (CRowd Extracted Expression of Differential Signatures).

- **Input:** `data/unprocessed/creeds.json`
- **Annotations:** `disease`
- **ecode:** `crowd`
- **Output:** `creeds_processed.parquet`
- **Note:** Filters for human organism entries with valid DOID terms. Disease IDs are mapped from DOID to MONDO. Control samples receive `term_id = "MONDO:0000000"` and `term_label = "control"`.

```bash
metahq-build process creeds
```

```python
processor = ProcessorRegistry.get("creeds")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/creeds.json"))
```

---

### URSA (`ursa`)

Expert-curated tissue annotations from URSA, providing UBERON/CL term IDs for GEO samples.

- **Input:** `data/unprocessed/ursa.csv`
- **Annotations:** `tissue`
- **ecode:** `expert`
- **Output:** `ursa_processed.parquet`

```bash
metahq-build process ursa
```

```python
processor = ProcessorRegistry.get("ursa")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/ursa.csv"))
```

### URSA-HD (`ursahd`)

Expert-curated disease, tissue, age, and sex annotations from URSA-HD for GEO samples.

- **Input:** `data/unprocessed/ursahd.csv`
- **Annotations:** `disease`, `tissue`, `age`, `sex`
- **ecode:** `expert`
- **Output:** `ursahd_processed.parquet`
- **Notes:**
  - Disease IDs are mapped from MESH to MONDO; control samples receive `term_id = "MONDO:0000000"`.
  - Tissue is resolved from a GSE-level mapping file; GSE3526 samples are resolved via raw tissue names from the GEO Sample Description.
  - Age is extracted from the GEO Sample Description and binned into age groups: `fetus`, `infant`, `child`, `adolescent`, `adult`, `older_adult`, `elderly_adult`.
  - Sex is extracted via regex and keyword fallback, normalized to `M` or `F`.

```bash
metahq-build process ursahd
```

```python
processor = ProcessorRegistry.get("ursahd")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/ursahd.csv"))
```

### Sirota 2011 (`sirota_2011`)

Expert-curated disease and tissue annotations from Sirota et al. 2011 (doi:10.1126/scitranslmed.3001318). Each row in the source CSV describes a GEO DataSet with comma-separated lists of control and disease GSM IDs.

- **Input:** `data/unprocessed/sirota_2011.csv`
- **Annotations:** `disease`, `tissue`
- **ecode:** `expert`
- **Output:** `sirota_2011_processed.parquet`
- **Notes:**
  - UMLS disease CUIs are mapped to MONDO via `data/helpers/sirota_2011_umls_mondo_manual_mappings.csv`.
  - UMLS tissue CUIs are mapped to UBERON via `data/helpers/sirota_2011_umls_uberon_manual_mappings.csv`.
  - Control samples receive `term_id = "MONDO:0000000"` and `term_label = "control"`.
  - GDS accession prefixes and GSM prefixes are added during processing.

```bash
metahq-build process sirota_2011
```

```python
processor = ProcessorRegistry.get("sirota_2011")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/sirota_2011.csv"))
```

---

### KrishnanLab (`krishnanlab`)

Expert-curated tissue and disease annotations for GEO samples.

- **Input:** `data/unprocessed/krishnanlab.tsv`
- **Annotations:** `tissue`, `disease`
- **ecode:** `expert`
- **Output:** `krishnanlab_processed.parquet`
- **Note:** Disease IDs are mapped from DOID to MONDO. Tissue IDs are already in UBERON/CL format. Both are filtered to system-level ontology descendants.

```bash
metahq-build process krishnanlab
```

```python
processor = ProcessorRegistry.get("krishnanlab")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/krishnanlab.tsv"))
```

---

### Johnson 2023 (`johnson_2023`)

Manually curated annotations from Johnson et al. 2023 for both microarray (GPL570/GEO) and RNA-seq (refine.bio/SRA) datasets.

- **Input:** `data/unprocessed/johnson_2023__microarray.tsv`, `data/unprocessed/johnson_2023__rnaseq.tsv`
- **Annotations:** `disease`, `tissue`, `sex`, `age`
- **ecode:** `expert`
- **Output:** `johnson_2023_processed.parquet`
- **Notes:**
  - Microarray: Disease and tissue are pipe-delimited MESH terms mapped to MONDO and UBERON/CL
  - RNA-seq: Disease uses DOID→MONDO mapping; tissue uses free-text→UBERON/CL mapping
  - Sex is normalized to M/F with PATO terms (PATO:0000384 male, PATO:0000383 female)
  - Age uses age_group bins from the input data

```bash
metahq-build process johnson_2023
```

```python
processor = ProcessorRegistry.get("johnson_2023")
df = processor.run()

# Override input paths
df = processor.run(
    microarray_input_path=Path("./data/unprocessed/johnson_2023__microarray.tsv"),
    rnaseq_input_path=Path("./data/unprocessed/johnson_2023__rnaseq.tsv")
)
```

---

### Gu 2023 (`gu`)

Expert-curated tissue and disease annotations from Gu et al. 2023 for SRA samples.

- **Input:** `data/unprocessed/gu_2023.csv`
- **Annotations:** `disease`, `tissue`
- **ecode:** `expert`
- **Output:** `gu_processed.parquet`
- **Note:** Disease names are mapped to MONDO and tissue names are mapped to UBERON via helper mapping files. Filtered to system-level descendants.

```bash
metahq-build process gu
```

```python
processor = ProcessorRegistry.get("gu")
df = processor.run()

# Override input path
df = processor.run(input_path=Path("./data/unprocessed/gu_2023.csv"))
```

---

## Adding a new processor

1. Create `processors/<source_name>/` with `__init__.py` and `processor.py`.
2. Inherit from `BaseProcessor` and implement `process` and `validate`.
3. Decorate the class with `@ProcessorRegistry.register`.
4. Export the class from `__init__.py`.

See `processors/base.py` for the full interface and `processors/ale/processor.py` as a reference implementation.
