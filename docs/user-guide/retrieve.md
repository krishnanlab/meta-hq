# Retrieve Commands

The `metahq retrieve` commands query the MetaHQ database to retrieve curated annotations and labels for tissues, diseases, sex, and age groups.

There is a command for each retrievable attribute:

- [`metahq retrieve tissues`](#tissues)
- [`metahq retrieve diseases`](#diseases)
- [`metahq retrieve sex`](#sex)
- [`metahq retrieve age`](#age)

## Citing annotation sources

The MetaHQ database contains annotations gathered from searchable databases, static project websites, GitHub repositories, data repositories (Zenodo, Figshare), and publication supplementary files.
Output files from `metahq retrieve` include which resources the retrieved annotations came from. We encourage users to cite these sources.

Please see our [citation documentation](../../about/citation.md) for instructions on how to cite MetaHQ and its annotation sources.

## Common Options

All retrieve commands share the following common options:

### Required Options

- `--level TEXT`: Annotation level to retrieve (`sample` or `series`). Default: `sample`
- `--filters TEXT`: Comma-separated filters in format `key=value`. Available filters:
    - `species`: Filter by species (e.g., `human`, `mouse`)
    - `ecode`: Evidence code (e.g., `expert`, `semi`, `crowd`, `any`)
    - `tech`: Technology type (e.g., `rnaseq`, `microarray`)
    - Combine multiple filters like so: `'species=human,ecode=expert,tech=rnaseq'`

### Output Options

- `--output PATH`: Output file path. Default: `annotations`
- `--fmt TEXT`: Output format (`tsv`, `csv`, or `json`). Default: `parquet`
- `--metadata TEXT`: Metadata level to include (`sample`, `series`, etc.). Default: `default` (matches `--level`)
    - Run `metahq supported` for all metadata fields.
    - Combine multiple filters like so: `'sample,series,description,srp'`

### Logging Options

- `--log-level TEXT`: Logging level (`debug`, `info`, `warning`, `error`). Default: `info`
- `--quiet`: Suppress console output (flag)

---

## Tissues

Retrieve tissue annotations and labels using UBERON ontology terms.

### Additional Options

- `--terms TEXT`: Comma-separated UBERON ontology IDs.
- `--mode MODE`: Annotation mode (`annotate` or `label`). Default: `annotate`
    - `annotate`: Returns inferred annotations using the ontology hierarchy
    - `label`: Returns +1, 0, and -1 labels indicating what a sample is, what it is not, or if it is unknown

### Usage

```bash
metahq retrieve tissues [OPTIONS]
```

### Examples

**Retrieve human RNA-seq samples with expert annotations with SRA metadata:**

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" \
    --level sample --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt tsv --output tissues.tsv --metadata "sample,srx,srp"
```

**Retrieve sample labels for all tissue terms with parquet output:**

```bash
metahq retrieve tissues --terms "all" \
    --level sample --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt parquet --output tissues.parquet
```

**Retrieve series-level annotations with JSON output:**

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" \
    --level series --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt json --output tissues.json
```

---

## Diseases

Retrieve disease annotations and labels using MONDO ontology terms.

### Additional Options

- `--terms TEXT`: Comma-separated MONDO ontology IDs.
  - Use `'all'` to query all disease terms.
- `--mode MODE`: Annotation mode (`annotate` or `label`). Default: `annotate`
  - `annotate`: Returns inferred annotations using the ontology hierarchy
  - `label`: Returns +1, 0, -1, and 2 labels indicating what a sample is, what it is not, or if it is unknown. Labels of 2 indicate is a sample is a healthy control for that disease.

### Examples

**Retrieve expert-curated human RNA-Seq samples with descriptions:**

```bash
metahq retrieve diseases --terms "MONDO:0004994" \
    --level sample --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt csv --output diseases_filtered.csv --metadata "sample,description"
```

**Retrieve crowd-sourced human microarray samples with descriptions:**

```bash
metahq retrieve diseases --terms "all" \
    --level sample --filters "species=human,ecode=crowd,tech=microarray" \
    --fmt parquet --output diseases_filtered.parquet --metadata "sample,description"
```

## Sex

Retrieve sex annotations.

### Additional Options

- `--terms TEXT`: Comma-separated sex terms.
    - Available terms: `male`, `female`

### Examples

**Retrieve all RNA-Seq sex-annotated samples:**

```bash
metahq retrieve sex --terms "male,female" \
    --level sample --filters "species=human,ecode=expert,tech=rnaseq"
```

**Retrieve all RNA-Seq sex-annotated datasets with SRA metadata:**

```bash
metahq retrieve sex --terms "male,female" \
    --level series --filters "species=human,ecode=expert,tech=rnaseq" \
    --metadata "series,srp,description"
```

## Age

Retrieve age group annotations.

### Additional Options

- `--terms TEXT`: Comma-separated age groups.
  - Check supported age groups with `metahq supported`.
  - Multiple groups can be combined: `fetus,adult`
  - Use `all` to retrieve all age groups

### Examples

**Retrieve all RNA-Seq age-annotated samples:**

```bash
metahq retrieve age --terms "all" \
    --level sample --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt csv --output ages.csv
```

**Retrieve all microarray age-annotated datasets with SRA metadata:**

```bash
metahq retrieve sex --terms "infant,adolescent,elderly_adult" \
    --level series --filters "species=human,ecode=expert,tech=microarray" \
    --metadata "series,srp,description"
```
