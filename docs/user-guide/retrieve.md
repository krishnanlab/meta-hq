# Retrieve Commands

The `metahq retrieve` commands query the MetaHQ database to retrieve curated annotations and labels for tissues, diseases, sex, and age groups.

There is a command for each retrievable attribute:

- [`metahq retrieve tissues`](#tissues)
- [`metahq retrieve diseases`](#diseases)
- [`metahq retrieve sex`](#sex)
- [`metahq retrieve age`](#age)

## Citing annotation sources

The MetaHQ database contains annotations gathered from searchable databases, static project websites, GitHub repositories, data repositories (Zenodo, Figshare), and publication supplementary files.
Output files from `metahq retrieve` include which resources the retrieved annotations came from. We require users to cite these sources.

Please see our [citation documentation](../about/citation.md) for instructions on how to cite MetaHQ and its annotation sources.

## Common Options

All retrieve commands share the following common options:

### Required Options

- `--level TEXT`: Annotation level to retrieve (`sample` or `series`). Default: `sample`
- `--filters TEXT`: Comma-separated filters in format `key=value`. Available filters:
    - `species`: Filter by species (e.g., `human`, `mouse`)
    - `ecode`: Evidence code (e.g., `expert`, `crowd`, `any`)
    - `tech`: Technology type (e.g., `rnaseq`, `microarray`)
    - Combine multiple filters like so: `'species=human,ecode=expert,tech=rnaseq'`
    - Run `metahq supported` to see available options for species, ecode, and tech
- `--license TEXT`: The license category of annotations (e.g, `any`, `permissive`, `nc`). Using `permissive` will retrieve annotations from sources with `CC0` and `CC BY` licenses. Using `nc` will retrieve sources with `CC BY-NC` or `Acedemic Only` licenses. Using `any` retrives annotations from any license. See our [citation documentation](../about/citation.md) for source license information. Default: `any`

### Output Options

- `--output PATH`: Path to the output directory containing the retrieval result and source citation information. Default: `./metahq_result`
- `--fmt TEXT`: Output format (`parquet`, `tsv`, `csv`, or `json`). Default: `parquet`
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
    --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt tsv --metadata "sample,srx,srp"
```

**Retrieve sample labels for all tissue terms with parquet output:**

```bash
metahq retrieve tissues --terms "all" \
    --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt parquet
```

**Retrieve series-level annotations with JSON output:**

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" \
    --filters "species=human,ecode=expert,tech=rnaseq" \
    --level series --fmt json
```

---

## Diseases

Retrieve disease annotations and labels using MONDO ontology terms.

### Additional Options

- `--terms TEXT`: Comma-separated MONDO ontology IDs.
    - Use `'all'` to query all disease terms.
- `--mode MODE`: Annotation mode (`annotate` or `label`). Default: `annotate`
    - `annotate`: Returns inferred annotations using the ontology hierarchy
    - `label`: Returns +1, 0, -1, and 2 labels indicating what a sample is, what it is not, or if it is unknown. Labels of 2 indicate is a sample was a control for a particular disease in the study that the sample came from.

### Examples

**Retrieve expert-curated human RNA-Seq samples with descriptions:**

```bash
metahq retrieve diseases --terms "MONDO:0004994" \
    --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt csv --metadata "sample,description"
```

**Retrieve crowd-sourced human microarray samples with descriptions:**

```bash
metahq retrieve diseases --terms "all" \
    --filters "species=human,ecode=crowd,tech=microarray" \
    --fmt parquet --metadata "sample,description"
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
    --filters "species=human,ecode=expert,tech=rnaseq"
```

**Retrieve all RNA-Seq sex-annotated studies with SRA metadata:**

```bash
metahq retrieve sex --terms "male,female" \
    --filters "species=human,ecode=expert,tech=rnaseq" \
    --metadata "series,srp,description" --level series
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
    --filters "species=human,ecode=expert,tech=rnaseq" \
    --fmt csv
```

**Retrieve all microarray age-annotated studies with SRA metadata:**

```bash
metahq retrieve age --terms "infant,adolescent,elderly_adult" \
    --filters "species=human,ecode=expert,tech=microarray" \
    --metadata "series,srp,description" --level series
```

## Example Output

If a user queried disease annotations with the following command:

```bash
metahq retrieve diseases --terms "MONDO:0002113,MONDO:0004994" \
    --filters="species=human,ecode=expert,tech=rnaseq" \
    --metadata "platform,srx" --fmt tsv --output disease_annotations
```

This creates a directory called `disease_annotations` storing a file called `result.tsv` that would look like so:

```
┌──────────┬────────────┬────────────┬─────────────────────────┬───────────────┬───────────────┐
│ platform ┆ srx        ┆ sample     ┆ sources                 ┆ MONDO:0002113 ┆ MONDO:0004994 │
│ ---      ┆ ---        ┆ ---        ┆ ---                     ┆ ---           ┆ ---           │
│ str      ┆ str        ┆ str        ┆ str                     ┆ i64           ┆ i64           │
╞══════════╪════════════╪════════════╪═════════════════════════╪═══════════════╪═══════════════╡
│ GPL16791 ┆ SRX2858505 ┆ GSM2641079 ┆ DiSignAtlas             ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858506 ┆ GSM2641080 ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858508 ┆ GSM2641082 ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858509 ┆ GSM2641083 ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858510 ┆ GSM2641084 ┆ DiSignAtlas             ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858511 ┆ GSM2641085 ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858512 ┆ GSM2641086 ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858513 ┆ GSM2641087 ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858514 ┆ GSM2641088 ┆ DiSignAtlas             ┆ 0             ┆ 1             │
│ GPL16791 ┆ SRX2858515 ┆ GSM2641089 ┆ DiSignAtlas             ┆ 0             ┆ 1             │
└──────────┴────────────┴────────────┴─────────────────────────┴───────────────┴───────────────┘
```

A 1 means the entry is annotated to the term, a 0 means it was not annotated to that term. Note that annotations
of 0 do not mean an entry is definitely not that term. It only means the entry was never annotated to it.
To get declarations of what an entry is, what it definitely is not, and what is unknown, use `--mode=label`.

Metadata associated with each annotation are included as their own column.

For JSON formats, metadata will be included as additional keys for the sample/study. For example, if a user
ran the following:

```bash
metahq retrieve diseases --terms "MONDO:0002113,MONDO:0004994" \
    --metadata "platform,srx" --filters="species=human,ecode=expert,tech=rnaseq" \
    --fmt json --output disease_annotations
```

They would retrieve the following:

```
{
    "MONDO:0004994": {
        "GSM2641079": {
            "platform": "GPL16791",
            "srx": "SRX2858505",
            "sources": "DiSignAtlas"
        },
        "GSM2641080": {
            "platform": "GPL16791",
            "srx": "SRX2858506",
            "sources": "KrishnanLab|DiSignAtlas"
        },
        "GSM2641082": {
            "platform": "GPL16791",
            "srx": "SRX2858508",
            "sources": "KrishnanLab|DiSignAtlas"
        }, ...
```

The sources of the annotations are also included in their own `sources` column or key. Additionally, we include
a file called `CITATION.txt` in the output directory of a query. This file stores information about the query
and the sources included in the dataset. We require users to cite these sources if they use MetaHQ annotations in their research.

See the [About](../about/citation.md) page for a source-to-citation map. See our [Terms and Conditions](../about/terms_conditions.md)
for more information.
