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

**NOTE:** Run `metahq supported` to see available options

### Required Options

- `--level TEXT`: Annotation level to retrieve (`sample` or `series`). Default: `sample`
- `--filters TEXT`: Comma-separated filters in format `key=value`. Available filters:
    - `species`: Filter by species (e.g., `human`, `mouse`)
    - `ecode`: Evidence code (e.g., `expert`, `crowd`, `any`)
    - `tech`: Technology type (e.g., `rnaseq`, `microarray`)
    - Combine multiple filters like so: `'species=human,ecode=expert,tech=rnaseq'`
- `--license TEXT`: The license category of annotations (e.g, `any`, `permissive`, `nc`). Using `permissive` will retrieve annotations from sources with `CC0` and `CC BY` licenses. Using `nc` will retrieve sources with `CC BY-NC` or `Academic Only` licenses. Using `any` retrives annotations from any license. See our [citation documentation](../about/citation.md) for source license information. Default: `any`

### Output Options

- `--output PATH`: Path to the output directory containing the retrieval result and source citation information. Default: `./metahq_result`
- `--fmt TEXT`: Output format (`parquet`, `tsv`, `csv`, or `json`). Default: `parquet`.
    - **Note:** MetaHQ is optimized for tabular exports. Wile JSON exports are supported, we recommend using tabular exports for large queries, particularly parquet.
- `--metadata TEXT`: Metadata level to include (`sample`, `series`, etc.). Default: `default` (matches `--level`)
    - Run `metahq supported` for all metadata fields.
    - Combine multiple filters like so: `'sample,series,description,srp'`
- `--refinebio`: Use to easily access gene expression data paired with your retrieved samples and studies. 
Adds mappings to [refine.bio](https://www.refine.bio/) samples and experiments and pre-populates a
refine.bio dataset with gene expression data matched with results from your query.

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
    - Available terms:
        - Female: `female`, `F`, `PATO:0000383`
        - Male: `male`, `M`, `PATO:0000384`

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

### Tabular

If a user queried disease annotations with the following command:

```bash
metahq retrieve diseases --terms "MONDO:0002113,MONDO:0004994" \
    --filters="species=human,ecode=expert,tech=rnaseq" \
    --metadata "platform,srx" --fmt parquet --output disease_annotations
```

This creates a directory called `disease_annotations` storing a file called `result.parquet` contain
the following table:

```
┌────────────┬──────────┬────────────┬─────────────────────────────────┬─────────────────────────┬───────────────┬───────────────┐
│ sample     ┆ platform ┆ srx        ┆ external_links                  ┆ sources                 ┆ MONDO:0002113 ┆ MONDO:0004994 │
│ ---        ┆ ---      ┆ ---        ┆ ---                             ┆ ---                     ┆ ---           ┆ ---           │
│ str        ┆ str      ┆ str        ┆ str                             ┆ str                     ┆ i32           ┆ i32           │
╞════════════╪══════════╪════════════╪═════════════════════════════════╪═════════════════════════╪═══════════════╪═══════════════╡
│ GSM1656436 ┆ GPL11154 ┆ SRX993404  ┆ null                            ┆ KrishnanLab|Gu_2023     ┆ 0             ┆ 1             │
│ GSM1656437 ┆ GPL11154 ┆ SRX993405  ┆ null                            ┆ KrishnanLab|Gu_2023     ┆ 0             ┆ 1             │
│ GSM1656438 ┆ GPL11154 ┆ SRX993406  ┆ null                            ┆ KrishnanLab|Gu_2023     ┆ 0             ┆ 1             │
│ GSM1841270 ┆ GPL11154 ┆ SRX1127523 ┆ {"DiSignAtlas": {"browse_url":… ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GSM1841273 ┆ GPL11154 ┆ SRX1127524 ┆ {"DiSignAtlas": {"browse_url":… ┆ KrishnanLab|DiSignAtlas ┆ 0             ┆ 1             │
│ GSM2290138 ┆ GPL16791 ┆ SRX2042217 ┆ null                            ┆ KrishnanLab             ┆ 0             ┆ 1             │
│ GSM2309518 ┆ GPL11154 ┆ SRX2161714 ┆ null                            ┆ KrishnanLab             ┆ 0             ┆ 1             │
│ GSM2309519 ┆ GPL11154 ┆ SRX2161715 ┆ null                            ┆ KrishnanLab             ┆ 0             ┆ 1             │
│ GSM2309520 ┆ GPL11154 ┆ SRX2161716 ┆ null                            ┆ KrishnanLab             ┆ 0             ┆ 1             │
│ GSM2309521 ┆ GPL11154 ┆ SRX2161717 ┆ null                            ┆ KrishnanLab             ┆ 0             ┆ 1             │
└────────────┴──────────┴────────────┴─────────────────────────────────┴─────────────────────────┴───────────────┴───────────────┘
```

A 1 means the entry is annotated to the term, a 0 means it was not annotated to that term. Note that annotations
of 0 do not mean an entry is definitely not that term. It only means the entry was never annotated to it.
To get declarations of what an entry is, what it definitely is not, and what is unknown, use `--mode=label`.

Metadata associated with each annotation are included as their own column.

### JSON

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
        "GSM1423130": {
            "platform": "GPL10999",
            "srx": "SRX642727",
            "external_links": null,
            "sources": "Gu_2023"
        },
        "GSM1542465": {
            "platform": "GPL11154",
            "srx": "SRX757100",
            "external_links": {
                "DiSignAtlas": {
                    "browse_url": null,
                    "records": [
                        {
                            "id": "DSA04633",
                            "url": "http://www.inbirg.com/disignatlas/detail/DSA04633"
                        }
                    ]
                }
            },
            "sources": "DiSignAtlas"
        },
    }
}
```

### External links

Some annotations in MetaHQ come from databases with web interfaces (e.g., Gemma, DiSignAtlas, Bgee).
For such annotations, we include an `external_links` column/key containing links to the original
annotation sources.

Some sources, such as Gemma and DiSignAtlas, computed differential expression signatures using
subsets of samples within a single series. The Gemma web interface provides a page where users
can view a list of all analyses performed for a single series - this url is provided under the
`browse_url` key. The `records` key contains links to the individual analyses themselves where the
`id` key notes the internal ID for that analysis in Gemma.

```
{
    "Gemma": {
        "browse_url": "https://gemma.msl.ubc.ca/browse/#/q/GSE97806",
        "records": [
            {
                "id": "16162",
                "url": "https://gemma.msl.ubc.ca/expressionExperiment/showExpressionExperiment.html?id=16162"
            },
            {
                "id": "16161",
                "url": "https://gemma.msl.ubc.ca/expressionExperiment/showExpressionExperiment.html?id=16161"
            }
        ]
    }
}
```

Other resources like DiSignAtlas do not have a "browse" page, but still perform multiple analyses
per series. These resouces have an empty `browse_url` key, but can still have multiple records.


### Cite the original annotation sources
The sources of the annotations are also included in their own `sources` column or key. Additionally, we include
a file called `CITATION.txt` in the output directory of a query. This file stores information about the query
and the sources included in the dataset. We require users to cite these sources if they use MetaHQ annotations in their research.

See the [About](../about/citation.md) page for a source-to-citation map. See our [Terms and Conditions](../about/terms_conditions.md)
for more information.

### Create a refine.bio dataset
[refine.bio](https://www.refine.bio/) is a database containing over 1.4 million harmonized gene expression
profiles from over 43,000 studies. For easy access to the gene expression data paired with your MetaHQ
annotations, we provide users with the option to create a refine.bio dataset including samples and
studies returned from your query. **Note:** not all samples and studies are available in refine.bio.

To create a refine.bio dataset, simply add the `--refinebio` flag to the retrieve command:

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" \
    --level sample \
    --filters "species=human,tech=rnaseq,ecode=expert" \
    --refinebio
```

The url that you can use to access and share your refine.bio dataset will be available in the following places

1. Saved in the `Query parameters` section of `CITATION.txt`
2. Printed to the screen after `metahq retrieve` is run

refine.bio uses SRA accessions for some samples and studies. For this reason, we also append mappings
to `refinebio_sample` (sample IDs) and `refinebio_experiment` (study IDs) to your export to seamlessly
port your annotations to the refine.bio identification schema:
```
┌────────────┬──────────────────┬──────────────────────┬─────────┬────────────────┬────────────────┐
│ sample     ┆ refinebio_sample ┆ refinebio_experiment ┆ sources ┆ UBERON:0000948 ┆ UBERON:0000955 │
│ ---        ┆ ---              ┆ ---                  ┆ ---     ┆ ---            ┆ ---            │
│ str        ┆ str              ┆ str                  ┆ str     ┆ i64            ┆ i64            │
╞════════════╪══════════════════╪══════════════════════╪═════════╪════════════════╪════════════════╡
│ GSM1060654 ┆ SRR646457        ┆ SRP017809            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM1060655 ┆ SRR646458        ┆ SRP017809            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM1060656 ┆ SRR646459        ┆ SRP017809            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM1060657 ┆ SRR646460        ┆ SRP017809            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM1063280 ┆ SRR648404        ┆ SRP017933            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ …          ┆ …                ┆ …                    ┆ …       ┆ …              ┆ …              │
│ GSM999587  ┆ SRR563553        ┆ SRP015668            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM999588  ┆ SRR563554        ┆ SRP015668            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM999589  ┆ SRR563555        ┆ SRP015668            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM999590  ┆ SRR563556        ┆ SRP015668            ┆ Gu_2023 ┆ 0              ┆ 1              │
│ GSM999591  ┆ SRR563557        ┆ SRP015668            ┆ Gu_2023 ┆ 0              ┆ 1              │
└────────────┴──────────────────┴──────────────────────┴─────────┴────────────────┴────────────────┘
```
