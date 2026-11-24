# Use cases

Recipes for common questions. They assume you have run `metahq setup`, and they
build on each other: later Python snippets read files written by earlier
commands, so run them from one working directory.

Outputs shown here are illustrative: counts depend on the database version.

!!! note "These recipes are tested"
    Every `metahq` command and Python block on this page runs in the test suite
    (`packages/cli/tests/use_cases/`) against a small synthetic data package,
    so a change that breaks a recipe fails CI. If you edit a command here, keep
    its `--output` directory unique on the page.

See [How MetaHQ works](../concepts/overview.md) for the concepts used below
(levels, evidence codes, annotate versus label).

## 1. Find ontology term IDs

Tissue and disease queries take ontology IDs. Search by name or synonym:

```bash
metahq search --query "heart attack" --type disease --ontology MONDO -k 3
metahq search --query liver --type tissue --ontology UBERON -k 3
```

```text
1. MONDO:0005068
    name: myocardial infarction
    synonyms: heart attack | infarction (mi), myocardial | ...
2. MONDO:0005264
    name: transient ischemic attack
...
```

The [EBI Ontology Lookup Service](https://www.ebi.ac.uk/ols4/) is useful for
browsing the hierarchy around a term.

## 2. Which samples are from a tissue?

Find human RNA-seq samples annotated to heart (`UBERON:0000948`) or liver
(`UBERON:0002107`), with their study and platform:

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0002107" \
    --metadata "sample,series,platform" --fmt tsv --output heart_liver
```

```text
sample      series    platform  sources  external_links  UBERON:0000948  UBERON:0002107
GSM1101970  GSE45326  GPL11154  Gu_2023                  1               0
GSM1101972  GSE45326  GPL11154  Gu_2023                  0               1
GSM1126612  GSE46224  GPL11154  Gu_2023                  1               0
```

Annotations propagate up the ontology, so a sample annotated to *myocardium*
appears with a `1` for heart. A `0` here only means "not annotated to this
term". Use label mode (next) when you need true negatives.

## 3. Labels for a classifier

`--mode label` returns every sample with a tissue annotation, labeled `1`
(the term or a descendant), `-1` (unrelated) or `0` (only annotated to
something more general, such as "organ"):

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0002107" --mode label \
    --metadata "sample,series" --output heart_liver_labels
```

Load it, drop the unknowns and keep the study ID for grouping:

```python
import polars as pl

labels = pl.read_parquet("heart_liver_labels/result.parquet")
heart = "UBERON:0000948"

train = labels.filter(pl.col(heart) != 0).select("sample", "series", heart)
print(train[heart].value_counts().sort(heart))
```

```text
┌────────────────┬───────┐
│ UBERON:0000948 ┆ count │
╞════════════════╪═══════╡
│ -1             ┆ 77050 │
│ 1              ┆ 1238  │
└────────────────┴───────┘
```

Samples from one study share batch effects, so split train and test sets by
`series` (for example with scikit-learn's `GroupKFold`) rather than by sample.
A sample deposited in several series has them pipe-joined (`GSE1|GSE2`).

## 4. Cases and controls from the same study

For diseases, label mode marks healthy controls with `2` when other samples
in their study are positive for the term. That makes it easy to find studies
with both cases and controls, for example for cardiomyopathy:

```bash
metahq retrieve diseases --terms "MONDO:0004994" --mode label \
    --metadata "sample,series" --output cardiomyopathy_labels
```

```python
import polars as pl

term = "MONDO:0004994"
cm = pl.read_parquet("cardiomyopathy_labels/result.parquet")

studies = (
    cm.group_by("series")
    .agg(
        cases=(pl.col(term) == 1).sum(),
        controls=(pl.col(term) == 2).sum(),
    )
    .filter((pl.col("cases") > 0) & (pl.col("controls") > 0))
    .sort("cases", descending=True)
)
print(studies.head())
```

```text
┌─────────────────────┬───────┬──────────┐
│ series              ┆ cases ┆ controls │
╞═════════════════════╪═══════╪══════════╡
│ GSE141910           ┆ 197   ┆ 166      │
│ GSE86356            ┆ 94    ┆ 26       │
│ GSE175384|GSE175386 ┆ 32    ┆ 40       │
│ GSE204804           ┆ 30    ┆ 6        │
│ GSE160997           ┆ 18    ┆ 2        │
└─────────────────────┴───────┴──────────┘
```

## 5. Find studies, not samples

Use `--level series` to list studies annotated to a disease, with their GEO
titles to skim:

```bash
metahq retrieve diseases --terms "MONDO:0007254" --level series \
    --metadata "series,title" --fmt tsv --output breast_cancer_studies
```

```text
series     title                                         sources       external_links                       MONDO:0007254
GSE100075  Discovery of naturally occurring ESR1 mu...   Gemma         {"Gemma": {"browse_url": "https://...  1
GSE100799  Genome-wide multi-omics profiling reveal...   Johnson_2023                                       1
```

`external_links` points to the source's own page for the study (Gemma,
Bgee, DiSignAtlas), where you can check the original curation.

## 6. Only commercially usable annotations

Add `--license permissive` to keep only CC0 and CC BY sources. The
`sources` column and `CITATION.txt` then list only those sources:

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0002107" --mode label \
    --license permissive --metadata "sample,series" \
    --output heart_liver_labels_permissive
```

Expect fewer samples: in the example above, 23,479 of 78,288 labeled samples
remain. Read the [Terms and Conditions](../about/terms_conditions.md) for what
each license allows.

## 7. Add sex and age as covariates

Sex and age are direct annotations (no ontology). Retrieve them and join them
to the labels from use case 3:

```bash
metahq retrieve sex --terms "male,female" --output sex
metahq retrieve age --terms "adult,older_adult" --output age
```

```python
import polars as pl

heart = "UBERON:0000948"
labels = pl.read_parquet("heart_liver_labels/result.parquet")
sex = pl.read_parquet("sex/result.parquet").select("sample", "M", "F")
age = pl.read_parquet("age/result.parquet").select("sample", "adult", "older_adult")

design = (
    labels.filter(pl.col(heart) == 1)
    .select("sample", "series", heart)
    .join(sex, on="sample", how="left")
    .join(age, on="sample", how="left")
)
print(design.head())
```

Sex and age are curated for fewer samples than tissue, so expect nulls after
the join: they mean "not annotated", not "neither".

## 8. Other species, platforms and evidence

Filters you leave out take their defaults (`species=human`, `tech=rnaseq`,
`ecode=any`). Mouse heart samples:

```bash
metahq retrieve tissues --terms "UBERON:0000948" --filters "species=mouse" \
    --output mouse_heart
```

Human microarrays, expert annotations only:

```bash
metahq retrieve tissues --terms "UBERON:0000948" \
    --filters "species=human,tech=microarray,ecode=expert" --output heart_arrays
```

Run `metahq supported` to list every species, technology, evidence code and
metadata field.

## Going further

- Many terms at once: pass `--terms all`, or a text file with one term ID per
  line (see the [demo](https://github.com/krishnanlab/meta-hq/tree/main/demo)).
  Write large queries to parquet.
- Expression data: add `--refinebio` to create a
  [refine.bio](https://www.refine.bio/) dataset for the retrieved samples (see
  [Retrieve](retrieve.md#create-a-refinebio-dataset)).
- Cite the sources: every result directory has a `CITATION.txt`. See
  [Citations](../about/citation.md).
