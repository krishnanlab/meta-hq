# MetaHQ demo

MetaHQ is a Python package and CLI for querying tissue, disease, sex, and age annotations of public microarray and RNA-Seq samples and series stored in the Gene Expression Omnibus (GEO).

## Setup

1. Install the CLI Python package:

```bash
pip install metahq-cli
# or uv tool install metahq-cli
```

2. Download the latest database and configure the CLI:

```bash
metahq setup
```

## Search

Now you're ready to start using MetaHQ. Let's retrieve some annotations. Say you want to search for heart and liver annotations, but don't know what the ontology term IDs are for these anatomical entities.
You can run `metahq search` to figure this out.

Heart:

```bash
metahq search --query heart --type tissue --ontology UBERON
```

Liver showing top 10 terms:

```bash
metahq search --query liver --type tissue --ontology UBERON -k 10
```

## Supported

Now that you have ontology term IDs, you can retrieve annotations from the MetaHQ database. However, there are lots of filters to include in the query and we know none of them.
Run `metahq supported` to see what's offered by MetaHQ.

```bash
metahq supported
```

## Retrieve tissues

Alright now you're ready to go. Let's retrieve our tissue annotations. The default annotations are sample-level. To retrieve study-level annotations,
simply pass `--level series` for any retrieve command ('studies' are called 'series' in GEO).

```bash
metahq retrieve tissues \
--terms "UBERON:0000948,UBERON:0002107" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" \
--fmt tsv --output tissue_annotations
```

If you need to know which samples are not heart and liver, change the mode to `--mode label` (default is `annotate`):

```bash
metahq retrieve tissues \
--terms "UBERON:0000948,UBERON:0002107" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" --mode label \
--fmt tsv --output tissue_labels
```

You can also perform a large query for all RNA-Seq (or microarray) samples annotated to all tissues and cell types in UBERON. This operation requires anywhere from 10 seconds to 3 minutes to complete depending on your compute capacity.
This also results in a larger file (10M as a `parquet`; 3.6G as a `tsv`) so we recommend saving to `parquet` for compression. We do not recommend saving to `json` for large queries.

```bash
metahq retrieve tissues \
--terms "all" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" --mode label \
--fmt tsv --output tissue_labels_parquet
```

Finally, you can query terms from a `txt` file. This is helpful if you have a list of terms that is long, but you don't want all terms. If you have the following file `terms.txt`:

```
UBERON:0000948
UBERON:0000955
CL:0000182
UBERON:0002107
UBERON:0000945
UBERON:0002113
```

You can query these with:

```bash
metahq retrieve tissues \
--terms terms.txt \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" --mode label \
--fmt tsv --output tissue_labels_parquet
```

## Retrieve diseases

You can also query disease annotations/labels. In a manner similar to tissue queries, you can select specific terms:

```bash
metahq retrieve diseases \
--terms "MONDO:0004994,MONDO:0008170" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" --mode label \
--fmt tsv --output disease_labels
```

Or query all available MONDO terms:

```bash
metahq retrieve diseases \
--terms "all" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" --mode label \
--fmt parquet --output disease_labels_parquet
```

## Retrieve sex

... and sex annotations:

```bash
metahq retrieve sex --terms "M,F" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--fmt tsv --output sex_annotations
```

Note that `sex` and `age` do not have a `--mode` argument. You can only query _direct annotations_ for sex and age, not labels. This is because converting annotations to labels requires definitions of the hierarchical relationships between terms and an ontology.

## Retrieve age

... and age annotations:

```bash
metahq retrieve age --terms "all" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--fmt tsv --output age_annotations
```

## Please cite the original sources

MetaHQ is a harmonized collection of annotation sets derived from various independent curation efforts. The authors of those efforts put a ton of time and energy into manual curation and should be properly attributed.
Because of this, **we require MetaHQ users to cite the original annotation sources along with MetaHQ**, provided they use annotations from that source. We help facilitate proper citations in two ways:

1. Every run of `metahq retrieve` returns a `CITATION.txt` file outlining the sources that contributed to the entire export.
2. Every annotation is paired with a source (or sources) in the `metahq retrieve` export itself.

See [the docs page](https://meta-hq.readthedocs.io/en/latest/user-guide/retrieve/#example-output) for more detail.

## Validate

To check the integrity of the MetaHQ data package, run the following command. Warnings will be raised for every file that is altered or corrupted.

```bash
metahq validate
```

## Delete

If you wish to remove the MetaHQ database, run the following. You will have to run `metahq setup` again to use the CLI.

```bash
metahq delete
```
