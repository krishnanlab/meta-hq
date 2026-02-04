# MetaHQ demo

MetaHQ is a Python package and CLI for querying tissue, disease, sex, and age annotations of public microarray and RNA-Seq samples and series stored in the Gene Expression Omnibus (GEO).

## Setup

1. Install the CLI Python package:

```bash
pip install metahq-cli
```

2. Download the lastest database and configure the CLI:

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
--fmt tsv --output annotations.tsv
```

If you need to know which samples are not heart and liver, change the mode to `--mode label` (defualt is `annotate`):

```bash
metahq retrieve tissues \
--terms "UBERON:0000948,UBERON:0002107" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" --mode label \
--fmt tsv --output annotations.tsv
```

## Retrieve diseases

You can also perform a large query for all RNA-Seq samples annotated to all diseases in MONDO. This operation requires anywhere from 10 seconds to 3 minutes to complete depending on your compute capacity.
This also results in a larger file (58MB as a `parquet`; 1.8G as a `tsv`) so we recommend saving to `parquet` for compression. We do not recommend saving to `json` for large queries.

```bash
metahq retrieve diseases \
--terms "all" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--metadata "series,platform" --mode label \
--fmt parquet --output annotations.parquet
```

## Retrieve sex

You can also query sex annotations:

```bash
metahq retrieve sex --terms "M,F" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--fmt tsv --output annotations.tsv
```

## Retrieve age

... and age annotations:

```bash
metahq retrieve age --terms "all" \
--filters "species=human,ecode=expert,tech=rnaseq" \
--fmt tsv --output annotations.tsv
```

## Validate

To check the integrity of the MetaHQ data package, run the following command. Warnings will be raised for every file that is altered or corrputed.

```bash
metahq validate
```

## Delete

If you wish to remove the MetaHQ data package, run the following. You will have to run `metahq setup` again to use the CLI.

```bash
metahq delete
```
