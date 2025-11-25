<img src="media/metahq_logo.png" alt="Logo" width="400" height="200"/>

![CLI Tests](https://github.com/krishnanlab/meta-hq/workflows/CLI%20Tests/badge.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
![pypi](https://img.shields.io/pypi/v/metahq-cli.svg)

A Package to query the MetaHQ database.

## Key features

- Query standardized and harmonized biomedical annotations
- Access metadata for samples and studies in GEO and SRA
- Convert annotations to labels
- Fast execution time
- Search for relevant ontology terms with free-text queries
- Contribute new annotation sets to the database

## Docs

Source Code: [https://github.com/krishnanlab/meta-hq/packages/cli](https://github.com/krishnanlab/meta-hq/tree/main/packages/cli)

## Installation and setup

**To install**:

```bash
pip install metahq-cli
```

**Setup**: This command must be run with every metahq-cli installation. The `setup` command downloads the
MetaHQ database from Zenodo.

```bash
metahq setup --doi latest
```

## Retrieve

Query the MetaHQ database to retrieve sample or study annotations for tissues, diseases, sex, and age.

```bash
metahq retrieve diseases --terms "MONDO:0004994,MONDO:0008903" \
--level sample --filters "species=human,tech=rnaseq,ecode=expert" \
--metadata "sample,series,platform,srp" --fmt parquet
```

## Search

If you do not have ontology terms memorized off-hand, you can run the following command to find
the ontology term IDs most similar to a free-text query of a disease, tissue, or cell line.

```bash
metahq search --query "heart attack" --type disease --ontology MONDO
```
