# CLI Quickstart

Every installation of the MetaHQ CLI requires the `setup` command to be run. This will download the MetaHQ database from Zenodo
and configure the package.

## Setup

Download the latest MetaHQ database and configure the CLI:

```bash
$ metahq setup
```

## Retrieve

Query the database for curated, biological context annotations. Below is an example of a query for expert-curated
tissue annotations from human RNA-Seq samples:

```bash
$ metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" \
    --level sample --filters "species=human,tech=rnaseq,ecode=expert" \
    --metadata "sample,series,description" --fmt tsv --output annotations.tsv
```

See the [retrieve documentation](../packages/cli/commands/retrieve.md) for more details.

## Search

Tissue and disease queries require standardized ontology term ID inputs. The following command will identify the top five most similar
MONDO disease ontology term IDs to "heart attack".

```bash
$ metahq search --query "heart attack" --type disease --ontology MONDO -k 5
```

The EBI Ontology Lookup Service is also an excellent way to find term IDs. See the links below:

**UBERON/CL** (tissues/celltypes): [https://www.ebi.ac.uk/ols4/ontologies/uberon](https://www.ebi.ac.uk/ols4/ontologies/uberon)\
**MONDO** (diseases): [https://www.ebi.ac.uk/ols4/ontologies/mondo](https://www.ebi.ac.uk/ols4/ontologies/mondo)
