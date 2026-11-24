# Quick Start

Every installation of the MetaHQ CLI requires the `setup` command to be run. This will download the MetaHQ database from Zenodo
and configure the package.

## Setup

If you haven't already, download the latest MetaHQ database and configure the CLI by running the following:

```bash
metahq setup
```

## Retrieve

Query the database for curated, biological context annotations. Below is an example of a query for expert-curated
tissue annotations from human RNA-Seq samples:

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" \
    --level sample --filters "species=human,tech=rnaseq,ecode=expert" \
    --metadata "sample,series,description" --fmt tsv --output annotations
```

This command creates a directory called `annotations` containing `result.tsv`, a table with columns
`[sample, series, description, sources, external_links, UBERON:0000948, UBERON:0000955]`, and `CITATION.txt`, which
lists the original sources of the annotations returned by your query.
Please cite the original sources of any annotations that you use along with MetaHQ.

Some annotations in MetaHQ come from sources with non-commercial licenses (e.g., CC BY-NC). You can filter for annotations
with permissive licenses using the `--license` argument:

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" \
    --level sample --filters "species=human,tech=rnaseq,ecode=expert" \
    --license permissive
```

See the [retrieve documentation](../user-guide/retrieve.md) for more details, and [use cases](../user-guide/use-cases.md)
for worked examples such as building classifier labels or finding case/control studies.

## Ontology term ID search

Tissue and disease queries require standardized ontology term ID inputs. The following command will identify the top five most similar
MONDO disease ontology term IDs to "heart attack".

```bash
metahq search --query "heart attack" --type disease --ontology MONDO -k 5
```

The EBI Ontology Lookup Service is also an excellent way to find term IDs. See the links below:

**UBERON/CL** (tissues/celltypes): <br> [https://www.ebi.ac.uk/ols4/ontologies/uberon](https://www.ebi.ac.uk/ols4/ontologies/uberon)

**MONDO** (diseases): <br> [https://www.ebi.ac.uk/ols4/ontologies/mondo](https://www.ebi.ac.uk/ols4/ontologies/mondo)
