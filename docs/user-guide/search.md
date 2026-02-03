# Search

This command identifies the ontology term IDs most similar to a free-text query. It leverages the 
BM25+ algorithm with the `rankbm25` Python package to perform a lexical search of a query term to
the term names and their synonyms in an ontology.

## Options

- `--query`: Any free text input (e.g., `"heart attack"`, `"hepatocyte"`).
- `--type`: Supported types are `tissue`, `disease`, and `celltype`.
- `--ontology`: Supported ontologies are `UBERON` for tissues and cell types, `CL` for cell types, and `MONDO` for diseases.
- `--max-results (-k)`: Specify the number of top term ID matches to the query that are returned.
- `--scores`: If passed, will return the score from the BM25 algorithm.
- `--extended`: If passed, will return the ontology and type in the results.
- `--scopes`: If passed, will return the scopes of term synonyms in the results.

## Usage

```bash
metahq search [OPTIONS]
```

## Examples

Search for the MONDO ID for "heart attack":

```bash
metahq search --query "heart attack" --type disease --ontology MONDO -k 5
```

Search for "hepatoctye" cells and return BM25 scores and view the synonym scopes:

```bash
metahq search --query "hepatocyte" --type celltype --ontology CL --scores --scopes
```
