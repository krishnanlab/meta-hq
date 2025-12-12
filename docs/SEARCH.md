# metahq search

This function identifies the ontology term IDs most similar to a user's free-text query. It leverages the BM25 algorithm with the rankbm25 package to
perform a lexical search of a query term to the term names and their synonyms in an ontology.

### query

Any free text input (e.g., "heart attack", "hepatocyte").

### type

Supported types are:

- _tissue_
- _disease_
- _celltype_

### ontology

Supported ontologies are:

- UBERON for tissues and cell types
- CL for cell types
- MONDO for diseases

### max-results (k)

Users can specify the number of top term ID matches to the query that are returned.

### scores

If passed, will return the score from the BM25 algorithm.

### extended

If passed, will return the ontology and type in the results.

### scopes

If passed, will return the scopes of term synonyms in the results.
