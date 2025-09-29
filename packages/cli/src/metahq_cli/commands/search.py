"""
CLI command to search the ontology database for terms. Relies
on metahq_core.search.search() to do the actual searching.

Author: Faisal Alquaddoomi
Date: 2025-09-25
"""

import click

from metahq_core.search import search as core_search, NoResultsFound
from metahq_core.util.supported import get_ontology_search_db

DEFAULT_DB = get_ontology_search_db()
DEFAULT_TOP_HITS = 3

@click.command(name="search", context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--query", "-q", type=str, required=True, help="Search query")
@click.option("--db", "-b", type=click.Path(exists=True, dir_okay=False), default=DEFAULT_DB, help="DuckDB file")
@click.option("--type", "-c", type=click.Choice(["disease","tissue","celltype","none"]), default=None, help="Filter by type")
@click.option("--ontology", "-o", type=click.Choice(["MONDO","UBERON","CL"]), default=None, help="Filter by ontology")
@click.option("--max-results", "-k", type=int, default=DEFAULT_TOP_HITS, help="Number of results to show")
@click.option("--scores", "-s", is_flag=True, default=False, help="Include scores in results")
@click.option("--extended", "-e", is_flag=True, default=False, help="Include ontology and type in results")
@click.option("--scopes", "-x", is_flag=True, default=False, help="Include scopes in synonym list")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Emits debug information")
def search(query, db, type, ontology, max_results, scores, extended, scopes, verbose):
    """Search for terms in the ontology database."""

    try:
        results = core_search(query, db=db, type=type, ontology=ontology, k=max_results, verbose=verbose)
        
        # format results like so:
        # 1. MONDO:0001234
        #    name: Some Disease
        #    synonyms: synonym1 | synonym2 | synonym3

        for idx, result in enumerate(results.iter_rows(named=True)):
            print(f"{idx+1}. {result['term_id']}")
            print(f"    name: {result['name']}")

            # if extended=True, include ontology and type
            if extended:
                print(f"    ontology: {result['ontology']}")
                print(f"    type: {result['type']}")
            
            # if scores=True, include score
            if scores:
                print(f"    score: {result['score']:.4f}")

            # if scopes=True, include synonyms' scopes in the synonym list
            # otherwise, just return the pipe-delimited list of synonyms
            if result['synonyms']:
                syns = " | ".join([
                    f"{s[0]} (scope: {s[1]})" if scopes else s[0]
                    for s in result['synonyms']
                ])
                print(f"    synonyms: {syns}")
            # add newline at end of output
            print("")

    except NoResultsFound as e:
        print(e)
