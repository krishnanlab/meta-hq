#!/usr/bin/env python3

"""
Build a DuckDB database with entries from the names_synonyms.json files
for MONDO, UBERON, and CL.

The `ontology_search_docs` table is used for BM25 searches; it weighs
the name and synonyms by simply repeating each element by its weight.
Since BM25 uses TF/IDF, this results in more repetitious elements
effectively being more 'important' in the document and thus more likely
to match a query that includes them.

Tables created in resulting DuckDB database:
  - ontology_terms(id, name, ontology, type)
  - ontology_synonyms(term_id, synonym, scope) # foreign-key'd to ontology_terms.id
  - ontology_search_docs(term_id, name, ontology, type, doc_text)

Example basic DuckDB queries (these require the duckdb CLI, see https://duckdb.org/docs/installation/):
- duckdb ontologies.duckdb -c "SELECT ontology, type, COUNT(*) FROM ontology_terms GROUP BY 1,2 ORDER BY 2,1;"
- duckdb ontologies.duckdb -c "SELECT * FROM ontology_synonyms WHERE term_id='CL:0000540' LIMIT 10;"
- duckdb ontologies.duckdb -c "SELECT * FROM ontology_search_docs WHERE ontology='UBERON' AND type='tissue' LIMIT 5;"
"""

import json
import os
from pathlib import Path
from typing import Iterable

import click
import duckdb
import polars as pl


# =============================================================================
# === constants
# =============================================================================

# names of tables in the DuckDB database
TABLE_TERMS = "ontology_terms"
TABLE_SYNS = "ontology_synonyms"
TABLE_DOCS = "ontology_search_docs"

# per the OBO 1.4 spec, synonyms with no scope are treated as RELATED
DEFAULT_SCOPE = "RELATED"

# maps ontology prefix to type
# any ontology not listed here will get type 'none'
TYPE_BY_ONTOLOGY = {
    "MONDO": "disease",
    "UBERON": "tissue",
    "CL": "celltype",
}

# =============================================================================
# === helpers for building dataframes, generating synonym columns, etc.
# =============================================================================

# break out syn_list into syn_ columns by scope, e.g., syn_exact, syn_broad, etc.
def _syns_by_scope(scope: str) -> pl.Expr:
    """
    Returns a polars expression that extracts synonyms of the given scope from
    the syn_list column, which is a list of structs with fields "synonym" and "scope".
    """

    return (
        pl
        .when(pl.col("syn_list").is_null() | (pl.col("syn_list").list.len() == 0))
        .then(pl.lit([], dtype=pl.List(pl.Utf8))) # return empty list if no synonyms
        .otherwise(
            pl.col("syn_list")
            .list.eval(
                pl.when(pl.element().struct.field("scope") == scope)
                .then(pl.element().struct.field("synonym"))
                .otherwise(None)
            )
            .list.drop_nulls()
        ) 
        .alias(f"syn_{scope.lower()}")
        # ^ extract synonyms of the given scope and name the column syn_<scope>
    )

def build_dataframes(
    namelists: list[dict[dict]]
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Given a series of name/synonym dicts (from MONDO, UBERON+CL) of the following form, build
    Polars DataFrames for terms, synonyms, and BM25 docs.

    "synonyms" in the below term definition are optional; if unspecified,
    they default to an empty list.

    {
        <term_id>: {
            "name": str,
            "synonyms": [
                {"text": str, "scope": str},
                ...
            ]
        },
        ...
    }

    :param namelists: list of name/synonym dicts, one per ontology
    :param strategy: list of strategies to use for building the doc_text table
    :returns: (df_terms, df_syns, df_docs)
    """
    term_rows = []
    syn_rows = []

    for namelist in namelists:
        for term_id, entry in namelist.items():
            name = entry.get("name")
            synonyms = entry.get("synonyms", [])

            # skip terms with no name
            if not name:
                print(f"Warning: term {term_id} has no name, skipping")
                continue

            # determine ontology and type
            # ont is one of MONDO, UBERON, CL, other (if COLLAPSE_ALTERNATE_ONTOLOGIES)
            # type is one of disease, tissue, celltype
            ont = term_id.split(":")[0]
            type = TYPE_BY_ONTOLOGY.get(ont, "none")

            term_rows.append({
                "id": term_id,
                "name": entry.get("name"),
                "ontology": ont,
                "type": type,
            })

            for syn in synonyms:
                syn_rows.append({
                    "term_id": term_id,
                    "synonym": syn["text"],
                    "scope": syn.get("scope", DEFAULT_SCOPE),  # EXACT/BROAD/NARROW/RELATED or None
                })

    df_terms = pl.DataFrame(term_rows)

    df_syns  = pl.DataFrame(syn_rows) if syn_rows else pl.DataFrame(
        {
            "term_id": pl.Series([], pl.Utf8),
            "synonym": pl.Series([], pl.Utf8),
            "scope":   pl.Series([], pl.Utf8)
         }
    )

    # merge synonyms into a list for each term (Polars group_by then join back)
    syn_grouped = (
        df_syns
            .group_by("term_id")
            .agg(pl.struct(["synonym", "scope"]).alias("syn_list"))
    ) if df_syns.height > 0 else pl.DataFrame({"term_id": [], "syn_list": []})

    df_terms_plus = df_terms.join(syn_grouped, left_on="id", right_on="term_id", how="left")
    df_terms_plus = df_terms_plus.with_columns(
        pl.col("syn_list").fill_null([])
    )

    # add columns for each synonym scope if using SYNCOLS strategy
    df_terms_plus = df_terms_plus.with_columns(
        _syns_by_scope("EXACT"),
        _syns_by_scope("NARROW"),
        _syns_by_scope("BROAD"),
        _syns_by_scope("RELATED"),
    )

    # build the docs table from the terms + synonym columns
    df_docs = pl.DataFrame({
        "term_id": df_terms_plus["id"],
        "ontology": df_terms_plus["ontology"],
        "type": df_terms_plus["type"],
        "name": df_terms_plus["name"],
        "syn_exact": df_terms_plus["syn_exact"],
        "syn_narrow": df_terms_plus["syn_narrow"],
        "syn_broad": df_terms_plus["syn_broad"],
        "syn_related": df_terms_plus["syn_related"],
    })

    return df_terms, df_syns, df_docs

def quote_ident(name: str) -> str:
    # double up internal quotes per SQL rules
    return '"' + name.replace('"', '""') + '"'


# =============================================================================
# === entrypoint
# =============================================================================

@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--mondo",  type=click.Path(exists=True, dir_okay=False, path_type=Path), required=False, help="Path to mondo's names_synonyms.json")
@click.option("--uberon-cl", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=False, help="Path to uberon/CL's names_synonyms.json")
@click.option("--out-db", type=click.Path(dir_okay=False, path_type=Path), required=False, show_default=True, help="Output DuckDB database path")
def cli(
    mondo: Path, uberon_cl: Path,
    out_db: Path
):
    """
    Build DuckDB tables to support ontology term + name + synonym searching and retrieval.
    """

    click.echo("Loading name/synonym lists...")

    # if lists aren't provided, infer their location from the Config
    if not mondo or not uberon_cl:
        from metahq_core.util.supported import get_ontology_dirs

        mondo = mondo or get_ontology_dirs("mondo") / "names_synonyms.json"
        uberon_cl = uberon_cl or get_ontology_dirs("uberon") / "names_synonyms.json"

    # if an out_db isn't provided, use the default location
    if not out_db:
        from metahq_core.util.supported import get_ontology_search_db
        out_db = get_ontology_search_db()

    # load the JSON files
    with open(mondo, "r") as fp:
        mondo_list = json.load(fp)
    with open(uberon_cl, "r") as fp:
        uberon_cl_list = json.load(fp)

    click.echo("Collecting terms + synonyms...")
    df_terms, df_syns, df_docs = build_dataframes([mondo_list, uberon_cl_list])

    click.echo(f"Writing DuckDB → {out_db} ...")

    with duckdb.connect(str(out_db)) as conn:
        conn.register("df_terms", df_terms.to_arrow())
        conn.register("df_syns",  df_syns.to_arrow())
        conn.register("df_docs",  df_docs.to_arrow())

        conn.execute(f"CREATE OR REPLACE TABLE {quote_ident(TABLE_TERMS)} AS SELECT * FROM df_terms")
        conn.execute(f"CREATE OR REPLACE TABLE {quote_ident(TABLE_SYNS)}  AS SELECT * FROM df_syns")
        conn.execute(f"CREATE OR REPLACE TABLE {quote_ident(TABLE_DOCS)}  AS SELECT * FROM df_docs")

        # generate full-text indices on columns of the docs table for
        # use with match_bm25().

        # since we're dealing with phrases and not sentences, we disable
        # stemming, stopword removal, and have a more lenient filter (i.e.,
        # ignore anything that's not alphanumeric, +, /, or -).

        conn.execute(f"""
        PRAGMA create_fts_index('{quote_ident(TABLE_DOCS)}', 'term_id',
            'name',
            'syn_exact',
            'syn_narrow',
            'syn_broad',
            'syn_related',
            stemmer = 'none', stopwords = 'none', ignore = '([^0-9a-z+/-])+',
            strip_accents = 1, lower = 1, overwrite = 1)
        """)

        # create a macro called 'ont_search' for performing weighted searches
        # of the ontology via duckdb. see scripts/search_macro.sql for details,
        # but the gist is that you can invoke the following to do simple searches:
        #  SELECT * FROM ont_search('heart attack', 10, 'MONDO', 'disease');
        with open("scripts/search_macro.sql", "r") as fp:
            sql = fp.read()
            conn.execute(sql)

    click.echo(f"Done. Terms/Docs: {df_terms.height:,} | Synonyms: {df_syns.height:,} | Docs: {df_docs.height:,}")

if __name__ == "__main__":
    cli()
