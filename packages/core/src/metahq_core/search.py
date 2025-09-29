import os
from pathlib import Path
import re
import duckdb
import polars as pl
from rank_bm25 import BM25Plus

TABLE_DOCS = "ontology_search_docs"

class NoResultsFound(Exception):
    pass

# weights for building doc_text, which is used for BM25 indexing
NAME_WEIGHT = int(os.environ.get("NAME_WEIGHT", "10"))

SCOPE_WEIGHTS = {
    "EXACT": int(os.environ.get("SCOPE_WEIGHT_EXACT", "8")),
    "NARROW": int(os.environ.get("SCOPE_WEIGHT_NARROW", "7")),
    "BROAD": int(os.environ.get("SCOPE_WEIGHT_BROAD", "3")),
    "RELATED": int(os.environ.get("SCOPE_WEIGHT_RELATED", "1")),
}
# per the OBO 1.4 spec, synonyms with no scope are treated as RELATED
DEFAULT_SCOPE = "RELATED"

def doc_text_for(name: str, syns: list[dict]) -> str:
    """
    Build the doc_text column for BM25 indexing from the name and synonyms.

    See the NAME_WEIGHT and SCOPE_WEIGHTS constants for how parts of the record
    are weighted in the resulting document.

    Per the OBO 1.4 spec, synonyms can have scopes in {EXACT, BROAD, NARROW,
    RELATED}. If no scope is given, it is treated as RELATED.

    :param name: The primary name of the term.
    :param syns: List of {"text": str, "scope": str|None} synonym entries.
    :returns: A string suitable for BM25 indexing.
    """
    parts = []

    if name:
        parts.extend([name] * NAME_WEIGHT)

    for s in syns:
        if not s["text"]:
            continue
        weight = SCOPE_WEIGHTS.get(s["scope"], SCOPE_WEIGHTS["RELATED"])
        parts.extend([s["text"]] * weight)
    
    return " \n ".join(parts)

def search(query: str, db: Path=None, k: int=20, type: str=None, ontology: str=None, verbose: bool=False) -> pl.DataFrame:
    """
    Given a query string, return the top k hits from the ontology search index.

    The search index is built from the ontology terms' names and synonyms, where
    names are weighted more heavily than synonyms. The search uses the BM25+ algorithm
    to rank the results.

    :param query: the query string
    :param db: path to the DuckDB database file, or None to use the default location
    :param k: the number of top hits to return
    :param type: if given, restrict results to this type (e.g. "celltype", "disease", or "tissue")
    :param ontology: if given, restrict results to this ontology (e.g. "CL", "UBERON", or "MONDO")
    :param verbose: if True, print debug information
    :return: a polars DataFrame with columns: term_id, ontology, name, type, synonyms, score
    """

    # if db is None, use the Config to get the default location
    if db is None:
        from metahq_core.util.supported import get_ontology_search_db
        db = str(get_ontology_search_db())

    with duckdb.connect(db) as con:
        # 1) Load docs (one per term)

        # construct SQL to get all possible documents, including building the WHERE clause if needed
        if type or ontology:
            clauses = [
                f"type='{type}'" if type else None,
                f"ontology='{ontology}'" if ontology else None,
            ]

            where = "WHERE " + (" AND ".join(x for x in clauses if x is not None))
        else:
            where = ""

        # in addition to fetching the normal info about each entity, we also
        # compute the column 'doc_text' on the fly.

        # we do this because rank_bm25 doesn't support field weights, so we
        # simulate it by repeating terms in the document.

        # we previously precomputed this column as 'doc_text' during the db
        # build, but it blows up the size of the db quite a bit, and it's not
        # that expensive to compute on the fly. we can also modify the weights
        # at query time with this approach.
        
        sql = f"""
        SELECT term_id, name, ontology, type,
            (
                SELECT REPEAT(name || ' ', {NAME_WEIGHT}) || ' ' ||
                (
                    SELECT COALESCE(
                        STRING_AGG(
                            REPEAT(
                                synonym || ' ', 
                                CASE scope
                                    WHEN 'EXACT' THEN {SCOPE_WEIGHTS['EXACT']}
                                    WHEN 'NARROW' THEN {SCOPE_WEIGHTS['NARROW']}
                                    WHEN 'BROAD' THEN {SCOPE_WEIGHTS['BROAD']}
                                    WHEN 'RELATED' THEN {SCOPE_WEIGHTS['RELATED']}
                                    ELSE {SCOPE_WEIGHTS['RELATED']} -- treat unknown scope as RELATED
                                END
                            ), ' '
                        ), ''
                    )
                )
                FROM ontology_synonyms
                WHERE term_id = ontology_search_docs.term_id
            ) AS doc_text
        FROM {TABLE_DOCS} {where}
        """

        if verbose:
            print(f"SQL: {sql}")
        
        # execute the query and get the results as a polars DataFrame
        try:
            # load into a polars DataFrame here
            # (we catch ValueError here as an indication that there weren't any
            # results, since we have no way of knowing if there are no results
            # until we try to convert to a DataFrame)
            df = con.execute(sql).pl()
        except ValueError:
            raise NoResultsFound(f"No entities matched the filters: ontology={ontology}, type={type}")
        
        # 2) Tokenize the corpus
        tokenize = lambda s: re.findall(r"[A-Za-z0-9]+", s.lower())
        corpus_tokens = [tokenize(t) for t in df["doc_text"].to_list()]

        # 3) Fit BM25Plus (BM25Okapi has issues with doc lengths biasing results)
        # bm25 = BM25Okapi(corpus_tokens)
        bm25 = BM25Plus(
            corpus_tokens,
            k1=1.2, b=0.8,
            delta=0.5
        )

        # 4) Tokenize the query, apply BM25 to get scores, and return the top k hits
        q_tokens = tokenize(query)
        scores = [x for x in bm25.get_scores(q_tokens) if x > 0]

        # if we have no scores > 0, then there are no results
        if len(scores) == 0:
            raise NoResultsFound(f"No results found for query: '{query}'")
        
        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:k]

        # for each hit, query for and return its synonyms ordered by scope specificity, then alphabetically
        # this could probably be optimized by doing a single query for all synonyms
        # but this is simpler and k is generally small
        result_synonyms = {}
        for i in top_idx:
            term_id = df["term_id"][i]
            syn_sql = f"""
            SELECT synonym, scope
            FROM ontology_synonyms
            WHERE term_id='{term_id}'
            ORDER BY CASE scope
                WHEN 'EXACT' THEN 0
                WHEN 'BROAD' THEN 1
                WHEN 'NARROW' THEN 2
                WHEN 'RELATED' THEN 3
                ELSE 9
            END, synonym
            """
            
            try:
                syn_df = pl.from_arrow(con.execute(syn_sql).arrow())
                result_synonyms[term_id] = [
                    (r["synonym"], r["scope"])
                    for r in syn_df.iter_rows(named=True)
                ]
            except ValueError:
                # no synonyms for this term
                result_synonyms[term_id] = []

    return pl.DataFrame({
        "term_id":  [ df["term_id"][i]                          for i in top_idx ],
        "ontology": [ df["ontology"][i]                         for i in top_idx ],
        "name":     [ df["name"][i]                             for i in top_idx ],
        "type":     [ df["type"][i]                             for i in top_idx ],
        "synonyms": [ result_synonyms.get(df["term_id"][i], []) for i in top_idx ],
        "score":    [ float(scores[i])                          for i in top_idx ],
    })
