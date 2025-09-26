-- INSTALL fts; LOAD fts; -- if not already loaded

CREATE OR REPLACE MACRO ont_search(
  -- core arguments
  -- q: search query string
  -- k_results: number of results to return (default 20)
  q,
  k_results := 20,

  -- optional filters
  -- ontology_filter: restrict to a specific ontology (one of "MONDO", "UBERON", "CL")
  -- type_filter: restrict to a specific type (one of "disease", "tissue", "celltype")
  ontology_filter := NULL,
  type_filter     := NULL,

  -- field weights
  w_name    := 10,
  w_exact   := 8,
  w_narrow  := 7,
  w_broad   := 3,
  w_related := 1,

  -- BM25 hyperparameters
  k1 := 1.2,
  b  := 0.8,
  conjunctive := 0  -- 0 = OR (default), 1 = AND (all terms must appear)
) AS TABLE
WITH
-- Restrict the candidate set early (so we only score what we need)
base AS (
  SELECT *
  FROM ontology_search_docs d
  WHERE (ontology_filter IS NULL OR d.ontology = ontology_filter)
    AND (type_filter     IS NULL OR d."type"  = type_filter)
),
scores AS (
  SELECT
    d.term_id,
    d.name,
    d.ontology,
    d."type",

    COALESCE(fts_main_ontology_search_docs.match_bm25(d.term_id, q, 'name',        k1, b, conjunctive), 0) * w_name    AS s_name,
    COALESCE(fts_main_ontology_search_docs.match_bm25(d.term_id, q, 'syn_exact',   k1, b, conjunctive), 0) * w_exact   AS s_exact,
    COALESCE(fts_main_ontology_search_docs.match_bm25(d.term_id, q, 'syn_narrow',  k1, b, conjunctive), 0) * w_narrow  AS s_narrow,
    COALESCE(fts_main_ontology_search_docs.match_bm25(d.term_id, q, 'syn_broad',   k1, b, conjunctive), 0) * w_broad   AS s_broad,
    COALESCE(fts_main_ontology_search_docs.match_bm25(d.term_id, q, 'syn_related', k1, b, conjunctive), 0) * w_related AS s_related
  FROM base d
),
ranked AS (
  SELECT
    term_id,
    name,
    ontology,
    "type",
    (s_name + s_exact + s_narrow + s_broad + s_related) AS score
  FROM scores
  WHERE (s_name + s_exact + s_narrow + s_broad + s_related) > 0
  ORDER BY score DESC, term_id
  LIMIT k_results
),
syns AS (
  SELECT
    term_id,
    -- Order synonyms by precision, then alphabetically within each bucket
    string_agg(
      synonym, ' | '
      ORDER BY
        CASE scope
          WHEN 'EXACT'  THEN 0
          WHEN 'NARROW' THEN 1
          WHEN 'BROAD'  THEN 2
          WHEN 'RELATED'THEN 3
          ELSE 9
        END,
        synonym
    ) AS synonyms
  FROM ontology_synonyms
  GROUP BY term_id
)
SELECT
  r.term_id,
  r.name,
  r.ontology,
  r."type",
  r.score,
  COALESCE(s.synonyms, '') AS synonyms
FROM ranked r
LEFT JOIN syns s USING (term_id)
ORDER BY r.score DESC, r.term_id;

-- Example usage:
-- ============================================================================
-- Basic search, return top 10 results
-- SELECT * FROM ont_search('heart attack', 10);

-- Filter to MONDO diseases only
-- SELECT * FROM ont_search('ovarian carcinoma', 10, 'MONDO', 'disease', 5,3,2,1,1, 1.2,0.75, 0);

-- Require all query terms to appear (conjunctive), and tweak weights
-- SELECT * FROM ont_search(
--     'transient ischemic attack', 15,
--     NULL, NULL,
--     6, 4, 2, 1, 1,
--     1.2, 0.75, 1
-- );
-- ============================================================================
