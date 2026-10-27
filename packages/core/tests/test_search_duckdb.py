"""
Test the search functionality of MetaHQ against a real DuckDB database.

test_search.py mocks the DuckDB connection, so it can't catch changes to
the DuckDB Python API itself (e.g. DuckDB 2.0 removing `.pl()` from
`DuckDBPyConnection`, issue #113). These tests build a small on-disk
database with the same tables as the ontology search index and run
`search` against it.

Authors: Emily Boyer
Date: 2026-10-26
"""

import logging

import duckdb
import pytest

from metahq_core.search import NoResultsFound, search


@pytest.fixture
def search_db(tmp_path):
    """Small ontology search index with the tables `search` queries."""
    db_path = tmp_path / "ontology_search.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.sql(
            "CREATE TABLE ontology_search_docs "
            "(term_id VARCHAR, name VARCHAR, ontology VARCHAR, type VARCHAR)"
        )
        con.sql(
            "CREATE TABLE ontology_synonyms "
            "(term_id VARCHAR, synonym VARCHAR, scope VARCHAR)"
        )
        con.sql("""
            INSERT INTO ontology_search_docs VALUES
                ('MONDO:0005068', 'myocardial infarction', 'MONDO', 'disease'),
                ('MONDO:0005015', 'diabetes mellitus', 'MONDO', 'disease'),
                ('UBERON:0000948', 'heart', 'UBERON', 'tissue'),
                ('CL:0000182', 'hepatocyte', 'CL', 'celltype')
            """)
        con.sql("""
            INSERT INTO ontology_synonyms VALUES
                ('MONDO:0005068', 'heart attack', 'EXACT'),
                ('MONDO:0005068', 'MI', 'RELATED'),
                ('UBERON:0000948', 'cardiac muscle organ', 'RELATED')
            """)
    return db_path


@pytest.fixture
def logger():
    return logging.getLogger("test_search_duckdb")


class TestSearchRealDuckDB:
    """Run `search` end to end on a real DuckDB file."""

    def test_returns_best_match_with_synonyms(self, search_db, logger):
        top = search("heart attack", db=search_db, k=2, logger=logger).to_dicts()[0]

        assert top["term_id"] == "MONDO:0005068"
        assert ["heart attack", "EXACT"] in top["synonyms"]

    def test_filters_by_ontology(self, search_db, logger):
        result = search("heart", db=search_db, k=5, ontology="UBERON", logger=logger)

        assert result["term_id"].to_list() == ["UBERON:0000948"]

    def test_term_without_synonyms(self, search_db, logger):
        top = search("hepatocyte", db=search_db, k=1, logger=logger).to_dicts()[0]

        assert top["term_id"] == "CL:0000182"
        assert top["synonyms"] == []

    def test_no_matching_filters_raises(self, search_db, logger):
        with pytest.raises(NoResultsFound):
            search("heart", db=search_db, ontology="HP", logger=logger)
