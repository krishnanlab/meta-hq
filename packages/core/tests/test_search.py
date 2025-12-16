"""
Test the search functionality of MetaHQ.

This module tests the search functionality that allows users to query
ontological terms by name and synonyms using BM25+ ranking.

These are unit tests that mock the DuckDB database connection to avoid
dependency on actual database files while maintaining full test coverage
of the search logic, BM25 ranking, and result processing.

Authors: Faisal Alquaddoomi, Parker Hicks
Date: 2025-09-25

Last updated: 2025-12-16 by Parker Hicks
"""

import re
from unittest.mock import patch

import polars as pl
import pytest

from metahq_core.search import NoResultsFound, search

# Tests use mocked database connections - the path doesn't matter
MOCK_DB_PATH = "mock_search.duckdb"


# ===== Mock Classes =====


class MockQueryResult:
    """
    Mock DuckDB query result that can be converted to Polars DataFrame.

    Simulates the result of con.execute(sql) which has a .pl() method
    to convert the result to a Polars DataFrame.
    """

    def __init__(self, data, result_type="terms"):
        """
        Initialize mock query result.

        Args:
            data: List of dicts (for term data) or list of tuples (for synonym data)
            result_type: Either "terms" or "synonyms" to determine DataFrame structure
        """
        self.data = data
        self.result_type = result_type

    def pl(self):
        """Convert to Polars DataFrame."""
        if not self.data:
            # Empty result
            if self.result_type == "synonyms":
                return pl.DataFrame(schema={"synonym": pl.String, "scope": pl.String})
            else:
                return pl.DataFrame(
                    schema={
                        "term_id": pl.String,
                        "name": pl.String,
                        "ontology": pl.String,
                        "type": pl.String,
                        "doc_text": pl.String,
                    }
                )

        if self.result_type == "synonyms":
            # Synonym data: list of (synonym, scope) tuples
            return pl.DataFrame(
                {
                    "synonym": [s[0] for s in self.data],
                    "scope": [s[1] for s in self.data],
                }
            )
        else:
            # Term data: list of dicts with term_id, name, ontology, type, doc_text
            return pl.DataFrame(self.data)


class MockDuckDBConnection:
    """
    Mock DuckDB connection for testing search functionality.

    This mock intercepts SQL queries and returns appropriate mock data
    based on the query type (main search query vs synonym query) and
    any filters specified in WHERE clauses.
    """

    def __init__(self, mock_data):
        """
        Initialize mock connection.

        Args:
            mock_data: Dict with 'terms' (list of term dicts) and
                      'synonyms' (dict mapping term_id to list of synonym tuples)
        """
        self.mock_data = mock_data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, sql):
        """
        Mock execute that returns appropriate data based on query.

        Detects whether this is a synonym query or main search query
        and returns the appropriate mock data with filters applied.
        """
        # Detect if this is a synonym query
        if "FROM ontology_synonyms" in sql and "WHERE term_id=" in sql:
            # Extract term_id from synonym query
            term_id = self._extract_term_id_from_query(sql)
            synonym_data = self.mock_data["synonyms"].get(term_id, [])
            return MockQueryResult(synonym_data, result_type="synonyms")
        else:
            # Main search query - apply filters and return matching terms
            filtered_terms = self._filter_terms_by_query(sql)
            return MockQueryResult(filtered_terms, result_type="terms")

    def _extract_term_id_from_query(self, sql):
        """Extract term_id from synonym query like WHERE term_id='MONDO:0005068'."""
        match = re.search(r"term_id\s*=\s*'([^']+)'", sql)
        return match.group(1) if match else None

    def _filter_terms_by_query(self, sql):
        """
        Filter mock terms based on WHERE clause in SQL.

        Parses type and ontology filters from the SQL WHERE clause
        and returns only matching terms.
        """
        terms = list(self.mock_data["terms"])

        # Parse type filter: type='disease'
        type_match = re.search(r"type\s*=\s*'([^']+)'", sql)
        if type_match:
            filter_type = type_match.group(1)
            terms = [t for t in terms if t["type"] == filter_type]

        # Parse ontology filter: ontology='MONDO'
        onto_match = re.search(r"ontology\s*=\s*'([^']+)'", sql)
        if onto_match:
            filter_onto = onto_match.group(1)
            terms = [t for t in terms if t["ontology"] == filter_onto]

        return terms


# ===== Fixtures =====


@pytest.fixture
def comprehensive_mock_data():
    """
    Comprehensive mock data covering all test scenarios.

    This fixture provides ontology terms and synonyms that satisfy
    all 14 tests in the TestSearch class. The doc_text is constructed
    to simulate the weighted repetition of names and synonyms used
    in the actual search implementation.

    Returns:
        Dict with 'terms' (list of dicts) and 'synonyms' (dict)
    """
    return {
        "terms": [
            # MONDO disease terms
            {
                "term_id": "MONDO:0005068",
                "name": "myocardial infarction",
                "ontology": "MONDO",
                "type": "disease",
                "doc_text": (
                    "myocardial infarction " * 10
                    + "heart attack " * 8
                    + "cardiac infarction " * 8
                    + "MI " * 1
                    + "myocardial infarct " * 7
                ),
            },
            {
                "term_id": "MONDO:0005550",
                "name": "infectious disease",
                "ontology": "MONDO",
                "type": "disease",
                "doc_text": "infectious disease " * 10 + "infection " * 3,
            },
            {
                "term_id": "MONDO:0045024",
                "name": "heart disease",
                "ontology": "MONDO",
                "type": "disease",
                "doc_text": "heart disease " * 10 + "cardiac disease " * 8,
            },
            {
                "term_id": "MONDO:0000001",
                "name": "disease",
                "ontology": "MONDO",
                "type": "disease",
                "doc_text": "disease " * 10,
            },
            # CL celltype terms
            {
                "term_id": "CL:0000182",
                "name": "hepatocyte",
                "ontology": "CL",
                "type": "celltype",
                "doc_text": "hepatocyte " * 10
                + "liver cell " * 8
                + "hepatic cell " * 1,
            },
            {
                "term_id": "CL:0000066",
                "name": "epithelial cell",
                "ontology": "CL",
                "type": "celltype",
                "doc_text": "epithelial cell " * 10,
            },
            {
                "term_id": "CL:0000000",
                "name": "cell",
                "ontology": "CL",
                "type": "celltype",
                "doc_text": "cell " * 10,
            },
            # UBERON tissue terms
            {
                "term_id": "UBERON:0000955",
                "name": "brain",
                "ontology": "UBERON",
                "type": "tissue",
                "doc_text": "brain " * 10 + "encephalon " * 8,
            },
            {
                "term_id": "UBERON:0000948",
                "name": "heart",
                "ontology": "UBERON",
                "type": "tissue",
                "doc_text": "heart " * 10 + "cardiac tissue " * 1,
            },
            {
                "term_id": "UBERON:0000178",
                "name": "blood",
                "ontology": "UBERON",
                "type": "tissue",
                "doc_text": "blood " * 10
                + "heart " * 2,  # Contains "heart" for unfiltered test
            },
        ],
        "synonyms": {
            "MONDO:0005068": [
                ("heart attack", "EXACT"),
                ("cardiac infarction", "EXACT"),
                ("myocardial infarct", "NARROW"),
                ("MI", "RELATED"),
            ],
            "CL:0000182": [
                ("liver cell", "EXACT"),
                ("hepatic cell", "RELATED"),
            ],
            "MONDO:0005550": [
                ("infection", "BROAD"),
            ],
            "MONDO:0045024": [
                ("cardiac disease", "EXACT"),
            ],
            "UBERON:0000955": [
                ("encephalon", "EXACT"),
            ],
            "UBERON:0000948": [
                ("cardiac tissue", "RELATED"),
            ],
            "MONDO:0000001": [],
            "CL:0000066": [],
            "CL:0000000": [],
            "UBERON:0000178": [],
        },
    }


@pytest.fixture
def mock_duckdb_connect(comprehensive_mock_data):
    """
    Fixture that patches duckdb.connect to return a mock connection.

    This mock connection handles both main search queries and synonym queries,
    applying filters as specified in the SQL WHERE clauses.

    Args:
        comprehensive_mock_data: The mock data fixture

    Yields:
        Mock object for duckdb.connect
    """

    def create_mock_connection(db_path):
        return MockDuckDBConnection(comprehensive_mock_data)

    with patch(
        "metahq_core.search.duckdb.connect", side_effect=create_mock_connection
    ) as mock:
        yield mock


class TestSearch:
    """Test class for search functionality."""

    def test_heart_attack_search(self, mock_duckdb_connect):
        """
        Test searching for 'heart attack' in MONDO disease ontology.

        Should return myocardial infarction as the top result with
        'heart attack' as a synonym.
        """
        results = search(
            query="heart attack",
            db=MOCK_DB_PATH,
            type="disease",
            ontology="MONDO",
            k=3,
        )

        # Verify we get results
        assert len(results) > 0, "No results returned for 'heart attack' query"
        assert len(results) <= 3, "Too many results returned"

        # Check that results are a polars DataFrame with expected columns
        expected_columns = {"term_id", "ontology", "name", "type", "synonyms", "score"}
        assert set(results.columns) == expected_columns

        # Convert to list of dicts for easier testing
        results_list = results.to_dicts()

        # Verify all results are from MONDO ontology and disease type
        for result in results_list:
            assert result["ontology"] == "MONDO"
            assert result["type"] == "disease"
            assert result["term_id"].startswith("MONDO:")

        # Check that myocardial infarction is in the results (should be top result)
        names = [result["name"].lower() for result in results_list]
        assert any(
            "myocardial infarction" in name for name in names
        ), "Expected 'myocardial infarction' in results"

        # Check that heart attack appears as a synonym in at least one result
        has_heart_attack_synonym = False
        for result in results_list:
            synonyms = result["synonyms"]
            if synonyms:
                synonym_text = " ".join([syn[0].lower() for syn in synonyms])
                if "heart attack" in synonym_text:
                    has_heart_attack_synonym = True
                    break

        assert (
            has_heart_attack_synonym
        ), "Expected 'heart attack' to appear as a synonym in search results"

    def test_hepatocyte_search(self, mock_duckdb_connect):
        """
        Test searching for 'hepatocyte' in CL celltype ontology.

        Should return hepatocyte and related hepatocyte cell types.
        """
        results = search(
            query="hepatocyte", db=MOCK_DB_PATH, type="celltype", ontology="CL", k=3
        )

        # Verify we get results
        assert len(results) > 0, "No results returned for 'hepatocyte' query"
        assert len(results) <= 3, "Too many results returned"

        # Check that results are a polars DataFrame with expected columns
        expected_columns = {"term_id", "ontology", "name", "type", "synonyms", "score"}
        assert set(results.columns) == expected_columns

        # Convert to list of dicts for easier testing
        results_list = results.to_dicts()

        # Verify all results are from CL ontology and celltype type
        for result in results_list:
            assert result["ontology"] == "CL"
            assert result["type"] == "celltype"
            assert result["term_id"].startswith("CL:")

        # Check that hepatocyte is in the results (should be top result)
        names = [result["name"].lower() for result in results_list]
        assert any(
            "hepatocyte" in name for name in names
        ), "Expected 'hepatocyte' in results"

        # Verify the top result is likely the exact match
        top_result = results_list[0]
        assert (
            "hepatocyte" in top_result["name"].lower()
        ), "Expected top result to contain 'hepatocyte'"

    def test_search_with_no_filters(self, mock_duckdb_connect):
        """Test search functionality without type or ontology filters."""
        results = search(query="heart", db=MOCK_DB_PATH, k=5)

        assert len(results) > 0, "No results returned for unfiltered search"
        assert len(results) <= 5, "Too many results returned"

        # Should get results from multiple ontologies and types
        results_list = results.to_dicts()
        ontologies = {result["ontology"] for result in results_list}
        types = {result["type"] for result in results_list}

        # With a broad search term like "heart", we should get diverse results
        assert len(ontologies) >= 1, "Expected results from at least one ontology"
        assert len(types) >= 1, "Expected results from at least one type"

    def test_search_with_type_filter_only(self, mock_duckdb_connect):
        """Test search with only type filter (no ontology filter)."""
        results = search(query="brain", db=MOCK_DB_PATH, type="tissue", k=3)

        assert len(results) > 0, "No results returned for type-filtered search"

        results_list = results.to_dicts()
        for result in results_list:
            assert result["type"] == "tissue"

    def test_search_with_ontology_filter_only(self, mock_duckdb_connect):
        """Test search with only ontology filter (no type filter)."""
        results = search(query="infection", db=MOCK_DB_PATH, ontology="MONDO", k=3)

        assert len(results) > 0, "No results returned for ontology-filtered search"

        results_list = results.to_dicts()
        for result in results_list:
            assert result["ontology"] == "MONDO"
            assert result["term_id"].startswith("MONDO:")

    def test_search_no_results_found(self, mock_duckdb_connect):
        """Test that NoResultsFound exception is raised when no matches exist."""
        with pytest.raises(NoResultsFound):
            search(
                query="zxvadsvascasdffdads",
                db=MOCK_DB_PATH,
                type="disease",
                ontology="MONDO",
                k=3,
            )

    def test_search_invalid_filters(self, mock_duckdb_connect):
        """Test search with filters that match no entities."""
        with pytest.raises(NoResultsFound):
            search(
                query="heart",
                db=MOCK_DB_PATH,
                type="nonexistent_type",
                ontology="MONDO",
                k=3,
            )

    def test_search_results_structure(self, mock_duckdb_connect):
        """Test that search results have the expected structure and data types."""
        results = search(query="disease", db=MOCK_DB_PATH, k=2)

        assert isinstance(results, pl.DataFrame), "Results should be a polars DataFrame"

        # Check column names and types
        expected_columns = ["term_id", "ontology", "name", "type", "synonyms", "score"]
        assert results.columns == expected_columns

        # Check data types
        results_list = results.to_dicts()
        if results_list:
            result = results_list[0]
            assert isinstance(result["term_id"], str)
            assert isinstance(result["ontology"], str)
            assert isinstance(result["name"], str)
            assert isinstance(result["type"], str)
            assert isinstance(result["synonyms"], list)
            assert isinstance(result["score"], float)

            # Check synonyms structure (should be list of tuples)
            if result["synonyms"]:
                for synonym in result["synonyms"]:
                    assert isinstance(synonym, list)
                    assert len(synonym) == 2
                    assert isinstance(synonym[0], str)  # synonym text
                    assert isinstance(synonym[1], str)  # scope

    def test_search_score_ordering(self, mock_duckdb_connect):
        """Test that search results are ordered by score (highest first)."""
        results = search(query="heart disease", db=MOCK_DB_PATH, k=5)

        if len(results) > 1:
            scores = results["score"].to_list()
            # Scores should be in descending order
            assert scores == sorted(
                scores, reverse=True
            ), "Results should be ordered by score (highest first)"

    def test_search_k_parameter(self, mock_duckdb_connect):
        """Test that the k parameter correctly limits the number of results."""
        for k in [1, 3, 5]:
            results = search(query="cell", db=MOCK_DB_PATH, k=k)

            assert (
                len(results) <= k
            ), f"Expected at most {k} results, got {len(results)}"

    def test_search_synonyms_format(self, mock_duckdb_connect):
        """Test that synonyms are properly formatted with scope information."""
        results = search(
            query="myocardial infarction",
            db=MOCK_DB_PATH,
            type="disease",
            ontology="MONDO",
            k=1,
        )

        if len(results) > 0:
            result = results.to_dicts()[0]
            synonyms = result["synonyms"]

            if synonyms:
                # Each synonym should be a tuple of (text, scope)
                for synonym in synonyms:
                    assert isinstance(synonym, list)
                    assert len(synonym) == 2
                    text, scope = synonym
                    assert isinstance(text, str)
                    assert isinstance(scope, str)
                    # Scope should be one of the known values
                    assert scope in ["EXACT", "BROAD", "NARROW", "RELATED"]

    @pytest.mark.parametrize(
        "query,expected_ontology,expected_type",
        [
            ("heart attack", "MONDO", "disease"),
            ("hepatocyte", "CL", "celltype"),
            ("brain", "UBERON", "tissue"),
        ],
    )
    def test_search_examples_parametrized(
        self, mock_duckdb_connect, query, expected_ontology, expected_type
    ):
        """Parametrized test for different search examples."""
        results = search(
            query=query,
            db=MOCK_DB_PATH,
            type=expected_type,
            ontology=expected_ontology,
            k=3,
        )

        assert len(results) > 0, f"No results for query '{query}'"

        results_list = results.to_dicts()
        for result in results_list:
            assert result["ontology"] == expected_ontology
            assert result["type"] == expected_type
