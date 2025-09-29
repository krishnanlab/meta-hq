"""
Test the search functionality of MetaHQ.

This module tests the search functionality that allows users to query
ontological terms by name and synonyms using BM25+ ranking.

Author: Faisal Alquaddoomi
Date: 2025-09-25
"""

import polars as pl
import pytest

from metahq_core.search import search, NoResultsFound
from metahq_core.util.supported import get_ontology_search_db

# Path to the database file
DEFAULT_DB = get_ontology_search_db()


class TestSearch:
    """Test class for search functionality."""

    def test_heart_attack_search(self):
        """
        Test searching for 'heart attack' in MONDO disease ontology.
        
        Should return myocardial infarction as the top result with 
        'heart attack' as a synonym.
        """
        results = search(
            query="heart attack",
            db=str(DEFAULT_DB),
            type="disease",
            ontology="MONDO",
            k=3
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
        assert any("myocardial infarction" in name for name in names), \
            "Expected 'myocardial infarction' in results"
        
        # Check that heart attack appears as a synonym in at least one result
        has_heart_attack_synonym = False
        for result in results_list:
            synonyms = result["synonyms"]
            if synonyms:
                synonym_text = " ".join([syn[0].lower() for syn in synonyms])
                if "heart attack" in synonym_text:
                    has_heart_attack_synonym = True
                    break
        
        assert has_heart_attack_synonym, \
            "Expected 'heart attack' to appear as a synonym in search results"

    def test_hepatocyte_search(self):
        """
        Test searching for 'hepatocyte' in CL celltype ontology.
        
        Should return hepatocyte and related hepatocyte cell types.
        """
        results = search(
            query="hepatocyte",
            db=str(DEFAULT_DB),
            type="celltype",
            ontology="CL",
            k=3
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
        assert any("hepatocyte" in name for name in names), \
            "Expected 'hepatocyte' in results"
        
        # Verify the top result is likely the exact match
        top_result = results_list[0]
        assert "hepatocyte" in top_result["name"].lower(), \
            "Expected top result to contain 'hepatocyte'"

    def test_search_with_no_filters(self):
        """Test search functionality without type or ontology filters."""
        results = search(
            query="heart",
            db=str(DEFAULT_DB),
            k=5
        )
        
        assert len(results) > 0, "No results returned for unfiltered search"
        assert len(results) <= 5, "Too many results returned"
        
        # Should get results from multiple ontologies and types
        results_list = results.to_dicts()
        ontologies = {result["ontology"] for result in results_list}
        types = {result["type"] for result in results_list}
        
        # With a broad search term like "heart", we should get diverse results
        assert len(ontologies) >= 1, "Expected results from at least one ontology"
        assert len(types) >= 1, "Expected results from at least one type"

    def test_search_with_type_filter_only(self):
        """Test search with only type filter (no ontology filter)."""
        results = search(
            query="brain",
            db=str(DEFAULT_DB),
            type="tissue",
            k=3
        )
        
        assert len(results) > 0, "No results returned for type-filtered search"
        
        results_list = results.to_dicts()
        for result in results_list:
            assert result["type"] == "tissue"

    def test_search_with_ontology_filter_only(self):
        """Test search with only ontology filter (no type filter)."""
        results = search(
            query="infection",
            db=str(DEFAULT_DB),
            ontology="MONDO",
            k=3
        )
        
        assert len(results) > 0, "No results returned for ontology-filtered search"
        
        results_list = results.to_dicts()
        for result in results_list:
            assert result["ontology"] == "MONDO"
            assert result["term_id"].startswith("MONDO:")

    def test_search_no_results_found(self):
        """Test that NoResultsFound exception is raised when no matches exist."""
        with pytest.raises(NoResultsFound):
            search(
                query="zxvadsvascasdffdads",
                db=str(DEFAULT_DB),
                type="disease",
                ontology="MONDO",
                k=3
            )

    def test_search_invalid_filters(self):
        """Test search with filters that match no entities."""
        with pytest.raises(NoResultsFound):
            search(
                query="heart",
                db=str(DEFAULT_DB),
                type="nonexistent_type",
                ontology="MONDO",
                k=3
            )

    def test_search_results_structure(self):
        """Test that search results have the expected structure and data types."""
        results = search(
            query="disease",
            db=str(DEFAULT_DB),
            k=2
        )
        
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

    def test_search_score_ordering(self):
        """Test that search results are ordered by score (highest first)."""
        results = search(
            query="heart disease",
            db=str(DEFAULT_DB),
            k=5
        )
        
        if len(results) > 1:
            scores = results["score"].to_list()
            # Scores should be in descending order
            assert scores == sorted(scores, reverse=True), \
                "Results should be ordered by score (highest first)"

    def test_search_k_parameter(self):
        """Test that the k parameter correctly limits the number of results."""
        for k in [1, 3, 5]:
            results = search(
                query="cell",
                db=str(DEFAULT_DB),
                k=k
            )
            
            assert len(results) <= k, f"Expected at most {k} results, got {len(results)}"

    def test_search_synonyms_format(self):
        """Test that synonyms are properly formatted with scope information."""
        results = search(
            query="myocardial infarction",
            db=str(DEFAULT_DB),
            type="disease",
            ontology="MONDO",
            k=1
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

    @pytest.mark.parametrize("query,expected_ontology,expected_type", [
        ("heart attack", "MONDO", "disease"),
        ("hepatocyte", "CL", "celltype"),
        ("brain", "UBERON", "tissue"),
    ])
    def test_search_examples_parametrized(self, query, expected_ontology, expected_type):
        """Parametrized test for different search examples."""
        results = search(
            query=query,
            db=str(DEFAULT_DB),
            type=expected_type,
            ontology=expected_ontology,
            k=3
        )
        
        assert len(results) > 0, f"No results for query '{query}'"
        
        results_list = results.to_dicts()
        for result in results_list:
            assert result["ontology"] == expected_ontology
            assert result["type"] == expected_type
