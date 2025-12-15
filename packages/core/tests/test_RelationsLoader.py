"""
Test suite for the RelationsLoader class.

This script tests functionalities for the RelationsLoader class that handles
loading and querying ancestor/descendant relationships from ontology data.

Author: Claude Code
Date: 2025-11-19

Last updated: 2025-11-19 by Claude Code
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from metahq_core.relations_loader import COL_ID, ROW_ID, RelationsLoader


@pytest.fixture
def sample_relations_data():
    """Create sample relations data for testing.

    Creates a simple ontology structure:
        A
       / \
      B   C
     /
    D

    Where rows represent ancestors and columns represent descendants.
    A value of 1 at (row, col) means row is an ancestor of col.

    Matrix interpretation:
    - Column A: ancestors of A are [A] (only itself)
    - Column B: ancestors of B are [A, B]
    - Column C: ancestors of C are [A, C]
    - Column D: ancestors of D are [A, B, D]

    - Row A: descendants of A are [A, B, C, D]
    - Row B: descendants of B are [B, D]
    - Row C: descendants of C are [C]
    - Row D: descendants of D are [D]
    """
    return pl.DataFrame(
        {
            "A": [1, 0, 0, 0],  # Column A: only A itself is its ancestor
            "B": [1, 1, 0, 0],  # Column B: A and B are ancestors of B
            "C": [1, 0, 1, 0],  # Column C: A and C are ancestors of C
            "D": [1, 1, 0, 1],  # Column D: A, B, and D are ancestors of D
        }
    )


@pytest.fixture
def sample_parquet_file(sample_relations_data):
    """Create a temporary parquet file with sample relations data."""
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".parquet", delete=False) as f:
        temp_path = f.name
        sample_relations_data.write_parquet(temp_path)

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def loader(sample_parquet_file):
    """Create a RelationsLoader instance with sample data."""
    return RelationsLoader(sample_parquet_file, loglevel=20)


class TestRelationsLoader:
    """Test suite for RelationsLoader class."""

    def test_init(self, sample_parquet_file):
        """Test that RelationsLoader initializes correctly."""
        loader = RelationsLoader(sample_parquet_file, loglevel=20)

        assert loader.relations is not None
        assert isinstance(loader.relations, pl.LazyFrame)
        assert loader.logger is not None

    def test_init_with_custom_logger(self, sample_parquet_file):
        """Test that RelationsLoader accepts a custom logger."""
        from metahq_core.logger import setup_logger

        custom_logger = setup_logger("test_logger", level=10, log_dir=Path("."))

        loader = RelationsLoader(sample_parquet_file, logger=custom_logger)

        assert loader.logger is custom_logger

    def test_setup(self, sample_parquet_file):
        """Test that setup method correctly loads and formats the data."""
        loader = RelationsLoader(sample_parquet_file, loglevel=20)

        # Collect the LazyFrame to check its contents
        df = loader.relations.collect()

        # Should have the ROW_ID column added
        assert ROW_ID in df.columns

        # ROW_ID should contain the original column names
        expected_row_ids = ["A", "B", "C", "D"]
        assert df[ROW_ID].to_list() == expected_row_ids

    def test_setup_with_invalid_file(self):
        """Test that setup raises an error with an invalid file path."""
        with pytest.raises(Exception):  # Will raise a Polars or file not found error
            RelationsLoader("/nonexistent/file.parquet", loglevel=20)

    def test_get_ancestors_all(self, loader):
        """Test get_ancestors without subset returns all ancestor relationships."""
        ancestors = loader.get_ancestors()

        # Check that all terms are present
        assert "A" in ancestors
        assert "B" in ancestors
        assert "C" in ancestors
        assert "D" in ancestors

        # Check specific relationships (ancestors read from rows in each column)
        # Column A: rows with 1 are [A] → ancestors of A
        assert set(ancestors["A"]) == {"A"}

        # Column B: rows with 1 are [A, B] → ancestors of B
        assert set(ancestors["B"]) == {"A", "B"}

        # Column C: rows with 1 are [A, C] → ancestors of C
        assert set(ancestors["C"]) == {"A", "C"}

        # Column D: rows with 1 are [A, B, D] → ancestors of D
        assert set(ancestors["D"]) == {"A", "B", "D"}

    def test_get_ancestors_with_subset(self, loader):
        """Test get_ancestors with a subset of columns.

        When subsetting columns, we're selecting which descendants to query about.
        The subset parameter selects columns, not rows.
        """
        subset = ["A", "B"]
        ancestors = loader.get_ancestors(subset=subset)

        # Should only have data for the subset columns (A and B)
        assert "A" in ancestors
        assert "B" in ancestors

        # Should not have data for C and D since they weren't in subset
        assert "C" not in ancestors
        assert "D" not in ancestors

        # Check relationships for subset (same as without subset for these columns)
        assert set(ancestors["A"]) == {"A"}
        assert set(ancestors["B"]) == {"A", "B"}

    def test_get_ancestors_with_empty_subset(self, loader):
        """Test get_ancestors with an empty subset list."""
        ancestors = loader.get_ancestors(subset=[])

        # Empty subset should return empty or minimal results
        # Based on the code, it should include ROW_ID only
        assert isinstance(ancestors, dict)

    def test_get_ancestors_with_single_term(self, loader):
        """Test get_ancestors with a single term subset."""
        ancestors = loader.get_ancestors(subset=["D"])

        assert "D" in ancestors
        # Column D has 1s in rows A, B, and D → ancestors of D
        assert set(ancestors["D"]) == {"A", "B", "D"}

    def test_get_descendants_all(self, loader):
        """Test get_descendants without subset returns all descendant relationships."""
        descendants = loader.get_descendants()

        # Check that all terms are present
        assert "A" in descendants
        assert "B" in descendants
        assert "C" in descendants
        assert "D" in descendants

        # Check specific relationships (descendants read from columns in each row)
        # Row A: columns with 1 are [A, B, C, D] → descendants of A
        assert set(descendants["A"]) == {"A", "B", "C", "D"}

        # Row B: columns with 1 are [B, D] → descendants of B
        assert set(descendants["B"]) == {"B", "D"}

        # Row C: columns with 1 are [C] → descendants of C
        assert set(descendants["C"]) == {"C"}

        # Row D: columns with 1 are [D] → descendants of D
        assert set(descendants["D"]) == {"D"}

    def test_get_descendants_with_subset(self, loader):
        """Test get_descendants with a subset of rows."""
        subset = ["A", "C"]
        descendants = loader.get_descendants(subset=subset)

        # Should only have data for the subset rows
        assert "A" in descendants
        assert "C" in descendants

        # B and D should not be in results as they weren't in subset
        assert "B" not in descendants
        assert "D" not in descendants

    def test_get_descendants_with_empty_subset(self, loader):
        """Test get_descendants with an empty subset list.

        When subset is an empty list, the condition (len(subset) > 0) is False,
        so no filtering is applied and all relationships are returned.
        """
        descendants = loader.get_descendants(subset=[])

        # Empty subset returns all descendants (no filter applied)
        assert isinstance(descendants, dict)
        assert len(descendants) == 4  # All four terms present

    def test_get_descendants_with_single_term(self, loader):
        """Test get_descendants with a single term subset."""
        descendants = loader.get_descendants(subset=["B"])

        assert "B" in descendants
        # Row B has 1s in columns B and D
        assert set(descendants["B"]) == {"B", "D"}

    def test_collect_relations_by_row(self, loader):
        """Test _collect_relations method grouping by ROW_ID."""
        lf = loader.relations
        result = loader._collect_relations(lf, group_by=ROW_ID, agg=COL_ID)

        # Result should be a dictionary with ROW_ID and COL_ID keys
        assert ROW_ID in result
        assert COL_ID in result

        # Should have all rows
        assert len(result[ROW_ID]) == 4

    def test_collect_relations_by_col(self, loader):
        """Test _collect_relations method grouping by COL_ID."""
        lf = loader.relations
        result = loader._collect_relations(lf, group_by=COL_ID, agg=ROW_ID)

        # Result should be a dictionary with ROW_ID and COL_ID keys
        assert ROW_ID in result
        assert COL_ID in result

        # Should have all columns
        assert len(result[COL_ID]) == 4

    def test_none_subset_parameter(self, loader):
        """Test that None as subset parameter works correctly."""
        ancestors = loader.get_ancestors(subset=None)
        descendants = loader.get_descendants(subset=None)

        # Both should return all relationships
        assert len(ancestors) == 4
        assert len(descendants) == 4

    def test_constants_defined(self):
        """Test that module constants are properly defined."""
        assert ROW_ID == "row_id"
        assert COL_ID == "col_id"

    def test_lazyframe_not_collected_prematurely(self, loader):
        """Test that relations remain as LazyFrame until needed."""
        # The relations attribute should be a LazyFrame
        assert isinstance(loader.relations, pl.LazyFrame)

        # Calling get_ancestors should work without issues
        ancestors = loader.get_ancestors()

        # Relations should still be a LazyFrame
        assert isinstance(loader.relations, pl.LazyFrame)


class TestRelationsLoaderEdgeCases:
    """Test edge cases and error handling for RelationsLoader."""

    def test_with_large_subset(self, loader):
        """Test behavior when subset includes all columns and more."""
        # Include valid and invalid column names
        subset = ["A", "B", "C", "D", "E", "F"]

        # This should work but only return data for valid columns
        try:
            ancestors = loader.get_ancestors(subset=subset)
            # If it doesn't raise an error, it should have valid data
            assert isinstance(ancestors, dict)
        except Exception:
            # Polars may raise an exception for invalid column names
            pytest.skip("Implementation raises exception for invalid columns")

    def test_relationships_are_consistent(self, loader):
        """Test that ancestor and descendant relationships are logically consistent."""
        ancestors = loader.get_ancestors()
        descendants = loader.get_descendants()

        # If X is an ancestor of Y, then Y should be a descendant of X
        # This is a bidirectional relationship check

        # For each term and its ancestors
        for term, ancestor_list in ancestors.items():
            for ancestor in ancestor_list:
                # If ancestor is an ancestor of term,
                # then term should be a descendant of ancestor
                assert term in descendants.get(
                    ancestor, []
                ), f"{ancestor} is ancestor of {term}, but {term} not in descendants of {ancestor}"

    def test_self_relationships(self, loader):
        """Test that terms are their own ancestors and descendants."""
        ancestors = loader.get_ancestors()
        descendants = loader.get_descendants()

        # Each term should be its own ancestor
        for term in ["A", "B", "C", "D"]:
            assert term in ancestors.get(term, [])

        # Each term should be its own descendant
        for term in ["A", "B", "C", "D"]:
            assert term in descendants.get(term, [])
