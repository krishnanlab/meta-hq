"""
Unit tests for metadata field validation.

Tests the new metadata fields added in the feat-metadata branch:
- Sample metadata: source_name_ch1, characteristics_ch1, source_name_ch2, characteristics_ch2
- Series metadata: title, summary, overall_design, sample_id

Author: Parker Hicks
Date: 2026-06-26

Last updated: 2026-06-26 by Parker Hicks
"""

import pytest

from metahq_core.util.supported import (
    geo_metadata_fields,
    metadata_fields,
)


class TestSampleMetadataFields:
    """Test sample-level metadata fields."""

    def test_sample_metadata_contains_required_fields(self):
        """Test that sample metadata contains all required fields."""
        sample_fields = metadata_fields("sample")

        required_fields = [
            "sample",
            "series",
            "platform",
            "description",
            "source_name_ch1",
            "characteristics_ch1",
            "source_name_ch2",
            "characteristics_ch2",
            "srx",
            "srs",
            "srp",
            "refinebio_sample",
            "refinebio_experiment",
        ]

        for field in required_fields:
            assert field in sample_fields, f"Missing required field: {field}"

    def test_sample_metadata_channel_fields_paired(self):
        """Test that channel 1 and channel 2 fields exist for both source_name and characteristics."""
        sample_meta = metadata_fields("sample")

        # Verify both channels exist for source_name
        assert "source_name_ch1" in sample_meta
        assert "source_name_ch2" in sample_meta

        # Verify both channels exist for characteristics
        assert "characteristics_ch1" in sample_meta
        assert "characteristics_ch2" in sample_meta

    def test_sample_metadata_no_duplicates(self):
        """Test that sample metadata has no duplicate fields."""
        sample_meta = metadata_fields("sample")

        # Check for duplicates
        assert len(sample_meta) == len(
            set(sample_meta)
        ), "Sample metadata contains duplicate fields"


class TestSeriesMetadataFields:
    """Test series-level metadata fields."""

    def test_series_metadata_contains_required_fields(self):
        """Test that series metadata contains all required fields."""
        series_fields = metadata_fields("series")

        required_fields = [
            "series",
            "platform",
            "title",
            "summary",
            "overall_design",
            "description",
            "sample_id",
            "srp",
            "refinebio_experiment",
        ]

        for field in required_fields:
            assert field in series_fields, f"Missing required field: {field}"

    def test_series_metadata_no_duplicates(self):
        """Test that series metadata has no duplicate fields."""
        series_meta = metadata_fields("series")

        # Check for duplicates
        assert len(series_meta) == len(
            set(series_meta)
        ), "Series metadata contains duplicate fields"


class TestGeoMetadataFields:
    """Test geo_metadata_fields function."""

    def test_geo_metadata_sample_fields(self):
        """Test that geo_metadata_fields returns correct sample-level fields."""
        geo_sample_fields = geo_metadata_fields("sample")

        expected_fields = [
            "description",
            "source_name_ch1",
            "characteristics_ch1",
            "source_name_ch2",
            "characteristics_ch2",
        ]

        assert set(geo_sample_fields) == set(
            expected_fields
        ), f"Expected {expected_fields}, got {geo_sample_fields}"

    def test_geo_metadata_series_fields(self):
        """Test that geo_metadata_fields returns correct series-level fields."""
        geo_series_fields = geo_metadata_fields("series")

        expected_fields = [
            "title",
            "summary",
            "overall_design",
            "description",
            "sample_id",
        ]

        assert set(geo_series_fields) == set(
            expected_fields
        ), f"Expected {expected_fields}, got {geo_series_fields}"

    def test_geo_metadata_sample_excludes_sra_fields(self):
        """Test that geo_metadata_fields for sample excludes SRA fields."""
        geo_sample_fields = geo_metadata_fields("sample")

        # SRA fields that should not be in GEO metadata
        sra_fields = ["srx", "srs", "srp"]

        for field in sra_fields:
            assert (
                field not in geo_sample_fields
            ), f"SRA field {field} should not be in GEO metadata"

    def test_geo_metadata_sample_excludes_refinebio_fields(self):
        """Test that geo_metadata_fields for sample excludes refine.bio fields."""
        geo_sample_fields = geo_metadata_fields("sample")

        # refine.bio fields that should not be in GEO metadata
        refinebio_fields = ["refinebio_sample", "refinebio_experiment"]

        for field in refinebio_fields:
            assert (
                field not in geo_sample_fields
            ), f"refine.bio field {field} should not be in GEO metadata"

    def test_geo_metadata_series_excludes_sra_fields(self):
        """Test that geo_metadata_fields for series excludes SRA fields."""
        geo_series_fields = geo_metadata_fields("series")

        # SRA fields that should not be in GEO metadata
        assert (
            "srp" not in geo_series_fields
        ), "SRA field 'srp' should not be in GEO metadata"

    def test_geo_metadata_series_excludes_refinebio_fields(self):
        """Test that geo_metadata_fields for series excludes refine.bio fields."""
        geo_series_fields = geo_metadata_fields("series")

        # refine.bio fields that should not be in GEO metadata
        assert (
            "refinebio_experiment" not in geo_series_fields
        ), "refine.bio field 'refinebio_experiment' should not be in GEO metadata"

    def test_geo_metadata_invalid_level_raises_error(self):
        """Test that geo_metadata_fields raises error for invalid level."""
        with pytest.raises(ValueError, match="Expected level in"):
            geo_metadata_fields("invalid_level")


class TestMetadataFieldsFunction:
    """Test metadata_fields function."""

    def test_metadata_fields_sample_returns_list(self):
        """Test that metadata_fields returns a list for sample level."""
        result = metadata_fields("sample")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_metadata_fields_series_returns_list(self):
        """Test that metadata_fields returns a list for series level."""
        result = metadata_fields("series")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_metadata_fields_invalid_level_raises_error(self):
        """Test that metadata_fields raises error for invalid level."""
        with pytest.raises(ValueError, match="Expected level in"):
            metadata_fields("invalid_level")


class TestMetadataFieldsConsistency:
    """Test consistency between metadata_fields and geo_metadata_fields."""

    def test_geo_metadata_subset_of_all_metadata_sample(self):
        """Test that GEO metadata fields are a subset of all metadata fields for sample."""
        all_sample_meta = set(metadata_fields("sample"))
        geo_sample_meta = set(geo_metadata_fields("sample"))

        assert geo_sample_meta.issubset(
            all_sample_meta
        ), "GEO metadata fields should be a subset of all metadata fields"

    def test_geo_metadata_subset_of_all_metadata_series(self):
        """Test that GEO metadata fields are a subset of all metadata fields for series."""
        all_series_meta = set(metadata_fields("series"))
        geo_series_meta = set(geo_metadata_fields("series"))

        assert geo_series_meta.issubset(
            all_series_meta
        ), "GEO metadata fields should be a subset of all metadata fields"

    def test_sample_metadata_includes_id_fields(self):
        """Test that sample metadata includes identifier fields."""
        sample_meta = metadata_fields("sample")

        id_fields = ["sample", "series", "platform"]
        for field in id_fields:
            assert field in sample_meta, f"Missing identifier field: {field}"

    def test_series_metadata_includes_id_fields(self):
        """Test that series metadata includes identifier fields."""
        series_meta = metadata_fields("series")

        id_fields = ["series", "platform"]
        for field in id_fields:
            assert field in series_meta, f"Missing identifier field: {field}"
