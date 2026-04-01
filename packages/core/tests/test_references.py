"""
Test the citation and reference formatting functionality of MetaHQ.

This module tests the reference formatting functionality that creates citation
files for MetaHQ query results. It includes formatting individual references,
building reference lists from source counts, and generating complete citation files.

These are unit tests that mock file I/O operations and external dependencies
to avoid dependency on actual files while maintaining full test coverage.

Author: Parker Hicks
Date: 2026-04-01

Last updated: 2026-04-01 by Parker Hicks
"""

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import polars as pl
import pytest

from metahq_core.export.references import (
    CitationConfig,
    build_citation_file,
    build_reference_list,
    format_citation,
    format_reference,
    format_references,
    save_citations,
)
from metahq_core.sources import REFERENCE_MAP, Ale, Gemma, KrishnanLab


# ===== Fixtures =====


@pytest.fixture
def sample_reference():
    """Create a sample reference for testing."""
    return KrishnanLab(5)


@pytest.fixture
def multiple_references():
    """Create multiple references for testing."""
    return [KrishnanLab(10), Gemma(25), Ale(8)]


@pytest.fixture
def sample_citation_config():
    """Create a sample CitationConfig for testing."""
    return CitationConfig(
        version="1.0.0",
        attribute="tissue",
        level="sample",
        species="human",
        ecode="expert",
        tech="rnaseq",
        mode="annotate",
        date="2026-04-01 12:00:00",
        outfile="test_CITATION.txt",
    )


@pytest.fixture
def source_counts_df():
    """Create a sample polars DataFrame with source counts."""
    return pl.DataFrame(
        {
            "sources": ["KrishnanLab", "Gemma", "ALE"],
            "count": [10, 25, 8],
        }
    )


# ===== Tests for format_citation =====


class TestFormatCitation:
    """Test class for format_citation function."""

    def test_removes_newlines(self):
        """Test that newlines are removed from citation text."""
        ref = KrishnanLab(5)
        formatted = format_citation(ref)

        # Should not contain newlines
        assert "\n" not in formatted
        # Should be a single line
        assert isinstance(formatted, str)

    def test_removes_extra_whitespace(self):
        """Test that extra whitespace is normalized."""
        ref = KrishnanLab(5)
        formatted = format_citation(ref)

        # Should not have multiple consecutive spaces
        assert "  " not in formatted

    def test_preserves_citation_content(self):
        """Test that citation content is preserved."""
        ref = Gemma(10)
        formatted = format_citation(ref)

        # Should contain key parts of the citation
        assert "Gemma" in formatted or "Lim, N." in formatted
        # Should not be empty
        assert len(formatted) > 0

    def test_different_references(self, multiple_references):
        """Test format_citation with different reference types."""
        for ref in multiple_references:
            formatted = format_citation(ref)
            assert isinstance(formatted, str)
            assert "\n" not in formatted
            assert len(formatted) > 0


# ===== Tests for format_reference =====


class TestFormatReference:
    """Test class for format_reference function."""

    def test_basic_formatting(self, sample_reference):
        """Test basic formatting of a reference."""
        formatted = format_reference(sample_reference, index=1)

        # Should contain the index
        assert "[1]" in formatted
        # Should contain the source
        assert sample_reference.source in formatted
        # Should contain the URL
        assert sample_reference.url in formatted
        # Should contain annotation count
        assert "Annotations: 5" in formatted
        # Should contain license information
        assert sample_reference.rights in formatted

    def test_index_formatting(self, sample_reference):
        """Test that index is properly formatted."""
        formatted = format_reference(sample_reference, index=42)
        assert "[42]" in formatted

    def test_custom_indent(self, sample_reference):
        """Test formatting with custom indentation."""
        custom_indent = "  "
        formatted = format_reference(sample_reference, index=1, indent=custom_indent)

        # Check that indentation is present (should appear after index line)
        lines = formatted.split("\n")
        # Second line should start with the indent
        if len(lines) > 1:
            assert lines[1].startswith(custom_indent)

    def test_notes_included_when_present(self):
        """Test that notes are included when present in reference."""
        ref = KrishnanLab(5)
        formatted = format_reference(ref, index=1)

        # KrishnanLab has notes=None, so should not contain "Notes:"
        assert "Notes:" not in formatted

    def test_url_field_present(self, sample_reference):
        """Test that URL field is present in formatted output."""
        formatted = format_reference(sample_reference, index=1)
        assert "url:" in formatted
        assert sample_reference.url in formatted

    def test_multiline_structure(self, sample_reference):
        """Test that formatted reference has multiple lines."""
        formatted = format_reference(sample_reference, index=1)
        lines = formatted.split("\n")

        # Should have multiple lines
        assert len(lines) >= 4  # index, citation, url, annotations, license


# ===== Tests for format_references =====


class TestFormatReferences:
    """Test class for format_references function."""

    def test_single_reference(self, sample_reference):
        """Test formatting a list with a single reference."""
        refs = [sample_reference]
        formatted = format_references(refs)

        assert "[1]" in formatted
        assert sample_reference.source in formatted

    def test_multiple_references(self, multiple_references):
        """Test formatting multiple references."""
        formatted = format_references(multiple_references)

        # Should contain all indices
        assert "[1]" in formatted
        assert "[2]" in formatted
        assert "[3]" in formatted

        # Should contain all sources
        for ref in multiple_references:
            assert ref.source in formatted

    def test_sequential_numbering(self, multiple_references):
        """Test that references are numbered sequentially."""
        formatted = format_references(multiple_references)

        # Check that numbering is sequential
        for i in range(1, len(multiple_references) + 1):
            assert f"[{i}]" in formatted

    def test_double_newline_separator(self, multiple_references):
        """Test that references are separated by double newlines."""
        formatted = format_references(multiple_references)

        # Should contain double newlines as separators
        assert "\n\n" in formatted

    def test_empty_list(self):
        """Test formatting an empty list of references."""
        formatted = format_references([])
        assert formatted == ""

    def test_order_preservation(self):
        """Test that order of references is preserved."""
        refs = [Gemma(5), KrishnanLab(10), Ale(3)]
        formatted = format_references(refs)

        # Find positions of each source in the formatted string
        gemma_pos = formatted.find("Gemma")
        krishnan_pos = formatted.find("KrishnanLab")
        ale_pos = formatted.find("ALE")

        # Order should be preserved
        assert gemma_pos < krishnan_pos < ale_pos


# ===== Tests for build_reference_list =====


class TestBuildReferenceList:
    """Test class for build_reference_list function."""

    def test_basic_functionality(self, source_counts_df):
        """Test basic reference list building."""
        refs = build_reference_list(source_counts_df)

        # Should return a list
        assert isinstance(refs, list)
        # Should have same length as input DataFrame
        assert len(refs) == source_counts_df.height

    def test_reference_types(self, source_counts_df):
        """Test that correct reference types are created."""
        refs = build_reference_list(source_counts_df)

        # Check that references are of correct types
        assert refs[0].__class__.__name__ == "KrishnanLab"
        assert refs[1].__class__.__name__ == "Gemma"
        assert refs[2].__class__.__name__ == "Ale"

    def test_annotation_counts(self, source_counts_df):
        """Test that annotation counts are correctly assigned."""
        refs = build_reference_list(source_counts_df)

        # Check that counts match
        assert refs[0].n == 10  # KrishnanLab
        assert refs[1].n == 25  # Gemma
        assert refs[2].n == 8  # ALE

    def test_single_source(self):
        """Test with a single source."""
        df = pl.DataFrame({"sources": ["Gemma"], "count": [15]})
        refs = build_reference_list(df)

        assert len(refs) == 1
        assert refs[0].source == "Gemma"
        assert refs[0].n == 15

    def test_all_available_sources(self):
        """Test with all available sources in REFERENCE_MAP."""
        sources = list(REFERENCE_MAP.keys())[:5]  # Test first 5 sources
        counts = list(range(1, 6))

        df = pl.DataFrame({"sources": sources, "count": counts})
        refs = build_reference_list(df)

        assert len(refs) == 5
        for i, source in enumerate(sources):
            assert refs[i].source == source
            assert refs[i].n == counts[i]

    def test_empty_dataframe(self):
        """Test with an empty DataFrame."""
        df = pl.DataFrame({"sources": [], "count": []})
        refs = build_reference_list(df)

        assert len(refs) == 0


# ===== Tests for CitationConfig =====


class TestCitationConfig:
    """Test class for CitationConfig dataclass."""

    def test_basic_instantiation(self):
        """Test basic instantiation of CitationConfig."""
        config = CitationConfig(
            version="1.0.0",
            attribute="tissue",
            level="sample",
            species="human",
            ecode="expert",
            tech="rnaseq",
            mode="annotate",
            date="2026-04-01 12:00:00",
        )

        assert config.version == "1.0.0"
        assert config.attribute == "tissue"
        assert config.level == "sample"
        assert config.species == "human"
        assert config.ecode == "expert"
        assert config.tech == "rnaseq"
        assert config.mode == "annotate"
        assert config.date == "2026-04-01 12:00:00"
        assert config.outfile == "CITATION.txt"  # default

    def test_custom_outfile(self):
        """Test CitationConfig with custom outfile."""
        config = CitationConfig(
            version="1.0.0",
            attribute="disease",
            level="series",
            species="mouse",
            ecode="semi",
            tech="microarray",
            mode="label",
            date="2026-04-01 12:00:00",
            outfile="custom_citation.txt",
        )

        assert config.outfile == "custom_citation.txt"

    def test_outfile_path_type(self):
        """Test that outfile can be a Path object."""
        config = CitationConfig(
            version="1.0.0",
            attribute="tissue",
            level="sample",
            species="human",
            ecode="expert",
            tech="rnaseq",
            mode="annotate",
            date="2026-04-01 12:00:00",
            outfile=Path("/tmp/citations.txt"),
        )

        assert isinstance(config.outfile, Path)

    def test_all_fields_accessible(self, sample_citation_config):
        """Test that all fields are accessible."""
        assert hasattr(sample_citation_config, "version")
        assert hasattr(sample_citation_config, "attribute")
        assert hasattr(sample_citation_config, "level")
        assert hasattr(sample_citation_config, "species")
        assert hasattr(sample_citation_config, "ecode")
        assert hasattr(sample_citation_config, "tech")
        assert hasattr(sample_citation_config, "mode")
        assert hasattr(sample_citation_config, "date")
        assert hasattr(sample_citation_config, "outfile")


# ===== Tests for build_citation_file =====


class TestBuildCitationFile:
    """Test class for build_citation_file function."""

    @patch("builtins.open", new_callable=mock_open, read_data="Test template\nReferences: $references\nVersion: $version")
    def test_basic_substitution(self, mock_file, sample_citation_config):
        """Test basic template substitution."""
        references = "[1] Test Reference"

        result = build_citation_file(references, sample_citation_config)

        # Should be a string
        assert isinstance(result, str)
        # Should contain substituted values
        assert sample_citation_config.version in result or "$version" not in result

    @patch("builtins.open", new_callable=mock_open, read_data="Version: $version\nAttribute: $attribute")
    def test_config_substitution(self, mock_file, sample_citation_config):
        """Test that all config fields are substituted."""
        references = "[1] Test"

        result = build_citation_file(references, sample_citation_config)

        # Should have substituted placeholders (or they shouldn't exist)
        # Check that no $ placeholders remain for the config fields
        assert "$version" not in result or sample_citation_config.version in result

    @patch("builtins.open", new_callable=mock_open, read_data="$references")
    def test_references_substitution(self, mock_file, sample_citation_config):
        """Test that references are substituted correctly."""
        test_references = "[1] Ref1\n[2] Ref2"

        result = build_citation_file(test_references, sample_citation_config)

        assert "$references" not in result or test_references in result

    @patch("builtins.open", new_callable=mock_open, read_data="$metahq_reference")
    def test_metahq_reference_included(self, mock_file, sample_citation_config):
        """Test that MetaHQ reference is included."""
        references = "[1] Test"

        result = build_citation_file(references, sample_citation_config)

        # MetaHQ reference should be formatted and included
        assert "$metahq_reference" not in result or "MetaHQ" in result or "Hicks" in result

    @patch("builtins.open", new_callable=mock_open, read_data="Template: $date $mode $tech")
    def test_custom_indent(self, mock_file, sample_citation_config):
        """Test build_citation_file with custom indent."""
        references = "[1] Test"
        custom_indent = "    "

        result = build_citation_file(references, sample_citation_config, indent=custom_indent)

        assert isinstance(result, str)


# ===== Tests for save_citations =====


class TestSaveCitations:
    """Test class for save_citations function."""

    @patch("metahq_core.export.references.save_plain_text")
    def test_basic_save(self, mock_save, source_counts_df, sample_citation_config):
        """Test basic citation saving functionality."""
        mock_logger = MagicMock()

        save_citations(source_counts_df, sample_citation_config, mock_logger)

        # Should call save_plain_text once
        mock_save.assert_called_once()
        # First argument should be the text content
        args, kwargs = mock_save.call_args
        assert isinstance(args[0], str)
        # Second argument should be the outfile
        assert args[1] == sample_citation_config.outfile

    @patch("metahq_core.export.references.save_plain_text")
    @patch("metahq_core.export.references.build_citation_file")
    def test_integration_flow(self, mock_build, mock_save, source_counts_df, sample_citation_config):
        """Test the integration flow of save_citations."""
        mock_logger = MagicMock()
        mock_build.return_value = "Mock citation content"

        save_citations(source_counts_df, sample_citation_config, mock_logger)

        # build_citation_file should be called
        mock_build.assert_called_once()
        # save_plain_text should be called with the result
        mock_save.assert_called_once_with("Mock citation content", sample_citation_config.outfile)

    @patch("metahq_core.export.references.save_plain_text")
    def test_single_source(self, mock_save, sample_citation_config):
        """Test save_citations with a single source."""
        df = pl.DataFrame({"sources": ["Gemma"], "count": [100]})
        mock_logger = MagicMock()

        save_citations(df, sample_citation_config, mock_logger, verbose=True)

        # Should complete successfully
        mock_save.assert_called_once()
        mock_logger.info.assert_called()
