"""
Unit tests for Query class and related helper classes.

Author: Parker Hicks
Date: 2025-12-16

Last updated: 2025-12-16 by Parker Hicks
"""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from metahq_core.query import (
    AccessionIDs,
    LongAnnotations,
    ParsedEntries,
    Query,
    UnParsedEntry,
)
from metahq_core.util.exceptions import NoResultsFound

# =======================================================
# ==== Fixtures
# =======================================================


@pytest.fixture
def sample_accession_ids():
    """Sample AccessionIDs for testing"""
    fields = ("sample", "series", "platform")
    return AccessionIDs(fields)


@pytest.fixture
def sample_parsed_entries():
    """Sample ParsedEntries for testing"""
    fields = ("sample", "series", "platform")
    return ParsedEntries(fields)


@pytest.fixture
def sample_long_annotations():
    """Sample LongAnnotations DataFrame for testing"""
    data = pl.DataFrame(
        {
            "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "series": ["GSE1", "GSE1", "GSE2", "GSE2"],
            "platform": ["GPL1", "GPL1", "GPL2", "GPL2"],
            "id": ["UBERON:0001", "UBERON:0002", "NA", "UBERON:0003"],
            "value": ["brain", "liver", "NA", "heart"],
        }
    )
    return LongAnnotations(data)


@pytest.fixture
def sample_annotation_entry():
    """Sample annotation entry from the annotations dictionary"""
    return {
        "organism": "homo sapiens",
        "tissue": {
            "source1": {
                "id": "UBERON:0001",
                "value": "brain",
                "ecode": "expert-curated",
            },
            "source2": {
                "id": "UBERON:0002",
                "value": "cerebral cortex",
                "ecode": "expert-curated",
            },
        },
        "disease": {
            "source1": {
                "id": "MONDO:0001",
                "value": "cancer",
                "ecode": "expert-curated",
            }
        },
        "accession_ids": {
            "sample": "GSM123456",
            "series": "GSE12345",
            "platform": "GPL570",
        },
    }


@pytest.fixture
def mock_annotations_dict():
    """Mock annotations dictionary with multiple entries"""
    return {
        "entry1": {
            "organism": "homo sapiens",
            "tissue": {
                "source1": {
                    "id": "UBERON:0001",
                    "value": "brain",
                    "ecode": "expert-curated",
                }
            },
            "accession_ids": {
                "sample": "GSM1",
                "series": "GSE1",
                "platform": "GPL570",
            },
        },
        "entry2": {
            "organism": "homo sapiens",
            "tissue": {
                "source1": {
                    "id": "UBERON:0002",
                    "value": "liver",
                    "ecode": "expert-curated",
                }
            },
            "accession_ids": {
                "sample": "GSM2",
                "series": "GSE1",
                "platform": "GPL570",
            },
        },
        "entry3": {
            "organism": "mus musculus",
            "tissue": {
                "source1": {
                    "id": "UBERON:0003",
                    "value": "heart",
                    "ecode": "expert-curated",
                }
            },
            "accession_ids": {
                "sample": "GSM3",
                "series": "GSE2",
                "platform": "GPL96",
            },
        },
    }


# =======================================================
# ==== AccessionIDs Tests
# =======================================================


class TestAccessionIDs:
    """Test AccessionIDs class"""

    def test_init(self, sample_accession_ids):
        """Test basic initialization"""
        assert sample_accession_ids.fields == ("sample", "series", "platform")
        assert sample_accession_ids.ids == {
            "sample": [],
            "series": [],
            "platform": [],
        }

    def test_add(self, sample_accession_ids):
        """Test adding entries"""
        sample_accession_ids.add(
            {"sample": "GSM123", "series": "GSE456", "platform": "GPL789"}
        )
        assert sample_accession_ids.ids["sample"] == ["GSM123"]
        assert sample_accession_ids.ids["series"] == ["GSE456"]
        assert sample_accession_ids.ids["platform"] == ["GPL789"]

    def test_add_with_na(self, sample_accession_ids):
        """Test adding entries with NA values"""
        sample_accession_ids.add({"sample": "NA", "series": "GSE456", "platform": "NA"})
        assert sample_accession_ids.ids["sample"] == ["NA"]
        assert sample_accession_ids.ids["series"] == ["GSE456"]
        assert sample_accession_ids.ids["platform"] == ["NA"]

    def test_add_ignores_extra_keys(self, sample_accession_ids):
        """Test that add ignores keys not in fields"""
        sample_accession_ids.add(
            {
                "sample": "GSM123",
                "series": "GSE456",
                "platform": "GPL789",
                "extra": "ignored",
            }
        )
        assert "extra" not in sample_accession_ids.ids
        assert sample_accession_ids.ids["sample"] == ["GSM123"]

    def test_to_polars(self, sample_accession_ids):
        """Test conversion to Polars DataFrame"""
        sample_accession_ids.add(
            {"sample": "GSM123", "series": "GSE456", "platform": "GPL789"}
        )
        sample_accession_ids.add(
            {"sample": "GSM124", "series": "GSE457", "platform": "GPL790"}
        )

        df = sample_accession_ids.to_polars()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert list(df.columns) == ["sample", "series", "platform"]
        assert df["sample"].to_list() == ["GSM123", "GSM124"]


# =======================================================
# ==== ParsedEntries Tests
# =======================================================


class TestParsedEntries:
    """Test ParsedEntries class"""

    def test_init(self, sample_parsed_entries):
        """Test basic initialization"""
        assert isinstance(sample_parsed_entries.accessions, AccessionIDs)
        assert sample_parsed_entries.entries == {"id": [], "value": []}

    def test_add(self, sample_parsed_entries):
        """Test adding an entry"""
        sample_parsed_entries.add(
            "UBERON:0001",
            "brain",
            {"sample": "GSM123", "series": "GSE456", "platform": "GPL789"},
        )
        assert sample_parsed_entries.entries["id"] == ["UBERON:0001"]
        assert sample_parsed_entries.entries["value"] == ["brain"]
        assert sample_parsed_entries.accessions.ids["sample"] == ["GSM123"]

    def test_add_multiple(self, sample_parsed_entries):
        """Test adding multiple entries"""
        sample_parsed_entries.add(
            "UBERON:0001",
            "brain",
            {"sample": "GSM123", "series": "GSE456", "platform": "GPL789"},
        )
        sample_parsed_entries.add(
            "UBERON:0002",
            "liver",
            {"sample": "GSM124", "series": "GSE457", "platform": "GPL790"},
        )

        assert len(sample_parsed_entries.entries["id"]) == 2
        assert len(sample_parsed_entries.entries["value"]) == 2
        assert len(sample_parsed_entries.accessions.ids["sample"]) == 2

    def test_to_polars(self, sample_parsed_entries):
        """Test conversion to Polars DataFrame"""
        sample_parsed_entries.add(
            "UBERON:0001",
            "brain",
            {"sample": "GSM123", "series": "GSE456", "platform": "GPL789"},
        )
        sample_parsed_entries.add(
            "UBERON:0002",
            "liver",
            {"sample": "GSM124", "series": "GSE457", "platform": "GPL790"},
        )

        df = sample_parsed_entries.to_polars()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        assert "id" in df.columns
        assert "value" in df.columns
        assert "sample" in df.columns
        assert "series" in df.columns
        assert "platform" in df.columns


# =======================================================
# ==== LongAnnotations Tests
# =======================================================


class TestLongAnnotations:
    """Test LongAnnotations class"""

    def test_init(self, sample_long_annotations):
        """Test basic initialization"""
        assert isinstance(sample_long_annotations.annotations, pl.DataFrame)

    def test_column_intersection_with(self, sample_long_annotations):
        """Test finding column intersection"""
        cols = ["sample", "series", "nonexistent", "id"]
        result = sample_long_annotations.column_intersection_with(cols)
        assert set(result) == {"sample", "series", "id"}

    def test_filter_na(self, sample_long_annotations):
        """Test filtering NA values from a column"""
        initial_height = sample_long_annotations.annotations.height
        sample_long_annotations.filter_na("id")
        # Should have filtered out one row with "NA" in id column
        assert sample_long_annotations.annotations.height == initial_height - 1
        assert "NA" not in sample_long_annotations.annotations["id"].to_list()

    def test_stage_level_sample(self, sample_long_annotations):
        """Test staging for sample level"""
        sample_long_annotations.stage_level("sample")
        # Should filter out rows with NA in sample column (none in this case)
        assert sample_long_annotations.annotations.height > 0

    def test_stage_level_series(self, sample_long_annotations):
        """Test staging for series level"""
        sample_long_annotations.stage_level("series")
        # Should filter out rows with NA in series column (none in this case)
        assert sample_long_annotations.annotations.height > 0

    def test_stage_level_invalid(self, sample_long_annotations):
        """Test staging with invalid level raises error"""
        with pytest.raises(ValueError, match="Expected level in"):
            sample_long_annotations.stage_level("invalid")

    def test_stage_anchor(self, sample_long_annotations):
        """Test staging anchor column"""
        initial_height = sample_long_annotations.annotations.height
        sample_long_annotations.stage_anchor("id")
        # Should filter out NA values from id column
        assert sample_long_annotations.annotations.height == initial_height - 1

    def test_stage(self, sample_long_annotations):
        """Test full staging process"""
        sample_long_annotations.stage("sample", "id")
        # Should have filtered NA values from both sample and id columns
        assert "NA" not in sample_long_annotations.annotations["id"].to_list()

    def test_pivot_wide(self):
        """Test pivoting to wide format"""
        data = pl.DataFrame(
            {
                "sample": ["GSM1", "GSM2", "GSM3"],
                "series": ["GSE1", "GSE1", "GSE2"],
                "platform": ["GPL1", "GPL1", "GPL2"],
                "id": ["UBERON:0001", "UBERON:0002", "UBERON:0001"],
                "value": ["brain", "liver", "brain"],
            }
        )
        long_anno = LongAnnotations(data)
        wide = long_anno.pivot_wide("sample", "id", ["sample", "series", "platform"])

        assert isinstance(wide, pl.DataFrame)
        assert "sample" in wide.columns
        assert "UBERON:0001" in wide.columns
        assert "UBERON:0002" in wide.columns
        assert wide.height == 3

    def test_pivot_wide_with_multiple_annotations(self):
        """Test pivoting when entries have multiple pipe-separated annotations"""
        data = pl.DataFrame(
            {
                "sample": ["GSM1", "GSM2"],
                "series": ["GSE1", "GSE1"],
                "platform": ["GPL1", "GPL1"],
                "id": ["UBERON:0001|UBERON:0002", "UBERON:0003"],
                "value": ["brain|liver", "heart"],
            }
        )
        long_anno = LongAnnotations(data)
        wide = long_anno.pivot_wide("sample", "id", ["sample", "series", "platform"])

        assert isinstance(wide, pl.DataFrame)
        # Should have separate columns for each annotation
        assert "UBERON:0001" in wide.columns
        assert "UBERON:0002" in wide.columns
        assert "UBERON:0003" in wide.columns


# =======================================================
# ==== UnParsedEntry Tests
# =======================================================


class TestUnParsedEntry:
    """Test UnParsedEntry class"""

    def test_init(self, sample_annotation_entry):
        """Test basic initialization"""
        entry = UnParsedEntry(
            sample_annotation_entry, "tissue", ["expert-curated"], "homo sapiens"
        )
        assert entry.entry == sample_annotation_entry
        assert entry.attribute == "tissue"
        assert entry.ecodes == ["expert-curated"]
        assert entry.species == "homo sapiens"

    def test_is_acceptable_valid(self, sample_annotation_entry):
        """Test is_acceptable returns True for valid entry"""
        entry = UnParsedEntry(
            sample_annotation_entry, "tissue", ["expert-curated"], "homo sapiens"
        )
        assert entry.is_acceptable() is True

    def test_is_acceptable_wrong_species(self, sample_annotation_entry):
        """Test is_acceptable returns False for wrong species"""
        entry = UnParsedEntry(
            sample_annotation_entry, "tissue", ["expert-curated"], "mus musculus"
        )
        assert entry.is_acceptable() is False

    def test_is_acceptable_missing_attribute(self, sample_annotation_entry):
        """Test is_acceptable returns False when attribute doesn't exist"""
        entry = UnParsedEntry(
            sample_annotation_entry, "age", ["expert-curated"], "homo sapiens"
        )
        assert entry.is_acceptable() is False

    def test_is_acceptable_empty_entry(self):
        """Test is_acceptable returns False for empty entry"""
        entry = UnParsedEntry({}, "tissue", ["expert-curated"], "homo sapiens")
        assert entry.is_acceptable() is False

    def test_get_id_value(self):
        """Test extracting ID and value from source annotation"""
        source_anno = {"id": "UBERON:0001", "value": "brain", "ecode": "expert-curated"}
        id_, value = UnParsedEntry.get_id_value(source_anno)
        assert id_ == "UBERON:0001"
        assert value == "brain"

    def test_get_id_value_missing_id(self):
        """Test extracting when ID is missing"""
        source_anno = {"value": "brain", "ecode": "expert-curated"}
        id_, value = UnParsedEntry.get_id_value(source_anno)
        assert id_ == "NA"
        assert value == "brain"

    def test_get_id_value_missing_value(self):
        """Test extracting when value is missing"""
        source_anno = {"id": "UBERON:0001", "ecode": "expert-curated"}
        id_, value = UnParsedEntry.get_id_value(source_anno)
        assert id_ == "UBERON:0001"
        assert value == "NA"

    def test_get_annotations_single_source(self):
        """Test getting annotations from a single source"""
        entry_data = {
            "organism": "homo sapiens",
            "tissue": {
                "source1": {
                    "id": "UBERON:0001",
                    "value": "brain",
                    "ecode": "expert-curated",
                }
            },
        }
        entry = UnParsedEntry(entry_data, "tissue", ["expert-curated"], "homo sapiens")
        ids, values = entry.get_annotations()
        assert ids == "UBERON:0001"
        assert values == "brain"

    def test_get_annotations_multiple_sources(self, sample_annotation_entry):
        """Test getting annotations from multiple sources"""
        entry = UnParsedEntry(
            sample_annotation_entry, "tissue", ["expert-curated"], "homo sapiens"
        )
        ids, values = entry.get_annotations()
        # Results are concatenated with | and order might vary due to set
        assert "UBERON:0001" in ids
        assert "UBERON:0002" in ids
        assert "|" in ids
        assert "brain" in values
        assert "cerebral cortex" in values

    def test_get_annotations_filtered_by_ecode(self):
        """Test that annotations are filtered by evidence code"""
        entry_data = {
            "organism": "homo sapiens",
            "tissue": {
                "source1": {
                    "id": "UBERON:0001",
                    "value": "brain",
                    "ecode": "expert-curated",
                },
                "source2": {
                    "id": "UBERON:0002",
                    "value": "liver",
                    "ecode": "crowd-sourced",
                },
            },
        }
        entry = UnParsedEntry(entry_data, "tissue", ["expert-curated"], "homo sapiens")
        ids, values = entry.get_annotations()
        # Should only include expert-curated annotation
        assert ids == "UBERON:0001"
        assert values == "brain"

    def test_get_annotations_not_acceptable(self):
        """Test getting annotations when entry is not acceptable"""
        entry_data = {
            "organism": "mus musculus",
            "tissue": {
                "source1": {
                    "id": "UBERON:0001",
                    "value": "brain",
                    "ecode": "expert-curated",
                }
            },
        }
        entry = UnParsedEntry(entry_data, "tissue", ["expert-curated"], "homo sapiens")
        ids, values = entry.get_annotations()
        assert ids == "NA"
        assert values == "NA"


# =======================================================
# ==== Query Class Tests
# =======================================================


class TestQueryInit:
    """Test Query class initialization"""

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_init_basic(self, mock_get_annotations, mock_load_bson):
        """Test basic initialization."""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        query = Query(
            "geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq"
        )

        assert query.database == "geo"
        assert query.attribute == "tissue"
        assert query.level == "sample"
        assert query.species == "homo sapiens"
        assert query.technology == "rnaseq"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_init_custom_parameters(self, mock_get_annotations, mock_load_bson):
        """Test initialization with custom parameters"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        query = Query(
            "geo",
            "disease",
            level="series",
            ecode="semi-curated",
            species="mouse",
            technology="microarray",
        )

        assert query.database == "geo"
        assert query.attribute == "disease"
        assert query.level == "series"
        assert query.technology == "microarray"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_load_ecode_shorthand(self, mock_get_annotations, mock_load_bson):
        """Test loading evidence code with shorthand"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        query = Query("geo", "tissue", "sample", "expert", "homo sapiens", "rnaseq")
        assert query.ecodes == ["expert-curated"]

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_load_ecode_full_name(self, mock_get_annotations, mock_load_bson):
        """Test loading evidence code with full name"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        assert query.ecodes == ["expert-curated"]

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_load_ecode_invalid(self, mock_get_annotations, mock_load_bson):
        """Test that invalid evidence code raises error"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        with pytest.raises(ValueError, match="Invalid ecode query"):
            Query("geo", "tissue", "sample", "invalid-ecode", "homo sapiens", "rnaseq")

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_load_species_shorthand(self, mock_get_annotations, mock_load_bson):
        """Test loading species with shorthand"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        query = Query("geo", "tissue", "sample", "expert-curated", "human", "rnaseq")
        assert query.species == "homo sapiens"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_load_species_invalid(self, mock_get_annotations, mock_load_bson):
        """Test that invalid species raises error"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        with pytest.raises(ValueError, match="Invalid species query"):
            Query("geo", "tissue", "sample", "expert-curated", "invalid-species", "rnaseq")


class TestQueryMethods:
    """Test Query class methods"""

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_assign_index_groups_sample(self, mock_get_annotations, mock_load_bson):
        """Test assigning index and groups for sample level"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        index, groups = query._assign_index_groups()

        assert index == "sample"
        assert groups == ("series", "platform")

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_assign_index_groups_series(self, mock_get_annotations, mock_load_bson):
        """Test assigning index and groups for series level"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {}

        query = Query("geo", "tissue", "series", "expert-curated", "homo sapiens", "rnaseq")
        index, groups = query._assign_index_groups()

        assert index == "series"
        assert groups == ("platform",)

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_get_accession_ids_sample_level(
        self, mock_get_annotations, mock_load_bson, mock_annotations_dict
    ):
        """Test getting accession IDs for sample level"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = mock_annotations_dict

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        accessions = query.get_accession_ids("entry1")

        assert accessions["sample"] == "GSM1"
        assert accessions["series"] == "GSE1"
        assert accessions["platform"] == "GPL570"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_get_accession_ids_series_level(
        self, mock_get_annotations, mock_load_bson, mock_annotations_dict
    ):
        """Test getting accession IDs for series level"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = mock_annotations_dict

        query = Query("geo", "tissue", "series", "expert-curated", "homo sapiens", "rnaseq")
        accessions = query.get_accession_ids("entry1")

        assert "sample" not in accessions
        assert accessions["series"] == "GSE1"
        assert accessions["platform"] == "GPL570"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_get_accession_ids_missing_ids(self, mock_get_annotations, mock_load_bson):
        """Test getting accession IDs when some are missing"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_annotations = {
            "entry1": {
                "organism": "homo sapiens",
                "tissue": {"source1": {"id": "UBERON:0001", "ecode": "expert-curated"}},
                "accession_ids": {
                    "sample": "GSM1",
                    # missing series and platform
                },
            }
        }
        mock_load_bson.return_value = mock_annotations

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        accessions = query.get_accession_ids("entry1")

        assert accessions["sample"] == "GSM1"
        assert accessions["series"] == "NA"
        assert accessions["platform"] == "NA"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_get_valid_annotations(
        self, mock_get_annotations, mock_load_bson, mock_annotations_dict
    ):
        """Test getting valid annotations from an entry"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = mock_annotations_dict

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        ids, values = query.get_valid_annotations("entry1")

        assert ids == "UBERON:0001"
        assert values == "brain"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    def test_get_valid_annotations_wrong_species(
        self, mock_get_annotations, mock_load_bson, mock_annotations_dict
    ):
        """Test getting annotations for wrong species returns NA"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = mock_annotations_dict

        query = Query("geo", "tissue", "sample", "expert-curated", "human", "rnaseq")
        # entry3 is mus musculus
        ids, values = query.get_valid_annotations("entry3")

        assert ids == "NA"
        assert values == "NA"

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    @patch("metahq_core.query.pl.scan_parquet")
    def test_compile_annotations(
        self,
        mock_scan_parquet,
        mock_get_annotations,
        mock_load_bson,
        mock_annotations_dict,
    ):
        """Test compiling annotations from the dictionary"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = mock_annotations_dict

        # Mock platform filtering
        mock_df = MagicMock()
        mock_df.filter.return_value.collect.return_value = {"id": ["GPL570", "GPL96"]}
        mock_scan_parquet.return_value = mock_df

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        result = query.compile_annotations(["sample", "series", "platform"])

        assert isinstance(result, pl.DataFrame)
        assert result.height > 0

    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    @patch("metahq_core.query.pl.scan_parquet")
    def test_compile_annotations_empty_raises_error(
        self, mock_scan_parquet, mock_get_annotations, mock_load_bson
    ):
        """Test that empty annotations raise RuntimeError"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = {
            "entry1": {
                "organism": "homo sapiens",
                "tissue": {"source1": {"id": "UBERON:0001", "ecode": "expert-curated"}},
                "accession_ids": {
                    "sample": "GSM1",
                    "series": "GSE1",
                    "platform": "GPL999",
                },
            }
        }

        # Mock platform filtering to return no valid platforms
        mock_df = MagicMock()
        mock_df.filter.return_value.collect.return_value = {"id": ["GPL570"]}
        mock_scan_parquet.return_value = mock_df

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")

        with pytest.raises(
            NoResultsFound, match="Unable to identify with provided parameters"
        ):
            query.compile_annotations(["sample", "series", "platform"])


class TestQueryIntegration:
    """Integration tests for Query class end-to-end workflows"""

    @patch("metahq_core.query.Annotations.from_df")
    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    @patch("metahq_core.query.pl.scan_parquet")
    def test_annotations_method(
        self,
        mock_scan_parquet,
        mock_get_annotations,
        mock_load_bson,
        mock_from_df,
        mock_annotations_dict,
    ):
        """Test the full annotations() method workflow"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = mock_annotations_dict

        # Mock platform filtering
        mock_df = MagicMock()
        mock_df.filter.return_value.collect.return_value = {"id": ["GPL570", "GPL96"]}
        mock_scan_parquet.return_value = mock_df

        # Mock the Annotations.from_df return
        mock_annotations_obj = MagicMock()
        mock_from_df.return_value = mock_annotations_obj

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        result = query.annotations(anchor="id")

        # Verify Annotations.from_df was called
        assert mock_from_df.called
        assert result == mock_annotations_obj

    @patch("metahq_core.query.Annotations.from_df")
    @patch("metahq_core.query.load_bson")
    @patch("metahq_core.query.get_annotations")
    @patch("metahq_core.query.pl.scan_parquet")
    def test_annotations_method_with_value_anchor(
        self,
        mock_scan_parquet,
        mock_get_annotations,
        mock_load_bson,
        mock_from_df,
        mock_annotations_dict,
    ):
        """Test annotations() method with value anchor"""
        mock_get_annotations.return_value = "path/to/annotations.bson"
        mock_load_bson.return_value = mock_annotations_dict

        # Mock platform filtering
        mock_df = MagicMock()
        mock_df.filter.return_value.collect.return_value = {"id": ["GPL570", "GPL96"]}
        mock_scan_parquet.return_value = mock_df

        mock_annotations_obj = MagicMock()
        mock_from_df.return_value = mock_annotations_obj

        query = Query("geo", "tissue", "sample", "expert-curated", "homo sapiens", "rnaseq")
        result = query.annotations(anchor="value")

        assert mock_from_df.called
        assert result == mock_annotations_obj
