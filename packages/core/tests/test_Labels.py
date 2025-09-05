"""
Unit tests for labelstations curations class.

Author: Parker Hicks
Date: 2025-09-01

Last updated: 2025-09-01 by Parker Hicks
"""

import numpy as np
import polars as pl
import pytest
from unittest.mock import Mock, patch, MagicMock

from curations.labels import Labels
from curations.index import Ids

["MONDO:0001657", "MONDO:0004790", "MONDO:0005147", "MONDO:0000000"]

@pytest.fixture
def sample_data():
    """basic test data for labelstations"""
    data = pl.DataFrame({
        "MONDO:0001657": [1, 0, 1, 0],  # brain cancer
        "MONDO:0004790": [0, 1, 0, 1],  # fatty liver
        "MONDO:0005147": [1, 1, 0, 0],  # diabetes
        "MONDO:0000000": [0, 0, 1, 1]  # control
    })
    
    ids = pl.DataFrame({
        "index": ["sample_1", "sample_2", "sample_3", "sample_4"],
        "group": ["study_a", "study_a", "study_b", "study_b"],
        "platform": ["GPL1", "GPL1", "GPL2", "GPL2"]
    })
    
    return data, ids


@pytest.fixture
def labels_instance(sample_data):
    """basic labelstations instance"""
    data, ids = sample_data
    return Labels(
        data=data,
        ids=ids,
        index_col="index",
        group_cols=("group", "platform"),
        collapsed=False
    )


class TestLabelsInit:
    """test initialization"""
    
    def test_init_basic(self, sample_data):
        """test basic initialization"""
        data, ids = sample_data
        labels = Labels(data, ids, "index", ("group", "platform"), False)
        
        assert labels.data.equals(data)
        assert labels.index_col == "index"
        assert labels.group_cols == ("group", "platform")
        assert labels.collapsed is False
        assert labels.controls is False
        
    def test_init_with_defaults(self, sample_data):
        """test initialization with default group_cols"""
        data, ids = sample_data
        labels = Labels(data, ids, "index")
        
        assert labels.group_cols == ("group", "platform")
        assert labels.collapsed is False


class TestLabelsProperties:
    """test property methods"""
    
    def test_entities(self, labels_instance):
        """test entities property"""
        entities = labels_instance.entities
        expected = ["MONDO:0001657", "MONDO:0004790", "MONDO:0005147", "MONDO:0000000"]
        assert entities == expected
        
    def test_groups(self, labels_instance):
        """test groups property"""
        groups = labels_instance.groups
        expected = ["study_a", "study_a", "study_b", "study_b"]
        assert groups == expected
        
    def test_ids(self, labels_instance):
        """test ids property"""
        ids_df = labels_instance.ids
        assert isinstance(ids_df, pl.DataFrame)
        assert "index" in ids_df.columns
        assert "group" in ids_df.columns
        
    def test_index(self, labels_instance):
        """test index property"""
        index = labels_instance.index
        expected = ["sample_1", "sample_2", "sample_3", "sample_4"]
        assert index == expected
        
    def test_n_indices(self, labels_instance):
        """test n_indices property"""
        assert labels_instance.n_indices == 4
        
    def test_n_entities(self, labels_instance):
        """test n_entities property"""
        assert labels_instance.n_entities == 4
        
    def test_unique_groups(self, labels_instance):
        """test unique_groups property"""
        unique_groups = labels_instance.unique_groups
        assert set(unique_groups) == {"study_a", "study_b"}


class TestLabelsBasicMethods:
    """test basic wrapper methods"""
    
    def test_drop(self, labels_instance):
        """test drop method"""
        labels_instance.drop("MONDO:0001657")
        assert "MONDO:0001657" not in labels_instance.entities
        assert len(labels_instance.entities) == 3
        
    def test_head(self, labels_instance):
        """test head method returns string representation"""
        result = labels_instance.head(2)
        assert isinstance(result, str)
        
    def test_to_numpy(self, labels_instance):
        """test to_numpy conversion"""
        arr = labels_instance.to_numpy()
        assert isinstance(arr, np.ndarray)
        assert arr.shape == (4, 4)
        
    def test_repr(self, labels_instance):
        """test string representation"""
        result = repr(labels_instance)
        assert isinstance(result, str)


class TestLabelsSelect:
    """test select method"""
    
    def test_select_columns(self, labels_instance):
        """test selecting specific columns"""
        selected = labels_instance.select("MONDO:0001657", "MONDO:0004790")
        
        assert isinstance(selected, Labels)
        assert selected.entities == ["MONDO:0001657", "MONDO:0004790"]
        assert selected.n_indices == 4
        assert selected.index_col == "index"
        assert selected.group_cols == ("group", "platform")
        
    def test_select_preserves_ids(self, labels_instance):
        """test that select preserves id data"""
        selected = labels_instance.select("MONDO:0001657")
        assert selected.ids.equals(labels_instance.ids)


class TestLabelsSlice:
    """test slice method"""
    
    def test_slice_basic(self, labels_instance):
        """test basic slicing"""
        sliced = labels_instance.slice(0, 2)
        
        assert isinstance(sliced, Labels)
        assert sliced.n_indices == 2
        assert sliced.n_entities == 4
        assert sliced.index == ["sample_1", "sample_2"]
        
    def test_slice_offset(self, labels_instance):
        """test slicing with offset"""
        sliced = labels_instance.slice(2, 2)
        
        assert sliced.n_indices == 2
        assert sliced.index == ["sample_3", "sample_4"]
        
    def test_slice_no_length(self, labels_instance):
        """test slicing without length parameter"""
        sliced = labels_instance.slice(1)
        
        assert sliced.n_indices == 3
        assert sliced.index == ["sample_2", "sample_3", "sample_4"]


class TestLabelsFilter:
    """test filter method"""
    
    def test_filter_basic(self, labels_instance):
        """test basic filtering"""
        filtered = labels_instance.filter(pl.col("MONDO:0001657") == 1)
        
        assert isinstance(filtered, Labels)
        assert filtered.n_indices == 2
        assert filtered.index == ["sample_1", "sample_3"]
        
    def test_filter_preserves_structure(self, labels_instance):
        """test that filtering preserves object structure"""
        filtered = labels_instance.filter(pl.col("MONDO:0004790") == 1)
        
        assert filtered.index_col == "index"
        assert filtered.group_cols == ("group", "platform")
        assert filtered.collapsed is False
        assert len(filtered.entities) == 4


class TestLabelsFromDf:
    """test from_df class method"""
    
    def test_from_df_basic(self, sample_data):
        """test creating labelstations from combined dataframe"""
        data, ids = sample_data
        combined_df = pl.concat([ids, data], how="horizontal")
        
        labels = Labels.from_df(
            combined_df, 
            index_col="index",
            group_cols=("group", "platform")
        )
        
        assert isinstance(labels, Labels)
        assert labels.index_col == "index"
        assert labels.group_cols == ("group", "platform")
        assert labels.n_indices == 4
        assert labels.n_entities == 4
        
    def test_from_df_with_defaults(self, sample_data):
        """test from_df with default group_cols"""
        data, ids = sample_data
        combined_df = pl.concat([ids, data], how="horizontal")
        
        labels = Labels.from_df(combined_df, index_col="index")
        assert labels.group_cols == ("group", "platform")


class TestLabelsToParquet:
    """test to_parquet method"""
    
    @patch('polars.DataFrame.write_parquet')
    def test_to_parquet(self, mock_write, labels_instance, tmp_path):
        """test saving to parquet file"""
        file_path = tmp_path / "test.parquet"
        
        labels_instance.to_parquet(file_path)
        
        mock_write.assert_called_once_with(file_path)


class TestLabelsEdgeCases:
    """test edge cases and error conditions"""
    
    def test_empty_dataframe(self):
        """test with empty dataframes"""
        empty_data = pl.DataFrame()
        empty_ids = pl.DataFrame({"index": []})
        
        labels = Labels(empty_data, empty_ids, "index")
        assert labels.n_indices == 0
        assert labels.n_entities == 0
        
    def test_single_row(self):
        """test with single row data"""
        data = pl.DataFrame({"MONDO:0001657": [1]})
        ids = pl.DataFrame({"index": ["sample_1"], "group": ["study_a"], "platform": ["rna"]})
        
        labels = Labels(data, ids, "index")
        assert labels.n_indices == 1
        assert labels.n_entities == 1
        assert labels.index == ["sample_1"]
        
    def test_filter_all_rows(self, labels_instance):
        """test filtering that removes all rows"""
        # filter condition that matches no rows
        filtered = labels_instance.filter(pl.col("MONDO:0001657") == 999)
        
        assert filtered.n_indices == 0
        assert len(filtered.index) == 0
