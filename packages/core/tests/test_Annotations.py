"""
Unit tests for annotations curations class.

Author: Parker Hicks
Date: 2025-09-01

Last updated: 2025-09-01 by Parker Hicks
"""

import numpy as np
import polars as pl
import pytest
from unittest.mock import Mock, patch, MagicMock

from curations.annotations import Annotations
from curations.labels import Labels
from curations.index import Ids

["MONDO:0001657", "MONDO:0004790", "MONDO:0005147", "MONDO:0000000"]

@pytest.fixture
def sample_data():
    """basic test data for annotations"""
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
def annotations_instance(sample_data):
    """basic annotations instance"""
    data, ids = sample_data
    return Annotations(
        data=data,
        ids=ids,
        index_col="index",
        group_cols=("group", "platform"),
        collapsed=False
    )


class TestAnnotationsInit:
    """test initialization"""
    
    def test_init_basic(self, sample_data):
        """test basic initialization"""
        data, ids = sample_data
        anno = Annotations(data, ids, "index", ("group", "platform"), False)
        
        assert anno.data.equals(data)
        assert anno.index_col == "index"
        assert anno.group_cols == ("group", "platform")
        assert anno.collapsed is False
        assert anno.controls is False
        
    def test_init_with_defaults(self, sample_data):
        """test initialization with default group_cols"""
        data, ids = sample_data
        anno = Annotations(data, ids, "index")
        
        assert anno.group_cols == ("group", "platform")
        assert anno.collapsed is False


class TestAnnotationsProperties:
    """test property methods"""
    
    def test_entities(self, annotations_instance):
        """test entities property"""
        entities = annotations_instance.entities
        expected = ["MONDO:0001657", "MONDO:0004790", "MONDO:0005147", "MONDO:0000000"]
        assert entities == expected
        
    def test_groups(self, annotations_instance):
        """test groups property"""
        groups = annotations_instance.groups
        expected = ["study_a", "study_a", "study_b", "study_b"]
        assert groups == expected
        
    def test_ids(self, annotations_instance):
        """test ids property"""
        ids_df = annotations_instance.ids
        assert isinstance(ids_df, pl.DataFrame)
        assert "index" in ids_df.columns
        assert "group" in ids_df.columns
        
    def test_index(self, annotations_instance):
        """test index property"""
        index = annotations_instance.index
        expected = ["sample_1", "sample_2", "sample_3", "sample_4"]
        assert index == expected
        
    def test_n_indices(self, annotations_instance):
        """test n_indices property"""
        assert annotations_instance.n_indices == 4
        
    def test_n_entities(self, annotations_instance):
        """test n_entities property"""
        assert annotations_instance.n_entities == 4
        
    def test_unique_groups(self, annotations_instance):
        """test unique_groups property"""
        unique_groups = annotations_instance.unique_groups
        assert set(unique_groups) == {"study_a", "study_b"}


class TestAnnotationsBasicMethods:
    """test basic wrapper methods"""
    
    def test_drop(self, annotations_instance):
        """test drop method"""
        annotations_instance.drop("MONDO:0001657")
        assert "MONDO:0001657" not in annotations_instance.entities
        assert len(annotations_instance.entities) == 3
        
    def test_head(self, annotations_instance):
        """test head method returns string representation"""
        result = annotations_instance.head(2)
        assert isinstance(result, str)
        
    def test_to_numpy(self, annotations_instance):
        """test to_numpy conversion"""
        arr = annotations_instance.to_numpy()
        assert isinstance(arr, np.ndarray)
        assert arr.shape == (4, 4)
        
    def test_repr(self, annotations_instance):
        """test string representation"""
        result = repr(annotations_instance)
        assert isinstance(result, str)


class TestAnnotationsSelect:
    """test select method"""
    
    def test_select_columns(self, annotations_instance):
        """test selecting specific columns"""
        selected = annotations_instance.select("MONDO:0001657", "MONDO:0004790")
        
        assert isinstance(selected, Annotations)
        assert selected.entities == ["MONDO:0001657", "MONDO:0004790"]
        assert selected.n_indices == 4
        assert selected.index_col == "index"
        assert selected.group_cols == ("group", "platform")
        
    def test_select_preserves_ids(self, annotations_instance):
        """test that select preserves id data"""
        selected = annotations_instance.select("MONDO:0001657")
        assert selected.ids.equals(annotations_instance.ids)


class TestAnnotationsSlice:
    """test slice method"""
    
    def test_slice_basic(self, annotations_instance):
        """test basic slicing"""
        sliced = annotations_instance.slice(0, 2)
        
        assert isinstance(sliced, Annotations)
        assert sliced.n_indices == 2
        assert sliced.n_entities == 4
        assert sliced.index == ["sample_1", "sample_2"]
        
    def test_slice_offset(self, annotations_instance):
        """test slicing with offset"""
        sliced = annotations_instance.slice(2, 2)
        
        assert sliced.n_indices == 2
        assert sliced.index == ["sample_3", "sample_4"]
        
    def test_slice_no_length(self, annotations_instance):
        """test slicing without length parameter"""
        sliced = annotations_instance.slice(1)
        
        assert sliced.n_indices == 3
        assert sliced.index == ["sample_2", "sample_3", "sample_4"]


class TestAnnotationsFilter:
    """test filter method"""
    
    def test_filter_basic(self, annotations_instance):
        """test basic filtering"""
        filtered = annotations_instance.filter(pl.col("MONDO:0001657") == 1)
        
        assert isinstance(filtered, Annotations)
        assert filtered.n_indices == 2
        assert filtered.index == ["sample_1", "sample_3"]
        
    def test_filter_preserves_structure(self, annotations_instance):
        """test that filtering preserves object structure"""
        filtered = annotations_instance.filter(pl.col("MONDO:0004790") == 1)
        
        assert filtered.index_col == "index"
        assert filtered.group_cols == ("group", "platform")
        assert filtered.collapsed is False
        assert len(filtered.entities) == 4


class TestAnnotationsFromDf:
    """test from_df class method"""
    
    def test_from_df_basic(self, sample_data):
        """test creating annotations from combined dataframe"""
        data, ids = sample_data
        combined_df = pl.concat([ids, data], how="horizontal")
        
        anno = Annotations.from_df(
            combined_df, 
            index_col="index",
            group_cols=("group", "platform")
        )
        
        assert isinstance(anno, Annotations)
        assert anno.index_col == "index"
        assert anno.group_cols == ("group", "platform")
        assert anno.n_indices == 4
        assert anno.n_entities == 4
        
    def test_from_df_with_defaults(self, sample_data):
        """test from_df with default group_cols"""
        data, ids = sample_data
        combined_df = pl.concat([ids, data], how="horizontal")
        
        anno = Annotations.from_df(combined_df, index_col="index")
        assert anno.group_cols == ("group", "platform")


class TestAnnotationsCollapse:
    """test collapse functionality"""
    
    def test_collapse_inplace(self, annotations_instance):
        """test collapsing inplace"""
        original_id = id(annotations_instance)
        result = annotations_instance.collapse("group", inplace=True)
        
        assert id(result) == original_id  # same object
        assert annotations_instance.collapsed is True
        assert annotations_instance.index_col == "group"
        assert "group" not in annotations_instance.group_cols
        assert annotations_instance.n_indices == 2  # collapsed to 2 groups
        
    def test_collapse_not_inplace(self, annotations_instance):
        """test collapsing without inplace"""
        result = annotations_instance.collapse("group", inplace=False)
        
        assert isinstance(result, Annotations)
        assert result is not annotations_instance  # different object
        assert annotations_instance.collapsed is False  # original unchanged
        assert result.collapsed is True
        assert result.index_col == "group"
        
    def test_collapse_updates_group_cols(self, annotations_instance):
        """test that collapse removes collapsed column from group_cols"""
        result = annotations_instance.collapse("platform", inplace=False)
        
        assert "platform" not in result.group_cols
        assert "group" in result.group_cols


class TestAnnotationsToParquet:
    """test to_parquet method"""
    
    @patch('polars.DataFrame.write_parquet')
    def test_to_parquet(self, mock_write, annotations_instance, tmp_path):
        """test saving to parquet file"""
        file_path = tmp_path / "test.parquet"
        
        annotations_instance.to_parquet(file_path)
        
        mock_write.assert_called_once_with(file_path)


class TestAnnotationsPrepareControlData:
    """test control data preparation"""
    
    def test_prepare_control_data_exists(self, annotations_instance):
        """test preparing control data when control column exists"""
        ctrl_ids = annotations_instance._prepare_control_data("MONDO:0000000")
        
        assert ctrl_ids is not None
        assert annotations_instance.controls is True
        assert "MONDO:0000000" not in annotations_instance.entities
        assert ctrl_ids.height == 2  # 2 control samples
        
    def test_prepare_control_data_missing(self, annotations_instance):
        """test preparing control data when control column missing"""
        ctrl_ids = annotations_instance._prepare_control_data("nonexistent_col")
        
        assert ctrl_ids is None
        assert annotations_instance.controls is False


class TestAnnotationsPropagateControls:
    """test control propagation"""
    
    def test_propagate_controls(self, annotations_instance):
        """test propagating control samples"""
        # setup control data
        ctrl_id = pl.DataFrame({
            "index": ["sample_3", "sample_4"],
            "group": ["study_b", "study_b"]
        })
        
        terms = ["MONDO:0005147", "MONDO:0001657"]
        labels = pl.DataFrame({
            "MONDO:0005147": [1, 1, 0, 0],
            "MONDO:0001657": [1, 0, 1, 0]
        })
        
        result = annotations_instance.propagate_controls(
            ctrl_id, terms, labels, "group"
        )
        
        assert result is not None
        # result should be a lazy frame
        assert hasattr(result, 'collect')


@patch('curations.annotations.Graph')
@patch('curations.annotations.Propagator')
@patch('curations.annotations.ontologies')
class TestAnnotationsToLabels:
    """test to_labels conversion"""
    
    def test_to_labels_base(self, mock_ontologies, mock_propagator_class, 
                            mock_graph_class, annotations_instance):
        """test basic to_labels conversion"""
        # setup mocks
        mock_graph = Mock()
        mock_graph.nodes = ["MONDO:0001657", "MONDO:0004790", "MONDO:0005147"]
        mock_graph_class.from_obo.return_value = mock_graph
        
        mock_propagator = Mock()
        mock_propagator.propagate.return_value = pl.DataFrame({
            "MONDO:0001675": [1, 0, 1, 0],
            "MONDO:0004790": [0, 1, 0, 1]
        })
        mock_propagator_class.return_value = mock_propagator
        
        mock_ontologies.return_value = "mock_ontology_path"
        
        # remove control column to avoid control logic
        annotations_instance.data = annotations_instance.data.drop("MONDO:0000000")
        
        result = annotations_instance.to_labels("mondo", to="all")
        
        assert isinstance(result, Labels)
        mock_graph_class.from_obo.assert_called_once()
        mock_propagator_class.assert_called_once()
        
    def test_to_labels_with_controls(self, mock_ontologies, mock_propagator_class,
                                   mock_graph_class, annotations_instance):
        """test to_labels with control propagation"""
        # setup mocks
        mock_graph = Mock()
        mock_graph.nodes = ["MONDO:0001657", "MONDO:0004790", "MONDO:0005147"]
        mock_graph_class.from_obo.return_value = mock_graph
        
        mock_propagator = Mock()
        mock_propagator.propagate.return_value = pl.DataFrame({
            "MONDO:0001657": [1, 0, 1, 0],
            "MONDO:0004790": [0, 1, 0, 1]
        })
        mock_propagator_class.return_value = mock_propagator
        
        mock_ontologies.return_value = "mock_ontology_path"
        
        with patch.object(annotations_instance, 'propagate_controls') as mock_prop_ctrl:
            mock_prop_ctrl.return_value = pl.DataFrame({
                "index": ["sample_3"],
                "tissue_brain": [2]
            }).lazy()
            
            result = annotations_instance.to_labels("mondo", to="all")
            
            assert isinstance(result, Labels)


class TestAnnotationsPrivateMethods:
    """test private helper methods"""
    
    @patch('curations.annotations.Graph')
    def test_get_union_terms(self, mock_graph_class, annotations_instance):
        """test getting union of terms"""
        mock_graph = Mock()
        mock_graph.nodes = ["MONDO:0001657", "MONDO:0004790", "unknown_term"]
        
        result = annotations_instance._get_union_terms(mock_graph)
        
        # should return intersection of graph nodes and entities
        expected_terms = ["MONDO:0001657", "MONDO:0004790"]
        assert len(result) == 2
        assert all(term in expected_terms for term in result)
        
    def test_prepare_targets_all(self, annotations_instance):
        """test preparing targets for 'all' option"""
        mock_graph = Mock()
        mock_graph.nodes = ["term1", "term2", "term3"]
        
        result = annotations_instance._prepare_targets(mock_graph, "mondo", "all")
        assert result == ["term1", "term2", "term3"]
        
    def test_prepare_targets_invalid(self, annotations_instance):
        """test preparing targets with invalid option"""
        mock_graph = Mock()
        
        with pytest.raises(ValueError):
            annotations_instance._prepare_targets(mock_graph, "mondo", "invalid")


class TestAnnotationsEdgeCases:
    """test edge cases and error conditions"""
    
    def test_empty_dataframe(self):
        """test with empty dataframes"""
        empty_data = pl.DataFrame()
        empty_ids = pl.DataFrame({"index": []})
        
        anno = Annotations(empty_data, empty_ids, "index")
        assert anno.n_indices == 0
        assert anno.n_entities == 0
        
    def test_single_row(self):
        """test with single row data"""
        data = pl.DataFrame({"MONDO:0001657": [1]})
        ids = pl.DataFrame({"index": ["sample_1"], "group": ["study_a"], "platform": ["rna"]})
        
        anno = Annotations(data, ids, "index")
        assert anno.n_indices == 1
        assert anno.n_entities == 1
        assert anno.index == ["sample_1"]
        
    def test_filter_all_rows(self, annotations_instance):
        """test filtering that removes all rows"""
        # filter condition that matches no rows
        filtered = annotations_instance.filter(pl.col("MONDO:0001657") == 999)
        
        assert filtered.n_indices == 0
        assert len(filtered.index) == 0
