"""
Unit tests for annotations curations class.

Author: Parker Hicks
Date: 2025-09-01

Last updated: 2025-09-26 by Parker Hicks
"""

import polars as pl
import pytest

from metahq_core.curations.annotations import Annotations
from metahq_core.curations.index import Ids


@pytest.fixture
def sample_data():
    """basic test data for annotations"""
    data = pl.DataFrame(
        {
            "MONDO:0001657": [1, 0, 1, 0],  # brain cancer
            "MONDO:0004790": [0, 1, 0, 1],  # fatty liver
            "MONDO:0005147": [1, 1, 0, 0],  # diabetes
            "MONDO:0000000": [0, 0, 1, 1],  # control
        }
    )

    ids = pl.DataFrame(
        {
            "sample": ["sample_1", "sample_2", "sample_3", "sample_4"],
            "series": ["study_a", "study_a", "study_b", "study_b"],
            "platform": ["GPL1", "GPL1", "GPL2", "GPL2"],
        }
    )

    return data, ids


@pytest.fixture
def annotations_instance(sample_data):
    """basic annotations instance"""
    data, ids = sample_data
    return Annotations(
        data=data,
        ids=ids,
        index_col="sample",
        group_cols=("series", "platform"),
        collapsed=False,
    )


class TestAnnotationsInit:
    """test initialization"""

    def test_init_basic(self, sample_data):
        """test basic initialization"""
        data, ids = sample_data
        anno = Annotations(data, ids, "sample", ("series", "platform"), False)

        assert anno.data.equals(data)
        assert anno.index_col == "sample"
        assert anno.group_cols == ("series", "platform")
        assert anno.collapsed is False
        assert anno.controls is False

    def test_init_with_defaults(self, sample_data):
        """test initialization with default group_cols"""
        data, ids = sample_data
        anno = Annotations(data, ids, "sample")

        assert anno.group_cols == ("series", "platform")
        assert anno.collapsed is False

    def test_init_creates_ids_object(self, sample_data):
        """test that initialization creates ids object correctly"""
        data, ids = sample_data
        anno = Annotations(data, ids, "sample")

        assert isinstance(anno._ids, Ids)
        assert anno._ids.data.equals(ids)


class TestAnnotationsProperties:
    """test property methods"""

    def test_entities(self, annotations_instance):
        """test entities property excludes id columns"""
        entities = annotations_instance.entities
        expected = ["MONDO:0001657", "MONDO:0004790", "MONDO:0005147", "MONDO:0000000"]
        assert set(entities) == set(expected)
        # ensure no id columns are included
        assert "sample" not in entities
        assert "series" not in entities
        assert "platform" not in entities

    def test_groups(self, annotations_instance):
        """test groups property"""
        groups = annotations_instance.groups
        expected = ["study_a", "study_a", "study_b", "study_b"]
        assert groups == expected

    def test_ids(self, annotations_instance):
        """test ids property returns dataframe"""
        ids_df = annotations_instance.ids
        assert isinstance(ids_df, pl.DataFrame)
        assert "sample" in ids_df.columns
        assert "series" in ids_df.columns
        assert "platform" in ids_df.columns

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
        """test drop method returns new instance"""
        result = annotations_instance.drop("MONDO:0001657")

        assert isinstance(result, Annotations)
        assert "MONDO:0001657" not in result.entities
        assert len(result.entities) == 3
        # original should be unchanged
        assert "MONDO:0001657" in annotations_instance.entities

    def test_head(self, annotations_instance):
        """test head method returns string representation"""
        result = annotations_instance.head(2)
        assert isinstance(result, str)

    def test_repr(self, annotations_instance):
        """test string representation"""
        result = repr(annotations_instance)
        assert isinstance(result, str)
        assert "sample_1" in result  # should include index info

    def test_sort_columns(self, annotations_instance):
        """test sort_columns method"""
        result = annotations_instance.sort_columns()

        assert isinstance(result, Annotations)
        # columns should be sorted alphabetically
        assert result.data.columns == sorted(annotations_instance.data.columns)


class TestAnnotationsSelect:
    """test select method"""

    def test_select_columns(self, annotations_instance):
        """test selecting specific columns"""
        selected = annotations_instance.select("MONDO:0001657", "MONDO:0004790")

        assert isinstance(selected, Annotations)
        assert set(selected.entities) == {"MONDO:0001657", "MONDO:0004790"}
        assert selected.n_indices == 4
        assert selected.index_col == "sample"
        assert selected.group_cols == ("series", "platform")

    def test_select_preserves_ids(self, annotations_instance):
        """test that select preserves full id data"""
        selected = annotations_instance.select("MONDO:0001657")
        assert selected.ids.equals(annotations_instance.ids)

    def test_select_single_column(self, annotations_instance):
        """test selecting single column"""
        selected = annotations_instance.select("MONDO:0001657")

        assert selected.entities == ["MONDO:0001657"]
        assert selected.n_indices == 4


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

    def test_slice_preserves_metadata(self, annotations_instance):
        """test that slicing preserves object metadata"""
        sliced = annotations_instance.slice(0, 2)

        assert sliced.index_col == annotations_instance.index_col
        assert sliced.group_cols == annotations_instance.group_cols
        assert sliced.collapsed == annotations_instance.collapsed


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

        assert filtered.index_col == "sample"
        assert filtered.group_cols == ("series", "platform")
        assert filtered.collapsed is False
        assert len(filtered.entities) == 4

    def test_filter_multiple_conditions(self, annotations_instance):
        """test filtering with multiple conditions"""
        condition = (pl.col("MONDO:0001657") == 1) & (pl.col("MONDO:0005147") == 1)
        filtered = annotations_instance.filter(condition)

        assert filtered.n_indices == 1
        assert filtered.index == ["sample_1"]

    def test_filter_no_matches(self, annotations_instance):
        """test filtering that returns no matches"""
        filtered = annotations_instance.filter(pl.col("MONDO:0001657") == 999)

        assert filtered.n_indices == 0
        assert filtered.index == []


class TestAnnotationsFromDf:
    """test from_df class method"""

    def test_from_df_basic(self, sample_data):
        """test creating annotations from combined dataframe"""
        data, ids = sample_data
        combined_df = pl.concat([ids, data], how="horizontal")

        anno = Annotations.from_df(
            combined_df, index_col="sample", group_cols=("series", "platform")
        )

        assert isinstance(anno, Annotations)
        assert anno.index_col == "sample"
        assert anno.group_cols == ("series", "platform")
        assert anno.n_indices == 4
        assert anno.n_entities == 4

    def test_from_df_with_defaults(self, sample_data):
        """test from_df with default group_cols"""
        data, ids = sample_data
        combined_df = pl.concat([ids, data], how="horizontal")

        anno = Annotations.from_df(combined_df, index_col="sample")
        assert anno.group_cols == ("series", "platform")

    def test_from_df_with_list_group_cols(self, sample_data):
        """test from_df converts list to tuple for group_cols"""
        data, ids = sample_data
        combined_df = pl.concat([ids, data], how="horizontal")

        anno = Annotations.from_df(
            combined_df,
            index_col="sample",
            group_cols=["series", "platform"],  # list instead of tuple
        )
        assert anno.group_cols == ("series", "platform")


class TestAnnotationsCollapse:
    """test collapse functionality"""

    def test_collapse_inplace(self, annotations_instance):
        """test collapsing inplace"""
        original_id = id(annotations_instance)
        result = annotations_instance.collapse("series", inplace=True)

        assert id(result) == original_id  # same object
        assert annotations_instance.collapsed is True
        assert annotations_instance.index_col == "series"
        assert "series" not in annotations_instance.group_cols
        assert annotations_instance.n_indices == 2  # collapsed to 2 groups

    def test_collapse_not_inplace(self, annotations_instance):
        """test collapsing without inplace"""
        result = annotations_instance.collapse("series", inplace=False)

        assert isinstance(result, Annotations)
        assert result is not annotations_instance  # different object
        assert annotations_instance.collapsed is False  # original unchanged
        assert result.collapsed is True
        assert result.index_col == "series"

    def test_collapse_updates_group_cols(self, annotations_instance):
        """test that collapse removes collapsed column from group_cols"""
        result = annotations_instance.collapse("platform", inplace=False)

        assert "platform" not in result.group_cols
        assert "series" in result.group_cols

    def test_collapse_aggregates_correctly(self, annotations_instance):
        """test that collapse properly aggregates annotation values"""
        result = annotations_instance.collapse("series", inplace=False)

        # verify aggregation logic - any positive value becomes 1
        assert result.n_indices == 2
        # check that values are 0 or 1 after aggregation
        for col in result.entities:
            values = result.data[col].to_list()
            assert all(v in [0, 1] for v in values)


class TestAnnotationsPrivateMethods:
    """test private helper methods"""

    def test_collapse_private_method(self, annotations_instance):
        """test _collapse private method returns correct structure"""
        params = annotations_instance._collapse("series")

        assert isinstance(params, dict)
        assert "data" in params
        assert "ids" in params
        assert "index_col" in params
        assert "group_cols" in params
        assert "collapsed" in params
        assert params["collapsed"] is True
        assert params["index_col"] == "series"

    def test_collapse_ids_private_method(self, annotations_instance):
        """test _collapse_ids private method"""
        keep_groups = ["study_a", "study_b"]
        result = annotations_instance._collapse_ids("series", keep_groups)

        assert isinstance(result, pl.DataFrame)
        assert "series" in result.columns
        # should not contain the index column
        assert "sample" not in result.columns


class TestAnnotationsEdgeCases:
    """test edge cases and error conditions"""

    def test_empty_dataframe(self):
        """test with empty dataframes"""
        empty_data = pl.DataFrame()
        empty_ids = pl.DataFrame({"sample": [], "series": [], "platform": []})

        anno = Annotations(empty_data, empty_ids, "sample")
        assert anno.n_indices == 0
        assert anno.n_entities == 0

    def test_single_row(self):
        """test with single row data"""
        data = pl.DataFrame({"MONDO:0001657": [1]})
        ids = pl.DataFrame(
            {"sample": ["sample_1"], "series": ["study_a"], "platform": ["rna"]}
        )

        anno = Annotations(data, ids, "sample")
        assert anno.n_indices == 1
        assert anno.n_entities == 1
        assert anno.index == ["sample_1"]

    def test_single_entity(self, annotations_instance):
        """test with single entity selection"""
        single = annotations_instance.select("MONDO:0001657")

        assert single.n_entities == 1
        assert single.entities == ["MONDO:0001657"]
        assert single.n_indices == 4

    def test_entities_excludes_id_columns_correctly(self, annotations_instance):
        """test that entities property correctly excludes all id columns"""
        # entities should not include any columns from ids dataframe
        id_columns = set(annotations_instance.ids.columns)
        entity_columns = set(annotations_instance.entities)

        assert len(id_columns.intersection(entity_columns)) == 0


class TestAnnotationsDataIntegrity:
    """test data integrity and consistency"""

    def test_data_ids_consistency(self, annotations_instance):
        """test that data and ids have consistent row counts"""
        assert annotations_instance.data.height == annotations_instance.ids.height
        assert len(annotations_instance.index) == annotations_instance.n_indices

    def test_operations_preserve_consistency(self, annotations_instance):
        """test that operations preserve data/ids consistency"""
        # test slice
        sliced = annotations_instance.slice(0, 2)
        assert sliced.data.height == sliced.ids.height

        # test filter
        filtered = annotations_instance.filter(pl.col("MONDO:0001657") == 1)
        assert filtered.data.height == filtered.ids.height

    def test_collapse_preserves_entity_columns(self, annotations_instance):
        """test that collapse preserves entity columns correctly"""
        original_entities = set(annotations_instance.entities)
        collapsed = annotations_instance.collapse("series", inplace=False)

        # entity columns should be the same (minus any removed group columns)
        collapsed_entities = set(collapsed.entities)
        # the entity columns themselves shouldn't change
        assert original_entities == collapsed_entities
