"""
Unit tests for the RefineBioExporter and DatasetCreator export classes.

Author: Parker Hicks
Date: 2026-06-19
"""

from unittest.mock import Mock, patch

import polars as pl
import pytest

from metahq_core.curations.annotations import Annotations
from metahq_core.curations.labels import Labels
from metahq_core.export.refinebio import DatasetCreator, RefineBioExporter


@pytest.fixture
def mock_logger():
    """fixture for mock logger"""
    return Mock()


@pytest.fixture
def refinebio_map():
    """small refine.bio ID mapping table for tests"""
    return pl.DataFrame(
        {
            "refinebio_sample": ["GSM1", "GSM2", "GSM3", "SRR1"],
            "refinebio_experiment": ["GSE1", "GSE1", "GSE2", "GSE1"],
            "gsm": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "gse": ["GSE1", "GSE1", "GSE2", "GSE1"],
        }
    )


@pytest.fixture
def exporter(mock_logger, refinebio_map):
    """RefineBioExporter pre-loaded with a mapping table"""
    exp = RefineBioExporter(logger=mock_logger, verbose=False)
    exp._map = refinebio_map
    return exp


@pytest.fixture
def sample_annotations():
    """sample-level annotations curation"""
    data = pl.DataFrame({"UBERON:0000948": [1, 0, 1, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "series": ["GSE1", "GSE1", "GSE2", "GSE1"],
        }
    )
    return Annotations(data, ids, index_col="sample", group_cols=("series",))


@pytest.fixture
def series_annotations():
    """series-level annotations curation"""
    data = pl.DataFrame({"UBERON:0000948": [1, 0]})
    ids = pl.DataFrame({"series": ["GSE1", "GSE2"]})
    return Annotations(data, ids, index_col="series", group_cols=())


@pytest.fixture
def sample_labels():
    """sample-level labels curation"""
    data = pl.DataFrame({"UBERON:0000948": [1, -1, 1, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "series": ["GSE1", "GSE1", "GSE2", "GSE1"],
        }
    )
    return Labels(data, ids, index_col="sample", group_cols=("series",))


class TestGeoCol:
    """test _geo_col private method"""

    def test_sample_index(self, exporter):
        assert exporter._geo_col("sample") == "gsm"

    def test_series_index(self, exporter):
        assert exporter._geo_col("series") == "gse"

    def test_invalid_index_raises(self, exporter):
        with pytest.raises(ValueError):
            exporter._geo_col("platform")

    def test_invalid_index_logs_when_verbose(self, mock_logger, refinebio_map):
        exp = RefineBioExporter(logger=mock_logger, verbose=True)
        exp._map = refinebio_map

        with pytest.raises(ValueError):
            exp._geo_col("platform")

        mock_logger.error.assert_called_once()

    def test_invalid_index_silent_does_not_log(self, exporter):
        with pytest.raises(ValueError):
            exporter._geo_col("platform")

        exporter.log.error.assert_not_called()


class TestLoadMap:
    """test _load_map private method"""

    @patch("metahq_core.export.refinebio.pl.read_parquet")
    @patch("metahq_core.export.refinebio.refinebio_metadata")
    def test_loads_and_caches(
        self, mock_metadata_path, mock_read_parquet, mock_logger
    ):
        """test the map is read once and cached for subsequent calls"""
        mock_metadata_path.return_value = "fake/path.parquet"
        mock_df = pl.DataFrame({"gsm": ["GSM1"]})
        mock_read_parquet.return_value = mock_df

        exp = RefineBioExporter(logger=mock_logger, verbose=False)
        result = exp._load_map()

        mock_read_parquet.assert_called_once_with("fake/path.parquet")
        assert result.equals(mock_df)

        exp._load_map()
        mock_read_parquet.assert_called_once()


class TestGetRefinebio:
    """test get_refinebio method"""

    def test_merges_sample_level_ids(self, exporter, sample_annotations):
        merged = exporter.get_refinebio(
            sample_annotations, ["refinebio_sample", "refinebio_experiment"]
        )

        assert isinstance(merged, Annotations)
        assert "refinebio_sample" in merged.ids.columns
        assert "refinebio_experiment" in merged.ids.columns
        assert merged.ids["refinebio_sample"].to_list() == [
            "GSM1",
            "GSM2",
            "GSM3",
            "SRR1",
        ]
        assert merged.ids["refinebio_experiment"].to_list() == [
            "GSE1",
            "GSE1",
            "GSE2",
            "GSE1",
        ]

    def test_merges_series_level_ids(self, exporter, series_annotations):
        merged = exporter.get_refinebio(series_annotations, ["refinebio_experiment"])

        assert merged.ids["refinebio_experiment"].to_list() == ["GSE1", "GSE2"]
        assert "refinebio_sample" not in merged.ids.columns

    def test_only_requested_fields_are_merged(self, exporter, sample_annotations):
        """passing only refinebio_experiment should not add refinebio_sample
        even though a 'sample' column is present."""
        merged = exporter.get_refinebio(sample_annotations, ["refinebio_experiment"])

        assert "refinebio_experiment" in merged.ids.columns
        assert "refinebio_sample" not in merged.ids.columns

    def test_fills_unmatched_sample_with_na_but_keeps_experiment(self, exporter):
        """a sample missing from the sample map should still resolve its
        experiment through its series."""
        data = pl.DataFrame({"UBERON:0000948": [1]})
        ids = pl.DataFrame({"sample": ["GSM_UNKNOWN"], "series": ["GSE1"]})
        anno = Annotations(data, ids, index_col="sample", group_cols=("series",))

        merged = exporter.get_refinebio(
            anno, ["refinebio_sample", "refinebio_experiment"]
        )

        assert merged.ids["refinebio_sample"].to_list() == ["NA"]
        assert merged.ids["refinebio_experiment"].to_list() == ["GSE1"]

    def test_fills_unmatched_ids_with_na(self, exporter):
        data = pl.DataFrame({"UBERON:0000948": [1]})
        ids = pl.DataFrame({"sample": ["GSM_UNKNOWN"]})
        anno = Annotations(data, ids, index_col="sample", group_cols=())

        merged = exporter.get_refinebio(anno, ["refinebio_sample"])

        assert merged.ids["refinebio_sample"].to_list() == ["NA"]

    def test_preserves_index_order(self, exporter, sample_annotations):
        merged = exporter.get_refinebio(sample_annotations, ["refinebio_sample"])
        assert merged.index == sample_annotations.index

    def test_works_with_labels_curation(self, exporter, sample_labels):
        merged = exporter.get_refinebio(sample_labels, ["refinebio_sample"])

        assert isinstance(merged, Labels)
        assert merged.ids["refinebio_sample"].to_list() == [
            "GSM1",
            "GSM2",
            "GSM3",
            "SRR1",
        ]


class TestToExperimentSamples:
    """test _to_experiment_samples private method"""

    def test_groups_by_experiment_sample_level(self, exporter, sample_annotations):
        result = exporter._to_experiment_samples(sample_annotations)

        assert set(result["GSE1"]) == {"GSM1", "GSM2", "SRR1"}
        assert result["GSE2"] == ["GSM3"]

    def test_groups_by_experiment_series_level(self, exporter, series_annotations):
        result = exporter._to_experiment_samples(series_annotations)

        assert set(result["GSE1"]) == {"GSM1", "GSM2", "SRR1"}
        assert result["GSE2"] == ["GSM3"]

    def test_excludes_unmatched_rows(self, exporter):
        data = pl.DataFrame({"UBERON:0000948": [1]})
        ids = pl.DataFrame({"sample": ["GSM1"]})
        anno = Annotations(data, ids, index_col="sample", group_cols=())

        result = exporter._to_experiment_samples(anno)

        assert result == {"GSE1": ["GSM1"]}


class TestCreateDataset:
    """test create_dataset method"""

    def test_calls_dataset_creator_with_grouped_data(self, exporter, sample_annotations):
        with patch("metahq_core.export.refinebio.DatasetCreator") as mock_creator_cls:
            mock_creator = Mock()
            mock_creator.create.return_value = {"id": "abc123"}
            mock_creator_cls.return_value = mock_creator

            result = exporter.create_dataset(sample_annotations)

            args, _ = mock_creator_cls.call_args
            assert set(args[0].keys()) == {"GSE1", "GSE2"}
            mock_creator.create.assert_called_once()
            assert result == {"id": "abc123"}


class TestDatasetCreator:
    """test DatasetCreator class"""

    @pytest.fixture
    def data(self):
        return {"GSE1": ["GSM1", "GSM2"], "GSE2": ["GSM3"]}

    @patch("metahq_core.export.refinebio.requests.post")
    def test_post_dataset_sends_expected_payload(self, mock_post, data, mock_logger):
        mock_response = Mock()
        mock_response.json.return_value = {"id": "abc123"}
        mock_post.return_value = mock_response

        creator = DatasetCreator(data, logger=mock_logger, verbose=False)
        result = creator.post_dataset()

        mock_post.assert_called_once_with(
            "https://api.refine.bio/v1/dataset/",
            json={"data": data, "email_ccdl_ok": False, "notify_me": False},
        )
        mock_response.raise_for_status.assert_called_once()
        assert result == {"id": "abc123"}

    @patch("metahq_core.export.refinebio.requests.post")
    def test_post_dataset_raises_on_http_error(self, mock_post, data, mock_logger):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = RuntimeError("boom")
        mock_post.return_value = mock_response

        creator = DatasetCreator(data, logger=mock_logger, verbose=False)

        with pytest.raises(RuntimeError, match="boom"):
            creator.post_dataset()

    def test_create_logs_dataset_cart_url_when_verbose(self, data, mock_logger):
        creator = DatasetCreator(data, logger=mock_logger, verbose=True)
        with patch.object(creator, "post_dataset", return_value={"id": "abc123"}):
            result = creator.create()

        assert result == {"id": "abc123"}
        assert mock_logger.info.call_count == 2
        url_call_args = mock_logger.info.call_args_list[1][0]
        assert "abc123" in url_call_args[1]

    def test_create_silent_mode_does_not_log(self, data, mock_logger):
        creator = DatasetCreator(data, logger=mock_logger, verbose=False)
        with patch.object(creator, "post_dataset", return_value={"id": "abc123"}):
            creator.create()

        mock_logger.info.assert_not_called()
