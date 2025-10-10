"""
Unit tests for Retriever class.

Author: Parker Hicks
Date: 2025-09-26

Last updated: 2025-09-26 by Parker Hicks
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from metahq_cli.retriever import CurationConfig, OutputConfig, QueryConfig, Retriever


# =====================================================
# ======== unit tests (mock instances)
# =====================================================
class TestQueryConfig:
    """test queryconfig dataclass"""

    def test_query_config_creation(self):
        config = QueryConfig(
            database="test_db",
            attribute="test_attr",
            level="test_level",
            ecode="test_ecode",
            species="test_species",
            technology="test_tech",
        )

        assert config.database == "test_db"
        assert config.attribute == "test_attr"
        assert config.level == "test_level"
        assert config.ecode == "test_ecode"
        assert config.species == "test_species"
        assert config.technology == "test_tech"


class TestCurationConfig:
    """test curationconfig dataclass"""

    def test_curation_config_creation(self):
        config = CurationConfig(
            mode="direct", terms=["term1", "term2"], ontology="test_ontology"
        )

        assert config.mode == "direct"
        assert config.terms == ["term1", "term2"]
        assert config.ontology == "test_ontology"


class TestOutputConfig:
    """test outputconfig dataclass"""

    def test_output_config_creation(self):
        config = OutputConfig(outfile="test.json", fmt="json", metadata="test_metadata")

        assert config.outfile == "test.json"
        assert config.fmt == "json"
        assert config.metadata == "test_metadata"


class TestRetriever:
    """test retriever class"""

    @pytest.fixture
    def sample_configs(self):
        """fixture for test configurations"""
        query_config = QueryConfig(
            database="test_db",
            attribute="test_attr",
            level="test_level",
            ecode="test_ecode",
            species="test_species",
            technology="test_tech",
        )

        curation_config = CurationConfig(
            mode="direct", terms=["term1", "term2"], ontology="test_ontology"
        )

        output_config = OutputConfig(
            outfile="test.json", fmt="json", metadata="test_metadata"
        )

        return query_config, curation_config, output_config

    @pytest.fixture
    def retriever(self, sample_configs):
        """fixture for retriever instance"""
        query_config, curation_config, output_config = sample_configs
        return Retriever(query_config, curation_config, output_config, verbose=False)

    @pytest.fixture
    def verbose_retriever(self, sample_configs):
        """fixture for verbose retriever instance"""
        query_config, curation_config, output_config = sample_configs
        return Retriever(query_config, curation_config, output_config, verbose=True)

    def test_retriever_initialization(self, sample_configs):
        """test retriever init stores configs correctly"""
        query_config, curation_config, output_config = sample_configs
        retriever = Retriever(
            query_config, curation_config, output_config, verbose=True
        )

        assert retriever.query_config == query_config
        assert retriever.curation_config == curation_config
        assert retriever.output_config == output_config
        assert retriever.verbose is True

    @patch("metahq_cli.retriever.Query")
    def test_query_silent(self, mock_query_class, retriever):
        """test silent query execution"""
        mock_query = Mock()
        mock_annotations = Mock()
        mock_query.annotations.return_value = mock_annotations
        mock_query_class.return_value = mock_query

        result = retriever.query()

        mock_query_class.assert_called_once_with(
            database="test_db",
            attribute="test_attr",
            level="test_level",
            ecode="test_ecode",
            species="test_species",
            technology="test_tech",
        )
        mock_query.annotations.assert_called_once()
        assert result == mock_annotations

    @patch("metahq_cli.retriever.Query")
    def test_query_verbose(self, mock_query_class, verbose_retriever):
        """test verbose query execution uses spinner"""
        mock_query = Mock()
        mock_annotations = Mock()
        mock_query.annotations.return_value = mock_annotations
        mock_query_class.return_value = mock_query

        result = verbose_retriever.query()

        mock_query_class.assert_called_once()
        assert result == mock_annotations

    def test_curate_direct_mode(self, retriever):
        """test curation with direct mode"""
        mock_annotations = Mock()
        retriever.curation_config.mode = "direct"

        with patch.object(retriever, "_direct_annotations") as mock_direct:
            mock_direct.return_value = mock_annotations
            result = retriever.curate(mock_annotations)

            mock_direct.assert_called_once_with(mock_annotations)
            assert result == mock_annotations

    def test_curate_propagate_mode(self, retriever):
        """test curation with propagate mode"""
        mock_annotations = Mock()
        retriever.curation_config.mode = "propagate"

        with patch.object(retriever, "_propagate_annotations") as mock_propagate:
            mock_propagate.return_value = mock_annotations
            result = retriever.curate(mock_annotations)

            mock_propagate.assert_called_once_with(mock_annotations, mode=0)
            assert result == mock_annotations

    def test_curate_label_mode(self, retriever):
        """test curation with label mode"""
        mock_annotations = Mock()
        retriever.curation_config.mode = "label"

        with patch.object(retriever, "_propagate_annotations") as mock_propagate:
            mock_labels = Mock()
            mock_propagate.return_value = mock_labels
            result = retriever.curate(mock_annotations)

            mock_propagate.assert_called_once_with(mock_annotations, mode=1)
            assert result == mock_labels

    @patch("metahq_cli.retriever.error")
    def test_curate_invalid_mode_raises_error(self, mock_error, retriever):
        """test invalid curation mode calls error function"""
        mock_annotations = Mock()
        retriever.curation_config.mode = "invalid_mode"

        retriever.curate(mock_annotations)

        mock_error.assert_called_once()
        error_call_args = mock_error.call_args[0][0]
        assert "invalid_mode" in error_call_args

    def test_direct_annotations_with_matching_terms(self, retriever):
        """test direct annotations when terms match entities"""
        mock_annotations = Mock()
        mock_annotations.entities = ["term1", "term2", "other_term"]
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected

        result = retriever._direct_annotations(mock_annotations)

        mock_annotations.select.assert_called_once_with(["term1", "term2"])
        mock_selected.filter.assert_called_once()
        assert result == mock_filtered

    @patch("metahq_cli.retriever.error")
    def test_direct_annotations_no_matches_calls_error(self, mock_error, retriever):
        """test direct annotations with no matching terms calls error"""
        mock_annotations = Mock()
        mock_annotations.entities = ["other_term1", "other_term2"]

        retriever._direct_annotations(mock_annotations)

        mock_error.assert_called_once()
        error_call_args = mock_error.call_args[0][0]
        assert "No direct annotations" in error_call_args

    @patch("metahq_cli.retriever.warning")
    def test_direct_annotations_partial_match_warns_verbose(
        self, mock_warning, verbose_retriever
    ):
        """test partial term matches trigger warning in verbose mode"""
        mock_annotations = Mock()
        mock_annotations.entities = ["term1"]  # only one of two terms
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected

        result = verbose_retriever._direct_annotations(mock_annotations)

        mock_warning.assert_called_once()
        warning_call_args = mock_warning.call_args[0][0]
        assert "term2" in warning_call_args
        assert result == mock_filtered

    def test_direct_annotations_partial_match_silent_mode(self, retriever):
        """test partial matches don't warn in silent mode"""
        mock_annotations = Mock()
        mock_annotations.entities = ["term1"]  # only one of two terms
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected

        with patch("metahq_cli.retriever.warning") as mock_warning:
            result = retriever._direct_annotations(mock_annotations)

            mock_warning.assert_not_called()
            assert result == mock_filtered

    def test_propagate_annotations(self, retriever):
        """test propagation wrapper calls annotations.propagate correctly"""
        mock_annotations = Mock()
        mock_result = Mock()
        mock_annotations.propagate.return_value = mock_result

        result = retriever._propagate_annotations(mock_annotations, mode=0)

        mock_annotations.propagate.assert_called_once_with(
            ["term1", "term2"], "test_ontology", mode=0, verbose=False
        )
        assert result == mock_result

    def test_propagate_annotations_verbose(self, verbose_retriever):
        """test propagation in verbose mode"""
        mock_annotations = Mock()
        mock_result = Mock()
        mock_annotations.propagate.return_value = mock_result

        result = verbose_retriever._propagate_annotations(mock_annotations, mode=1)

        mock_annotations.propagate.assert_called_once_with(
            ["term1", "term2"], "test_ontology", mode=1, verbose=True
        )
        assert result == mock_result

    def test_save_curation_silent(self, retriever):
        """test save curation in silent mode"""
        mock_curation = Mock()

        with patch.object(retriever, "_save_silent") as mock_save_silent:
            retriever.save_curation(mock_curation)
            mock_save_silent.assert_called_once_with(mock_curation)

    def test_save_curation_verbose(self, verbose_retriever):
        """test save curation in verbose mode calls both methods"""
        mock_curation = Mock()

        with patch.object(
            verbose_retriever, "_save_verbose"
        ) as mock_save_verbose, patch.object(
            verbose_retriever, "_save_silent"
        ) as mock_save_silent:

            verbose_retriever.save_curation(mock_curation)

            mock_save_verbose.assert_called_once_with(mock_curation)
            mock_save_silent.assert_called_once_with(mock_curation)

    def test_save_calls_curation_save_method(self, retriever):
        """test _save calls the save method on curation object"""
        mock_curation = Mock()

        retriever._save(mock_curation)

        mock_curation.save.assert_called_once_with(
            outfile="test.json", fmt="json", metadata="test_metadata"
        )

    @patch.object(Retriever, "query")
    @patch.object(Retriever, "curate")
    @patch.object(Retriever, "save_curation")
    def test_retrieve_pipeline(self, mock_save, mock_curate, mock_query, retriever):
        """test retrieve method executes full pipeline"""
        mock_annotations = Mock()
        mock_curated = Mock()

        mock_query.return_value = mock_annotations
        mock_curate.return_value = mock_curated

        retriever.retrieve()

        mock_query.assert_called_once()
        mock_curate.assert_called_once_with(mock_annotations)
        mock_save.assert_called_once_with(mock_curated)

    @pytest.mark.parametrize(
        "mode,expected_mode",
        [
            ("direct", None),  # direct doesn't call propagate
            ("propagate", 0),
            ("label", 1),
        ],
    )
    def test_curate_by_mode_parameter_mapping(self, retriever, mode, expected_mode):
        """test different modes map to correct parameters"""
        mock_annotations = Mock()
        retriever.curation_config.mode = mode

        if mode == "direct":
            with patch.object(retriever, "_direct_annotations") as mock_method:
                retriever._curate_by_mode(mock_annotations)
                mock_method.assert_called_once_with(mock_annotations)
        else:
            with patch.object(retriever, "_propagate_annotations") as mock_method:
                retriever._curate_by_mode(mock_annotations)
                mock_method.assert_called_once_with(
                    mock_annotations, mode=expected_mode
                )

    def test_output_config_with_pathlib_path(self):
        """test output config accepts pathlib path objects"""
        path_obj = Path("test_file.csv")
        config = OutputConfig(outfile=path_obj, fmt="csv", metadata="test")

        assert config.outfile == path_obj
        assert isinstance(config.outfile, Path)
