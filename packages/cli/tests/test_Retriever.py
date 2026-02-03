"""
Unit tests for Retriever class.

Author: Parker Hicks
Date: 2025-09-26

Last updated: 2026-02-02 by Parker Hicks
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from metahq_core.util.exceptions import NoResultsFound

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
            tech="test_tech",
        )

        assert config.database == "test_db"
        assert config.attribute == "test_attr"
        assert config.level == "test_level"
        assert config.ecode == "test_ecode"
        assert config.species == "test_species"
        assert config.tech == "test_tech"


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
        config = OutputConfig(
            outfile="test.json",
            fmt="json",
            metadata="test_metadata",
            attribute="test_attr",
            level="test_level",
        )

        assert config.outfile == "test.json"
        assert config.fmt == "json"
        assert config.metadata == "test_metadata"
        assert config.attribute == "test_attr"
        assert config.level == "test_level"


class TestRetriever:
    """test retriever class"""

    @pytest.fixture
    def mock_logger(self):
        """fixture for mock logger"""
        return Mock()

    @pytest.fixture
    def sample_configs(self):
        """fixture for test configurations"""
        query_config = QueryConfig(
            database="test_db",
            attribute="test_attr",
            level="test_level",
            ecode="test_ecode",
            species="test_species",
            tech="test_tech",
        )

        curation_config = CurationConfig(
            mode="direct", terms=["term1", "term2"], ontology="test_ontology"
        )

        output_config = OutputConfig(
            outfile="test.json",
            fmt="json",
            metadata="test_metadata",
            attribute="test_attr",
            level="test_level",
        )

        return query_config, curation_config, output_config

    @pytest.fixture
    def retriever(self, sample_configs, mock_logger):
        """fixture for retriever instance"""
        query_config, curation_config, output_config = sample_configs
        return Retriever(
            query_config,
            curation_config,
            output_config,
            logger=mock_logger,
            verbose=False,
        )

    @pytest.fixture
    def verbose_retriever(self, sample_configs, mock_logger):
        """fixture for verbose retriever instance"""
        query_config, curation_config, output_config = sample_configs
        return Retriever(
            query_config,
            curation_config,
            output_config,
            logger=mock_logger,
            verbose=True,
        )

    def test_retriever_initialization(self, sample_configs, mock_logger):
        """test retriever init stores configs correctly"""
        query_config, curation_config, output_config = sample_configs
        retriever = Retriever(
            query_config,
            curation_config,
            output_config,
            logger=mock_logger,
            verbose=True,
        )

        assert retriever.query_config == query_config
        assert retriever.curation_config == curation_config
        assert retriever.output_config == output_config
        assert retriever.verbose is True
        assert retriever.log == mock_logger

    def test_retriever_initialization_logs_configs_when_verbose(
        self, sample_configs, mock_logger
    ):
        """test retriever logs configs in verbose mode"""
        query_config, curation_config, output_config = sample_configs
        Retriever(
            query_config,
            curation_config,
            output_config,
            logger=mock_logger,
            verbose=True,
        )

        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert "Using configs:" in call_args[0]

    def test_retriever_initialization_no_logs_when_silent(
        self, sample_configs, mock_logger
    ):
        """test retriever does not log configs in silent mode"""
        query_config, curation_config, output_config = sample_configs
        Retriever(
            query_config,
            curation_config,
            output_config,
            logger=mock_logger,
            verbose=False,
        )

        mock_logger.debug.assert_not_called()

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
            logger=retriever.log,
            verbose=False,
        )
        mock_query.annotations.assert_called_once()
        assert result == mock_annotations

    @patch("metahq_cli.retriever.Query")
    def test_query_verbose(self, mock_query_class, verbose_retriever):
        """test verbose query execution"""
        mock_query = Mock()
        mock_annotations = Mock()
        mock_query.annotations.return_value = mock_annotations
        mock_query_class.return_value = mock_query

        result = verbose_retriever.query()

        mock_query_class.assert_called_once_with(
            database="test_db",
            attribute="test_attr",
            level="test_level",
            ecode="test_ecode",
            species="test_species",
            technology="test_tech",
            logger=verbose_retriever.log,
            verbose=True,
        )
        assert result == mock_annotations
        verbose_retriever.log.info.assert_called_with("Querying...")

    def test_curate_raises_error_when_no_annotations(self, retriever):
        """test curate raises NoResultsFound when there are no annotations"""
        mock_annotations = Mock()
        mock_annotations.n_indices = 0

        with pytest.raises(NoResultsFound) as exc_info:
            retriever.curate(mock_annotations)

        assert "No annotations for any terms" in str(exc_info.value)
        retriever.log.error.assert_called_once()

    def test_curate_direct_mode(self, retriever):
        """test curation with direct mode"""
        mock_annotations = Mock()
        mock_annotations.n_indices = 10
        mock_annotations.entities = ["term1", "term2"]
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected

        retriever.curation_config.mode = "direct"
        result = retriever.curate(mock_annotations)

        mock_annotations.select.assert_called_once_with(["term1", "term2"])
        assert result == mock_filtered

    def test_curate_propagate_mode(self, retriever):
        """test curation with propagate mode"""
        mock_annotations = Mock()
        mock_annotations.n_indices = 10
        retriever.curation_config.mode = "annotate"

        with patch.object(retriever, "_propagate_annotations") as mock_propagate:
            mock_propagate.return_value = mock_annotations
            result = retriever.curate(mock_annotations)

            mock_propagate.assert_called_once_with(mock_annotations, mode=0)
            assert result == mock_annotations

    def test_curate_label_mode(self, retriever):
        """test curation with label mode"""
        mock_annotations = Mock()
        mock_annotations.n_indices = 10
        retriever.curation_config.mode = "label"

        with patch.object(retriever, "_propagate_annotations") as mock_propagate:
            mock_labels = Mock()
            mock_propagate.return_value = mock_labels
            result = retriever.curate(mock_annotations)

            mock_propagate.assert_called_once_with(mock_annotations, mode=1)
            assert result == mock_labels

    def test_curate_logs_info_when_verbose(self, verbose_retriever):
        """test curate logs info message in verbose mode"""
        mock_annotations = Mock()
        mock_annotations.n_indices = 10
        mock_annotations.entities = ["term1"]
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected

        verbose_retriever.curate(mock_annotations)

        verbose_retriever.log.info.assert_called_with("Curating...")

    def test_curate_by_mode_invalid_mode_raises_error(self, retriever):
        """test invalid curation mode raises ValueError"""
        mock_annotations = Mock()
        retriever.curation_config.mode = "invalid_mode"

        with pytest.raises(ValueError):
            retriever._curate_by_mode(mock_annotations)

    def test_curate_by_mode_invalid_mode_logs_error_when_verbose(
        self, verbose_retriever
    ):
        """test invalid curation mode logs error in verbose mode"""
        mock_annotations = Mock()
        verbose_retriever.curation_config.mode = "invalid_mode"

        with pytest.raises(ValueError):
            verbose_retriever._curate_by_mode(mock_annotations)

        verbose_retriever.log.error.assert_called_once()

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

    def test_direct_annotations_no_matches_raises_error(self, retriever):
        """test direct annotations with no matching terms raises NoResultsFound"""
        mock_annotations = Mock()
        mock_annotations.entities = ["other_term1", "other_term2"]

        with pytest.raises(NoResultsFound) as exc_info:
            retriever._direct_annotations(mock_annotations)

        assert "No annotations for any terms" in str(exc_info.value)
        retriever.log.error.assert_called_once()

    def test_direct_annotations_partial_match_warns_verbose(self, verbose_retriever):
        """test partial term matches trigger warning in verbose mode"""
        mock_annotations = Mock()
        mock_annotations.entities = ["term1"]  # only one of two terms
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected

        result = verbose_retriever._direct_annotations(mock_annotations)

        verbose_retriever.log.warning.assert_called_once()
        warning_call_args = verbose_retriever.log.warning.call_args[0]
        assert "have no annotations" in warning_call_args[0]
        assert result == mock_filtered

    def test_direct_annotations_partial_match_silent_mode(self, retriever):
        """test partial matches don't warn in silent mode"""
        mock_annotations = Mock()
        mock_annotations.entities = ["term1"]  # only one of two terms
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected

        result = retriever._direct_annotations(mock_annotations)

        retriever.log.warning.assert_not_called()
        assert result == mock_filtered

    def test_filter_missing_entities_returns_matching_terms(self, retriever):
        """test _filter_missing_entities returns terms that exist in entities"""
        mock_annotations = Mock()
        mock_annotations.entities = ["term1", "term2", "other_term"]

        result = retriever._filter_missing_entities(mock_annotations)

        assert result == ["term1", "term2"]

    def test_filter_missing_entities_raises_when_no_matches(self, retriever):
        """test _filter_missing_entities raises NoResultsFound when no terms match"""
        mock_annotations = Mock()
        mock_annotations.entities = ["other_term1", "other_term2"]

        with pytest.raises(NoResultsFound) as exc_info:
            retriever._filter_missing_entities(mock_annotations)

        assert "No annotations for any terms" in str(exc_info.value)

    def test_propagate_annotations(self, retriever):
        """test propagation wrapper calls annotations.propagate correctly"""
        mock_annotations = Mock()
        mock_result = Mock()
        mock_annotations.propagate.return_value = mock_result

        result = retriever._propagate_annotations(mock_annotations, mode=0)

        mock_annotations.propagate.assert_called_once_with(
            ["term1", "term2"], "test_ontology", mode=0
        )
        assert result == mock_result

    def test_propagate_annotations_with_label_mode(self, retriever):
        """test propagation in label mode (mode=1)"""
        mock_annotations = Mock()
        mock_result = Mock()
        mock_annotations.propagate.return_value = mock_result

        result = retriever._propagate_annotations(mock_annotations, mode=1)

        mock_annotations.propagate.assert_called_once_with(
            ["term1", "term2"], "test_ontology", mode=1
        )
        assert result == mock_result

    def test_save_curation(self, retriever):
        """test save_curation calls _save"""
        mock_curation = Mock()

        with patch.object(retriever, "_save") as mock_save:
            retriever.save_curation(mock_curation)
            mock_save.assert_called_once_with(mock_curation)

    def test_save_curation_logs_info_when_verbose(self, verbose_retriever):
        """test save_curation logs info in verbose mode"""
        mock_curation = Mock()

        verbose_retriever.save_curation(mock_curation)

        verbose_retriever.log.info.assert_called_with(
            "Saving to %s...", verbose_retriever.output_config.outfile
        )

    def test_save_calls_curation_save_method(self, retriever):
        """test _save calls the save method on curation object"""
        mock_curation = Mock()

        retriever._save(mock_curation)

        mock_curation.save.assert_called_once_with(
            outfile="test.json",
            fmt="json",
            metadata="test_metadata",
            attribute="test_attr",
            level="test_level",
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
            ("annotate", 0),
            ("label", 1),
        ],
    )
    def test_curate_by_mode_parameter_mapping(self, retriever, mode, expected_mode):
        """test different modes map to correct parameters"""
        mock_annotations = Mock()
        mock_annotations.n_indices = 10
        mock_annotations.entities = ["term1", "term2"]
        mock_filtered = Mock()
        mock_selected = Mock()
        mock_selected.filter.return_value = mock_filtered
        mock_annotations.select.return_value = mock_selected
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
        config = OutputConfig(
            outfile=path_obj,
            fmt="csv",
            metadata="test",
            attribute="test_attr",
            level="test_level",
        )

        assert config.outfile == path_obj
        assert isinstance(config.outfile, Path)

    @pytest.mark.parametrize(
        "fmt",
        ["json", "parquet", "csv", "tsv"],
    )
    def test_output_config_supports_all_formats(self, fmt):
        """test output config accepts all supported formats"""
        config = OutputConfig(
            outfile="test.file",
            fmt=fmt,
            metadata="test",
            attribute="test_attr",
            level="test_level",
        )
        assert config.fmt == fmt
