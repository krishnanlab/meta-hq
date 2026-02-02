"""
Unit tests for Builder class.

Author: Parker Hicks
Date: 2025-11-06

Last updated: 2025-11-06 by Parker Hicks
"""

from unittest.mock import Mock, patch

import pytest
from metahq_core.util.exceptions import NoResultsFound

from metahq_cli.retrieval_builder import Builder
from metahq_cli.retriever import CurationConfig, OutputConfig, QueryConfig


class TestBuilder:
    """test builder class"""

    @pytest.fixture
    def mock_logger(self):
        """fixture for mock logger"""
        return Mock()

    @pytest.fixture
    def builder(self, mock_logger):
        """fixture for builder instance"""
        return Builder(logger=mock_logger, verbose=False)

    @pytest.fixture
    def verbose_builder(self, mock_logger):
        """fixture for verbose builder instance"""
        return Builder(logger=mock_logger, verbose=True)

    def test_builder_initialization(self, mock_logger):
        """test builder init stores logger and verbose correctly"""
        builder = Builder(logger=mock_logger, verbose=True)

        assert builder.log == mock_logger
        assert builder.verbose is True

    def test_builder_initialization_default_verbose(self, mock_logger):
        """test builder defaults to verbose=True"""
        builder = Builder(logger=mock_logger)

        assert builder.verbose is True

    def test_get_filters_success(self, builder):
        """test get_filters parses and returns filters correctly"""
        with patch.object(builder, "_parse_filters") as mock_parse:
            with patch.object(builder, "report_bad_filters") as mock_report:
                expected_filters = {
                    "species": "human",
                    "ecode": "expert",
                    "tech": "rnaseq",
                }
                mock_parse.return_value = expected_filters

                result = builder.get_filters("species=human,ecode=expert,tech=rnaseq")

                mock_parse.assert_called_once_with(
                    "species=human,ecode=expert,tech=rnaseq"
                )
                mock_report.assert_called_once_with(expected_filters)
                assert result == expected_filters

    @patch("metahq_cli.retrieval_builder.required_filters")
    def test_parse_filters_success(self, mock_required, builder):
        """test _parse_filters successfully parses valid filter string"""
        mock_required.return_value = ["species", "ecode", "tech"]

        result = builder._parse_filters("species=human,ecode=expert,tech=rnaseq")

        assert result == {
            "species": "human",
            "ecode": "expert",
            "tech": "rnaseq",
        }

    @patch("metahq_cli.retrieval_builder.required_filters")
    def test_parse_filters_missing_required_filters(self, mock_required, builder):
        """test _parse_filters raises RuntimeError when required filters are missing"""
        mock_required.return_value = ["species", "ecode", "tech"]

        with pytest.raises(RuntimeError) as exc_info:
            builder._parse_filters("species=human,ecode=expert")

        assert "Missing required filters" in str(exc_info.value)
        assert "tech" in str(exc_info.value)

    @patch("metahq_cli.retrieval_builder.required_filters")
    def test_parse_filters_logs_error_on_missing_filters(
        self, mock_required, verbose_builder
    ):
        """test _parse_filters logs error when required filters are missing"""
        mock_required.return_value = ["species", "ecode", "tech"]

        with pytest.raises(RuntimeError):
            verbose_builder._parse_filters("species=human,ecode=expert")

        verbose_builder.log.error.assert_called_once()
        call_args = verbose_builder.log.error.call_args[0][0]
        assert "Missing required filters" in call_args

    @patch("metahq_cli.retrieval_builder.required_filters")
    def test_parse_filters_multiple_missing_filters(self, mock_required, builder):
        """test _parse_filters reports all missing required filters"""
        mock_required.return_value = ["species", "ecode", "tech"]

        with pytest.raises(RuntimeError) as exc_info:
            builder._parse_filters("species=human")

        error_msg = str(exc_info.value)
        assert "Missing required filters" in error_msg
        assert "ecode" in error_msg
        assert "tech" in error_msg

    @patch("metahq_cli.retrieval_builder.required_filters")
    def test_parse_filters_parses_correctly(self, mock_required, builder):
        """test _parse_filters correctly parses key=value pairs"""
        mock_required.return_value = ["species", "ecode", "tech"]

        result = builder._parse_filters("species=mouse,ecode=all,tech=microarray")

        assert result["species"] == "mouse"
        assert result["ecode"] == "all"
        assert result["tech"] == "microarray"
        assert len(result) == 3

    @patch("metahq_cli.retrieval_builder.check_filter")
    def test_query_config_creates_config(self, mock_check, builder):
        """test query_config creates QueryConfig with correct parameters"""
        filters = {"species": "human", "ecode": "expert", "tech": "rnaseq"}

        result = builder.query_config("geo", "tissue", "sample", filters)

        assert isinstance(result, QueryConfig)
        assert result.database == "geo"
        assert result.attribute == "tissue"
        assert result.level == "sample"
        assert result.species == "human"
        assert result.ecode == "expert"
        assert result.tech == "rnaseq"

        # Check that filters were validated
        assert mock_check.call_count == 3

    @patch("metahq_cli.retrieval_builder.check_filter")
    def test_query_config_validates_filters(self, mock_check, builder):
        """test query_config validates all filter parameters"""
        filters = {"species": "human", "ecode": "expert", "tech": "rnaseq"}

        builder.query_config("geo", "tissue", "sample", filters)

        # Verify each filter was checked
        mock_check.assert_any_call("ecodes", "expert")
        mock_check.assert_any_call("species", "human")
        mock_check.assert_any_call("technologies", "rnaseq")

    @patch("metahq_cli.retrieval_builder.check_metadata")
    @patch("metahq_cli.retrieval_builder.check_format")
    @patch("metahq_cli.retrieval_builder.check_outfile")
    def test_output_config_creates_config(
        self, mock_outfile, mock_format, mock_metadata, builder
    ):
        """test output_config creates OutputConfig with correct parameters"""
        result = builder.output_config("output.parquet", "parquet", "sample", "sample", "test_attr")

        assert isinstance(result, OutputConfig)
        assert result.outfile == "output.parquet"
        assert result.fmt == "parquet"
        assert result.metadata == "sample"
        assert result.attribute == "test_attr"
        assert result.level == "sample"

        mock_metadata.assert_called_once_with("sample", "sample")
        mock_format.assert_called_once_with("parquet")
        mock_outfile.assert_called_once_with("output.parquet")

    def test_map_sex_to_id_maps_male_and_female(self, builder):
        """test map_sex_to_id converts male/female to M/F"""
        result = builder._map_sex_to_id(["male", "female"])

        assert result == ["M", "F"]

    def test_map_sex_to_id_preserves_ids(self, builder):
        """test map_sex_to_id preserves M and F unchanged"""
        result = builder._map_sex_to_id(["M", "F"])

        assert result == ["M", "F"]

    def test_map_sex_to_id_mixed_input(self, builder):
        """test map_sex_to_id handles mixed input correctly"""
        result = builder._map_sex_to_id(["male", "F", "female", "M"])

        assert result == ["M", "F", "F", "M"]

    @patch("metahq_cli.retrieval_builder.check_filter_keys")
    def test_report_bad_filters_no_bad_filters(self, mock_check_keys, builder):
        """test report_bad_filters passes when filters are valid"""
        mock_check_keys.return_value = []
        filters = {"species": "human", "ecode": "expert", "tech": "rnaseq"}

        # Should not raise any exception
        builder.report_bad_filters(filters)

    @patch("metahq_cli.retrieval_builder.check_filter_keys")
    def test_report_bad_filters_raises_on_bad_filters(self, mock_check_keys, builder):
        """test report_bad_filters raises ValueError for bad filters"""
        mock_check_keys.return_value = ["bad_filter"]
        filters = {"bad_filter": "value"}

        with pytest.raises(ValueError):
            builder.report_bad_filters(filters)

    @patch("metahq_cli.retrieval_builder.check_filter_keys")
    def test_report_bad_filters_logs_error_when_verbose(
        self, mock_check_keys, verbose_builder
    ):
        """test report_bad_filters logs error in verbose mode"""
        mock_check_keys.return_value = ["bad_filter"]
        filters = {"bad_filter": "value"}

        with pytest.raises(ValueError):
            verbose_builder.report_bad_filters(filters)

        verbose_builder.log.error.assert_called_once()

    @patch("metahq_cli.retrieval_builder.check_if_txt")
    @patch("metahq_cli.retrieval_builder.check_mode")
    def test_make_sex_curation_with_all(self, mock_check_mode, mock_check_txt, builder):
        """test make_sex_curation with 'all' returns all sexes"""
        mock_check_txt.return_value = "all"

        with patch("metahq_core.util.supported.sexes") as mock_sexes:
            mock_sexes.return_value = ["M", "F"]
            result = builder.make_sex_curation("all", "direct")

            assert isinstance(result, CurationConfig)
            assert result.mode == "direct"
            assert result.terms == ["M", "F"]
            assert result.ontology == "sex"
            mock_check_mode.assert_called_once_with("sex", "direct")

    @patch("metahq_cli.retrieval_builder.check_if_txt")
    @patch("metahq_cli.retrieval_builder.check_mode")
    def test_make_sex_curation_with_string_terms(
        self, mock_check_mode, mock_check_txt, builder
    ):
        """test make_sex_curation with comma-separated string"""
        mock_check_txt.return_value = "male,female"

        result = builder.make_sex_curation("male,female", "direct")

        assert isinstance(result, CurationConfig)
        assert result.mode == "direct"
        assert result.terms == ["M", "F"]
        assert result.ontology == "sex"

    @patch("metahq_cli.retrieval_builder.check_if_txt")
    @patch("metahq_cli.retrieval_builder.check_mode")
    def test_make_age_curation_with_all(self, mock_check_mode, mock_check_txt, builder):
        """test make_age_curation with 'all' returns all age groups"""
        mock_check_txt.return_value = "all"

        with patch("metahq_core.util.supported._age_groups") as mock_age_groups:
            mock_age_groups.return_value = [
                "fetus",
                "infant",
                "child",
                "adolescent",
                "adult",
                "older_adult",
                "elderly",
            ]
            result = builder.make_age_curation("all", "direct")

            assert isinstance(result, CurationConfig)
            assert result.mode == "direct"
            assert result.terms == [
                "fetus",
                "infant",
                "child",
                "adolescent",
                "adult",
                "older_adult",
                "elderly",
            ]

            assert result.ontology == "age"
            mock_check_mode.assert_called_once_with("age", "direct")

    @patch("metahq_cli.retrieval_builder.check_if_txt")
    @patch("metahq_cli.retrieval_builder.check_mode")
    def test_make_age_curation_with_string_terms(
        self, mock_check_mode, mock_check_txt, builder
    ):
        """test make_age_curation with comma-separated string"""
        mock_check_txt.return_value = "fetus,adult"

        result = builder.make_age_curation("fetus,adult", "direct")

        assert isinstance(result, CurationConfig)
        assert result.mode == "direct"
        assert result.terms == ["fetus", "adult"]
        assert result.ontology == "age"

    def test_curation_config_delegates_to_make_sex_curation(self, builder):
        """test curation_config calls make_sex_curation for sex ontology"""
        with patch.object(builder, "make_sex_curation") as mock_sex:
            mock_sex.return_value = CurationConfig("direct", ["M", "F"], "sex")

            result = builder.curation_config("male,female", "direct", "sex")

            mock_sex.assert_called_once_with("male,female", "direct")
            assert result.ontology == "sex"

    def test_curation_config_delegates_to_make_age_curation(self, builder):
        """test curation_config calls make_age_curation for age ontology"""
        with patch.object(builder, "make_age_curation") as mock_age:
            mock_age.return_value = CurationConfig("direct", ["adult"], "age")

            result = builder.curation_config("adult", "direct", "age")

            mock_age.assert_called_once_with("adult", "direct")
            assert result.ontology == "age"

    @patch("metahq_cli.retrieval_builder.check_if_txt")
    def test_curation_config_for_ontology_with_all(self, mock_check_txt, builder):
        """test curation_config handles 'all' for regular ontologies"""
        with patch.object(builder, "parse_onto_terms") as mock_parse:
            mock_parse.return_value = ["UBERON:0000001", "UBERON:0000002"]

            result = builder.curation_config("all", "direct", "uberon")

            # parse_onto_terms is called twice: once for "all", once for the result
            assert mock_parse.call_count == 2
            mock_parse.assert_any_call("all", "uberon")
            mock_parse.assert_any_call(["UBERON:0000001", "UBERON:0000002"], "uberon")
            assert result.terms == ["UBERON:0000001", "UBERON:0000002"]
            assert result.ontology == "uberon"

    @patch("metahq_cli.retrieval_builder.check_if_txt")
    def test_curation_config_for_ontology_with_string_terms(
        self, mock_check_txt, builder
    ):
        """test curation_config handles comma-separated terms for regular ontologies"""
        mock_check_txt.return_value = "UBERON:0000001,UBERON:0000002"

        with patch.object(builder, "parse_onto_terms") as mock_parse:
            mock_parse.return_value = ["UBERON:0000001", "UBERON:0000002"]

            result = builder.curation_config(
                "UBERON:0000001,UBERON:0000002", "propagate", "uberon"
            )

            mock_parse.assert_called_once_with(
                ["UBERON:0000001", "UBERON:0000002"], "uberon"
            )
            assert result.terms == ["UBERON:0000001", "UBERON:0000002"]
            assert result.mode == "propagate"

    def test_parse_returns_matching_terms(self, builder):
        """test _parse returns only terms that are in available list"""
        terms = ["term1", "term2", "term3"]
        available = ["term1", "term3", "term4"]

        result = builder._parse(terms, available)

        assert result == ["term1", "term3"]

    def test_parse_returns_empty_when_no_matches(self, builder):
        """test _parse returns empty list when no terms match"""
        terms = ["term1", "term2"]
        available = ["term3", "term4"]

        result = builder._parse(terms, available)

        assert result == []

    @patch("metahq_cli.retrieval_builder.pl.scan_parquet")
    def test_parse_onto_terms_raises_when_no_results(self, mock_scan, builder):
        """test parse_onto_terms raises NoResultsFound when no terms match"""
        mock_schema = Mock()
        mock_schema.names.return_value = ["UBERON:0000001", "UBERON:0000002"]
        mock_lazyframe = Mock()
        mock_lazyframe.collect_schema.return_value = mock_schema
        mock_scan.return_value = mock_lazyframe

        with patch("metahq_cli.retrieval_builder.get_ontology_families") as mock_get:
            mock_get.return_value = {"relations": "path/to/relations.parquet"}

            with pytest.raises(NoResultsFound) as exc_info:
                builder.parse_onto_terms(["MONDO:0000001"], "uberon")

            assert "have no annotations" in str(exc_info.value)

    @patch("metahq_cli.retrieval_builder.pl.scan_parquet")
    def test_parse_onto_terms_logs_error_when_verbose(self, mock_scan, verbose_builder):
        """test parse_onto_terms logs error in verbose mode when no results"""
        mock_schema = Mock()
        mock_schema.names.return_value = ["UBERON:0000001"]
        mock_lazyframe = Mock()
        mock_lazyframe.collect_schema.return_value = mock_schema
        mock_scan.return_value = mock_lazyframe

        with patch("metahq_cli.retrieval_builder.get_ontology_families") as mock_get:
            mock_get.return_value = {"relations": "path/to/relations.parquet"}

            with pytest.raises(NoResultsFound):
                verbose_builder.parse_onto_terms(["MONDO:0000001"], "uberon")

            verbose_builder.log.error.assert_called_once()

    @patch("metahq_cli.retrieval_builder.pl.scan_parquet")
    def test_parse_onto_terms_logs_warning_for_partial_match(
        self, mock_scan, verbose_builder
    ):
        """test parse_onto_terms logs warning when some terms don't match"""
        mock_schema = Mock()
        mock_schema.names.return_value = ["UBERON:0000001", "UBERON:0000002"]
        mock_lazyframe = Mock()
        mock_lazyframe.collect_schema.return_value = mock_schema
        mock_scan.return_value = mock_lazyframe

        with patch("metahq_cli.retrieval_builder.get_ontology_families") as mock_get:
            mock_get.return_value = {"relations": "path/to/relations.parquet"}

            result = verbose_builder.parse_onto_terms(
                ["UBERON:0000001", "MONDO:0000001"], "uberon"
            )

            assert result == ["UBERON:0000001"]
            verbose_builder.log.warning.assert_called_once()

    @patch("metahq_cli.retrieval_builder.pl.scan_parquet")
    def test_parse_onto_terms_no_warning_when_silent(self, mock_scan, builder):
        """test parse_onto_terms doesn't log in silent mode"""
        mock_schema = Mock()
        mock_schema.names.return_value = ["UBERON:0000001"]
        mock_lazyframe = Mock()
        mock_lazyframe.collect_schema.return_value = mock_schema
        mock_scan.return_value = mock_lazyframe

        with patch("metahq_cli.retrieval_builder.get_ontology_families") as mock_get:
            mock_get.return_value = {"relations": "path/to/relations.parquet"}

            result = builder.parse_onto_terms(
                ["UBERON:0000001", "MONDO:0000001"], "uberon"
            )

            assert result == ["UBERON:0000001"]
            builder.log.warning.assert_not_called()

    @patch("metahq_cli.retrieval_builder.pl.scan_parquet")
    def test_parse_onto_terms_handles_all_keyword(self, mock_scan, builder):
        """test parse_onto_terms handles 'all' keyword correctly"""
        mock_schema = Mock()
        mock_schema.names.return_value = ["UBERON:0000001", "UBERON:0000002"]
        mock_lazyframe = Mock()
        mock_lazyframe.collect_schema.return_value = mock_schema
        mock_scan.return_value = mock_lazyframe

        with patch("metahq_cli.retrieval_builder.get_ontology_families") as mock_get:
            mock_get.return_value = {"relations": "path/to/relations.parquet"}
            result = builder.parse_onto_terms("all", "uberon")

            assert "UBERON:0000001" in result
            assert "UBERON:0000002" in result
            # UBERON:0000003 is not in available list, so should not be included
            assert "UBERON:0000003" not in result
