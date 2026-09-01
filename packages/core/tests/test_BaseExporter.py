"""
Unit tests for exporter behavior shared between AnnotationsExporter and
LabelsExporter. BaseExporter cannot be instantiated directly (to_json stays
abstract), so these tests exercise the shared behavior through the concrete
subclasses.

Author: Parker Hicks
Date: 2026-08-10
"""

from unittest.mock import Mock, patch

import polars as pl
import pytest

from metahq_core.curations.annotations import Annotations
from metahq_core.curations.labels import Labels
from metahq_core.export.annotations import AnnotationsExporter
from metahq_core.export.labels import LabelsExporter
from metahq_core.export.references import CitationConfig
from metahq_core.util.alltypes import MetadataField


@pytest.fixture
def mock_logger():
    return Mock()


@pytest.fixture
def sample_annotations():
    """sample-level annotations curation, index unsorted on purpose so the
    tabular save-order behavior is actually exercised."""
    data = pl.DataFrame({"UBERON:0000948": [0, 1, 1, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM3", "GSM1", "GSM4", "GSM2"],
            "series": ["GSE2", "GSE1", "GSE1", "GSE1"],
            "sources": ["KrishnanLab", "KrishnanLab", "KrishnanLab", "KrishnanLab"],
        }
    )
    return Annotations(data, ids, index_col="sample", group_cols=("series", "sources"))


@pytest.fixture
def sample_labels():
    data = pl.DataFrame({"UBERON:0000948": [1, -1, 1, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM3", "GSM1", "GSM4", "GSM2"],
            "series": ["GSE2", "GSE1", "GSE1", "GSE1"],
            "sources": ["KrishnanLab", "KrishnanLab", "KrishnanLab", "KrishnanLab"],
        }
    )
    return Labels(data, ids, index_col="sample", group_cols=("series", "sources"))


@pytest.fixture
def anno_exporter(mock_logger):
    exp = AnnotationsExporter(
        attribute="tissue", level="sample", logger=mock_logger, verbose=True
    )
    exp._refinebio._map = pl.DataFrame(
        {
            "gsm": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "gse": ["GSE1", "GSE1", "GSE2", "GSE1"],
            "refinebio_sample": ["RB_GSM1", "RB_GSM2", "RB_GSM3", "RB_GSM4"],
            "refinebio_experiment": ["RB_GSE1", "RB_GSE1", "RB_GSE2", "RB_GSE1"],
        }
    )
    return exp


@pytest.fixture
def labels_exporter(mock_logger):
    exp = LabelsExporter(
        attribute="tissue", level="sample", logger=mock_logger, verbose=True
    )
    exp._refinebio._map = pl.DataFrame(
        {
            "gsm": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "gse": ["GSE1", "GSE1", "GSE2", "GSE1"],
            "refinebio_sample": ["RB_GSM1", "RB_GSM2", "RB_GSM3", "RB_GSM4"],
            "refinebio_experiment": ["RB_GSE1", "RB_GSE1", "RB_GSE2", "RB_GSE1"],
        }
    )
    return exp


@pytest.fixture
def citation_config(tmp_path):
    return CitationConfig(
        version="1.0.0",
        terms="tissue",
        attribute="tissue",
        level="sample",
        species="human",
        ecode="expert",
        tech="rna-seq",
        mode="annotate",
        license="all",
        date="2026-08-10 00:00:00",
        outfile=tmp_path / "CITATION.txt",
    )


class TestGetSra:
    """test get_sra, shared identically between both exporters"""

    def test_merges_sra_ids_for_annotations(self, anno_exporter, sample_annotations):
        merged = anno_exporter.get_sra(sample_annotations, ["srr", "srx"])

        assert isinstance(merged, Annotations)
        by_sample = dict(
            zip(merged.ids["sample"].to_list(), merged.ids["srr"].to_list())
        )
        assert by_sample["GSM1"] == "SRR1"
        assert by_sample["GSM3"] == "NA"

    def test_missing_field_filled_with_na(self, anno_exporter, sample_annotations):
        merged = anno_exporter.get_sra(sample_annotations, ["srx"])
        by_sample = dict(
            zip(merged.ids["sample"].to_list(), merged.ids["srx"].to_list())
        )
        assert by_sample["GSM2"] == "NA"

    def test_preserves_index_order(self, anno_exporter, sample_annotations):
        merged = anno_exporter.get_sra(sample_annotations, ["srr"])
        assert merged.index == sample_annotations.index

    def test_works_for_labels_curation(self, labels_exporter, sample_labels):
        merged = labels_exporter.get_sra(sample_labels, ["srr"])
        assert isinstance(merged, Labels)
        assert merged.ids["srr"].to_list() == ["NA", "SRR1", "SRR4", "SRR2"]


class TestAddRequiredFieldNames:
    """test _add_required_field_names, shared identically between both exporters"""

    def test_appends_external_links_and_sources_when_file_exists(self, anno_exporter):
        resolved = anno_exporter._add_required_field_names([MetadataField.SAMPLE])

        assert MetadataField.EXTERNAL_LINKS in resolved
        assert MetadataField.SOURCES in resolved

    def test_skips_external_links_and_warns_when_file_missing(
        self, anno_exporter, mock_logger, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            "metahq_core.export.base.get_external_links",
            lambda: tmp_path / "does_not_exist.parquet",
        )

        resolved = anno_exporter._add_required_field_names([MetadataField.SAMPLE])

        assert MetadataField.EXTERNAL_LINKS not in resolved
        assert MetadataField.SOURCES in resolved
        mock_logger.warning.assert_called_once()
        assert "old version" in mock_logger.warning.call_args[0][0]


class TestParseMetafields:
    """test _parse_metafields, shared identically between both exporters"""

    def test_index_field_requested_explicitly_does_not_warn(
        self, anno_exporter, mock_logger
    ):
        anno_exporter._parse_metafields("sample", "sample,description")
        mock_logger.warning.assert_not_called()

    def test_series_allowed_as_sample_level_metadata(self, anno_exporter, mock_logger):
        resolved = anno_exporter._parse_metafields("sample", "sample,series")

        assert MetadataField.SERIES in resolved
        mock_logger.warning.assert_not_called()


class TestSave:
    """test save, shared identically between both exporters"""

    def test_dispatches_to_to_csv(self, anno_exporter, sample_annotations, tmp_path):
        outfile = tmp_path / "out.csv"
        with patch.object(anno_exporter, "to_csv") as mock_to_csv:
            anno_exporter.save(sample_annotations, "csv", outfile, Mock())
        mock_to_csv.assert_called_once()

    def test_logs_saved_when_verbose(
        self, anno_exporter, sample_annotations, tmp_path, mock_logger
    ):
        outfile = tmp_path / "out.csv"
        with patch.object(anno_exporter, "to_csv"):
            anno_exporter.save(sample_annotations, "csv", outfile, Mock())
        mock_logger.info.assert_any_call("Saved!")

    def test_silent_when_not_verbose(self, mock_logger, sample_annotations, tmp_path):
        exp = AnnotationsExporter(
            attribute="tissue", level="sample", logger=mock_logger, verbose=False
        )
        outfile = tmp_path / "out.csv"
        with patch.object(exp, "to_csv"):
            exp.save(sample_annotations, "csv", outfile, Mock())
        assert "Saved!" not in [c.args[0] for c in mock_logger.info.call_args_list]

    def test_creates_parent_directory(
        self, anno_exporter, sample_annotations, tmp_path
    ):
        outfile = tmp_path / "nested" / "dir" / "out.csv"
        with patch.object(anno_exporter, "to_csv"):
            anno_exporter.save(sample_annotations, "csv", outfile, Mock())
        assert outfile.parent.is_dir()


class TestToRefinebioDataset:
    """test to_refinebio_dataset, shared identically between both exporters"""

    def test_delegates_to_refinebio_create_dataset(
        self, anno_exporter, sample_annotations
    ):
        with patch.object(
            anno_exporter._refinebio, "create_dataset", return_value={"id": "abc"}
        ) as mock_create:
            result = anno_exporter.to_refinebio_dataset(sample_annotations)

        mock_create.assert_called_once_with(sample_annotations)
        assert result == {"id": "abc"}


class TestToNumpy:
    """test to_numpy, shared identically between both exporters"""

    def test_returns_data_as_numpy(self, anno_exporter, sample_annotations):
        result = anno_exporter.to_numpy(sample_annotations)
        assert (result == sample_annotations.data.to_numpy()).all()

    def test_works_for_labels(self, labels_exporter, sample_labels):
        result = labels_exporter.to_numpy(sample_labels)
        assert (result == sample_labels.data.to_numpy()).all()


class TestSaveTabular:
    """test _save_tabular (via to_csv), shared - modulo the sort-order fix -
    between both exporters"""

    def test_output_sorted_by_index_without_geo_metadata(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.csv"
        anno_exporter.to_csv(
            sample_annotations, outfile, citation_config, metadata="sample"
        )

        written = pl.read_csv(outfile)
        assert written["sample"].to_list() == sorted(written["sample"].to_list())

    def test_labels_output_already_sorted_by_index(
        self, labels_exporter, sample_labels, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.csv"
        labels_exporter.to_csv(
            sample_labels, outfile, citation_config, metadata="sample"
        )

        written = pl.read_csv(outfile)
        assert written["sample"].to_list() == sorted(written["sample"].to_list())

    def test_merges_sra_fields_when_requested(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.csv"
        anno_exporter.to_csv(
            sample_annotations, outfile, citation_config, metadata="sample,srr"
        )

        written = pl.read_csv(outfile)
        assert "srr" in written.columns

    def test_base_tabular_save_places_index_column_first(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.csv"
        anno_exporter.to_csv(
            sample_annotations, outfile, citation_config, metadata="series,srx"
        )

        written = pl.read_csv(outfile)

        assert written.columns[0] == "sample"

    def test_merges_refinebio_fields_when_requested(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.csv"
        anno_exporter.to_csv(
            sample_annotations,
            outfile,
            citation_config,
            metadata="sample,refinebio_sample",
        )

        written = pl.read_csv(outfile)
        assert "refinebio_sample" in written.columns

    def test_geo_metadata_branch_sorted_and_merged(
        self, anno_exporter, sample_annotations, citation_config, tmp_path, monkeypatch
    ):
        geo_parquet = tmp_path / "geo.parquet"
        pl.DataFrame(
            {
                "description": ["d1", "d2", "d3", "d4"],
                "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            }
        ).write_parquet(geo_parquet)
        monkeypatch.setattr(
            "metahq_core.export.base.geo_metadata", lambda level: geo_parquet
        )

        outfile = tmp_path / "out.csv"
        anno_exporter.to_csv(
            sample_annotations, outfile, citation_config, metadata="sample,description"
        )

        written = pl.read_csv(outfile)
        assert written["sample"].to_list() == sorted(written["sample"].to_list())
        assert written.filter(pl.col("sample") == "GSM1")["description"].item() == "d1"

    def test_geo_metadata_branch_places_index_column_first(
        self, anno_exporter, sample_annotations, citation_config, tmp_path, monkeypatch
    ):
        geo_parquet = tmp_path / "geo.parquet"
        pl.DataFrame(
            {
                "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
                "description": ["d1", "d2", "d3", "d4"],
            }
        ).write_parquet(geo_parquet)
        monkeypatch.setattr(
            "metahq_core.export.base.geo_metadata", lambda level: geo_parquet
        )

        outfile = tmp_path / "out.csv"
        anno_exporter.to_csv(
            sample_annotations, outfile, citation_config, metadata="description,sample"
        )

        written = pl.read_csv(outfile)
        assert written.columns[0] == "sample"

    def test_skips_external_links_join_when_file_missing(
        self, anno_exporter, sample_annotations, citation_config, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            "metahq_core.export.base.get_external_links",
            lambda: tmp_path / "does_not_exist.parquet",
        )

        outfile = tmp_path / "out.csv"
        anno_exporter.to_csv(
            sample_annotations, outfile, citation_config, metadata="sample"
        )

        written = pl.read_csv(outfile)
        assert "external_links" not in written.columns

    def test_invalid_fmt_raises_readable_valueerror(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.yaml"
        with pytest.raises(ValueError, match=r"Expected fmt in .*, got yaml\.") as exc:
            anno_exporter._save_tabular(
                "yaml", sample_annotations, outfile, citation_config, metadata="sample"
            )

        assert isinstance(exc.value.args[0], str)
