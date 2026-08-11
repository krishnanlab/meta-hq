"""
Unit tests for AnnotationsExporter.to_json and its private helpers - the
logic that stays specific to AnnotationsExporter after the shared save/csv/
parquet/tsv/get_sra behavior is consolidated onto BaseExporter.

Author: Parker Hicks
Date: 2026-08-10
"""

import json
from unittest.mock import Mock

import polars as pl
import pytest

from metahq_core.curations.annotations import Annotations
from metahq_core.export.annotations import AnnotationsExporter
from metahq_core.export.references import CitationConfig

ANNOTATIONS_DB = {
    "GSM1": {"accession_ids": {"srr": "SRR1"}},
    "GSM2": {"accession_ids": {"srr": "SRR2"}},
    "GSM3": {"accession_ids": {}},
    "GSM4": {"accession_ids": {"srr": "SRR4"}},
}


@pytest.fixture(autouse=True)
def stub_annotations_db(monkeypatch):
    """get_annotations() is evaluated eagerly as an argument to load_bson(...),
    so it must be stubbed too - otherwise it resolves the MetaHQ config/data
    dir (absent on CI) before load_bson ever runs."""
    monkeypatch.setattr(
        "metahq_core.export.base.get_annotations", lambda level: level
    )
    monkeypatch.setattr(
        "metahq_core.export.base.load_bson", lambda *a, **k: ANNOTATIONS_DB
    )


@pytest.fixture
def sample_annotations():
    data = pl.DataFrame({"UBERON:0000948": [1, 0, 1, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "series": ["GSE1", "GSE1", "GSE2", "GSE1"],
            "sources": ["KrishnanLab", "KrishnanLab", "KrishnanLab", "KrishnanLab"],
        }
    )
    return Annotations(data, ids, index_col="sample", group_cols=("series", "sources"))


@pytest.fixture
def anno_exporter():
    return AnnotationsExporter(
        attribute="tissue", level="sample", logger=Mock(), verbose=False
    )


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


class TestToJson:
    """test to_json happy paths and error path"""

    def test_no_metadata_includes_only_index_entries(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        anno_exporter.to_json(sample_annotations, outfile, citation_config, metadata=None)

        result = json.loads(outfile.read_text())
        assert set(result["UBERON:0000948"].keys()) == {"GSM1", "GSM3"}

    def test_with_metadata_attaches_requested_fields(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        anno_exporter.to_json(
            sample_annotations, outfile, citation_config, metadata="sample,srr"
        )

        result = json.loads(outfile.read_text())
        assert result["UBERON:0000948"]["GSM1"]["srr"] == "SRR1"

    def test_merges_refinebio_fields_when_requested(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        anno_exporter._refinebio._map = pl.DataFrame(
            {
                "gsm": ["GSM1", "GSM2", "GSM3", "GSM4"],
                "gse": ["GSE1", "GSE1", "GSE2", "GSE1"],
                "refinebio_sample": ["RB1", "RB2", "RB3", "RB4"],
                "refinebio_experiment": ["RBE1", "RBE1", "RBE2", "RBE1"],
            }
        )
        outfile = tmp_path / "out.json"
        anno_exporter.to_json(
            sample_annotations,
            outfile,
            citation_config,
            metadata="sample,refinebio_sample",
        )

        result = json.loads(outfile.read_text())
        assert result["UBERON:0000948"]["GSM1"]["refinebio_sample"] == "RB1"

    def test_only_index_and_string_metadata_produce_same_shape(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        no_metadata = tmp_path / "no_metadata.json"
        anno_exporter.to_json(
            sample_annotations, no_metadata, citation_config, metadata=None
        )

        explicit_index = tmp_path / "explicit_index.json"
        anno_exporter.to_json(
            sample_annotations, explicit_index, citation_config, metadata="sample"
        )

        assert json.loads(no_metadata.read_text()) == json.loads(
            explicit_index.read_text()
        )

    def test_non_str_non_none_metadata_raises_readable_valueerror(
        self, anno_exporter, sample_annotations, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        with pytest.raises(ValueError, match=r"Unexpected metadata argument: 123") as exc:
            anno_exporter.to_json(sample_annotations, outfile, citation_config, metadata=123)

        assert isinstance(exc.value.args[0], str)
