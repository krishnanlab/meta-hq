"""
Unit tests for LabelsExporter.to_json and its private helpers - the logic
that stays specific to LabelsExporter after the shared save/csv/parquet/tsv/
get_sra behavior is consolidated onto BaseExporter.

Author: Parker Hicks
Date: 2026-08-10
"""

import json
from unittest.mock import Mock

import polars as pl
import pytest

from metahq_core.curations.labels import Labels
from metahq_core.export.labels import LabelsExporter
from metahq_core.export.references import CitationConfig

@pytest.fixture
def sample_labels():
    """positive (1), negative (-1), control (2), and unlabeled (0) rows for
    a disease ontology entity so both bucket branches are exercised."""
    data = pl.DataFrame({"MONDO:0005148": [1, -1, 2, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "series": ["GSE1", "GSE1", "GSE2", "GSE1"],
            "sources": ["KrishnanLab", "KrishnanLab", "KrishnanLab", "KrishnanLab"],
        }
    )
    return Labels(data, ids, index_col="sample", group_cols=("series", "sources"))


@pytest.fixture
def sample_labels_no_controls():
    """a non-disease entity, which never gets a 'control' bucket."""
    data = pl.DataFrame({"UBERON:0000948": [1, -1, 1, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "series": ["GSE1", "GSE1", "GSE2", "GSE1"],
            "sources": ["KrishnanLab", "KrishnanLab", "KrishnanLab", "KrishnanLab"],
        }
    )
    return Labels(data, ids, index_col="sample", group_cols=("series", "sources"))


@pytest.fixture
def sample_labels_with_linked_source():
    """labels curation whose 'sources' column names a real external-link
    provider (Gemma) for GSE1, so add_external_links has a match to resolve
    end-to-end."""
    data = pl.DataFrame({"MONDO:0005148": [1, -1, 2, 0]})
    ids = pl.DataFrame(
        {
            "sample": ["GSM1", "GSM2", "GSM3", "GSM4"],
            "series": ["GSE1", "GSE1", "GSE2", "GSE1"],
            "sources": ["Gemma", "Gemma", "KrishnanLab", "Gemma"],
        }
    )
    return Labels(data, ids, index_col="sample", group_cols=("series", "sources"))


@pytest.fixture
def labels_exporter():
    return LabelsExporter(
        attribute="disease", level="sample", logger=Mock(), verbose=False
    )


@pytest.fixture
def citation_config(tmp_path):
    return CitationConfig(
        version="1.0.0",
        terms="disease",
        attribute="disease",
        level="sample",
        species="human",
        ecode="expert",
        tech="rna-seq",
        mode="label",
        license="all",
        date="2026-08-10 00:00:00",
        outfile=tmp_path / "CITATION.txt",
    )


class TestToJson:
    """test to_json happy paths, bucket structure, and error path"""

    def test_disease_ontology_entity_gets_control_bucket(
        self, labels_exporter, sample_labels, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        labels_exporter.to_json(sample_labels, outfile, citation_config, metadata=None)

        result = json.loads(outfile.read_text())
        assert set(result["MONDO:0005148"].keys()) == {
            "positive",
            "negative",
            "control",
        }

    def test_non_disease_entity_has_no_control_bucket(
        self, labels_exporter, sample_labels_no_controls, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        labels_exporter.to_json(
            sample_labels_no_controls, outfile, citation_config, metadata=None
        )

        result = json.loads(outfile.read_text())
        assert set(result["UBERON:0000948"].keys()) == {"positive", "negative"}

    def test_labels_placed_in_correct_buckets(
        self, labels_exporter, sample_labels, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        labels_exporter.to_json(sample_labels, outfile, citation_config, metadata=None)

        result = json.loads(outfile.read_text())
        entity = result["MONDO:0005148"]
        assert [next(iter(entry)) for entry in entity["positive"]] == ["GSM1"]
        assert [next(iter(entry)) for entry in entity["negative"]] == ["GSM2"]
        assert [next(iter(entry)) for entry in entity["control"]] == ["GSM3"]
        # GSM4 (label 0) is unlabeled and must not appear in any bucket
        all_ids = {
            idx for bucket in entity.values() for entry in bucket for idx in entry
        }
        assert "GSM4" not in all_ids

    def test_with_metadata_attaches_requested_fields(
        self, labels_exporter, sample_labels, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        labels_exporter.to_json(
            sample_labels, outfile, citation_config, metadata="sample,srr"
        )

        result = json.loads(outfile.read_text())
        positive_entry = result["MONDO:0005148"]["positive"][0]
        assert positive_entry["GSM1"]["srr"] == "SRR1"

    def test_only_index_and_string_metadata_produce_same_shape(
        self, labels_exporter, sample_labels, citation_config, tmp_path
    ):
        no_metadata = tmp_path / "no_metadata.json"
        labels_exporter.to_json(
            sample_labels, no_metadata, citation_config, metadata=None
        )

        explicit_index = tmp_path / "explicit_index.json"
        labels_exporter.to_json(
            sample_labels, explicit_index, citation_config, metadata="sample"
        )

        assert json.loads(no_metadata.read_text()) == json.loads(
            explicit_index.read_text()
        )

    def test_external_links_attached_when_requested(
        self,
        labels_exporter,
        sample_labels_with_linked_source,
        citation_config,
        tmp_path,
        stub_external_links_with_data,
    ):
        outfile = tmp_path / "out.json"
        labels_exporter.to_json(
            sample_labels_with_linked_source,
            outfile,
            citation_config,
            metadata="sample,external_links",
        )

        result = json.loads(outfile.read_text())
        positive_entry = result["MONDO:0005148"]["positive"][0]
        links = positive_entry["GSM1"]["external_links"]
        assert isinstance(links, dict)
        assert links["Gemma"] == "https://gemma.msl.ubc.ca/GSE1"

    def test_merges_refinebio_fields_when_requested(
        self, labels_exporter, sample_labels, citation_config, tmp_path
    ):
        labels_exporter._refinebio._map = pl.DataFrame(
            {
                "gsm": ["GSM1", "GSM2", "GSM3", "GSM4"],
                "gse": ["GSE1", "GSE1", "GSE2", "GSE1"],
                "refinebio_sample": ["RB1", "RB2", "RB3", "RB4"],
                "refinebio_experiment": ["RBE1", "RBE1", "RBE2", "RBE1"],
            }
        )
        outfile = tmp_path / "out.json"
        labels_exporter.to_json(
            sample_labels,
            outfile,
            citation_config,
            metadata="sample,refinebio_sample",
        )

        result = json.loads(outfile.read_text())
        positive_entry = result["MONDO:0005148"]["positive"][0]
        assert positive_entry["GSM1"]["refinebio_sample"] == "RB1"

    def test_non_str_non_none_metadata_raises_readable_valueerror(
        self, labels_exporter, sample_labels, citation_config, tmp_path
    ):
        outfile = tmp_path / "out.json"
        with pytest.raises(
            ValueError, match=r"Unexpected metadata argument: 123"
        ) as exc:
            labels_exporter.to_json(
                sample_labels, outfile, citation_config, metadata=123
            )

        assert isinstance(exc.value.args[0], str)
