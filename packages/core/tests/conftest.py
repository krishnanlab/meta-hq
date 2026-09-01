"""
Shared pytest fixtures for metahq_core tests.

Author: Parker Hicks
"""

import polars as pl
import pytest

ANNOTATIONS_DB = {
    "GSM1": {"accession_ids": {"srr": "SRR1", "srx": "SRX1"}},
    "GSM2": {"accession_ids": {"srr": "SRR2"}},
    "GSM3": {"accession_ids": {}},
    "GSM4": {"accession_ids": {"srr": "SRR4"}},
}


@pytest.fixture(autouse=True)
def stub_annotations_db(monkeypatch):
    """Stub the bson annotations-database filesystem read used by
    BaseExporter._load_annotations (called from __init__ and get_sra).

    get_annotations() must be stubbed too: it's evaluated eagerly as an
    argument to load_bson(...), so mocking load_bson alone doesn't stop it
    from resolving the MetaHQ config/data dir (which doesn't exist on a
    fresh CI runner) before load_bson ever runs."""
    monkeypatch.setattr(
        "metahq_core.export.base.get_annotations", lambda level: level
    )
    monkeypatch.setattr(
        "metahq_core.export.base.load_bson", lambda *a, **k: ANNOTATIONS_DB
    )


@pytest.fixture(autouse=True)
def stub_external_links(monkeypatch, tmp_path):
    """get_external_links() resolves through get_config()/get_data_dir(), which
    requires a real ~/.metahq_home/config.yaml - absent on a fresh CI runner.

    Stub it to point at an empty (schema-only) external links parquet so
    BaseExporter.add_exernal_links has nothing to look up but doesn't crash."""
    external_links_file = tmp_path / "external_links.parquet"
    pl.DataFrame(
        {
            "series": pl.Series([], dtype=pl.String),
            "Gemma": pl.Series([], dtype=pl.String),
            "BGee": pl.Series([], dtype=pl.String),
            "DiSignAtlas": pl.Series([], dtype=pl.String),
        }
    ).write_parquet(external_links_file)
    monkeypatch.setattr(
        "metahq_core.export.base.get_external_links", lambda: external_links_file
    )


@pytest.fixture
def stub_external_links_with_data(monkeypatch, tmp_path):
    """Populated external-links parquet (unlike the empty-schema autouse
    stub above), so tests can assert real link data flows end-to-end
    through BaseExporter.add_external_links instead of only exercising the
    no-match path."""
    external_links_file = tmp_path / "external_links_with_data.parquet"
    pl.DataFrame(
        {
            "series": pl.Series(["GSE1"], dtype=pl.String),
            "Gemma": pl.Series(["https://gemma.msl.ubc.ca/GSE1"], dtype=pl.String),
            "BGee": pl.Series([None], dtype=pl.String),
            "DiSignAtlas": pl.Series([None], dtype=pl.String),
        }
    ).write_parquet(external_links_file)
    monkeypatch.setattr(
        "metahq_core.export.base.get_external_links", lambda: external_links_file
    )
    return external_links_file
