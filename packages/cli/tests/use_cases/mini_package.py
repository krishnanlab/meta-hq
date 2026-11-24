"""
A miniature MetaHQ data package for the documented use-case tests.

These tests run `metahq ...` commands end to end against a miniature MetaHQ data
package built in a temporary directory. The package has the same layout and
file schemas as the real one on Zenodo (see docs/concepts/data-model.md), but
holds a dozen hand-written samples and two toy ontologies so that every
expected value in the tests can be worked out by hand.

The mini package
----------------

Ontologies (ancestor -> descendants; every term is its own ancestor):

    UBERON:0000062 organ
    ├── UBERON:0000948 heart
    │   └── UBERON:0002349 myocardium
    ├── UBERON:0002107 liver
    ├── UBERON:0000955 brain
    │   └── UBERON:0000956 cerebral cortex
    └── UBERON:0002113 kidney

    MONDO:0000001 disease
    ├── MONDO:0004992 cancer
    │   ├── MONDO:0007254 breast cancer
    │   └── MONDO:0008170 ovarian cancer
    ├── MONDO:0005267 heart disorder
    │   └── MONDO:0004994 cardiomyopathy
    └── MONDO:0005015 diabetes mellitus
        └── MONDO:0005148 type 2 diabetes mellitus

Samples (all human RNA-seq on GPL20301 unless noted):

    sample   series  tissue (source, ecode)              disease (source)               sex age
    GSM101   GSE1    heart (KrishnanLab)                 cardiomyopathy (KrishnanLab)   M   adult
    GSM102   GSE1    myocardium (KrishnanLab)            control (KrishnanLab)          F   adult
    GSM103   GSE1    heart (KrishnanLab, URSA)           cardiomyopathy (Gemma)         M   older_adult
    GSM201   GSE2    liver (URSA)                        -                              F   -
    GSM202   GSE2    cerebral cortex (URSA)              -                              -   child
    GSM203   GSE2    brain (CREEDS, crowd-sourced)        -                              -   -
    GSM301   GSE3    organ (Gu_2023)                     breast cancer (Gu_2023)        F   adult
    GSM302   GSE3    kidney (Gu_2023)                    cancer (Gu_2023)               -   -
    GSM303   GSE3    liver (Gu_2023)                     type 2 diabetes (Gu_2023)      M   -
    GSM401   GSE4    heart (KrishnanLab)  [mouse, GPL21103 RNA-seq]
    GSM501   GSE5    heart (KrishnanLab)  [human, GPL570 microarray]

KrishnanLab, CREEDS and Gu_2023 are permissive (CC0/CC BY) sources; URSA and Gemma
are non-commercial (see metahq_core.sources.SOURCE_LICENSE_CATEGORY).
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import polars as pl
import yaml
from bson import BSON

EXPERT = "expert-curated"
CROWD = "crowd-sourced"

HUMAN_RNASEQ = "GPL20301"
MOUSE_RNASEQ = "GPL21103"
HUMAN_ARRAY = "GPL570"

# ---------------------------------------------------------------------------
# ontologies: term -> parent (None for the root). Transitive closure is
# computed below so the tests only have to state the tree once.
# ---------------------------------------------------------------------------
UBERON_PARENTS: dict[str, str | None] = {
    "UBERON:0000062": None,
    "UBERON:0000948": "UBERON:0000062",
    "UBERON:0002349": "UBERON:0000948",
    "UBERON:0002107": "UBERON:0000062",
    "UBERON:0000955": "UBERON:0000062",
    "UBERON:0000956": "UBERON:0000955",
    "UBERON:0002113": "UBERON:0000062",
}
UBERON_NAMES = {
    "UBERON:0000062": "organ",
    "UBERON:0000948": "heart",
    "UBERON:0002349": "myocardium",
    "UBERON:0002107": "liver",
    "UBERON:0000955": "brain",
    "UBERON:0000956": "cerebral cortex",
    "UBERON:0002113": "kidney",
}

MONDO_PARENTS: dict[str, str | None] = {
    "MONDO:0000001": None,
    "MONDO:0004992": "MONDO:0000001",
    "MONDO:0007254": "MONDO:0004992",
    "MONDO:0008170": "MONDO:0004992",
    "MONDO:0005267": "MONDO:0000001",
    "MONDO:0004994": "MONDO:0005267",
    "MONDO:0005015": "MONDO:0000001",
    "MONDO:0005148": "MONDO:0005015",
}
MONDO_NAMES = {
    "MONDO:0000001": "disease",
    "MONDO:0004992": "cancer",
    "MONDO:0007254": "breast cancer",
    "MONDO:0008170": "ovarian cancer",
    "MONDO:0005267": "heart disorder",
    "MONDO:0004994": "cardiomyopathy",
    "MONDO:0005015": "diabetes mellitus",
    "MONDO:0005148": "type 2 diabetes mellitus",
}

CONTROL = "MONDO:0000000"


def _anno(source: str, id_: str, ecode: str = EXPERT) -> dict[str, dict[str, str]]:
    return {source: {"id": id_, "ecode": ecode}}


def _merge(*annos: dict) -> dict:
    out: dict = {}
    for a in annos:
        out.update(a)
    return out


# ---------------------------------------------------------------------------
# sample-level annotations database (combined__level-sample.bson)
# ---------------------------------------------------------------------------
def _sample(gsm, gse, platform, organism="homo sapiens", srx=None, **attrs):
    accession_ids = {"sample": gsm, "series": gse, "platform": platform}
    if srx:
        accession_ids["srx"] = srx
    return gsm, {"accession_ids": accession_ids, "organism": organism, **attrs}


SAMPLE_DB = dict(
    [
        _sample(
            "GSM101",
            "GSE1",
            HUMAN_RNASEQ,
            srx="SRX101",
            tissue=_anno("KrishnanLab", "UBERON:0000948"),
            disease=_anno("KrishnanLab", "MONDO:0004994"),
            sex=_anno("KrishnanLab", "M"),
            age=_anno("KrishnanLab", "adult"),
        ),
        _sample(
            "GSM102",
            "GSE1",
            HUMAN_RNASEQ,
            srx="SRX102",
            tissue=_anno("KrishnanLab", "UBERON:0002349"),
            disease=_anno("KrishnanLab", CONTROL),
            sex=_anno("KrishnanLab", "F"),
            age=_anno("KrishnanLab", "adult"),
        ),
        _sample(
            "GSM103",
            "GSE1",
            HUMAN_RNASEQ,
            tissue=_merge(
                _anno("KrishnanLab", "UBERON:0000948"), _anno("URSA", "UBERON:0000948")
            ),
            disease=_anno("Gemma", "MONDO:0004994"),
            sex=_anno("KrishnanLab", "M"),
            age=_anno("KrishnanLab", "older_adult"),
        ),
        _sample(
            "GSM201",
            "GSE2",
            HUMAN_RNASEQ,
            tissue=_anno("URSA", "UBERON:0002107"),
            sex=_anno("URSA", "F"),
        ),
        _sample(
            "GSM202",
            "GSE2",
            HUMAN_RNASEQ,
            tissue=_anno("URSA", "UBERON:0000956"),
            age=_anno("URSA", "child"),
        ),
        _sample(
            "GSM203",
            "GSE2",
            HUMAN_RNASEQ,
            tissue=_anno("CREEDS", "UBERON:0000955", ecode=CROWD),
        ),
        _sample(
            "GSM301",
            "GSE3",
            HUMAN_RNASEQ,
            tissue=_anno("Gu_2023", "UBERON:0000062"),
            disease=_anno("Gu_2023", "MONDO:0007254"),
            sex=_anno("Gu_2023", "F"),
            age=_anno("Gu_2023", "adult"),
        ),
        _sample(
            "GSM302",
            "GSE3",
            HUMAN_RNASEQ,
            tissue=_anno("Gu_2023", "UBERON:0002113"),
            disease=_anno("Gu_2023", "MONDO:0004992"),
        ),
        _sample(
            "GSM303",
            "GSE3",
            HUMAN_RNASEQ,
            tissue=_anno("Gu_2023", "UBERON:0002107"),
            disease=_anno("Gu_2023", "MONDO:0005148"),
            sex=_anno("Gu_2023", "M"),
        ),
        _sample(
            "GSM401",
            "GSE4",
            MOUSE_RNASEQ,
            organism="mus musculus",
            tissue=_anno("KrishnanLab", "UBERON:0000948"),
        ),
        _sample(
            "GSM501",
            "GSE5",
            HUMAN_ARRAY,
            tissue=_anno("KrishnanLab", "UBERON:0000948"),
        ),
    ]
)

# ---------------------------------------------------------------------------
# series-level annotations database (combined__level-series.bson)
# ---------------------------------------------------------------------------
SERIES_DB = {
    "GSE1": {
        "accession_ids": {"series": "GSE1", "platform": HUMAN_RNASEQ, "srp": "SRP1"},
        "organism": "homo sapiens",
        "tissue": _anno("KrishnanLab", "UBERON:0000948"),
        "disease": _merge(
            _anno("KrishnanLab", "MONDO:0004994"), _anno("Gemma", "MONDO:0004994")
        ),
    },
    "GSE2": {
        "accession_ids": {"series": "GSE2", "platform": HUMAN_RNASEQ},
        "organism": "homo sapiens",
        "tissue": _anno("URSA", "UBERON:0002107|UBERON:0000956"),
    },
    "GSE3": {
        "accession_ids": {"series": "GSE3", "platform": HUMAN_RNASEQ},
        "organism": "homo sapiens",
        "tissue": _anno("Gu_2023", "UBERON:0002113|UBERON:0002107"),
        "disease": _anno("Gu_2023", "MONDO:0007254|MONDO:0004992|MONDO:0005148"),
    },
}


# ---------------------------------------------------------------------------
# builders
# ---------------------------------------------------------------------------
def _closure(parents: dict[str, str | None]) -> dict[str, set[str]]:
    """term -> set of its ancestors, including itself."""
    out: dict[str, set[str]] = {}
    for term in parents:
        seen = {term}
        cur = parents[term]
        while cur is not None:
            seen.add(cur)
            cur = parents[cur]
        out[term] = seen
    return out


def relations_frame(
    parents: dict[str, str | None], sort_terms: bool = True
) -> pl.DataFrame:
    """terms x terms matrix: row r, column c is 1 if r is an ancestor of c.

    Terms are sorted, as in the real data package. metahq_core's Propagator
    relies on that order (see test_unsorted_relations_misalign_labels).
    """
    terms = sorted(parents) if sort_terms else list(parents)
    ancestors = _closure(parents)
    return pl.DataFrame(
        {col: [int(row in ancestors[col]) for row in terms] for col in terms},
        schema={t: pl.Int8 for t in terms},
    )


def _write_ontology(onto_dir: Path, parents, names, sort_terms=True) -> None:
    onto_dir.mkdir(parents=True, exist_ok=True)
    relations_frame(parents, sort_terms).write_parquet(onto_dir / "relations.parquet")
    pl.DataFrame({"id": list(names), "name": list(names.values())}).write_parquet(
        onto_dir / "id_map.parquet"
    )
    roots = [t for t, p in parents.items() if p is None]
    (onto_dir / "systems.txt").write_text("\n".join(roots) + "\n")
    (onto_dir / "names_synonyms.json").write_text(
        json.dumps({t: {"name": n, "synonyms": []} for t, n in names.items()})
    )


def _write_search_db(path: Path) -> None:
    with duckdb.connect(str(path)) as con:
        con.sql(
            "CREATE TABLE ontology_search_docs "
            "(term_id VARCHAR, name VARCHAR, ontology VARCHAR, type VARCHAR)"
        )
        con.sql(
            "CREATE TABLE ontology_synonyms "
            "(term_id VARCHAR, synonym VARCHAR, scope VARCHAR)"
        )
        rows = [(t, n, "UBERON", "tissue") for t, n in UBERON_NAMES.items()]
        rows += [(t, n, "MONDO", "disease") for t, n in MONDO_NAMES.items()]
        con.executemany("INSERT INTO ontology_search_docs VALUES (?, ?, ?, ?)", rows)
        con.executemany(
            "INSERT INTO ontology_synonyms VALUES (?, ?, ?)",
            [
                ("MONDO:0004994", "cardiomyopathies", "EXACT"),
                ("MONDO:0007254", "breast carcinoma", "RELATED"),
                ("UBERON:0002107", "hepar", "EXACT"),
            ],
        )


def build_mini_package(root: Path, sort_terms: bool = True) -> Path:
    """Write a miniature MetaHQ data package under `root` and return its path."""
    pkg = root / "metahq_data"

    annotations = pkg / "annotations"
    annotations.mkdir(parents=True)
    (annotations / "combined__level-sample.bson").write_bytes(BSON.encode(SAMPLE_DB))
    (annotations / "combined__level-series.bson").write_bytes(BSON.encode(SERIES_DB))

    metadata = pkg / "metadata"
    metadata.mkdir()
    pl.DataFrame(
        {
            "id": [HUMAN_RNASEQ, MOUSE_RNASEQ, HUMAN_ARRAY],
            "name": ["high-throughput sequencing"] * 2 + ["in situ oligonucleotide"],
            "technology": ["rnaseq", "rnaseq", "microarray"],
        }
    ).write_parquet(metadata / "technologies.parquet")

    samples = list(SAMPLE_DB)
    pl.DataFrame(
        {
            "sample": samples,
            "title": [f"title {s}" for s in samples],
            "description": [f"description of {s}" for s in samples],
            "source_name_ch1": [f"source {s}" for s in samples],
            "characteristics_ch1": [f"tissue: x|sample: {s}" for s in samples],
            "source_name_ch2": ["NA"] * len(samples),
            "characteristics_ch2": ["NA"] * len(samples),
        }
    ).write_parquet(metadata / "metadata__level-sample.parquet")

    series = ["GSE1", "GSE2", "GSE3", "GSE4", "GSE5"]
    pl.DataFrame(
        {
            "series": series,
            "title": [f"study {s}" for s in series],
            "summary": [f"summary of {s}" for s in series],
            "overall_design": [f"design of {s}" for s in series],
            "sample_id": [
                "|".join(
                    k for k, v in SAMPLE_DB.items() if v["accession_ids"]["series"] == s
                )
                for s in series
            ],
            "description": [f"Title: study {s}" for s in series],
        }
    ).write_parquet(metadata / "metadata__level-series.parquet")

    record = pl.Struct({"id": pl.String, "url": pl.String})
    pl.DataFrame(
        {
            "series": ["GSE1"],
            "BGee": [None],
            "DiSignAtlas": [None],
            "Gemma": [
                {
                    "browse_url": "https://gemma.msl.ubc.ca/browse/#/q/GSE1",
                    "records": [{"id": "1", "url": "https://gemma.example/1"}],
                }
            ],
        },
        schema={
            "series": pl.String,
            "BGee": pl.Struct({"records": pl.List(record)}),
            "DiSignAtlas": pl.Struct({"records": pl.List(record)}),
            "Gemma": pl.Struct({"browse_url": pl.String, "records": pl.List(record)}),
        },
    ).write_parquet(metadata / "external_links.parquet")

    pl.DataFrame(
        {
            "refinebio_sample": ["GSM101", "SRR102"],
            "refinebio_experiment": ["GSE1", "SRP1"],
            "gsm": ["GSM101", "GSM102"],
            "gse": ["GSE1", "GSE1"],
        }
    ).write_parquet(metadata / "refinebio_map.parquet")

    ontology = pkg / "ontology"
    _write_ontology(ontology / "uberon_ext", UBERON_PARENTS, UBERON_NAMES, sort_terms)
    _write_ontology(ontology / "mondo", MONDO_PARENTS, MONDO_NAMES, sort_terms)
    _write_search_db(ontology / "ontology_search.duckdb")

    return pkg


def configure_metahq(tmp_path: Path, monkeypatch, sort_terms: bool = True) -> Path:
    """Point a fresh MetaHQ config at a new mini package; return a work dir.

    Redirects the home directory so `~/MetaHQ/config.yaml` is written inside
    `tmp_path`, then changes into a scratch working directory so outputs land
    there too.
    """
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))  # Path.home() on Windows

    pkg = build_mini_package(tmp_path, sort_terms=sort_terms)
    metahq_home = home / "MetaHQ"
    metahq_home.mkdir()
    config = {
        "version": "test-mini",
        "zenodo_doi": "0",
        "data_dir": str(pkg),
        "logs": str(metahq_home),
    }
    (metahq_home / "config.yaml").write_text(yaml.safe_dump(config))

    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    return work
