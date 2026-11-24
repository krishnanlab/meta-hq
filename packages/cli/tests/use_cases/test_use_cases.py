"""
End-to-end tests for the recipes in docs/user-guide/use-cases.md.

Each test class matches one use case in that page and runs the same `metahq`
command against the miniature data package from `conftest.py`, then checks the
output against values worked out by hand from the fixture. A final test runs
every `metahq` command and Python snippet on the docs page itself, in order, so
the page cannot drift away from what the CLI accepts.
"""

from __future__ import annotations

import json
import re
import shlex
from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner
from mini_package import configure_metahq

from metahq_cli.main import main

HEART = "UBERON:0000948"
LIVER = "UBERON:0002107"
BRAIN = "UBERON:0000955"
CARDIOMYOPATHY = "MONDO:0004994"
CANCER = "MONDO:0004992"
BREAST_CANCER = "MONDO:0007254"

DOCS_PAGE = Path(__file__).resolve().parents[4] / "docs" / "user-guide" / "use-cases.md"


def metahq(*args: str):
    """Run `metahq <args>` in-process and fail loudly if it does not exit 0."""
    result = CliRunner().invoke(main, list(args), catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return result


def read_tsv(path: Path) -> pl.DataFrame:
    return pl.read_csv(path, separator="\t")


def as_map(df: pl.DataFrame, index: str, column: str) -> dict[str, int]:
    return dict(zip(df[index].to_list(), df[column].to_list()))


# ---------------------------------------------------------------------------
# Use case 1: find ontology term IDs from free text
# ---------------------------------------------------------------------------
class TestFindTermIds:
    def test_search_matches_synonym(self, mini_metahq):
        out = metahq(
            "search",
            "--query",
            "hepar",
            "--type",
            "tissue",
            "--ontology",
            "UBERON",
            "-k",
            "3",
        ).output
        first = out.strip().splitlines()[0]
        assert first == f"1. {LIVER}"

    def test_search_disease(self, mini_metahq):
        out = metahq(
            "search",
            "--query",
            "cardiomyopathies",
            "--type",
            "disease",
            "--ontology",
            "MONDO",
            "-k",
            "1",
        ).output
        assert f"1. {CARDIOMYOPATHY}" in out


# ---------------------------------------------------------------------------
# Use case 2: which samples are annotated to a tissue? (annotate mode)
# ---------------------------------------------------------------------------
class TestAnnotateTissues:
    @pytest.fixture
    def result(self, mini_metahq) -> pl.DataFrame:
        metahq(
            "retrieve", "tissues",
            "--terms", f"{HEART},{LIVER}",
            "--metadata", "sample,series,platform",
            "--fmt", "tsv",
            "--output", "heart_liver",
            "--quiet",
        )  # fmt: skip
        return read_tsv(mini_metahq / "heart_liver" / "result.tsv")

    def test_columns(self, result):
        assert result.columns == [
            "sample",
            "series",
            "platform",
            "sources",
            "external_links",
            HEART,
            LIVER,
        ]

    def test_descendant_annotations_propagate_up(self, result):
        # GSM102 is annotated to myocardium, a part of the heart
        assert as_map(result, "sample", HEART) == {
            "GSM101": 1,
            "GSM102": 1,
            "GSM103": 1,
            "GSM201": 0,
            "GSM303": 0,
        }
        assert as_map(result, "sample", LIVER)["GSM201"] == 1

    def test_default_filters_exclude_other_species_tech_and_crowd(self, result):
        # GSM401 is mouse, GSM501 is microarray, GSM203 is crowd-sourced brain
        assert not {"GSM401", "GSM501", "GSM203"} & set(result["sample"])

    def test_writes_citation_file(self, result, mini_metahq):
        citation = (mini_metahq / "heart_liver" / "CITATION.txt").read_text()
        for source in ("KrishnanLab", "URSA", "Gu_2023"):
            assert source in citation


# ---------------------------------------------------------------------------
# Use case 3: build a label matrix for a classifier (label mode)
# ---------------------------------------------------------------------------
class TestLabelsForClassifier:
    @pytest.fixture
    def labels(self, mini_metahq) -> pl.DataFrame:
        metahq(
            "retrieve", "tissues",
            "--terms", HEART,
            "--mode", "label",
            "--metadata", "sample,series",
            "--fmt", "parquet",
            "--output", "heart_labels",
            "--quiet",
        )  # fmt: skip
        return pl.read_parquet(mini_metahq / "heart_labels" / "result.parquet")

    def test_label_values(self, labels):
        assert as_map(labels, "sample", HEART) == {
            "GSM101": 1,  # heart
            "GSM102": 1,  # myocardium (descendant of heart)
            "GSM103": 1,  # heart
            "GSM201": -1,  # liver: unrelated to heart
            "GSM202": -1,  # cerebral cortex: unrelated
            "GSM301": 0,  # "organ": an ancestor of heart, so unknown
            "GSM302": -1,  # kidney
            "GSM303": -1,  # liver
        }

    def test_positive_negative_split(self, labels):
        positives = labels.filter(pl.col(HEART) == 1)["sample"].to_list()
        negatives = labels.filter(pl.col(HEART) == -1)["sample"].to_list()
        assert sorted(positives) == ["GSM101", "GSM102", "GSM103"]
        assert sorted(negatives) == ["GSM201", "GSM202", "GSM302", "GSM303"]


# ---------------------------------------------------------------------------
# Use case 4: case/control samples from the same study
# ---------------------------------------------------------------------------
class TestCaseControl:
    @pytest.fixture
    def labels(self, mini_metahq) -> pl.DataFrame:
        metahq(
            "retrieve", "diseases",
            "--terms", f"{CARDIOMYOPATHY},{CANCER}",
            "--mode", "label",
            "--metadata", "sample,series",
            "--fmt", "parquet",
            "--output", "disease_labels",
            "--quiet",
        )  # fmt: skip
        return pl.read_parquet(mini_metahq / "disease_labels" / "result.parquet")

    def test_controls_get_label_2(self, labels):
        # GSM102 is a control in GSE1, where GSM101/GSM103 have cardiomyopathy
        assert as_map(labels, "sample", CARDIOMYOPATHY) == {
            "GSM101": 1,
            "GSM102": 2,
            "GSM103": 1,
            "GSM301": -1,  # breast cancer
            "GSM302": -1,  # cancer
            "GSM303": -1,  # type 2 diabetes
        }

    def test_controls_are_unknown_for_terms_not_studied(self, labels):
        # nobody in GSE1 has cancer, so the GSE1 control is 0 (unknown) for it
        assert as_map(labels, "sample", CANCER)["GSM102"] == 0

    def test_descendants_count_as_positive(self, labels):
        cancer = as_map(labels, "sample", CANCER)
        assert cancer["GSM301"] == 1  # breast cancer is a cancer
        assert cancer["GSM302"] == 1  # annotated to cancer directly
        assert cancer["GSM303"] == -1  # type 2 diabetes

    def test_series_with_cases_and_controls(self, labels):
        both = (
            labels.group_by("series")
            .agg(
                (pl.col(CARDIOMYOPATHY) == 1).sum().alias("cases"),
                (pl.col(CARDIOMYOPATHY) == 2).sum().alias("controls"),
            )
            .filter((pl.col("cases") > 0) & (pl.col("controls") > 0))
        )
        assert both.rows() == [("GSE1", 2, 1)]


# ---------------------------------------------------------------------------
# Use case 5: find studies (series level) with descriptive metadata
# ---------------------------------------------------------------------------
class TestStudyLevel:
    def test_series_annotations_with_titles(self, mini_metahq):
        metahq(
            "retrieve", "diseases",
            "--terms", BREAST_CANCER,
            "--level", "series",
            "--metadata", "series,title",
            "--fmt", "tsv",
            "--output", "breast_cancer_studies",
            "--quiet",
        )  # fmt: skip
        result = read_tsv(mini_metahq / "breast_cancer_studies" / "result.tsv")
        assert result.columns[:2] == ["series", "title"]
        assert result.select("series", "title", BREAST_CANCER).rows() == [
            ("GSE3", "study GSE3", 1)
        ]

    def test_external_links_are_json(self, mini_metahq):
        metahq(
            "retrieve", "diseases",
            "--terms", CARDIOMYOPATHY,
            "--level", "series",
            "--fmt", "tsv",
            "--output", "cardiomyopathy_studies",
            "--quiet",
        )  # fmt: skip
        result = read_tsv(mini_metahq / "cardiomyopathy_studies" / "result.tsv")
        row = result.row(0, named=True)
        assert row["series"] == "GSE1"
        assert set(row["sources"].split("|")) == {"KrishnanLab", "Gemma"}
        links = json.loads(row["external_links"])
        assert links["Gemma"]["browse_url"].endswith("GSE1")


# ---------------------------------------------------------------------------
# Use case 6: restrict to commercially usable (permissive) sources
# ---------------------------------------------------------------------------
class TestLicenseFilter:
    def _sources(self, df: pl.DataFrame) -> set[str]:
        return {s for joined in df["sources"] for s in joined.split("|")}

    def test_permissive_drops_nc_sources(self, mini_metahq):
        metahq(
            "retrieve", "tissues",
            "--terms", f"{HEART},{LIVER}",
            "--license", "permissive",
            "--fmt", "tsv",
            "--output", "permissive",
            "--quiet",
        )  # fmt: skip
        result = read_tsv(mini_metahq / "permissive" / "result.tsv")
        assert self._sources(result) == {"KrishnanLab", "Gu_2023"}
        # GSM201's only liver annotation comes from URSA (CC BY-NC)
        assert "GSM201" not in result["sample"].to_list()
        citation = (mini_metahq / "permissive" / "CITATION.txt").read_text()
        assert "URSA" not in citation
        assert "License: permissive" in citation

    def test_nc_keeps_only_nc_sources(self, mini_metahq):
        metahq(
            "retrieve", "tissues",
            "--terms", f"{HEART},{LIVER}",
            "--license", "nc",
            "--fmt", "tsv",
            "--output", "nc",
            "--quiet",
        )  # fmt: skip
        result = read_tsv(mini_metahq / "nc" / "result.tsv")
        assert self._sources(result) == {"URSA"}
        assert sorted(result["sample"]) == ["GSM103", "GSM201"]


# ---------------------------------------------------------------------------
# Use case 7: sex and age covariates
# ---------------------------------------------------------------------------
class TestCovariates:
    def test_sex(self, mini_metahq):
        metahq(
            "retrieve", "sex", "--terms", "male,female",
            "--fmt", "tsv", "--output", "sex", "--quiet",
        )  # fmt: skip
        result = read_tsv(mini_metahq / "sex" / "result.tsv")
        assert as_map(result, "sample", "M") == {
            "GSM101": 1,
            "GSM102": 0,
            "GSM103": 1,
            "GSM201": 0,
            "GSM301": 0,
            "GSM303": 1,
        }

    def test_age_accepts_comma_separated_groups(self, mini_metahq):
        metahq(
            "retrieve", "age", "--terms", "adult,older_adult",
            "--fmt", "tsv", "--output", "age", "--quiet",
        )  # fmt: skip
        result = read_tsv(mini_metahq / "age" / "result.tsv")
        assert result.columns[-2:] == ["adult", "older_adult"]
        assert as_map(result, "sample", "older_adult") == {
            "GSM101": 0,
            "GSM102": 0,
            "GSM103": 1,
            "GSM301": 0,
        }

    def test_age_rejects_unknown_group(self, mini_metahq):
        result = CliRunner().invoke(
            main, ["retrieve", "age", "--terms", "adult,teenager", "--quiet"]
        )
        assert result.exit_code == 2
        assert "teenager" in result.output


# ---------------------------------------------------------------------------
# Use case 8: other species, technologies and evidence codes
# ---------------------------------------------------------------------------
class TestFilters:
    @pytest.mark.parametrize(
        "filters, term, expected",
        [
            ("species=mouse", HEART, ["GSM401"]),
            ("species=human,tech=microarray", HEART, ["GSM501"]),
            ("ecode=crowd", BRAIN, ["GSM203"]),
            # ecode=any keeps expert and crowd: GSM202 (cerebral cortex) + GSM203
            ("ecode=any", BRAIN, ["GSM202", "GSM203"]),
        ],
    )
    def test_filters(self, mini_metahq, filters, term, expected):
        metahq(
            "retrieve", "tissues", "--terms", term, "--filters", filters,
            "--fmt", "tsv", "--output", "filtered", "--quiet",
        )  # fmt: skip
        result = read_tsv(mini_metahq / "filtered" / "result.tsv")
        assert sorted(result["sample"]) == expected


# ---------------------------------------------------------------------------
# Output formats
# ---------------------------------------------------------------------------
class TestOutputFormats:
    def test_json_is_keyed_by_term_then_sample(self, mini_metahq):
        metahq(
            "retrieve", "tissues", "--terms", HEART, "--metadata", "sample,series",
            "--fmt", "json", "--output", "heart_json", "--quiet",
        )  # fmt: skip
        data = json.loads((mini_metahq / "heart_json" / "result.json").read_text())
        assert set(data) == {HEART}
        assert set(data[HEART]) == {"GSM101", "GSM102", "GSM103"}
        assert data[HEART]["GSM102"]["series"] == "GSE1"

    def test_label_json_groups_by_label(self, mini_metahq):
        metahq(
            "retrieve", "diseases", "--terms", CARDIOMYOPATHY, "--mode", "label",
            "--metadata", "sample,series", "--fmt", "json", "--output", "cm_json",
            "--quiet",
        )  # fmt: skip
        data = json.loads((mini_metahq / "cm_json" / "result.json").read_text())
        groups = data[CARDIOMYOPATHY]

        def ids(label):
            return sorted(k for entry in groups[label] for k in entry)

        assert ids("positive") == ["GSM101", "GSM103"]
        assert ids("negative") == ["GSM301", "GSM302", "GSM303"]
        assert ids("control") == ["GSM102"]
        assert groups["positive"][0]["GSM101"]["external_links"]["Gemma"]

    def test_existing_output_dir_gets_suffix(self, mini_metahq):
        for _ in range(2):
            metahq(
                "retrieve", "tissues", "--terms", HEART, "--output", "out", "--quiet"
            )
        assert (mini_metahq / "out" / "result.parquet").exists()
        siblings = sorted(
            p.name for p in mini_metahq.iterdir() if p.name.startswith("out")
        )
        assert len(siblings) == 2


# ---------------------------------------------------------------------------
# Data package contract: relations.parquet term order
# ---------------------------------------------------------------------------
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Propagator assumes relations.parquet columns are sorted; with an "
        "unsorted file, multi-term labels are misaligned. Real packages are "
        "sorted, so this is latent. Remove the xfail once Propagator aligns "
        "terms by name."
    ),
)
def test_unsorted_relations_misalign_labels(tmp_path, monkeypatch):
    work = configure_metahq(tmp_path, monkeypatch, sort_terms=False)
    metahq(
        "retrieve", "diseases",
        "--terms", f"{CARDIOMYOPATHY},{CANCER}",
        "--mode", "label",
        "--fmt", "tsv",
        "--output", "unsorted",
        "--quiet",
    )  # fmt: skip
    labels = read_tsv(work / "unsorted" / "result.tsv")
    # breast cancer is unrelated to cardiomyopathy, so this should be -1
    assert as_map(labels, "sample", CARDIOMYOPATHY)["GSM301"] == -1


# ---------------------------------------------------------------------------
# The docs page itself
# ---------------------------------------------------------------------------
FENCE = re.compile(r"^```(bash|python)\n(.*?)^```", re.M | re.S)
NOT_RUNNABLE = ("metahq setup", "metahq delete", "metahq validate")


def docs_blocks(text: str) -> list[tuple[str, str]]:
    """Return (kind, payload) for each runnable thing on the page, in order.

    bash blocks yield one item per `metahq` command (with `\\` continuations
    joined); python blocks yield the whole block.
    """
    items: list[tuple[str, str]] = []
    for lang, body in FENCE.findall(text):
        if lang == "python":
            items.append(("python", body))
            continue
        for command in body.replace("\\\n", " ").splitlines():
            command = command.strip()
            if command.startswith("metahq ") and not command.startswith(NOT_RUNNABLE):
                items.append(("bash", command))
    return items


@pytest.mark.skipif(not DOCS_PAGE.exists(), reason="docs not available")
def test_docs_page_runs(mini_metahq):
    """Every metahq command and Python snippet on the use-cases page runs."""
    items = docs_blocks(DOCS_PAGE.read_text(encoding="utf-8"))
    assert sum(kind == "bash" for kind, _ in items) >= 8
    assert any(kind == "python" for kind, _ in items)

    for kind, payload in items:
        if kind == "bash":
            metahq(*shlex.split(payload)[1:])
        else:
            exec(compile(payload, str(DOCS_PAGE), "exec"), {})
